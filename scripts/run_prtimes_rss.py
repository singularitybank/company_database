# -*- coding: utf-8 -*-
"""
PR Times RSS 収集バッチ エントリーポイント

[処理フロー]
  STEP 1: グローバル RSS 取得    prtimes_rss_crawler.fetch_global_rss()
  STEP 2: RSS パース             prtimes_rss_parser.parse()
  STEP 3: DB 保存（INSERT）      prtimes_db_loader.load()
  STEP 4: 記事 HTML 取得         prtimes_article_crawler.scrape_articles()
  STEP 5: 記事パース             prtimes_article_parser.parse()
  STEP 6: DB 更新（UPDATE）      prtimes_db_loader.update_articles()
  STEP 7: 企業ページ HTML 取得   prtimes_company_crawler.scrape_companies()
  STEP 8: 企業パース             prtimes_company_parser.parse()
  STEP 9: 企業 DB 更新（UPDATE） prtimes_db_loader.update_companies()

[実行方法]
  # 通常実行（1時間毎にタスクスケジューラから呼び出す）
  python scripts/run_prtimes_rss.py

  # STEP 1-3（RSS保存）のみ実行
  python scripts/run_prtimes_rss.py --rss-only

  # STEP 4-6（記事スクレイピング）のみ実行
  python scripts/run_prtimes_rss.py --scrape-only

[タスクスケジューラ]
  scripts/run_prtimes_rss.bat から呼び出す
  トリガー: 毎日 00:00 から 1時間おきに繰り返す
"""
import argparse
import logging
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.config import prtimes as _cfg
from src.signals.prtimes.crawlers.article_crawler import scrape_articles
from src.signals.prtimes.crawlers.company_crawler import scrape_companies
from src.signals.prtimes.crawlers.rss_crawler import fetch_global_rss
from src.signals.prtimes.loaders.db_loader import load, update_articles, update_companies
from src.common.logging_setup import setup_logging
from src.signals.prtimes.parsers.article_parser import parse as parse_articles
from src.signals.prtimes.parsers.company_parser import parse as parse_companies
from src.signals.prtimes.parsers.rss_parser import parse as parse_rss

JST     = timezone(timedelta(hours=9))
DB_PATH = PROJECT_ROOT / _cfg["db_path"]

# ---------------------------------------------------------------------------
# メイン処理
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description="PR Times RSS 収集バッチ")
    parser.add_argument(
        "--rss-only",
        action="store_true",
        help="STEP 1-3（RSS 取得・DB 保存）のみ実行する",
    )
    parser.add_argument(
        "--scrape-only",
        action="store_true",
        help="STEP 4-6（記事 HTML 取得・パース・DB 更新）のみ実行する",
    )
    args = parser.parse_args()

    now_jst  = datetime.now(JST)
    date_str = now_jst.strftime("%Y%m%d")

    setup_logging(PROJECT_ROOT / "logs" / "prtimes", log_filename=f"prtimes_rss_{date_str}")
    logger = logging.getLogger(__name__)

    logger.info("=" * 60)
    logger.info("PR Times RSS 収集バッチ 開始  %s", now_jst.strftime("%Y-%m-%d %H:%M:%S"))
    logger.info("=" * 60)

    batch_start = time.time()
    new_rss_count = 0

    # =========================================================================
    # STEP 1-3: RSS 取得 → パース → DB 保存
    # =========================================================================
    if not args.scrape_only:

        # ── STEP 1: RSS 取得 ──────────────────────────────────────────────────
        logger.info("[STEP 1/6] グローバル RSS 取得 開始")
        step_start = time.time()
        try:
            entries = fetch_global_rss()
        except Exception:
            logger.exception("[STEP 1/6] RSS 取得中に予期しないエラーが発生しました")
            return 1

        if not entries:
            logger.warning("[STEP 1/6] RSS エントリが 0 件でした")
            if args.rss_only:
                return 0
        else:
            logger.info("[STEP 1/6] RSS 取得完了: %d件 (%.1f秒)",
                        len(entries), time.time() - step_start)

            # ── STEP 2: RSS パース ────────────────────────────────────────────
            logger.info("[STEP 2/6] RSS パース 開始")
            step_start = time.time()
            try:
                records = parse_rss(entries)
            except Exception:
                logger.exception("[STEP 2/6] RSS パース中に予期しないエラーが発生しました")
                return 1
            logger.info("[STEP 2/6] RSS パース完了: %d件 (%.1f秒)",
                        len(records), time.time() - step_start)

            # ── STEP 3: DB 保存（INSERT）──────────────────────────────────────
            logger.info("[STEP 3/6] DB 保存 開始")
            step_start = time.time()
            try:
                result = load(records, db_path=DB_PATH)
            except Exception:
                logger.exception("[STEP 3/6] DB 保存中に予期しないエラーが発生しました")
                return 1

            if result.error:
                logger.error("[STEP 3/6] DB 保存エラー: %s", result.error)
                return 1

            new_rss_count = result.new_count
            logger.info("[STEP 3/6] DB 保存完了: 入力=%d件, 新規=%d件, スキップ=%d件 (%.1f秒)",
                        result.total, result.new_count, result.skipped,
                        time.time() - step_start)

        if args.rss_only:
            elapsed = time.time() - batch_start
            logger.info("=" * 60)
            logger.info("PR Times RSS 収集バッチ 正常終了（RSS のみ）  新規記事: %d件  所要時間: %.1f秒",
                        new_rss_count, elapsed)
            logger.info("=" * 60)
            return 0

    # =========================================================================
    # STEP 4-6: 記事 HTML 取得 → パース → DB 更新
    # =========================================================================

    # ── STEP 4: 記事 HTML 取得 ────────────────────────────────────────────────
    logger.info("[STEP 4/9] 記事 HTML 取得 開始")
    step_start = time.time()
    try:
        html_results = scrape_articles(db_path=DB_PATH)
    except Exception:
        logger.exception("[STEP 4/9] 記事 HTML 取得中に予期しないエラーが発生しました")
        return 1

    success_count = sum(1 for r in html_results if r.success)
    error_count   = sum(1 for r in html_results if not r.success)
    logger.info("[STEP 4/9] 記事 HTML 取得完了: 成功=%d件, エラー=%d件 (%.1f秒)",
                success_count, error_count, time.time() - step_start)

    if not html_results:
        logger.info("[STEP 4/9] 未スクレイプ記事なし → STEP 5-6 をスキップ")
        elapsed = time.time() - batch_start
        logger.info("=" * 60)
        logger.info("PR Times RSS 収集バッチ 正常終了  新規RSS: %d件  所要時間: %.1f秒",
                    new_rss_count, elapsed)
        logger.info("=" * 60)
        return 0

    # ── STEP 5: 記事パース ────────────────────────────────────────────────────
    logger.info("[STEP 5/9] 記事パース 開始")
    step_start = time.time()
    try:
        parse_results = parse_articles(html_results)
    except Exception:
        logger.exception("[STEP 5/9] 記事パース中に予期しないエラーが発生しました")
        return 1
    logger.info("[STEP 5/9] 記事パース完了: %d件 (%.1f秒)",
                len(parse_results), time.time() - step_start)

    # ── STEP 6: DB 更新（UPDATE）──────────────────────────────────────────────
    logger.info("[STEP 6/9] DB 更新 開始")
    step_start = time.time()
    try:
        updated, update_errors = update_articles(parse_results, db_path=DB_PATH)
    except Exception:
        logger.exception("[STEP 6/9] DB 更新中に予期しないエラーが発生しました")
        return 1
    logger.info("[STEP 6/9] DB 更新完了: 更新=%d件, エラー=%d件 (%.1f秒)",
                updated, update_errors, time.time() - step_start)

    # =========================================================================
    # STEP 7-9: 企業ページ取得 → パース → DB 更新
    # =========================================================================

    # ── STEP 7: 企業ページ HTML 取得 ──────────────────────────────────────────
    logger.info("[STEP 7/9] 企業ページ HTML 取得 開始（scraped_at IS NULL のみ）")
    step_start = time.time()
    try:
        company_html_results = scrape_companies(db_path=DB_PATH, headless=True)
    except Exception:
        logger.exception("[STEP 7/9] 企業ページ取得中に予期しないエラーが発生しました")
        return 1

    if not company_html_results:
        logger.info("[STEP 7/9] 未取得企業なし → STEP 8-9 をスキップ")
    else:
        c_success = sum(1 for r in company_html_results if r.success)
        c_errors  = sum(1 for r in company_html_results if not r.success)
        logger.info("[STEP 7/9] 企業ページ HTML 取得完了: 成功=%d件, エラー=%d件 (%.1f秒)",
                    c_success, c_errors, time.time() - step_start)

        # ── STEP 8: 企業パース ────────────────────────────────────────────────
        logger.info("[STEP 8/9] 企業パース 開始")
        step_start = time.time()
        try:
            company_parse_results = parse_companies(company_html_results)
        except Exception:
            logger.exception("[STEP 8/9] 企業パース中に予期しないエラーが発生しました")
            return 1
        logger.info("[STEP 8/9] 企業パース完了: %d件 (%.1f秒)",
                    len(company_parse_results), time.time() - step_start)

        # ── STEP 9: 企業 DB 更新（UPDATE）────────────────────────────────────
        logger.info("[STEP 9/9] 企業 DB 更新 開始")
        step_start = time.time()
        try:
            c_updated, c_update_errors = update_companies(company_parse_results, db_path=DB_PATH)
        except Exception:
            logger.exception("[STEP 9/9] 企業 DB 更新中に予期しないエラーが発生しました")
            return 1
        logger.info("[STEP 9/9] 企業 DB 更新完了: 更新=%d件, エラー=%d件 (%.1f秒)",
                    c_updated, c_update_errors, time.time() - step_start)

    elapsed = time.time() - batch_start
    logger.info("=" * 60)
    logger.info(
        "PR Times RSS 収集バッチ 正常終了  新規RSS: %d件  記事更新: %d件  所要時間: %.1f秒",
        new_rss_count, updated, elapsed,
    )
    logger.info("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
