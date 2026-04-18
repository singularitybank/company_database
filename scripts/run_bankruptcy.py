# -*- coding: utf-8 -*-
"""
倒産情報収集バッチ エントリーポイント

[処理フロー]
  STEP 1: TSR RSS取得 → DB INSERT（INSERT OR IGNORE）
  STEP 2: TSR詳細ページスクレイピング（detail_scraped_at IS NULL のみ）
  STEP 3: TSR詳細パース → DB UPDATE
  STEP 4: TDB RSS取得 → DB INSERT（INSERT OR IGNORE）
  STEP 5: TDB詳細ページスクレイピング（detail_scraped_at IS NULL のみ）
  STEP 6: TDB詳細パース → DB UPDATE
  STEP 7: 名寄せ（TDB↔TSR、未マッチ案件のみ）

[実行方法]
  python scripts/run_bankruptcy.py              # 全ステップ実行
  python scripts/run_bankruptcy.py --rss-only   # STEP 1,4（RSS保存）のみ
  python scripts/run_bankruptcy.py --no-match   # STEP 7（名寄せ）をスキップ

[タスクスケジューラ]
  scripts/run_bankruptcy.bat から呼び出す
"""

import argparse
import logging
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.config import bankruptcy as _cfg
from src.signals.bankruptcy.crawlers.tsr_rss_crawler  import fetch_rss as tsr_fetch_rss
from src.signals.bankruptcy.crawlers.tsr_detail_crawler import scrape as tsr_scrape
from src.signals.bankruptcy.parsers.tsr_detail_parser  import parse as tsr_parse
from src.signals.bankruptcy.crawlers.tdb_rss_crawler  import fetch_rss as tdb_fetch_rss
from src.signals.bankruptcy.crawlers.tdb_detail_crawler import scrape as tdb_scrape
from src.signals.bankruptcy.parsers.tdb_detail_parser  import parse as tdb_parse
from src.signals.bankruptcy.loaders.db_loader import (
    insert_tsr_rss, insert_tdb_rss,
    update_tsr_detail, update_tdb_detail,
    get_tsr_unscraped, get_tdb_unscraped,
    log_rss_fetch,
)
from src.signals.bankruptcy.matchers.name_matcher import run_matching
from src.signals.bankruptcy.models.schema import init_db
from src.common.logging_setup import setup_logging

JST      = timezone(timedelta(hours=9))
DB_PATH  = PROJECT_ROOT / _cfg["db_path"]

COMPANIES_DB = PROJECT_ROOT / "data" / "companies.db"
GBIZINFO_DB  = PROJECT_ROOT / "data" / "gbizinfo.db"


def main() -> int:
    parser = argparse.ArgumentParser(description="倒産情報収集バッチ")
    parser.add_argument("--rss-only",  action="store_true", help="STEP 1,4（RSS保存）のみ実行")
    parser.add_argument("--no-match",  action="store_true", help="STEP 7（名寄せ）をスキップ")
    args = parser.parse_args()

    now_jst  = datetime.now(JST)
    date_str = now_jst.strftime("%Y%m%d")

    setup_logging(PROJECT_ROOT / "logs" / "bankruptcy", log_filename=f"bankruptcy_{date_str}")
    logger = logging.getLogger(__name__)

    logger.info("=" * 60)
    logger.info("倒産情報収集バッチ 開始  %s", now_jst.strftime("%Y-%m-%d %H:%M:%S"))
    logger.info("=" * 60)

    batch_start = time.time()

    # DB 初期化（テーブルが存在しない場合のみ作成）
    conn = init_db(DB_PATH)

    # =========================================================================
    # STEP 1: TSR RSS 取得 → DB INSERT
    # =========================================================================
    logger.info("[STEP 1/7] TSR RSS 取得")
    step_start = time.time()
    tsr_entries = []
    try:
        tsr_entries = tsr_fetch_rss()
        new_tsr = insert_tsr_rss(conn, tsr_entries) if tsr_entries else 0
        log_rss_fetch(conn, "tsr", len(tsr_entries), new_tsr)
        logger.info("[STEP 1/7] 完了: %d件取得, %d件新規 (%.1f秒)",
                    len(tsr_entries), new_tsr, time.time() - step_start)
    except Exception:
        logger.exception("[STEP 1/7] TSR RSS 取得エラー")
        log_rss_fetch(conn, "tsr", 0, 0, "fetch error")

    # =========================================================================
    # STEP 4: TDB RSS 取得 → DB INSERT
    # =========================================================================
    logger.info("[STEP 4/7] TDB RSS 取得")
    step_start = time.time()
    tdb_entries = []
    try:
        tdb_entries = tdb_fetch_rss()
        new_tdb = insert_tdb_rss(conn, tdb_entries) if tdb_entries else 0
        log_rss_fetch(conn, "tdb", len(tdb_entries), new_tdb)
        logger.info("[STEP 4/7] 完了: %d件取得, %d件新規 (%.1f秒)",
                    len(tdb_entries), new_tdb, time.time() - step_start)
    except Exception:
        logger.exception("[STEP 4/7] TDB RSS 取得エラー")
        log_rss_fetch(conn, "tdb", 0, 0, "fetch error")

    if args.rss_only:
        logger.info("--rss-only モード: STEP 2,3,5,6,7 をスキップ")
        logger.info("バッチ完了 (%.1f秒)", time.time() - batch_start)
        conn.close()
        return 0

    # =========================================================================
    # STEP 2: TSR 詳細スクレイピング
    # =========================================================================
    logger.info("[STEP 2/7] TSR 詳細スクレイピング")
    step_start = time.time()
    tsr_html_results = []
    try:
        unscraped = get_tsr_unscraped(conn)
        logger.info("[STEP 2/7] 未取得: %d件", len(unscraped))
        if unscraped:
            tsr_html_results = tsr_scrape(unscraped)
            ok = sum(1 for r in tsr_html_results if r.success)
            logger.info("[STEP 2/7] 完了: 成功=%d件, エラー=%d件 (%.1f秒)",
                        ok, len(tsr_html_results) - ok, time.time() - step_start)
    except Exception:
        logger.exception("[STEP 2/7] TSR 詳細スクレイピングエラー")

    # =========================================================================
    # STEP 3: TSR 詳細パース → DB UPDATE
    # =========================================================================
    if tsr_html_results:
        logger.info("[STEP 3/7] TSR 詳細パース → DB 更新")
        step_start = time.time()
        try:
            parsed = tsr_parse(tsr_html_results)
            updated = update_tsr_detail(conn, parsed)
            logger.info("[STEP 3/7] 完了: %d件更新 (%.1f秒)", updated, time.time() - step_start)
        except Exception:
            logger.exception("[STEP 3/7] TSR 詳細パースエラー")

    # =========================================================================
    # STEP 5: TDB 詳細スクレイピング
    # =========================================================================
    logger.info("[STEP 5/7] TDB 詳細スクレイピング")
    step_start = time.time()
    tdb_html_results = []
    try:
        unscraped = get_tdb_unscraped(conn)
        logger.info("[STEP 5/7] 未取得: %d件", len(unscraped))
        if unscraped:
            tdb_html_results = tdb_scrape(unscraped)
            ok = sum(1 for r in tdb_html_results if r.success)
            logger.info("[STEP 5/7] 完了: 成功=%d件, エラー=%d件 (%.1f秒)",
                        ok, len(tdb_html_results) - ok, time.time() - step_start)
    except Exception:
        logger.exception("[STEP 5/7] TDB 詳細スクレイピングエラー")

    # =========================================================================
    # STEP 6: TDB 詳細パース → DB UPDATE
    # =========================================================================
    if tdb_html_results:
        logger.info("[STEP 6/7] TDB 詳細パース → DB 更新")
        step_start = time.time()
        try:
            parsed = tdb_parse(tdb_html_results)
            updated = update_tdb_detail(conn, parsed)
            logger.info("[STEP 6/7] 完了: %d件更新 (%.1f秒)", updated, time.time() - step_start)
        except Exception:
            logger.exception("[STEP 6/7] TDB 詳細パースエラー")

    # =========================================================================
    # STEP 7: 名寄せ
    # =========================================================================
    if not args.no_match:
        logger.info("[STEP 7/7] 名寄せ 開始")
        step_start = time.time()
        try:
            cdb = str(COMPANIES_DB) if COMPANIES_DB.exists() else None
            gdb = str(GBIZINFO_DB)  if GBIZINFO_DB.exists()  else None
            results = run_matching(conn, companies_db=cdb, gbizinfo_db=gdb)
            confirmed = sum(1 for r in results if r.is_confirmed)
            logger.info("[STEP 7/7] 完了: マッチ %d件（確定=%d件）(%.1f秒)",
                        len(results), confirmed, time.time() - step_start)
        except Exception:
            logger.exception("[STEP 7/7] 名寄せエラー")

    conn.close()
    logger.info("=" * 60)
    logger.info("倒産情報収集バッチ 完了  合計 %.1f秒", time.time() - batch_start)
    logger.info("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
