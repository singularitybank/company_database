# -*- coding: utf-8 -*-
"""
Google Newsキーワード収集バッチ エントリーポイント

[処理フロー（定期実行）]
  STEP 1: 夜間チェック（22:00〜07:00 JST はスキップ、--force で上書き）
  STEP 2: DB初期化
  STEP 3: activeなキーワード一覧取得
  STEP 4: キーワードごとにRSS取得 → DB保存 → ログ記録

[キーワード管理]
  python scripts/run_google_news.py --add-keyword "工場火災"
  python scripts/run_google_news.py --disable-keyword "工場火災"

[実行方法]
  python scripts/run_google_news.py                         # 全キーワード実行
  python scripts/run_google_news.py --force                 # 夜間スキップ無効
  python scripts/run_google_news.py --keyword "工場火災"    # 特定キーワードのみ

[タスクスケジューラ]
  scripts/run_google_news.bat から呼び出す
"""

import argparse
import logging
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.config import google_news as _cfg
from src.signals.google_news.crawlers.rss_crawler import fetch_by_keyword
from src.signals.google_news.loaders.db_loader import (
    add_keyword, disable_keyword, get_active_keywords,
    insert_articles, log_fetch,
)
from src.signals.google_news.models.schema import init_db
from src.common.logging_setup import setup_logging

JST     = timezone(timedelta(hours=9))
DB_PATH = PROJECT_ROOT / _cfg["db_path"]

NIGHT_SKIP_ENABLED = _cfg["night_skip"]["enabled"]
NIGHT_SKIP_START   = _cfg["night_skip"]["start_hour"]
NIGHT_SKIP_END     = _cfg["night_skip"]["end_hour"]
BASE_URL           = _cfg["base_url"]
PARAMS             = _cfg["params"]
TIMEOUT            = _cfg["request"]["timeout"]
RETRY_COUNT        = _cfg["request"]["retry_count"]
WAIT_BETWEEN       = _cfg["request"]["wait_between_keywords"]


def _is_night_time(now: datetime) -> bool:
    h = now.hour
    if NIGHT_SKIP_START > NIGHT_SKIP_END:
        return h >= NIGHT_SKIP_START or h < NIGHT_SKIP_END
    return NIGHT_SKIP_START <= h < NIGHT_SKIP_END


def main() -> int:
    parser = argparse.ArgumentParser(description="Google Newsキーワード収集バッチ")
    parser.add_argument("--force",           action="store_true", help="夜間スキップを無効化")
    parser.add_argument("--keyword",         type=str, default=None, metavar="KW",
                        help="特定キーワードのみ実行")
    parser.add_argument("--add-keyword",     type=str, default=None, metavar="KW",
                        help="キーワードを追加してアクティブ化")
    parser.add_argument("--disable-keyword", type=str, default=None, metavar="KW",
                        help="キーワードを無効化")
    args = parser.parse_args()

    now_jst  = datetime.now(JST)
    date_str = now_jst.strftime("%Y%m%d")

    setup_logging(PROJECT_ROOT / "logs" / "google_news", log_filename=f"google_news_{date_str}")
    logger = logging.getLogger(__name__)

    # キーワード管理操作（--add / --disable）
    if args.add_keyword or args.disable_keyword:
        conn = init_db(DB_PATH)
        if args.add_keyword:
            add_keyword(conn, args.add_keyword)
            logger.info("キーワード追加完了: 「%s」", args.add_keyword)
        if args.disable_keyword:
            if not disable_keyword(conn, args.disable_keyword):
                logger.error("キーワードが見つかりません: 「%s」", args.disable_keyword)
        conn.close()
        return 0

    # =========================================================================
    # STEP 1: 夜間チェック
    # =========================================================================
    if NIGHT_SKIP_ENABLED and not args.force and _is_night_time(now_jst):
        logger.info("夜間のためスキップ（%s）。--force で強制実行可能。", now_jst.strftime("%H:%M"))
        return 0

    logger.info("=" * 60)
    logger.info("Google News収集バッチ 開始  %s", now_jst.strftime("%Y-%m-%d %H:%M:%S"))
    logger.info("=" * 60)
    batch_start = time.time()

    # =========================================================================
    # STEP 2: DB初期化
    # =========================================================================
    conn = init_db(DB_PATH)

    # =========================================================================
    # STEP 3: キーワード一覧取得
    # =========================================================================
    if args.keyword:
        # 指定キーワードをDBから検索（存在しない場合も実行）
        all_kws = get_active_keywords(conn)
        kw_map  = {k.keyword: k for k in all_kws}
        if args.keyword in kw_map:
            keywords = [kw_map[args.keyword]]
        else:
            # DBに未登録のキーワードでも一時的に実行できるようにする
            from src.signals.google_news.loaders.db_loader import KeywordRow
            keywords = [KeywordRow(keyword_id=-1, keyword=args.keyword)]
            logger.warning("キーワード「%s」はDBに未登録。一時実行します。", args.keyword)
    else:
        keywords = get_active_keywords(conn)

    if not keywords:
        logger.warning("実行対象のキーワードがありません。--add-keyword で追加してください。")
        conn.close()
        return 0

    logger.info("対象キーワード: %d件", len(keywords))

    # =========================================================================
    # STEP 4: キーワードごとにRSS取得 → 保存
    # =========================================================================
    total_new = 0
    for i, kw in enumerate(keywords):
        logger.info("[%d/%d] キーワード「%s」", i + 1, len(keywords), kw.keyword)
        step_start = time.time()

        try:
            entries = fetch_by_keyword(
                keyword    = kw.keyword,
                keyword_id = kw.keyword_id,
                base_url   = BASE_URL,
                params     = PARAMS,
                timeout    = TIMEOUT,
                retry_count= RETRY_COUNT,
            )
            new_cnt  = insert_articles(conn, entries) if entries else 0
            log_fetch(conn, kw.keyword, len(entries), new_cnt)
            total_new += new_cnt
            logger.info("[%s] 完了: %d件取得, %d件新規 (%.1f秒)",
                        kw.keyword, len(entries), new_cnt, time.time() - step_start)
        except Exception:
            logger.exception("[%s] エラー", kw.keyword)
            log_fetch(conn, kw.keyword, 0, 0, "fetch error")

        if i < len(keywords) - 1:
            time.sleep(WAIT_BETWEEN)

    conn.close()
    logger.info("=" * 60)
    logger.info("Google News収集バッチ 完了  新規合計=%d件  合計 %.1f秒",
                total_new, time.time() - batch_start)
    logger.info("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
