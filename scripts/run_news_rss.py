# -*- coding: utf-8 -*-
"""
ニュースRSS収集バッチ エントリーポイント

[処理フロー]
  STEP 1: 夜間チェック（22:00〜07:00 JST はスキップ、--force で上書き）
  STEP 2: DB初期化
  STEP 3: 全ソースに対してRSS取得 → DB保存 → ログ記録

[実行方法]
  python scripts/run_news_rss.py                   # 全10ソース実行
  python scripts/run_news_rss.py --force           # 夜間スキップ無効
  python scripts/run_news_rss.py --source nhk      # 特定ソースのみ実行

[タスクスケジューラ]
  scripts/run_news_rss.bat から呼び出す
"""

import argparse
import logging
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.config import news as _cfg
from src.signals.news.crawlers.rss_crawler import fetch_rss
from src.signals.news.loaders.db_loader import insert_articles, log_rss_fetch
from src.signals.news.models.schema import init_db
from src.common.logging_setup import setup_logging

JST     = timezone(timedelta(hours=9))
DB_PATH = PROJECT_ROOT / _cfg["db_path"]

NIGHT_SKIP_ENABLED = _cfg["night_skip"]["enabled"]
NIGHT_SKIP_START   = _cfg["night_skip"]["start_hour"]
NIGHT_SKIP_END     = _cfg["night_skip"]["end_hour"]
TIMEOUT            = _cfg["request"]["timeout"]
RETRY_COUNT        = _cfg["request"]["retry_count"]
WAIT_BETWEEN       = _cfg["request"]["wait_between_sources"]


def _is_night_time(now: datetime) -> bool:
    h = now.hour
    if NIGHT_SKIP_START > NIGHT_SKIP_END:
        return h >= NIGHT_SKIP_START or h < NIGHT_SKIP_END
    return NIGHT_SKIP_START <= h < NIGHT_SKIP_END


def main() -> int:
    parser = argparse.ArgumentParser(description="ニュースRSS収集バッチ")
    parser.add_argument("--force",  action="store_true", help="夜間スキップを無効化")
    parser.add_argument("--source", type=str, default=None, metavar="KEY",
                        help="特定ソースのみ実行（例: nhk）")
    args = parser.parse_args()

    now_jst  = datetime.now(JST)
    date_str = now_jst.strftime("%Y%m%d")

    setup_logging(PROJECT_ROOT / "logs" / "news", log_filename=f"news_{date_str}")
    logger = logging.getLogger(__name__)

    # =========================================================================
    # STEP 1: 夜間チェック
    # =========================================================================
    if NIGHT_SKIP_ENABLED and not args.force and _is_night_time(now_jst):
        logger.info("夜間のためスキップ（%s）。--force で強制実行可能。", now_jst.strftime("%H:%M"))
        return 0

    logger.info("=" * 60)
    logger.info("ニュースRSS収集バッチ 開始  %s", now_jst.strftime("%Y-%m-%d %H:%M:%S"))
    logger.info("=" * 60)
    batch_start = time.time()

    # =========================================================================
    # STEP 2: DB初期化
    # =========================================================================
    conn = init_db(DB_PATH)

    # =========================================================================
    # STEP 3: ソースごとにRSS取得 → 保存
    # =========================================================================
    sources = _cfg["sources"]
    if args.source:
        sources = [s for s in sources if s["key"] == args.source]
        if not sources:
            logger.error("ソース「%s」が config.yaml に見つかりません", args.source)
            conn.close()
            return 1

    total_new = 0
    for i, src in enumerate(sources):
        key          = src["key"]
        display_name = src["display_name"]
        url          = src["url"]
        category     = src["category"]

        logger.info("[%d/%d] %s (%s)", i + 1, len(sources), display_name, key)
        step_start = time.time()

        try:
            entries  = fetch_rss(key, url, category, timeout=TIMEOUT, retry_count=RETRY_COUNT)
            new_cnt  = insert_articles(conn, entries) if entries else 0
            log_rss_fetch(conn, key, len(entries), new_cnt)
            total_new += new_cnt
            logger.info("[%s] 完了: %d件取得, %d件新規 (%.1f秒)",
                        key, len(entries), new_cnt, time.time() - step_start)
        except Exception:
            logger.exception("[%s] エラー", key)
            log_rss_fetch(conn, key, 0, 0, "fetch error")

        if i < len(sources) - 1:
            time.sleep(WAIT_BETWEEN)

    conn.close()
    logger.info("=" * 60)
    logger.info("ニュースRSS収集バッチ 完了  新規合計=%d件  合計 %.1f秒",
                total_new, time.time() - batch_start)
    logger.info("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
