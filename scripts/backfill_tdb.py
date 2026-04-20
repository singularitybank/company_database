# -*- coding: utf-8 -*-
"""
TDB 過去記事一括スクレイピング（バックフィル）

[動作概要]
  --end-id から --start-id に向かって降順（最新優先）に記事番号をスキャンし、
  未取得のものだけスクレイピング → DB INSERT OR IGNORE → パース → DB UPDATE する。
  HTTP で取得できなかった番号（404 など）は静かにスキップする。
  Selenium フォールバックは使用しない（多数の存在しないURLへのアクセスを避けるため）。

[実行方法]
  python scripts/backfill_tdb.py --end-id 250000 --start-id 240000

[スキップ条件（いずれかに該当すれば処理しない）]
  - C:/Temp/html/bankruptcy/tdb/{case_id}.html が存在する
  - tdb_cases テーブルで detail_scraped_at IS NOT NULL
"""

import argparse
import hashlib
import logging
import random
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.config import bankruptcy as _cfg
from src.signals.bankruptcy.crawlers.tdb_detail_crawler import scrape
from src.signals.bankruptcy.parsers.tdb_detail_parser import parse as tdb_parse
from src.signals.bankruptcy.loaders.db_loader import update_tdb_detail
from src.signals.bankruptcy.models.schema import init_db
from src.common.logging_setup import setup_logging

JST      = timezone(timedelta(hours=9))
DB_PATH  = PROJECT_ROOT / _cfg["db_path"]
HTML_DIR = Path(_cfg["html_dir"]) / "tdb"
WAIT     = _cfg["wait_between_requests"]

TDB_DETAIL_URL = "https://www.tdb.co.jp/report/bankruptcy/flash/{article_id}/"


# ---------------------------------------------------------------------------
# ヘルパー
# ---------------------------------------------------------------------------

def _make_case_id(url: str) -> str:
    return hashlib.sha256(url.encode()).hexdigest()[:16]


@dataclass
class _Entry:
    case_id:    str
    source_url: str


def _is_done(conn: sqlite3.Connection, case_id: str, html_path: Path) -> bool:
    """HTML 保存済み、または DB で detail_scraped_at が埋まっていれば True。"""
    if html_path.exists():
        return True
    row = conn.execute(
        "SELECT 1 FROM tdb_cases WHERE case_id = ? AND detail_scraped_at IS NOT NULL",
        (case_id,),
    ).fetchone()
    return row is not None


def _insert_if_new(conn: sqlite3.Connection, case_id: str, source_url: str) -> None:
    """tdb_cases に INSERT OR IGNORE する。company_name / published_at は後のパースで埋まる。"""
    fetched_at = datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")
    conn.execute(
        """
        INSERT OR IGNORE INTO tdb_cases
            (case_id, source_url, company_name, published_at, rss_fetched_at)
        VALUES (?, ?, NULL, NULL, ?)
        """,
        (case_id, source_url, fetched_at),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# メイン処理
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description="TDB 過去記事一括スクレイピング")
    parser.add_argument("--start-id", type=int, required=True,
                        help="開始記事番号（この番号を含む）")
    parser.add_argument("--end-id",   type=int, required=True,
                        help="終了記事番号（この番号を含む）")
    args = parser.parse_args()

    if args.start_id > args.end_id:
        print("--start-id は --end-id 以下にしてください", file=sys.stderr)
        return 1

    now_jst  = datetime.now(JST)
    date_str = now_jst.strftime("%Y%m%d")
    setup_logging(
        PROJECT_ROOT / "logs" / "bankruptcy",
        log_filename=f"backfill_tdb_{date_str}",
    )
    logger = logging.getLogger(__name__)

    conn = init_db(DB_PATH)

    # 降順（最新 → 古い順）でスキャン
    article_ids = range(args.end_id, args.start_id - 1, -1)
    total   = len(article_ids)
    scraped = 0
    skipped = 0

    logger.info("=" * 60)
    logger.info("TDB バックフィル開始: %d → %d (%d件)",
                args.end_id, args.start_id, total)
    logger.info("=" * 60)

    for i, article_id in enumerate(article_ids, 1):
        url       = TDB_DETAIL_URL.format(article_id=article_id)
        case_id   = _make_case_id(url)
        html_path = HTML_DIR / f"{case_id}.html"

        if _is_done(conn, case_id, html_path):
            skipped += 1
            logger.debug("[%d/%d] スキップ（取得済み）: article_id=%d", i, total, article_id)
            continue

        logger.info("[%d/%d] 取得: article_id=%d", i, total, article_id)

        results = scrape([_Entry(case_id=case_id, source_url=url)])
        result  = results[0]

        # リクエストを行った場合は成否によらず待機
        time.sleep(random.uniform(WAIT, WAIT * 2.5))

        if not result.success:
            # 404 や接続エラーは静かにスキップ
            logger.debug("スキップ（取得失敗）: article_id=%d", article_id)
            continue

        # DB に新規登録（RSS 経由ではない記事も登録できるよう INSERT OR IGNORE）
        _insert_if_new(conn, case_id, url)

        # パース → DB UPDATE
        parsed  = tdb_parse(results)
        updated = update_tdb_detail(conn, parsed)
        if updated:
            logger.info("DB 更新完了: article_id=%d  case_id=%s", article_id, case_id)
        scraped += 1

    logger.info("=" * 60)
    logger.info("TDB バックフィル完了: 新規取得=%d件, スキップ=%d件", scraped, skipped)
    logger.info("=" * 60)
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
