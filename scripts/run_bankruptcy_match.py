# -*- coding: utf-8 -*-
"""
倒産情報 名寄せのみ再実行スクリプト

[用途]
  - 名寄せロジック修正後の再実行
  - 手動で名寄せをやり直す場合

[実行方法]
  python scripts/run_bankruptcy_match.py
"""

import logging
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.config import bankruptcy as _cfg
from src.signals.bankruptcy.matchers.name_matcher import run_matching
from src.signals.bankruptcy.models.schema import init_db
from src.common.logging_setup import setup_logging

JST     = timezone(timedelta(hours=9))
DB_PATH = PROJECT_ROOT / _cfg["db_path"]

COMPANIES_DB = PROJECT_ROOT / "data" / "companies.db"
GBIZINFO_DB  = PROJECT_ROOT / "data" / "gbizinfo.db"


def main() -> int:
    now_jst  = datetime.now(JST)
    date_str = now_jst.strftime("%Y%m%d")

    setup_logging(PROJECT_ROOT / "logs" / "bankruptcy", log_filename=f"bankruptcy_match_{date_str}")
    logger = logging.getLogger(__name__)

    logger.info("名寄せ再実行 開始  %s", now_jst.strftime("%Y-%m-%d %H:%M:%S"))
    start = time.time()

    conn = init_db(DB_PATH)

    cdb = str(COMPANIES_DB) if COMPANIES_DB.exists() else None
    gdb = str(GBIZINFO_DB)  if GBIZINFO_DB.exists()  else None

    if not cdb:
        logger.warning("companies.db が見つかりません: NTA住所・代表者スコアをスキップ")
    if not gdb:
        logger.warning("gbizinfo.db が見つかりません: 資本金スコアをスキップ")

    try:
        results = run_matching(conn, companies_db=cdb, gbizinfo_db=gdb)
        confirmed = sum(1 for r in results if r.is_confirmed)
        logger.info("名寄せ完了: マッチ %d件（確定=%d件）(%.1f秒)",
                    len(results), confirmed, time.time() - start)
    except Exception:
        logger.exception("名寄せエラー")
        conn.close()
        return 1

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
