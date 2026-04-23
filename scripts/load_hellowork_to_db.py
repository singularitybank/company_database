# -*- coding: utf-8 -*-
"""
ハローワーク Parquet → SQLite バックフィルスクリプト

既存の全 Parquet ファイルを hellowork.db に一括投入する。
Parquet は以下の2か所を対象とする:
  - data/staging/hellowork/hellowork_*.parquet  （新形式）
  - data/staging/hellowork_*.parquet            （旧形式）

[実行方法]
  # 全Parquetを投入
  python scripts/load_hellowork_to_db.py

  # 特定日付のみ投入
  python scripts/load_hellowork_to_db.py --date 2026-04-10
"""

import argparse
import logging
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.common.db_utils import configure_for_bulk_load
from src.common.logging_setup import setup_logging
from src.config import DATA_DIR
from src.signals.hellowork.loaders.db_loader import load_parquet
from src.signals.hellowork.models.schema import init_db

STAGING_DIR = DATA_DIR / "staging"
DB_PATH = DATA_DIR / "hellowork.db"


def _collect_parquet_files(date_str: str | None) -> list[Path]:
    """投入対象の Parquet ファイルを新旧2か所から収集して日付順に返す。"""
    pattern = f"hellowork_{date_str}.parquet" if date_str else "hellowork_*.parquet"

    files: list[Path] = []
    # 新形式: data/staging/hellowork/
    files += sorted((STAGING_DIR / "hellowork").glob(pattern))
    # 旧形式: data/staging/
    files += sorted(STAGING_DIR.glob(pattern))

    # 重複除去（同名ファイルが両方に存在する場合は新形式を優先）
    seen: set[str] = set()
    unique: list[Path] = []
    for f in files:
        if f.name not in seen:
            seen.add(f.name)
            unique.append(f)

    return unique


def main() -> int:
    parser = argparse.ArgumentParser(description="ハローワーク Parquet → SQLite バックフィル")
    parser.add_argument(
        "--date",
        metavar="YYYY-MM-DD",
        help="特定日付のみ投入する（例: 2026-04-10）。省略時は全Parquetを対象とする",
    )
    args = parser.parse_args()

    setup_logging(PROJECT_ROOT / "logs" / "hellowork", log_filename="load_hellowork_to_db")
    logger = logging.getLogger(__name__)

    date_str = args.date.replace("-", "") if args.date else None
    files = _collect_parquet_files(date_str)

    if not files:
        logger.error("投入対象の Parquet ファイルが見つかりません（staging_dir=%s）", STAGING_DIR)
        return 1

    logger.info("=" * 60)
    logger.info("ハローワーク DBバックフィル 開始: %d件", len(files))
    logger.info("DB: %s", DB_PATH)
    logger.info("=" * 60)

    conn = init_db(DB_PATH)
    configure_for_bulk_load(conn)

    total_rows = 0
    start = time.time()

    for i, path in enumerate(files, 1):
        logger.info("[%d/%d] %s", i, len(files), path.name)
        try:
            rows = load_parquet(conn, path)
            total_rows += rows
        except Exception:
            logger.exception("  投入エラー: %s", path.name)

    conn.close()

    elapsed = time.time() - start
    logger.info("=" * 60)
    logger.info(
        "バックフィル完了: 合計 %d件 / %.1f分",
        total_rows, elapsed / 60,
    )
    logger.info("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
