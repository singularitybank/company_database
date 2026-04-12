# -*- coding: utf-8 -*-
"""
国税庁 全件ダウンロード 処理スクリプト

[処理フロー]
  STEP 1: CSV → Parquet 変換   convert_raw_to_staging()
  STEP 2: データ概要確認        summarize()
  STEP 3: Parquet → SQLite 投入 load_to_db()

[実行方法]
  # 通常（data/raw/nta/ の最新CSVを処理）
  python scripts/run_nta.py

  # 日付指定でCSVを直接指定
  python scripts/run_nta.py --csv data/raw/nta/00_zenkoku_all_20260331.csv

  # staging 変換をスキップし、既存 Parquet から DB 投入のみ実施
  python scripts/run_nta.py --skip-staging

  # DB パスを変更
  python scripts/run_nta.py --db data/companies_test.db
"""
import argparse
import logging
import sys
import time
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from src.converters.nta_to_parquet import convert_raw_to_staging, load_staging, summarize
from src.loaders.nta_to_sqlite import load_to_db
from src.logging_setup import setup_logging

# ---------------------------------------------------------------------------
# パス定数
# ---------------------------------------------------------------------------

RAW_DIR     = BASE_DIR / "data" / "raw" / "nta"
STAGING_DIR = BASE_DIR / "data" / "staging"
DB_PATH     = BASE_DIR / "data" / "companies.db"

# ---------------------------------------------------------------------------
# メイン処理
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description="国税庁 全件ダウンロード処理")
    parser.add_argument(
        "--csv", dest="csv_path", default=None,
        help="入力CSVファイルパス。省略時は data/raw/nta/ の最新ファイルを使用",
    )
    parser.add_argument(
        "--skip-staging", action="store_true",
        help="CSV → Parquet 変換をスキップし、staging の最新 Parquet を使用する",
    )
    parser.add_argument(
        "--db", default=str(DB_PATH),
        help="SQLiteファイルパス（デフォルト: data/companies.db）",
    )
    args = parser.parse_args()

    setup_logging(BASE_DIR / "logs", filename_prefix="nta")
    logger = logging.getLogger(__name__)

    batch_start = time.time()
    logger.info("=" * 60)
    logger.info("国税庁 全件ダウンロード処理 開始")
    logger.info("=" * 60)

    # ── STEP 1: CSV → Parquet ────────────────────────────────────────────────
    if args.skip_staging:
        logger.info("[STEP 1/3] --skip-staging 指定のため変換をスキップ")
        parquet_files = sorted(STAGING_DIR.glob("nta_*.parquet"))
        if not parquet_files:
            logger.error("staging に Parquet ファイルが見つかりません: %s", STAGING_DIR)
            return 1
        parquet_path = parquet_files[-1]
        logger.info("[STEP 1/3] 既存 Parquet を使用: %s", parquet_path.name)
    else:
        logger.info("[STEP 1/3] CSV → Parquet 変換 開始")
        if args.csv_path:
            csv_path = Path(args.csv_path)
        else:
            csv_files = sorted(RAW_DIR.glob("*.csv"))
            if not csv_files:
                logger.error("raw ディレクトリに CSV ファイルが見つかりません: %s", RAW_DIR)
                return 1
            csv_path = csv_files[-1]

        logger.info("  対象CSV: %s", csv_path.name)
        step_start = time.time()
        try:
            parquet_path = convert_raw_to_staging(csv_path, STAGING_DIR)
        except Exception:
            logger.exception("[STEP 1/3] CSV 変換中にエラーが発生しました")
            return 1
        logger.info("[STEP 1/3] 変換完了 (%.1f秒)", time.time() - step_start)

    # ── STEP 2: 概要確認 ─────────────────────────────────────────────────────
    logger.info("[STEP 2/3] データ概要確認")
    try:
        df = load_staging(parquet_path)
        summarize(df)
        del df  # メモリ解放
    except Exception:
        logger.exception("[STEP 2/3] 概要確認中にエラーが発生しました")
        return 1

    # ── STEP 3: Parquet → SQLite ─────────────────────────────────────────────
    logger.info("[STEP 3/3] DB 投入 開始: %s", args.db)
    step_start = time.time()
    try:
        rows = load_to_db(parquet_path, args.db)
        logger.info("[STEP 3/3] DB 投入完了: %d行 (%.1f秒)", rows, time.time() - step_start)
    except Exception:
        logger.exception("[STEP 3/3] DB 投入中にエラーが発生しました")
        return 1

    elapsed = time.time() - batch_start
    logger.info("=" * 60)
    logger.info("国税庁 全件ダウンロード処理 正常終了  所要時間: %.1f秒", elapsed)
    logger.info("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
