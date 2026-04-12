# -*- coding: utf-8 -*-
"""
職場情報総合サイト 処理スクリプト

[処理フロー]
  STEP 1: shokuba.mhlw.go.jp から CSV をダウンロード
  STEP 2: CSV → Parquet 変換（8テーブル分）
  STEP 3: Parquet → SQLite 投入（全件入れ替え）

[実行方法]
  # 全ステップ実行（ダウンロードから）
  python scripts/run_shokuba.py

  # ダウンロードをスキップ（既存 CSV を使用）
  python scripts/run_shokuba.py --skip-download

  # CSV を直接指定してダウンロードをスキップ
  python scripts/run_shokuba.py --csv data/raw/shokuba/Shokubajoho_20260412.csv

  # Parquet 変換もスキップして DB 投入のみ
  python scripts/run_shokuba.py --skip-download --skip-convert

  # DB パスを変更
  python scripts/run_shokuba.py --db data/shokuba_test.db

  # 特定テーブルのみ DB 投入
  python scripts/run_shokuba.py --skip-download --skip-convert --tables shokuba_basic shokuba_childcare
"""

import argparse
import logging
import sys
import time
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from src.logging_setup import setup_logging

RAW_DIR     = BASE_DIR / "data" / "raw" / "shokuba"
STAGING_DIR = BASE_DIR / "data" / "staging" / "shokuba"
DB_PATH     = BASE_DIR / "data" / "shokuba.db"

log = logging.getLogger(__name__)


def _find_latest_csv() -> Path | None:
    candidates = sorted(RAW_DIR.glob("Shokubajoho_*.csv"), reverse=True)
    if candidates:
        return candidates[0]
    fallback = RAW_DIR / "Shokubajoho_UTF-8.csv"
    return fallback if fallback.exists() else None


def main() -> int:
    parser = argparse.ArgumentParser(description="職場情報総合サイト 処理スクリプト")
    parser.add_argument(
        "--skip-download", action="store_true",
        help="ダウンロードをスキップし、既存 CSV を使用する",
    )
    parser.add_argument(
        "--skip-convert", action="store_true",
        help="CSV → Parquet 変換をスキップし、既存 Parquet を使用する",
    )
    parser.add_argument(
        "--csv", dest="csv_path", default=None, type=Path,
        help="入力 CSV パス（省略時: data/raw/shokuba/ 以下の最新ファイル）",
    )
    parser.add_argument(
        "--db", default=str(DB_PATH), type=Path,
        help="SQLite ファイルパス（デフォルト: data/shokuba.db）",
    )
    parser.add_argument(
        "--tables", nargs="+", default=None,
        help="DB 投入するテーブル名（省略時: 全テーブル）",
    )
    args = parser.parse_args()

    setup_logging(BASE_DIR / "logs", filename_prefix="shokuba")

    batch_start = time.time()
    log.info("=" * 60)
    log.info("職場情報総合サイト 処理 開始")
    log.info("=" * 60)

    # ── STEP 1: ダウンロード ──────────────────────────────────────────────────
    if args.skip_download:
        log.info("[STEP 1/3] --skip-download 指定のためダウンロードをスキップ")
        csv_path = args.csv_path or _find_latest_csv()
        if csv_path is None or not csv_path.exists():
            log.error("CSV が見つかりません。--csv でパスを指定してください。")
            return 1
        log.info("[STEP 1/3] 使用 CSV: %s", csv_path)
    else:
        from src.downloaders.shokuba_downloader import download
        log.info("[STEP 1/3] ダウンロード 開始")
        step_start = time.time()
        try:
            csv_path = download(output_path=args.csv_path)
            log.info("[STEP 1/3] ダウンロード完了 (%.1fs)", time.time() - step_start)
        except Exception:
            log.exception("[STEP 1/3] ダウンロード中にエラーが発生しました")
            return 1

    # ── STEP 2: CSV → Parquet ────────────────────────────────────────────────
    if args.skip_convert:
        log.info("[STEP 2/3] --skip-convert 指定のため変換をスキップ")
        # 既存 Parquet の確認
        missing = [
            name for name, _, _ in __import__(
                "src.converters.shokuba_schema",
                fromlist=["TABLE_RANGES"]
            ).TABLE_RANGES
            if not (STAGING_DIR / f"{name}.parquet").exists()
        ]
        if missing:
            log.warning("[STEP 2/3] 以下の Parquet が見つかりません: %s", missing)
    else:
        from src.converters.shokuba_to_parquet import convert
        log.info("[STEP 2/3] CSV → Parquet 変換 開始: %s", csv_path.name)
        step_start = time.time()
        try:
            parquet_map = convert(csv_path)
            log.info(
                "[STEP 2/3] 変換完了: %d テーブル (%.1fs)",
                len(parquet_map), time.time() - step_start,
            )
        except Exception:
            log.exception("[STEP 2/3] 変換中にエラーが発生しました")
            return 1

    # ── STEP 3: Parquet → SQLite ─────────────────────────────────────────────
    from src.loaders.shokuba_to_sqlite import ALL_TABLES, load_table
    from src.utils.db_utils import configure_for_bulk_load, open_connection
    from datetime import datetime, timezone

    targets = args.tables or ALL_TABLES
    invalid = [t for t in targets if t not in ALL_TABLES]
    if invalid:
        log.error("不明なテーブル名: %s / 有効: %s", invalid, ALL_TABLES)
        return 1

    log.info("[STEP 3/3] DB 投入 開始: %s", args.db)
    step_start = time.time()
    loaded_at  = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    conn = open_connection(args.db)
    configure_for_bulk_load(conn)
    try:
        for table in targets:
            log.info("  ▶ %s", table)
            load_table(conn, table, loaded_at)
    except Exception:
        log.exception("[STEP 3/3] DB 投入中にエラーが発生しました")
        conn.close()
        return 1
    finally:
        conn.close()

    log.info("[STEP 3/3] DB 投入完了 (%.1fs)", time.time() - step_start)

    elapsed = time.time() - batch_start
    log.info("=" * 60)
    log.info("職場情報総合サイト 処理 正常終了  所要時間: %.1f秒", elapsed)
    log.info("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
