# -*- coding: utf-8 -*-
"""
職場情報総合サイト CSV → Parquet 変換

入力 : data/raw/shokuba/Shokubajoho_*.csv
出力 : data/staging/shokuba/{table_name}.parquet  ×8テーブル分

動作:
  1. CSV ヘッダーを読み込み、各列を COLUMN_MAP で英語名に変換
     マップにない列は col_NNNN 形式（N は 1-indexed 列番号）
  2. TABLE_RANGES に基づき列をテーブルごとに分割
  3. 各テーブルの先頭に corporate_number を確保（既存の場合は重複しない）
  4. 50,000 行チャンクで Parquet 出力（snappy 圧縮）

使い方:
  python src/converters/shokuba_to_parquet.py
  python src/converters/shokuba_to_parquet.py --csv data/raw/shokuba/Shokubajoho_20260412.csv
"""

import argparse
import csv
import logging
import sys
import time
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from src.converters.shokuba_schema import (
    COLUMN_MAP,
    DATE_COLUMNS,
    TABLE_RANGES,
)
from src.utils.date_utils import normalize_iso_date

RAW_DIR     = REPO_ROOT / "data" / "raw" / "shokuba"
STAGING_DIR = REPO_ROOT / "data" / "staging" / "shokuba"
CHUNK_SIZE  = 50_000
COMPRESSION = "snappy"

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 列名変換
# ---------------------------------------------------------------------------

def _build_col_rename(raw_header: list[str]) -> dict[str, str]:
    """
    CSV ヘッダー（日本語）→ 英語 snake_case 変換辞書を生成する。
    COLUMN_MAP にない列は col_NNNN 形式にフォールバック。
    重複する英語名には _dup_{N} サフィックスを付与して一意にする。
    """
    rename: dict[str, str] = {}
    seen: dict[str, int] = {}

    for idx, jp_name in enumerate(raw_header):
        en_name = COLUMN_MAP.get(jp_name)
        if not en_name:
            en_name = f"col_{idx + 1:04d}"

        # 重複回避
        if en_name in seen:
            seen[en_name] += 1
            en_name = f"{en_name}_dup{seen[en_name]}"
        else:
            seen[en_name] = 0

        rename[jp_name] = en_name

    return rename


# ---------------------------------------------------------------------------
# テーブル分割定義
# ---------------------------------------------------------------------------

def _build_table_col_map(
    raw_header: list[str],
    rename: dict[str, str],
) -> dict[str, list[str]]:
    """
    TABLE_RANGES をもとに、各テーブルが受け持つ英語カラム名リストを生成する。
    各テーブルに corporate_number が含まれない場合は先頭に追加する。
    """
    en_header = [rename[jp] for jp in raw_header]
    corp_num_en = COLUMN_MAP.get("法人番号", "corporate_number")

    table_cols: dict[str, list[str]] = {}
    for table_name, start, end in TABLE_RANGES:
        cols = en_header[start:end]
        # corporate_number が含まれていない場合は先頭に追加
        if corp_num_en not in cols:
            cols = [corp_num_en] + cols
        table_cols[table_name] = cols

    return table_cols


# ---------------------------------------------------------------------------
# Parquet ライター管理
# ---------------------------------------------------------------------------

class _WriterSet:
    def __init__(
        self,
        staging_dir: Path,
        table_cols: dict[str, list[str]],
    ) -> None:
        self._writers: dict[str, pq.ParquetWriter] = {}
        self._schemas: dict[str, pa.Schema] = {}
        self._table_cols = table_cols
        self._staging_dir = staging_dir
        staging_dir.mkdir(parents=True, exist_ok=True)

    def write(self, table: str, df: pd.DataFrame) -> None:
        if table not in self._writers:
            schema = pa.Schema.from_pandas(df)
            path = self._staging_dir / f"{table}.parquet"
            self._writers[table] = pq.ParquetWriter(path, schema, compression=COMPRESSION)
            self._schemas[table] = schema
        schema = self._schemas[table]
        self._writers[table].write_table(pa.Table.from_pandas(df, schema=schema))

    def close(self) -> None:
        for w in self._writers.values():
            w.close()


# ---------------------------------------------------------------------------
# メイン変換処理
# ---------------------------------------------------------------------------

def convert(csv_path: Path) -> dict[str, Path]:
    """
    CSV を読み込み、8テーブル分の Parquet ファイルを出力する。

    Returns:
        {table_name: parquet_path} の辞書
    """
    log.info("CSV: %s", csv_path)

    # ヘッダー先読み
    with open(csv_path, encoding="utf-8-sig", newline="") as fh:
        raw_header = next(csv.reader(fh))

    log.info("CSV列数: %d", len(raw_header))

    rename      = _build_col_rename(raw_header)
    table_cols  = _build_table_col_map(raw_header, rename)
    corp_num_en = COLUMN_MAP.get("法人番号", "corporate_number")

    for table, cols in table_cols.items():
        log.info("  %s: %d列", table, len(cols))

    writer_set = _WriterSet(STAGING_DIR, table_cols)
    total_rows = 0
    start = time.time()

    try:
        reader = pd.read_csv(
            csv_path,
            encoding="utf-8-sig",
            header=0,
            dtype=str,
            keep_default_na=False,
            chunksize=CHUNK_SIZE,
            low_memory=False,
        )

        for chunk_no, chunk in enumerate(reader):
            # 列名を英語に変換
            chunk.columns = [rename[jp] for jp in raw_header]

            # 日付列を正規化
            for col in DATE_COLUMNS:
                if col in chunk.columns:
                    chunk[col] = normalize_iso_date(chunk[col])

            # 空文字 → NA
            chunk = chunk.replace("", pd.NA)

            # テーブルごとに分割して書き込む
            for table_name, cols in table_cols.items():
                # corporate_number は全テーブルで保持（col_NNNN に変換されても対応）
                available = [c for c in cols if c in chunk.columns]
                sub = chunk[available].copy()
                writer_set.write(table_name, sub)

            total_rows += len(chunk)
            elapsed = time.time() - start
            log.info(
                "  chunk %4d | 累計 %8d行 | 経過 %5.1fs",
                chunk_no + 1, total_rows, elapsed,
            )

    finally:
        writer_set.close()

    elapsed = time.time() - start
    log.info("変換完了: %d行 / %.1fs", total_rows, elapsed)

    result: dict[str, Path] = {}
    for table_name, _, _ in TABLE_RANGES:
        path = STAGING_DIR / f"{table_name}.parquet"
        if path.exists():
            size_mb = path.stat().st_size / 1024 / 1024
            log.info("  %s: %.1f MB", path.name, size_mb)
            result[table_name] = path

    return result


# ---------------------------------------------------------------------------
# CSV ファイル自動検索
# ---------------------------------------------------------------------------

def find_latest_csv() -> Path | None:
    """data/raw/shokuba/ 以下で最新の Shokubajoho_*.csv を探す。"""
    candidates = sorted(RAW_DIR.glob("Shokubajoho_*.csv"), reverse=True)
    if candidates:
        return candidates[0]
    # 日付なしファイル名にもフォールバック
    fallback = RAW_DIR / "Shokubajoho_UTF-8.csv"
    return fallback if fallback.exists() else None


# ---------------------------------------------------------------------------
# エントリーポイント
# ---------------------------------------------------------------------------

def main() -> None:
    from src.logging_setup import setup_logging
    setup_logging(filename_prefix="shokuba")

    parser = argparse.ArgumentParser(
        description="職場情報総合サイト CSV → Parquet 変換"
    )
    parser.add_argument(
        "--csv", "-c",
        type=Path,
        default=None,
        help="入力 CSV パス（省略時: data/raw/shokuba/ 以下の最新ファイル）",
    )
    args = parser.parse_args()

    csv_path = args.csv or find_latest_csv()
    if csv_path is None or not csv_path.exists():
        log.error(
            "CSV が見つかりません。--csv オプションでパスを指定するか "
            "data/raw/shokuba/ にファイルを配置してください。"
        )
        sys.exit(1)

    log.info("=== 職場情報 CSV → Parquet 変換 開始 ===")
    convert(csv_path)
    log.info("=== 完了 ===")


if __name__ == "__main__":
    main()
