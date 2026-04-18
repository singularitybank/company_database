# -*- coding: utf-8 -*-
"""
gbizinfo CSV → Parquet 変換スクリプト

出力先: data/staging/gbizinfo/
  {name}_core.parquet  - コア列（英語カラム名）
  {name}_meta.parquet  - メタデータ列（corporate_number + metadata JSON blob）

使い方:
  # 全データセット変換
  python src/converters/gbizinfo_to_parquet.py

  # 特定データセットのみ
  python src/converters/gbizinfo_to_parquet.py kihonjoho tokkyojoho
"""

import json
import logging
import sys
import time
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# プロジェクトルートを sys.path に追加
REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from src.converters.gbizinfo_schema import DATASETS
from src.utils.date_utils import normalize_iso_date

# ---------------------------------------------------------------------------
# 設定
# ---------------------------------------------------------------------------

RAW_DIR     = REPO_ROOT / "data/raw/gbizinfo"
STAGING_DIR = REPO_ROOT / "data/staging/gbizinfo"
CHUNK_SIZE  = 50_000       # 1チャンクあたりの行数
COMPRESSION = "snappy"     # parquet 圧縮形式

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 型変換ヘルパー
# ---------------------------------------------------------------------------

def _to_int(series: pd.Series) -> pd.Series:
    """整数変換（空文字・非数値は NA）"""
    return pd.to_numeric(series.replace("", pd.NA), errors="coerce").astype("Int64")


# ---------------------------------------------------------------------------
# チャンク変換
# ---------------------------------------------------------------------------

def _transform_chunk(
    chunk: pd.DataFrame,
    core_cols: list[str],
    date_cols: set[str],
    int_cols: set[str],
    meta_col_indices: list[int],
    raw_header: list[str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """1チャンクを core DataFrame と meta DataFrame に変換して返す"""

    total_cols = len(raw_header)

    # ---- core ----
    core_df = chunk.iloc[:, :len(core_cols)].copy()
    core_df.columns = core_cols

    for col in date_cols:
        if col in core_df.columns:
            core_df[col] = normalize_iso_date(core_df[col])

    for col in int_cols:
        if col in core_df.columns:
            core_df[col] = _to_int(core_df[col])

    # 空文字 → NA（文字列カラム）
    str_cols = [c for c in core_df.columns if c not in date_cols and c not in int_cols]
    core_df[str_cols] = core_df[str_cols].replace("", pd.NA)

    # ---- meta ----
    if meta_col_indices:
        meta_raw = chunk.iloc[:, meta_col_indices]
        meta_headers = [raw_header[i] for i in meta_col_indices]

        # 各行を {列名: 値} の JSON 文字列に変換（null・空文字は省略）
        def _row_to_json(row: pd.Series) -> str:
            d = {
                k: v
                for k, v in zip(meta_headers, row)
                if v and str(v).strip()
            }
            return json.dumps(d, ensure_ascii=False) if d else None

        meta_df = pd.DataFrame({
            "corporate_number": core_df["corporate_number"].values,
            "metadata":         meta_raw.apply(_row_to_json, axis=1).values,
        })
    else:
        meta_df = pd.DataFrame({"corporate_number": core_df["corporate_number"].values})

    return core_df, meta_df


# ---------------------------------------------------------------------------
# メイン変換処理
# ---------------------------------------------------------------------------

def convert(dataset_key: str) -> None:
    cfg = DATASETS[dataset_key]
    csv_path = RAW_DIR / cfg["csv_file"]

    if not csv_path.exists():
        log.warning("CSVが見つかりません: %s", csv_path)
        return

    STAGING_DIR.mkdir(parents=True, exist_ok=True)
    core_out = STAGING_DIR / f"{dataset_key}_core.parquet"
    meta_out = STAGING_DIR / f"{dataset_key}_meta.parquet"

    core_count  = cfg["core_count"]
    core_cols   = cfg["core_columns"]
    date_cols   = cfg["date_columns"]
    int_cols    = cfg["int_columns"]

    # CSV ヘッダーを先読み（メタ列の日本語名を JSON key に使う）
    with open(csv_path, encoding="utf-8-sig") as fh:
        raw_header = next(
            __import__("csv").reader(fh)
        )
    meta_col_indices = list(range(core_count, len(raw_header)))

    log.info("=== %s ===", dataset_key)
    log.info("  CSV    : %s", csv_path.name)
    log.info("  core   : %d列 → %s", core_count, core_out.name)
    log.info("  meta   : %d列 → %s", len(meta_col_indices), meta_out.name)

    core_writer: pq.ParquetWriter | None = None
    meta_writer: pq.ParquetWriter | None = None
    total_rows   = 0
    start        = time.time()

    try:
        reader = pd.read_csv(
            csv_path,
            encoding="utf-8-sig",
            header=0,
            dtype=str,          # 全列 str で読み込み（型変換は後で行う）
            keep_default_na=False,
            chunksize=CHUNK_SIZE,
            low_memory=False,
        )

        for i, chunk in enumerate(reader):
            core_df, meta_df = _transform_chunk(
                chunk, core_cols, date_cols, int_cols,
                meta_col_indices, raw_header,
            )

            # Parquet ライター初期化（最初のチャンクでスキーマ確定）
            if core_writer is None:
                core_schema = pa.Schema.from_pandas(core_df)
                meta_schema = pa.Schema.from_pandas(meta_df)
                core_writer = pq.ParquetWriter(core_out, core_schema, compression=COMPRESSION)
                meta_writer = pq.ParquetWriter(meta_out, meta_schema, compression=COMPRESSION)

            core_writer.write_table(pa.Table.from_pandas(core_df, schema=core_schema))
            meta_writer.write_table(pa.Table.from_pandas(meta_df, schema=meta_schema))

            total_rows += len(chunk)
            elapsed = time.time() - start
            log.info("  chunk %4d | 累計 %8d行 | 経過 %5.1fs", i + 1, total_rows, elapsed)

    finally:
        if core_writer:
            core_writer.close()
        if meta_writer:
            meta_writer.close()

    elapsed = time.time() - start
    core_mb = core_out.stat().st_size / 1024 / 1024
    meta_mb = meta_out.stat().st_size / 1024 / 1024
    log.info("  完了: %d行 | %.1fs | core %.1fMB | meta %.1fMB",
             total_rows, elapsed, core_mb, meta_mb)


# ---------------------------------------------------------------------------
# エントリーポイント
# ---------------------------------------------------------------------------

def main() -> None:
    from src.logging_setup import setup_logging
    setup_logging()

    targets = sys.argv[1:] if len(sys.argv) > 1 else list(DATASETS.keys())

    invalid = [t for t in targets if t not in DATASETS]
    if invalid:
        print(f"不明なデータセット: {invalid}")
        print(f"有効なキー: {list(DATASETS.keys())}")
        sys.exit(1)

    log.info("変換対象: %s", targets)
    overall_start = time.time()

    for key in targets:
        convert(key)

    log.info("全処理完了 (%.1fs)", time.time() - overall_start)


if __name__ == "__main__":
    main()
