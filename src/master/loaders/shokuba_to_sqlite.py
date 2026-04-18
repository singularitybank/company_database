# -*- coding: utf-8 -*-
"""
職場情報総合サイト Parquet → SQLite ローダー

入力 : data/staging/shokuba/{table_name}.parquet  ×8テーブル
出力 : data/shokuba.db

更新戦略: 全件入れ替え（差分APIなし）
  1. 既存テーブルを DROP
  2. Parquet スキーマから CREATE TABLE を自動生成
  3. 50,000 行バッチで INSERT
  4. corporate_number にインデックス作成

使い方:
  # 全テーブル
  python src/loaders/shokuba_to_sqlite.py

  # 特定テーブルのみ
  python src/loaders/shokuba_to_sqlite.py shokuba_basic shokuba_childcare
"""

import logging
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

REPO_ROOT   = Path(__file__).resolve().parents[2]
STAGING_DIR = REPO_ROOT / "data" / "staging" / "shokuba"
DB_PATH     = REPO_ROOT / "data" / "shokuba.db"
BATCH_SIZE  = 50_000

sys.path.insert(0, str(REPO_ROOT))

from src.master.converters.shokuba_schema import TABLE_INDEXES, TABLE_RANGES
from src.common.db_utils import configure_for_bulk_load, open_connection

log = logging.getLogger(__name__)

# テーブル名リスト（TABLE_RANGES の順序を保持）
ALL_TABLES = [name for name, _, _ in TABLE_RANGES]


# ---------------------------------------------------------------------------
# 型変換ヘルパー
# ---------------------------------------------------------------------------

def _pa_type_to_sqlite(pa_type: pa.DataType) -> str:
    """PyArrow 型 → SQLite 型名"""
    if pa.types.is_integer(pa_type):
        return "INTEGER"
    if pa.types.is_floating(pa_type):
        return "REAL"
    return "TEXT"


# ---------------------------------------------------------------------------
# DDL 生成
# ---------------------------------------------------------------------------

def _build_ddl(table: str, parquet_schema: pa.Schema) -> str:
    """Parquet スキーマから CREATE TABLE DDL を生成する。"""
    cols = []
    for i in range(len(parquet_schema)):
        field = parquet_schema.field(i)
        if field.name == "__null_dask_index__":
            continue
        dtype = _pa_type_to_sqlite(field.type)
        cols.append(f"  {field.name} {dtype}")
    cols.append("  loaded_at TEXT")
    return f"CREATE TABLE IF NOT EXISTS {table} (\n" + ",\n".join(cols) + "\n)"


# ---------------------------------------------------------------------------
# テーブル初期化（DROP → CREATE）
# ---------------------------------------------------------------------------

def _init_table(conn: sqlite3.Connection, table: str, parquet_path: Path) -> pa.Schema:
    """既存テーブルを削除して再作成し、Parquet スキーマを返す。"""
    conn.execute(f"DROP TABLE IF EXISTS {table}")
    conn.commit()

    schema = pq.read_schema(parquet_path)
    ddl    = _build_ddl(table, schema)
    conn.execute(ddl)
    conn.commit()
    log.info("[%s] テーブル作成完了", table)
    return schema


# ---------------------------------------------------------------------------
# データ挿入
# ---------------------------------------------------------------------------

def _insert_parquet(
    conn: sqlite3.Connection,
    table: str,
    parquet_path: Path,
    schema: pa.Schema,
    loaded_at: str,
) -> int:
    """Parquet を読み込んでバッチ INSERT する。挿入行数を返す。"""
    col_names = [
        schema.field(i).name
        for i in range(len(schema))
        if schema.field(i).name != "__null_dask_index__"
    ]
    all_cols     = col_names + ["loaded_at"]
    placeholders = ", ".join("?" * len(all_cols))
    col_list     = ", ".join(all_cols)
    sql          = f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})"

    pf    = pq.ParquetFile(parquet_path)
    total = 0

    for batch in pf.iter_batches(batch_size=BATCH_SIZE):
        d    = batch.to_pydict()
        n    = len(d[col_names[0]])
        rows = [
            tuple(d[c][i] for c in col_names) + (loaded_at,)
            for i in range(n)
        ]
        conn.executemany(sql, rows)
        total += n

    conn.commit()
    return total


# ---------------------------------------------------------------------------
# インデックス作成
# ---------------------------------------------------------------------------

def _create_indexes(conn: sqlite3.Connection, table: str) -> None:
    for col in TABLE_INDEXES.get(table, []):
        idx = f"idx_{table}_{col}"
        conn.execute(f"CREATE INDEX IF NOT EXISTS {idx} ON {table} ({col})")
    conn.commit()


# ---------------------------------------------------------------------------
# 1テーブルのロード
# ---------------------------------------------------------------------------

def load_table(
    conn: sqlite3.Connection,
    table: str,
    loaded_at: str,
) -> None:
    parquet_path = STAGING_DIR / f"{table}.parquet"
    if not parquet_path.exists():
        log.warning("[%s] Parquet が見つかりません: %s", table, parquet_path)
        return

    t0     = time.time()
    schema = _init_table(conn, table, parquet_path)

    rows = _insert_parquet(conn, table, parquet_path, schema, loaded_at)
    log.info("[%s] INSERT 完了: %d行 (%.1fs)", table, rows, time.time() - t0)

    _create_indexes(conn, table)
    log.info("[%s] インデックス作成完了 (%.1fs)", table, time.time() - t0)


# ---------------------------------------------------------------------------
# メイン
# ---------------------------------------------------------------------------

def main() -> None:
    from src.common.logging_setup import setup_logging
    setup_logging(filename_prefix="shokuba")

    targets = sys.argv[1:] if len(sys.argv) > 1 else ALL_TABLES
    invalid = [t for t in targets if t not in ALL_TABLES]
    if invalid:
        print(f"不明なテーブル: {invalid}")
        print(f"有効なテーブル: {ALL_TABLES}")
        sys.exit(1)

    loaded_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    log.info("DB: %s", DB_PATH)
    log.info("ロード対象: %s", targets)
    log.info("loaded_at: %s", loaded_at)

    conn = open_connection(DB_PATH)
    configure_for_bulk_load(conn)
    try:
        total_start = time.time()
        for table in targets:
            log.info("===== %s =====", table)
            load_table(conn, table, loaded_at)
        log.info("全完了: %.1fs", time.time() - total_start)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
