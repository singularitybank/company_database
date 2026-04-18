# -*- coding: utf-8 -*-
"""
gbizinfo Parquet → SQLite ローダー

入力 : data/staging/gbizinfo/{dataset}_core.parquet
       data/staging/gbizinfo/{dataset}_meta.parquet
出力 : data/gbizinfo.db

使い方:
  # 全データセット
  python src/loaders/gbizinfo_to_sqlite.py

  # 特定データセットのみ
  python src/loaders/gbizinfo_to_sqlite.py kihonjoho tokkyojoho
"""

import logging
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq

REPO_ROOT   = Path(__file__).resolve().parents[2]
STAGING_DIR = REPO_ROOT / "data/staging/gbizinfo"
DB_PATH     = REPO_ROOT / "data/gbizinfo.db"
BATCH_SIZE  = 50_000

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# スキーマ設定（gbizinfo_db_schema.py から）
# ---------------------------------------------------------------------------
sys.path.insert(0, str(REPO_ROOT))
from src.master.loaders.gbizinfo_db_schema import (
    META_CONFIGS,
    TABLE_CONFIGS,
    pa_type_to_sqlite,
)
from src.common.db_utils import open_connection, configure_for_bulk_load

# ---------------------------------------------------------------------------
# SQLite 接続・初期設定
# ---------------------------------------------------------------------------

def open_db(db_path: Path) -> sqlite3.Connection:
    conn = open_connection(db_path)
    configure_for_bulk_load(conn)
    return conn


# ---------------------------------------------------------------------------
# DDL 生成
# ---------------------------------------------------------------------------

def build_core_ddl(table: str, parquet_schema, pk: str | None) -> str:
    cols = []
    for i in range(len(parquet_schema)):
        field = parquet_schema.field(i)
        name  = field.name
        if name == "__null_dask_index__":
            continue
        dtype = pa_type_to_sqlite(field.type)
        if name == pk:
            cols.append(f"  {name} {dtype} PRIMARY KEY")
        else:
            cols.append(f"  {name} {dtype}")
    cols.append("  loaded_at TEXT")
    return f"CREATE TABLE IF NOT EXISTS {table} (\n" + ",\n".join(cols) + "\n)"


def build_meta_ddl(table: str) -> str:
    return (
        f"CREATE TABLE IF NOT EXISTS {table} (\n"
        "  corporate_number TEXT,\n"
        "  kanpou_data_name TEXT,\n"
        "  metadata         TEXT,\n"
        "  loaded_at        TEXT\n"
        ")"
    )


# ---------------------------------------------------------------------------
# データ挿入
# ---------------------------------------------------------------------------

def insert_parquet(
    conn: sqlite3.Connection,
    table: str,
    parquet_path: Path,
    pk: str | None,
    loaded_at: str,
) -> int:
    """Parquetファイルを読み込み、テーブルへバッチ挿入する。行数を返す。"""
    pf        = pq.ParquetFile(parquet_path)
    schema    = pf.schema_arrow
    col_names = [
        schema.field(i).name
        for i in range(len(schema))
        if schema.field(i).name != "__null_dask_index__"
    ]
    all_cols  = col_names + ["loaded_at"]
    placeholders = ", ".join("?" * len(all_cols))
    col_list     = ", ".join(all_cols)

    if pk:
        sql = f"INSERT OR REPLACE INTO {table} ({col_list}) VALUES ({placeholders})"
    else:
        sql = f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})"

    total = 0
    for batch in pf.iter_batches(batch_size=BATCH_SIZE):
        df   = batch.to_pydict()
        rows = [
            tuple(df[c][i] for c in col_names) + (loaded_at,)
            for i in range(len(df[col_names[0]]))
        ]
        conn.executemany(sql, rows)
        total += len(rows)

    conn.commit()
    return total


def insert_meta_parquet(
    conn: sqlite3.Connection,
    table: str,
    parquet_path: Path,
    loaded_at: str,
) -> int:
    """メタParquetを読み込み、メタテーブルへ挿入する。"""
    sql = (
        f"INSERT INTO {table} (corporate_number, kanpou_data_name, metadata, loaded_at) "
        "VALUES (?, ?, ?, ?)"
    )
    pf    = pq.ParquetFile(parquet_path)
    total = 0
    for batch in pf.iter_batches(batch_size=BATCH_SIZE):
        d     = batch.to_pydict()
        n     = len(d["corporate_number"])
        # kanpou_data_name は kessanjoho のみ存在する
        kdn   = d.get("kanpou_data_name", [None] * n)
        rows  = [
            (d["corporate_number"][i], kdn[i], d["metadata"][i], loaded_at)
            for i in range(n)
        ]
        conn.executemany(sql, rows)
        total += len(rows)
    conn.commit()
    return total


# ---------------------------------------------------------------------------
# インデックス作成
# ---------------------------------------------------------------------------

def create_indexes(conn: sqlite3.Connection, table: str, indexes: list[str]) -> None:
    for col in indexes:
        idx_name = f"idx_{table}_{col}"
        conn.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table} ({col})")
    conn.commit()


# ---------------------------------------------------------------------------
# 1データセットのロード
# ---------------------------------------------------------------------------

def load_dataset(conn: sqlite3.Connection, key: str, loaded_at: str) -> None:
    cfg      = TABLE_CONFIGS[key]
    meta_cfg = META_CONFIGS[key]
    table    = cfg["table"]
    meta_tbl = meta_cfg["table"]
    pk       = cfg["pk"]
    indexes  = cfg["indexes"]

    core_path = STAGING_DIR / f"{key}_core.parquet"
    meta_path = STAGING_DIR / f"{key}_meta.parquet"

    # ---- コアテーブル ----
    if not core_path.exists():
        log.warning("コアParquetが見つかりません: %s", core_path)
    else:
        core_schema = pq.read_schema(core_path)
        ddl = build_core_ddl(table, core_schema, pk)
        conn.execute(ddl)
        conn.commit()
        log.info("[%s] テーブル作成: %s", key, table)

        t0   = time.time()
        rows = insert_parquet(conn, table, core_path, pk, loaded_at)
        log.info("[%s] 挿入完了: %d行 (%.1fs)", key, rows, time.time() - t0)

        t0 = time.time()
        create_indexes(conn, table, indexes)
        log.info("[%s] インデックス作成完了 (%.1fs)", key, time.time() - t0)

    # ---- メタテーブル ----
    if not meta_path.exists():
        log.warning("メタParquetが見つかりません: %s", meta_path)
    else:
        ddl = build_meta_ddl(meta_tbl)
        conn.execute(ddl)
        conn.commit()

        t0   = time.time()
        rows = insert_meta_parquet(conn, meta_tbl, meta_path, loaded_at)
        log.info("[%s] メタ挿入完了: %d行 (%.1fs)", key, rows, time.time() - t0)

        # メタテーブルにも corporate_number インデックス
        create_indexes(conn, meta_tbl, ["corporate_number"])


# ---------------------------------------------------------------------------
# メイン
# ---------------------------------------------------------------------------

def main() -> None:
    from src.common.logging_setup import setup_logging
    setup_logging()

    keys = sys.argv[1:] if len(sys.argv) > 1 else list(TABLE_CONFIGS.keys())
    invalid = [k for k in keys if k not in TABLE_CONFIGS]
    if invalid:
        log.error("無効なデータセットキー: %s", invalid)
        sys.exit(1)

    loaded_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    log.info("DB: %s", DB_PATH)
    log.info("ロード対象: %s", keys)
    log.info("loaded_at: %s", loaded_at)

    conn = open_db(DB_PATH)
    try:
        total_start = time.time()
        for key in keys:
            log.info("===== %s =====", key)
            load_dataset(conn, key, loaded_at)
        log.info("全完了: %.1fs", time.time() - total_start)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
