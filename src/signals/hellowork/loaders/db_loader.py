# -*- coding: utf-8 -*-
"""
ハローワーク求人 Parquet → SQLite ローダー

[公開関数]
  load_parquet(conn, parquet_path) - Parquetファイルの内容を job_postings テーブルへ投入する
"""

import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

_BATCH_SIZE = 50_000


def load_parquet(conn: sqlite3.Connection, parquet_path: "str | Path") -> int:
    """Parquetファイルを読み込み job_postings テーブルへ INSERT OR REPLACE する。

    同一 job_number が既に存在する場合は最新内容で上書きする。

    Args:
        conn:         初期化済みの sqlite3.Connection
        parquet_path: 読み込む Parquet ファイルパス

    Returns:
        投入した行数
    """
    parquet_path = Path(parquet_path)
    df = pd.read_parquet(parquet_path)

    if df.empty:
        logger.warning("Parquetが空です: %s", parquet_path.name)
        return 0

    loaded_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    df["loaded_at"] = loaded_at

    # Parquet の NaN を None（SQL NULL）に変換
    df = df.where(df.notna(), other=None)

    cols = list(df.columns)
    placeholders = ", ".join(["?"] * len(cols))
    cols_str = ", ".join(cols)
    sql = f"INSERT OR REPLACE INTO job_postings ({cols_str}) VALUES ({placeholders})"

    total = 0
    for start in range(0, len(df), _BATCH_SIZE):
        batch = df.iloc[start : start + _BATCH_SIZE]
        rows = [tuple(r) for r in batch.itertuples(index=False, name=None)]
        conn.executemany(sql, rows)
        conn.commit()
        total += len(rows)
        logger.debug("  投入済み: %d / %d 件", total, len(df))

    logger.info("Parquet投入完了: %s → %d件", parquet_path.name, total)
    return total
