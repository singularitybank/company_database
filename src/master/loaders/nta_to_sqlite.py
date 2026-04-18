# -*- coding: utf-8 -*-
"""
国税庁 法人番号データ Parquet → SQLite ローダー

[入力]  data/staging/nta_*.parquet
[出力]  data/companies.db（companies テーブルへの全件置換）

処理方式: フルリフレッシュ（既存データを全削除してから再投入）
          差分更新は diff_processor モジュールで別途行う。

[使い方（単体実行）]
  python src/loaders/nta_to_sqlite.py
  python src/loaders/nta_to_sqlite.py --parquet path/to/file.parquet --db data/companies.db
"""
import argparse
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from src.models.schema import COLUMN_MAP, init_db
from src.utils.db_utils import CACHE_64MB

# ---------------------------------------------------------------------------
# 定数
# ---------------------------------------------------------------------------

CHUNK_SIZE = 50_000  # バッチ投入サイズ（行数）

# ---------------------------------------------------------------------------
# ロガー
# ---------------------------------------------------------------------------

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DB 投入
# ---------------------------------------------------------------------------

def load_to_db(
    staging_path: "str | Path",
    db_path: "str | Path",
    chunk_size: int = CHUNK_SIZE,
) -> int:
    """Staging の Parquet を SQLite companies テーブルに一括投入する。

    既存テーブルは全件置き換える（フルリフレッシュ）。

    Args:
        staging_path: 入力 Parquet ファイルパス
        db_path:      SQLite ファイルパス
        chunk_size:   バッチ投入サイズ（行数）

    Returns:
        投入した行数
    """
    staging_path = Path(staging_path)
    db_path = Path(db_path)

    logger.info("DB投入開始: %s → %s", staging_path.name, db_path.name)

    conn = init_db(db_path)

    # パフォーマンス設定（バルクロード用: 全件置換のため安全性より速度を優先）
    conn.execute("PRAGMA synchronous = OFF;")
    conn.execute(f"PRAGMA cache_size = {CACHE_64MB};")

    df = pd.read_parquet(staging_path, engine="pyarrow")

    # カラム名をDB形式に変換（Parquetに存在するカラムのみ選択）
    available = [k for k in COLUMN_MAP if k in df.columns]
    df = df[available].rename(columns={k: COLUMN_MAP[k] for k in available})

    # NaN → None（SQLite の NULL として扱う）
    df = df.where(df.notna(), other=None)

    # 投入タイムスタンプ付与
    loaded_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    df["loaded_at"] = loaded_at

    # 既存データを全削除してから投入（フルリフレッシュ）
    conn.execute("DELETE FROM companies;")
    conn.commit()

    cols = df.columns.tolist()
    placeholders = ", ".join(["?"] * len(cols))
    insert_sql = f"INSERT INTO companies ({', '.join(cols)}) VALUES ({placeholders})"

    total = 0
    cursor = conn.cursor()
    for start in range(0, len(df), chunk_size):
        batch = df.iloc[start : start + chunk_size]
        cursor.executemany(insert_sql, batch.itertuples(index=False, name=None))
        conn.commit()
        total += len(batch)
        logger.info("  投入済み: %d / %d行", total, len(df))

    # インデックス再構築
    conn.execute("ANALYZE;")
    conn.commit()

    # 同期設定を元に戻す
    conn.execute("PRAGMA synchronous = NORMAL;")
    conn.commit()
    conn.close()

    logger.info("DB投入完了: %d行", total)
    return total


# ---------------------------------------------------------------------------
# エントリーポイント（Parquet → SQLite 投入のみ）
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    from src.logging_setup import setup_logging
    setup_logging()

    parser = argparse.ArgumentParser(description="国税庁 Parquet → SQLite 投入")
    parser.add_argument("--parquet", dest="parquet_path", default=None,
                        help="入力Parquetファイルパス。省略時は data/staging/ の最新 nta_*.parquet を使用")
    parser.add_argument("--db", default=str(REPO_ROOT / "data" / "companies.db"),
                        help="SQLiteファイルパス")
    args = parser.parse_args()

    if args.parquet_path:
        parquet_path = Path(args.parquet_path)
    else:
        staging_dir = REPO_ROOT / "data" / "staging"
        parquet_files = sorted(staging_dir.glob("nta_*.parquet"))
        if not parquet_files:
            logger.error("stagingにParquetファイルが見つかりません: %s", staging_dir)
            raise SystemExit(1)
        parquet_path = parquet_files[-1]
        logger.info("対象ファイル: %s", parquet_path.name)

    load_to_db(parquet_path, args.db)
