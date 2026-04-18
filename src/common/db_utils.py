# -*- coding: utf-8 -*-
"""
SQLite 接続・設定ユーティリティ

[提供する定数]
  CACHE_64MB   - SQLite cache_size PRAGMA 値（64 MB）
  CACHE_128MB  - SQLite cache_size PRAGMA 値（128 MB）
  MMAP_1GB     - SQLite mmap_size PRAGMA 値（1 GB）

[提供する関数]
  open_connection         - WAL モードで SQLite に接続する（共通ベース）
  configure_for_bulk_load - バルクロード用の高速化 PRAGMA を適用する
"""
import sqlite3
from pathlib import Path

# ---------------------------------------------------------------------------
# SQLite PRAGMA 値定数
# ---------------------------------------------------------------------------

# cache_size に負値を指定すると KiB 単位になる（SQLite 仕様）
CACHE_64MB:  int = -65_536       # -65536 KiB = 64 MB
CACHE_128MB: int = -131_072      # -131072 KiB = 128 MB
MMAP_1GB:    int = 1_073_741_824 # 1 GB（バイト単位）

# ---------------------------------------------------------------------------
# 接続ヘルパー
# ---------------------------------------------------------------------------

def open_connection(db_path: "str | Path") -> sqlite3.Connection:
    """SQLiteデータベースに接続し、WAL モードを設定して返す。

    親ディレクトリが存在しない場合は自動で作成する。

    Args:
        db_path: SQLite ファイルパス

    Returns:
        WAL モードが有効な sqlite3.Connection
    """
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


def configure_for_bulk_load(conn: sqlite3.Connection) -> None:
    """バルクロード用の SQLite パフォーマンス設定を適用する。

    大量データ投入時の速度を最大化するための設定。
    処理終了後に接続を閉じることで設定は自動的にリセットされる。

    適用設定:
      synchronous = NORMAL  : WAL モードでの安全な高速化
      cache_size  = 128 MB  : 読み書きキャッシュの拡大
      temp_store  = MEMORY  : 一時テーブルをメモリ上に展開
      mmap_size   = 1 GB    : メモリマップ I/O で読み取りを高速化

    Args:
        conn: 設定を適用する sqlite3.Connection
    """
    conn.execute("PRAGMA synchronous  = NORMAL")
    conn.execute(f"PRAGMA cache_size   = {CACHE_128MB}")
    conn.execute("PRAGMA temp_store   = MEMORY")
    conn.execute(f"PRAGMA mmap_size    = {MMAP_1GB}")
