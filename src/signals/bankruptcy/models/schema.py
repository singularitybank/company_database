# -*- coding: utf-8 -*-
"""
倒産情報データベーススキーマ定義 / 初期化

テーブル構成:
  tdb_cases          - 帝国データバンク（TDB）倒産案件
  tsr_cases          - 東京商工リサーチ（TSR）倒産案件
  bankruptcy_matches - TDB↔TSR名寄せ結果（法人番号紐付け）
  rss_fetch_log      - RSS収集ログ（重複排除・運用監視用）
"""

import logging
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[5]))
from src.common.db_utils import open_connection

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

DDL_TDB_CASES = """
CREATE TABLE IF NOT EXISTS tdb_cases (
    case_id              TEXT PRIMARY KEY,
    source_url           TEXT NOT NULL,
    company_name         TEXT,
    tdb_company_code     TEXT,
    prefecture           TEXT,
    city                 TEXT,
    business_description TEXT,
    bankruptcy_type      TEXT,
    liabilities_text     TEXT,
    liabilities_amount   INTEGER,
    body_capital_text    TEXT,
    body_capital_amount  INTEGER,
    body_address         TEXT,
    body_representative  TEXT,
    body_employees       INTEGER,
    published_at         TEXT,
    rss_fetched_at       TEXT NOT NULL,
    detail_scraped_at    TEXT,
    html_path            TEXT,
    body_text            TEXT
);
"""

DDL_TSR_CASES = """
CREATE TABLE IF NOT EXISTS tsr_cases (
    case_id              TEXT PRIMARY KEY,
    source_url           TEXT NOT NULL,
    company_name         TEXT,
    corporate_number     TEXT,
    tsr_code             TEXT,
    prefecture           TEXT,
    industry             TEXT,
    business_description TEXT,
    bankruptcy_type      TEXT,
    liabilities_text     TEXT,
    liabilities_amount   INTEGER,
    body_capital_text    TEXT,
    body_capital_amount  INTEGER,
    body_address         TEXT,
    body_established     TEXT,
    published_at         TEXT,
    rss_fetched_at       TEXT NOT NULL,
    detail_scraped_at    TEXT,
    html_path            TEXT,
    body_text            TEXT
);
"""

# tsr_cases に後から追加されたカラム（ALTER TABLE マイグレーション用）
_NEW_TSR_COLUMNS = [
    ("body_capital_text",   "TEXT"),
    ("body_capital_amount", "INTEGER"),
    ("body_address",        "TEXT"),
    ("body_established",    "TEXT"),
]

DDL_BANKRUPTCY_MATCHES = """
CREATE TABLE IF NOT EXISTS bankruptcy_matches (
    match_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    tdb_case_id      TEXT NOT NULL,
    tsr_case_id      TEXT NOT NULL,
    corporate_number TEXT,
    match_score      REAL,
    name_score       REAL,
    address_score    REAL,
    capital_score    REAL,
    rep_score        REAL,
    match_method     TEXT,
    is_confirmed     INTEGER NOT NULL DEFAULT 0,
    matched_at       TEXT NOT NULL,
    FOREIGN KEY (tdb_case_id) REFERENCES tdb_cases (case_id),
    FOREIGN KEY (tsr_case_id) REFERENCES tsr_cases (case_id),
    UNIQUE (tdb_case_id, tsr_case_id)
);
"""

DDL_RSS_FETCH_LOG = """
CREATE TABLE IF NOT EXISTS rss_fetch_log (
    fetch_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    source        TEXT NOT NULL,
    fetched_at    TEXT NOT NULL,
    article_count INTEGER NOT NULL DEFAULT 0,
    new_count     INTEGER NOT NULL DEFAULT 0,
    error         TEXT
);
"""

DDL_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_tdb_published      ON tdb_cases (published_at);",
    "CREATE INDEX IF NOT EXISTS idx_tdb_prefecture     ON tdb_cases (prefecture);",
    "CREATE INDEX IF NOT EXISTS idx_tdb_company_code   ON tdb_cases (tdb_company_code);",
    "CREATE INDEX IF NOT EXISTS idx_tdb_scraped        ON tdb_cases (detail_scraped_at);",
    "CREATE INDEX IF NOT EXISTS idx_tsr_published      ON tsr_cases (published_at);",
    "CREATE INDEX IF NOT EXISTS idx_tsr_prefecture     ON tsr_cases (prefecture);",
    "CREATE INDEX IF NOT EXISTS idx_tsr_corp_num       ON tsr_cases (corporate_number);",
    "CREATE INDEX IF NOT EXISTS idx_tsr_scraped        ON tsr_cases (detail_scraped_at);",
    "CREATE INDEX IF NOT EXISTS idx_match_tdb          ON bankruptcy_matches (tdb_case_id);",
    "CREATE INDEX IF NOT EXISTS idx_match_tsr          ON bankruptcy_matches (tsr_case_id);",
    "CREATE INDEX IF NOT EXISTS idx_match_corp_num     ON bankruptcy_matches (corporate_number);",
    "CREATE INDEX IF NOT EXISTS idx_match_confirmed    ON bankruptcy_matches (is_confirmed);",
    "CREATE INDEX IF NOT EXISTS idx_fetch_log_source   ON rss_fetch_log (source, fetched_at);",
]

# ---------------------------------------------------------------------------
# 初期化
# ---------------------------------------------------------------------------

def init_db(db_path: "str | Path") -> sqlite3.Connection:
    """DBファイルを作成し、全テーブル・インデックスを初期化して接続を返す。

    既にテーブルが存在する場合は何もしない（IF NOT EXISTS）。
    """
    conn = open_connection(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")

    conn.execute(DDL_TDB_CASES)
    conn.execute(DDL_TSR_CASES)
    conn.execute(DDL_BANKRUPTCY_MATCHES)
    conn.execute(DDL_RSS_FETCH_LOG)
    for idx_ddl in DDL_INDEXES:
        conn.execute(idx_ddl)

    _migrate_tsr_cases(conn)

    conn.commit()
    logger.info("DB初期化完了: %s", db_path)
    return conn


def _migrate_tsr_cases(conn: sqlite3.Connection) -> None:
    existing = {row[1] for row in conn.execute("PRAGMA table_info(tsr_cases)")}
    for col_name, col_type in _NEW_TSR_COLUMNS:
        if col_name not in existing:
            conn.execute(f"ALTER TABLE tsr_cases ADD COLUMN {col_name} {col_type}")
            logger.info("マイグレーション: tsr_cases に %s カラム追加", col_name)
