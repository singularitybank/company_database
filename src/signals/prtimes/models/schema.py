# -*- coding: utf-8 -*-
"""
PR Times データベーススキーマ定義 / 初期化

テーブル構成:
  prtimes_companies  - PR Times企業マスタ（企業ページスクレイピング由来）
  prtimes_articles   - プレスリリース記事（RSS + 記事スクレイピング由来）
  rss_fetch_log      - RSS収集ログ（重複排除・運用監視用）
"""

import logging
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.common.db_utils import open_connection

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

DDL_PRTIMES_COMPANIES = """
CREATE TABLE IF NOT EXISTS prtimes_companies (
    prtimes_company_id   INTEGER PRIMARY KEY,
    company_name         TEXT NOT NULL,
    company_name_kana    TEXT,
    industry             TEXT,
    prefecture           TEXT,
    address              TEXT,
    phone_number         TEXT,
    representative       TEXT,
    listed               TEXT,
    capital              TEXT,
    established          TEXT,
    company_description  TEXT,
    website_url          TEXT,
    x_url                TEXT,
    facebook_url         TEXT,
    youtube_url          TEXT,
    press_release_count  INTEGER,
    corporate_number     TEXT,
    name_match_score     REAL,
    scraped_at           TEXT,
    updated_at           TEXT NOT NULL
);
"""

DDL_PRTIMES_ARTICLES = """
CREATE TABLE IF NOT EXISTS prtimes_articles (
    article_id           TEXT PRIMARY KEY,
    prtimes_company_id   INTEGER,
    pr_number            INTEGER,
    title                TEXT NOT NULL,
    article_url          TEXT NOT NULL,
    company_name_rss     TEXT,
    business_form        TEXT,
    published_at         TEXT,
    summary              TEXT,
    image_url            TEXT,
    source               TEXT NOT NULL,
    fetched_at           TEXT NOT NULL,
    html_path            TEXT,
    body_text            TEXT,
    article_scraped_at   TEXT,
    FOREIGN KEY (prtimes_company_id) REFERENCES prtimes_companies (prtimes_company_id)
);
"""

DDL_RSS_FETCH_LOG = """
CREATE TABLE IF NOT EXISTS rss_fetch_log (
    fetch_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    fetched_at     TEXT NOT NULL,
    article_count  INTEGER NOT NULL DEFAULT 0,
    new_count      INTEGER NOT NULL DEFAULT 0,
    error          TEXT
);
"""

DDL_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_articles_company    ON prtimes_articles (prtimes_company_id);",
    "CREATE INDEX IF NOT EXISTS idx_articles_published  ON prtimes_articles (published_at);",
    "CREATE INDEX IF NOT EXISTS idx_articles_fetched    ON prtimes_articles (fetched_at);",
    "CREATE INDEX IF NOT EXISTS idx_articles_scraped    ON prtimes_articles (article_scraped_at);",
    "CREATE INDEX IF NOT EXISTS idx_companies_corp_num  ON prtimes_companies (corporate_number);",
    "CREATE INDEX IF NOT EXISTS idx_fetch_log_fetched   ON rss_fetch_log (fetched_at);",
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

    conn.execute(DDL_PRTIMES_COMPANIES)
    conn.execute(DDL_PRTIMES_ARTICLES)
    conn.execute(DDL_RSS_FETCH_LOG)
    for idx_ddl in DDL_INDEXES:
        conn.execute(idx_ddl)

    _migrate_companies(conn)

    conn.commit()
    logger.info("DB初期化完了: %s", db_path)
    return conn


_NEW_COMPANY_COLUMNS = [
    ("phone_number",       "TEXT"),
    ("representative",     "TEXT"),
    ("listed",             "TEXT"),
    ("capital",            "TEXT"),
    ("established",        "TEXT"),
    ("company_description","TEXT"),
    ("x_url",              "TEXT"),
    ("facebook_url",       "TEXT"),
    ("youtube_url",        "TEXT"),
]


def _migrate_companies(conn: sqlite3.Connection) -> None:
    existing = {row[1] for row in conn.execute("PRAGMA table_info(prtimes_companies)")}
    for col_name, col_type in _NEW_COMPANY_COLUMNS:
        if col_name not in existing:
            conn.execute(f"ALTER TABLE prtimes_companies ADD COLUMN {col_name} {col_type}")
            logger.info("マイグレーション: prtimes_companies に %s カラム追加", col_name)
