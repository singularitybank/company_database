# -*- coding: utf-8 -*-
"""
Google Newsキーワード収集データベーススキーマ定義 / 初期化

テーブル構成:
  google_news_keywords  - 収集対象キーワード（追加・無効化で管理）
  google_news_articles  - キーワードごとに収集した記事
  google_news_fetch_log - キーワードごとのRSS収集ログ
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

DDL_KEYWORDS = """
CREATE TABLE IF NOT EXISTS google_news_keywords (
    keyword_id INTEGER PRIMARY KEY AUTOINCREMENT,
    keyword    TEXT NOT NULL UNIQUE,
    is_active  INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
"""

DDL_ARTICLES = """
CREATE TABLE IF NOT EXISTS google_news_articles (
    article_id   TEXT PRIMARY KEY,
    keyword_id   INTEGER NOT NULL,
    keyword      TEXT NOT NULL,
    title        TEXT NOT NULL,
    source_name  TEXT,
    link         TEXT NOT NULL,
    published_at TEXT,
    summary      TEXT,
    fetched_at   TEXT NOT NULL,
    FOREIGN KEY (keyword_id) REFERENCES google_news_keywords (keyword_id)
);
"""

DDL_FETCH_LOG = """
CREATE TABLE IF NOT EXISTS google_news_fetch_log (
    fetch_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    keyword       TEXT NOT NULL,
    fetched_at    TEXT NOT NULL,
    article_count INTEGER NOT NULL DEFAULT 0,
    new_count     INTEGER NOT NULL DEFAULT 0,
    error         TEXT
);
"""

DDL_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_gnews_keyword    ON google_news_articles (keyword);",
    "CREATE INDEX IF NOT EXISTS idx_gnews_published  ON google_news_articles (published_at);",
    "CREATE INDEX IF NOT EXISTS idx_gnews_source     ON google_news_articles (source_name);",
    "CREATE INDEX IF NOT EXISTS idx_gnews_log        ON google_news_fetch_log (keyword, fetched_at);",
]

# ---------------------------------------------------------------------------
# 初期化
# ---------------------------------------------------------------------------

def init_db(db_path: "str | Path") -> sqlite3.Connection:
    """DBファイルを作成し、全テーブル・インデックスを初期化して接続を返す。"""
    conn = open_connection(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute(DDL_KEYWORDS)
    conn.execute(DDL_ARTICLES)
    conn.execute(DDL_FETCH_LOG)
    for idx_ddl in DDL_INDEXES:
        conn.execute(idx_ddl)
    conn.commit()
    logger.info("DB初期化完了: %s", db_path)
    return conn
