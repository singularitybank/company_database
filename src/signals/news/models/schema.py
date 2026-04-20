# -*- coding: utf-8 -*-
"""
ニュース収集データベーススキーマ定義 / 初期化

テーブル構成:
  news_articles  - 各RSSソースから収集したニュース記事
  rss_fetch_log  - RSS収集ログ（ソースごと・実行ごとに記録）
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

DDL_NEWS_ARTICLES = """
CREATE TABLE IF NOT EXISTS news_articles (
    article_id   TEXT PRIMARY KEY,
    source       TEXT NOT NULL,
    source_url   TEXT NOT NULL,
    title        TEXT NOT NULL,
    published_at TEXT,
    summary      TEXT,
    image_url    TEXT,
    category     TEXT,
    fetched_at   TEXT NOT NULL
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
    "CREATE INDEX IF NOT EXISTS idx_news_source      ON news_articles (source);",
    "CREATE INDEX IF NOT EXISTS idx_news_published   ON news_articles (published_at);",
    "CREATE INDEX IF NOT EXISTS idx_news_category    ON news_articles (category);",
    "CREATE INDEX IF NOT EXISTS idx_fetch_log_source ON rss_fetch_log (source, fetched_at);",
]

# ---------------------------------------------------------------------------
# 初期化
# ---------------------------------------------------------------------------

def init_db(db_path: "str | Path") -> sqlite3.Connection:
    """DBファイルを作成し、全テーブル・インデックスを初期化して接続を返す。"""
    conn = open_connection(db_path)
    conn.execute(DDL_NEWS_ARTICLES)
    conn.execute(DDL_RSS_FETCH_LOG)
    for idx_ddl in DDL_INDEXES:
        conn.execute(idx_ddl)
    conn.commit()
    logger.info("DB初期化完了: %s", db_path)
    return conn
