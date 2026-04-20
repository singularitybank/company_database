# -*- coding: utf-8 -*-
"""
ニュース収集 DB ローダー

[公開関数]
  insert_articles(conn, entries)                     NewsRssEntry リストを news_articles に INSERT OR IGNORE
  log_rss_fetch(conn, source, article_count, ...)    rss_fetch_log に INSERT
"""

import logging
import sqlite3
from datetime import datetime, timezone, timedelta
from typing import Optional

logger = logging.getLogger(__name__)

JST = timezone(timedelta(hours=9))


def insert_articles(conn: sqlite3.Connection, entries: list) -> int:
    """NewsRssEntry リストを news_articles に INSERT OR IGNORE する。

    Returns:
        新規挿入件数
    """
    sql = """
        INSERT OR IGNORE INTO news_articles
            (article_id, source, source_url, title, published_at,
             summary, image_url, category, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    new_count = 0
    for e in entries:
        cur = conn.execute(sql, (
            e.article_id,
            e.source,
            e.source_url,
            e.title,
            e.published_at,
            e.summary,
            e.image_url,
            e.category,
            e.fetched_at,
        ))
        new_count += cur.rowcount
    conn.commit()
    logger.info("news_articles INSERT OR IGNORE: %d件中 %d件新規", len(entries), new_count)
    return new_count


def log_rss_fetch(
    conn: sqlite3.Connection,
    source: str,
    article_count: int,
    new_count: int,
    error: Optional[str] = None,
) -> None:
    """rss_fetch_log にレコードを挿入する。"""
    fetched_at = datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")
    conn.execute(
        "INSERT INTO rss_fetch_log (source, fetched_at, article_count, new_count, error) VALUES (?, ?, ?, ?, ?)",
        (source, fetched_at, article_count, new_count, error),
    )
    conn.commit()
