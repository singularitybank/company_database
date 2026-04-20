# -*- coding: utf-8 -*-
"""
Google News収集 DB ローダー

[公開関数]
  add_keyword(conn, keyword)                         キーワードを追加する
  disable_keyword(conn, keyword)                     キーワードを無効化する
  get_active_keywords(conn)                          activeなキーワード一覧を返す
  insert_articles(conn, entries)                     GoogleNewsEntry リストを INSERT OR IGNORE
  log_fetch(conn, keyword, article_count, ...)       google_news_fetch_log に INSERT
"""

import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Optional

logger = logging.getLogger(__name__)

JST = timezone(timedelta(hours=9))


# ---------------------------------------------------------------------------
# キーワード管理
# ---------------------------------------------------------------------------

@dataclass
class KeywordRow:
    keyword_id: int
    keyword:    str


def add_keyword(conn: sqlite3.Connection, keyword: str) -> bool:
    """キーワードを google_news_keywords に追加する。

    Returns:
        True: 新規追加, False: 既に存在
    """
    now = datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")
    cur = conn.execute(
        "INSERT OR IGNORE INTO google_news_keywords (keyword, is_active, created_at, updated_at) VALUES (?, 1, ?, ?)",
        (keyword, now, now),
    )
    conn.commit()
    if cur.rowcount > 0:
        logger.info("キーワード追加: 「%s」", keyword)
        return True
    # 既存キーワードを再アクティブ化
    conn.execute(
        "UPDATE google_news_keywords SET is_active = 1, updated_at = ? WHERE keyword = ?",
        (now, keyword),
    )
    conn.commit()
    logger.info("キーワード再アクティブ化: 「%s」", keyword)
    return False


def disable_keyword(conn: sqlite3.Connection, keyword: str) -> bool:
    """キーワードを無効化する（is_active = 0）。

    Returns:
        True: 無効化成功, False: 対象キーワードなし
    """
    now = datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")
    cur = conn.execute(
        "UPDATE google_news_keywords SET is_active = 0, updated_at = ? WHERE keyword = ?",
        (now, keyword),
    )
    conn.commit()
    if cur.rowcount > 0:
        logger.info("キーワード無効化: 「%s」", keyword)
        return True
    logger.warning("キーワードが見つかりません: 「%s」", keyword)
    return False


def get_active_keywords(conn: sqlite3.Connection) -> list[KeywordRow]:
    """is_active = 1 のキーワード一覧を返す。"""
    rows = conn.execute(
        "SELECT keyword_id, keyword FROM google_news_keywords WHERE is_active = 1 ORDER BY keyword_id"
    ).fetchall()
    return [KeywordRow(r[0], r[1]) for r in rows]


# ---------------------------------------------------------------------------
# 記事保存
# ---------------------------------------------------------------------------

def insert_articles(conn: sqlite3.Connection, entries: list) -> int:
    """GoogleNewsEntry リストを google_news_articles に INSERT OR IGNORE する。

    Returns:
        新規挿入件数
    """
    sql = """
        INSERT OR IGNORE INTO google_news_articles
            (article_id, keyword_id, keyword, title, source_name,
             link, published_at, summary, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    new_count = 0
    for e in entries:
        cur = conn.execute(sql, (
            e.article_id,
            e.keyword_id,
            e.keyword,
            e.title,
            e.source_name,
            e.link,
            e.published_at,
            e.summary,
            e.fetched_at,
        ))
        new_count += cur.rowcount
    conn.commit()
    logger.info("google_news_articles INSERT OR IGNORE: %d件中 %d件新規", len(entries), new_count)
    return new_count


# ---------------------------------------------------------------------------
# 収集ログ
# ---------------------------------------------------------------------------

def log_fetch(
    conn: sqlite3.Connection,
    keyword: str,
    article_count: int,
    new_count: int,
    error: Optional[str] = None,
) -> None:
    """google_news_fetch_log にレコードを挿入する。"""
    fetched_at = datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")
    conn.execute(
        "INSERT INTO google_news_fetch_log (keyword, fetched_at, article_count, new_count, error) VALUES (?, ?, ?, ?, ?)",
        (keyword, fetched_at, article_count, new_count, error),
    )
    conn.commit()
