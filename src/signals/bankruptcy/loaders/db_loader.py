# -*- coding: utf-8 -*-
"""
倒産情報 DB ローダー

[役割]
  RSS エントリ / 詳細パース結果を bankruptcy.db に保存・更新する。
  重複は INSERT OR IGNORE（RSS）または UPDATE（詳細）で処理する。

[公開関数]
  insert_tsr_rss(conn, entries)          TsrRssEntry リストを tsr_cases に INSERT OR IGNORE
  insert_tdb_rss(conn, entries)          TdbRssEntry リストを tdb_cases に INSERT OR IGNORE
  update_tsr_detail(conn, parsed)        TsrDetailParseResult リストで tsr_cases を UPDATE
  update_tdb_detail(conn, parsed)        TdbDetailParseResult リストで tdb_cases を UPDATE
  log_rss_fetch(conn, source, count, new_count, error)  rss_fetch_log に INSERT
  get_tsr_unscraped(conn)                detail_scraped_at IS NULL の tsr_cases を返す
  get_tdb_unscraped(conn)                detail_scraped_at IS NULL の tdb_cases を返す
"""

import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Optional

logger = logging.getLogger(__name__)

JST = timezone(timedelta(hours=9))


# ---------------------------------------------------------------------------
# RSS INSERT
# ---------------------------------------------------------------------------

def insert_tsr_rss(conn: sqlite3.Connection, entries: list) -> int:
    """TsrRssEntry リストを tsr_cases に INSERT OR IGNORE する。

    Returns:
        新規挿入件数
    """
    sql = """
        INSERT OR IGNORE INTO tsr_cases
            (case_id, source_url, company_name, published_at, rss_fetched_at)
        VALUES (?, ?, ?, ?, ?)
    """
    new_count = 0
    for e in entries:
        cur = conn.execute(sql, (
            e.case_id,
            e.source_url,
            e.company_name,
            e.published_at,
            e.rss_fetched_at,
        ))
        new_count += cur.rowcount
    conn.commit()
    logger.info("tsr_cases INSERT OR IGNORE: %d件中 %d件新規", len(entries), new_count)
    return new_count


def insert_tdb_rss(conn: sqlite3.Connection, entries: list) -> int:
    """TdbRssEntry リストを tdb_cases に INSERT OR IGNORE する。

    Returns:
        新規挿入件数
    """
    sql = """
        INSERT OR IGNORE INTO tdb_cases
            (case_id, source_url, company_name, published_at, rss_fetched_at)
        VALUES (?, ?, ?, ?, ?)
    """
    new_count = 0
    for e in entries:
        cur = conn.execute(sql, (
            e.case_id,
            e.source_url,
            e.company_name,
            e.published_at,
            e.rss_fetched_at,
        ))
        new_count += cur.rowcount
    conn.commit()
    logger.info("tdb_cases INSERT OR IGNORE: %d件中 %d件新規", len(entries), new_count)
    return new_count


# ---------------------------------------------------------------------------
# 詳細ページ UPDATE
# ---------------------------------------------------------------------------

def update_tsr_detail(conn: sqlite3.Connection, parsed: list) -> int:
    """TsrDetailParseResult リストで tsr_cases の詳細フィールドを UPDATE する。

    Returns:
        更新件数
    """
    sql = """
        UPDATE tsr_cases SET
            company_name         = ?,
            published_at         = COALESCE(?, published_at),
            prefecture           = ?,
            industry             = ?,
            business_description = ?,
            bankruptcy_type      = ?,
            liabilities_text     = ?,
            tsr_code             = ?,
            corporate_number     = ?,
            body_capital_text    = ?,
            body_capital_amount  = ?,
            body_address         = ?,
            body_established     = ?,
            body_text            = ?,
            html_path            = ?,
            detail_scraped_at    = ?
        WHERE case_id = ?
    """
    updated = 0
    for p in parsed:
        if not p.success:
            continue
        cur = conn.execute(sql, (
            p.company_name,
            p.published_at,
            p.prefecture,
            p.industry,
            p.business_description,
            p.bankruptcy_type,
            p.liabilities_text,
            p.tsr_code,
            p.corporate_number,
            p.body_capital_text,
            p.body_capital_amount,
            p.body_address,
            p.body_established,
            p.body_text,
            p.html_path,
            p.detail_scraped_at,
            p.case_id,
        ))
        updated += cur.rowcount
    conn.commit()
    logger.info("tsr_cases UPDATE: %d件", updated)
    return updated


def update_tdb_detail(conn: sqlite3.Connection, parsed: list) -> int:
    """TdbDetailParseResult リストで tdb_cases の詳細フィールドを UPDATE する。

    Returns:
        更新件数
    """
    sql = """
        UPDATE tdb_cases SET
            company_name         = ?,
            published_at         = COALESCE(?, published_at),
            tdb_company_code     = ?,
            prefecture           = ?,
            city                 = ?,
            business_description = ?,
            bankruptcy_type      = ?,
            liabilities_text     = ?,
            body_capital_text    = ?,
            body_capital_amount  = ?,
            body_address         = ?,
            body_representative  = ?,
            body_employees       = ?,
            body_text            = ?,
            html_path            = ?,
            detail_scraped_at    = ?
        WHERE case_id = ?
    """
    updated = 0
    for p in parsed:
        if not p.success:
            continue
        cur = conn.execute(sql, (
            p.company_name,
            p.published_at,
            p.tdb_company_code,
            p.prefecture,
            p.city,
            p.business_description,
            p.bankruptcy_type,
            p.liabilities_text,
            p.body_capital_text,
            p.body_capital_amount,
            p.body_address,
            p.body_representative,
            p.body_employees,
            p.body_text,
            p.html_path,
            p.detail_scraped_at,
            p.case_id,
        ))
        updated += cur.rowcount
    conn.commit()
    logger.info("tdb_cases UPDATE: %d件", updated)
    return updated


# ---------------------------------------------------------------------------
# 未スクレイピング取得
# ---------------------------------------------------------------------------

@dataclass
class _CaseStub:
    """scrape() に渡すための軽量スタブ（case_id と source_url のみ）"""
    case_id:    str
    source_url: str


def get_tsr_unscraped(conn: sqlite3.Connection) -> list[_CaseStub]:
    """detail_scraped_at IS NULL の tsr_cases を返す。"""
    rows = conn.execute(
        "SELECT case_id, source_url FROM tsr_cases WHERE detail_scraped_at IS NULL"
    ).fetchall()
    return [_CaseStub(r[0], r[1]) for r in rows]


def get_tdb_unscraped(conn: sqlite3.Connection) -> list[_CaseStub]:
    """detail_scraped_at IS NULL の tdb_cases を返す。"""
    rows = conn.execute(
        "SELECT case_id, source_url FROM tdb_cases WHERE detail_scraped_at IS NULL"
    ).fetchall()
    return [_CaseStub(r[0], r[1]) for r in rows]


# ---------------------------------------------------------------------------
# RSS取得ログ
# ---------------------------------------------------------------------------

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
