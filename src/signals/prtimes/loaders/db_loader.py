# -*- coding: utf-8 -*-
"""
PR Times DB ローダー

[提供する関数]
  load()            : ArticleRecord リストを prtimes_articles に INSERT（RSS 収集後）
  update_articles() : ArticleParseResult リストで prtimes_articles を UPDATE（記事スクレイピング後）

[処理方式]
  - prtimes_articles INSERT : INSERT OR IGNORE（article_id PK による重複スキップ）
  - prtimes_articles UPDATE : html_path / image_url / body_text / published_at / article_scraped_at
  - prtimes_companies       : 未登録の company_id は最小レコードで先行登録
  - rss_fetch_log           : 実行ごとに取得件数・新規件数を記録
"""

import logging
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from src.config import prtimes as _cfg
from src.signals.prtimes.models.schema import init_db
from src.signals.prtimes.parsers.rss_parser import ArticleRecord
from src.signals.prtimes.parsers.article_parser import ArticleParseResult
from src.signals.prtimes.parsers.company_parser import CompanyParseResult

logger = logging.getLogger(__name__)

JST = timezone(timedelta(hours=9))

# ---------------------------------------------------------------------------
# 定数
# ---------------------------------------------------------------------------

DB_PATH = REPO_ROOT / _cfg["db_path"]

# ---------------------------------------------------------------------------
# 結果データクラス
# ---------------------------------------------------------------------------

@dataclass
class LoadResult:
    total:     int  # 入力件数
    new_count: int  # 新規 INSERT 件数
    skipped:   int  # 重複スキップ件数
    error:     Optional[str] = None


# ---------------------------------------------------------------------------
# 内部ヘルパー
# ---------------------------------------------------------------------------

def _ensure_companies(conn: sqlite3.Connection, company_ids: set[int]) -> int:
    """未登録の prtimes_company_id を companies テーブルに先行登録する。

    企業ページのスクレイピングが完了するまでの FK 制約を満たすための
    最小レコード（company_name は暫定値 "未取得"）を INSERT OR IGNORE する。

    Returns:
        新規登録件数
    """
    if not company_ids:
        return 0

    now = datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")
    sql = """
        INSERT OR IGNORE INTO prtimes_companies
            (prtimes_company_id, company_name, updated_at)
        VALUES (?, ?, ?)
    """
    params = [(cid, "未取得", now) for cid in company_ids]
    cursor = conn.cursor()
    cursor.executemany(sql, params)
    return cursor.rowcount  # 実際に挿入された行数（IGNORE した分は含まない）


def _insert_articles(
    conn: sqlite3.Connection,
    records: list[ArticleRecord],
) -> tuple[int, int]:
    """ArticleRecord リストを prtimes_articles に一括 INSERT する。

    Returns:
        (new_count, skipped) のタプル
    """
    sql = """
        INSERT OR IGNORE INTO prtimes_articles (
            article_id, prtimes_company_id, pr_number,
            title, article_url, company_name_rss, business_form,
            published_at, summary, image_url,
            source, fetched_at, html_path, body_text, article_scraped_at
        ) VALUES (
            :article_id, :prtimes_company_id, :pr_number,
            :title, :article_url, :company_name_rss, :business_form,
            :published_at, :summary, :image_url,
            :source, :fetched_at, :html_path, :body_text, :article_scraped_at
        )
    """
    cursor = conn.cursor()
    before = _count_articles(conn)
    cursor.executemany(sql, [r.to_dict() for r in records])
    after = _count_articles(conn)

    new_count = after - before
    skipped   = len(records) - new_count
    return new_count, skipped


def _count_articles(conn: sqlite3.Connection) -> int:
    return conn.execute("SELECT COUNT(*) FROM prtimes_articles").fetchone()[0]


def _write_fetch_log(
    conn: sqlite3.Connection,
    fetched_at: str,
    article_count: int,
    new_count: int,
    error: Optional[str],
) -> None:
    conn.execute(
        """
        INSERT INTO rss_fetch_log (fetched_at, article_count, new_count, error)
        VALUES (?, ?, ?, ?)
        """,
        (fetched_at, article_count, new_count, error),
    )


# ---------------------------------------------------------------------------
# メイン処理
# ---------------------------------------------------------------------------

def load(
    records: list[ArticleRecord],
    db_path: "str | Path" = DB_PATH,
) -> LoadResult:
    """ArticleRecord リストを prtimes.db に保存する。

    Args:
        records: prtimes_rss_parser.parse() の戻り値
        db_path: 保存先 SQLite ファイルパス（省略時は config の値）

    Returns:
        LoadResult
    """
    db_path = Path(db_path)
    conn = init_db(db_path)
    fetched_at = records[0].fetched_at if records else datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")
    error_msg: Optional[str] = None

    try:
        # 1. 未登録 company_id を先行登録
        company_ids = {
            r.prtimes_company_id
            for r in records
            if r.prtimes_company_id is not None
        }
        new_companies = _ensure_companies(conn, company_ids)
        if new_companies:
            logger.info("新規企業登録（暫定）: %d 件", new_companies)

        # 2. 記事を INSERT OR IGNORE
        new_count, skipped = _insert_articles(conn, records)

        conn.commit()
        logger.info(
            "DB保存完了: 入力=%d件, 新規=%d件, スキップ=%d件",
            len(records), new_count, skipped,
        )

    except Exception as e:
        conn.rollback()
        error_msg = str(e)
        logger.error("DB保存エラー: %s", e)
        new_count, skipped = 0, 0

    finally:
        # 3. fetch ログを記録（エラー時も残す）
        try:
            _write_fetch_log(conn, fetched_at, len(records), new_count, error_msg)
            conn.commit()
        except Exception as log_err:
            logger.warning("fetch_log 書き込み失敗: %s", log_err)
        conn.close()

    return LoadResult(
        total     = len(records),
        new_count = new_count,
        skipped   = skipped,
        error     = error_msg,
    )


# ---------------------------------------------------------------------------
# 記事スクレイピング結果の UPDATE
# ---------------------------------------------------------------------------

def update_articles(
    parse_results: list[ArticleParseResult],
    db_path: "str | Path" = DB_PATH,
) -> tuple[int, int]:
    """ArticleParseResult リストで prtimes_articles を UPDATE する。

    成功レコードのみ更新する。published_at は記事ページ値（秒精度）で上書きする。

    Args:
        parse_results: prtimes_article_parser.parse() の戻り値
        db_path:       SQLite ファイルパス

    Returns:
        (updated_count, error_count) のタプル
    """
    db_path = Path(db_path)
    conn = sqlite3.connect(str(db_path))

    sql = """
        UPDATE prtimes_articles
        SET
            html_path          = :html_path,
            image_url          = :image_url,
            published_at       = COALESCE(:published_at, published_at),
            body_text          = :body_text,
            article_scraped_at = :article_scraped_at
        WHERE article_id = :article_id
    """

    updated = error_count = 0
    try:
        for r in parse_results:
            if not r.success:
                error_count += 1
                continue
            conn.execute(sql, {
                "html_path":          r.html_path,
                "image_url":          r.image_url,
                "published_at":       r.published_at,
                "body_text":          r.body_text,
                "article_scraped_at": r.article_scraped_at,
                "article_id":         r.article_id,
            })
            updated += 1
        conn.commit()
        logger.info("記事 UPDATE 完了: 更新=%d件, エラー=%d件", updated, error_count)
    except Exception as e:
        conn.rollback()
        logger.error("記事 UPDATE エラー: %s", e)
    finally:
        conn.close()

    return updated, error_count


# ---------------------------------------------------------------------------
# 企業スクレイピング結果の UPDATE
# ---------------------------------------------------------------------------

def update_companies(
    parse_results: list[CompanyParseResult],
    db_path: "str | Path" = DB_PATH,
) -> tuple[int, int]:
    """CompanyParseResult リストで prtimes_companies を UPDATE する。

    Args:
        parse_results: prtimes_company_parser.parse() の戻り値
        db_path:       SQLite ファイルパス

    Returns:
        (updated_count, error_count) のタプル
    """
    db_path = Path(db_path)
    conn = sqlite3.connect(str(db_path))

    sql = """
        UPDATE prtimes_companies
        SET
            company_name        = :company_name,
            industry            = :industry,
            prefecture          = :prefecture,
            address             = :address,
            phone_number        = :phone_number,
            representative      = :representative,
            listed              = :listed,
            capital             = :capital,
            established         = :established,
            company_description = :company_description,
            website_url         = :website_url,
            x_url               = :x_url,
            facebook_url        = :facebook_url,
            youtube_url         = :youtube_url,
            scraped_at          = :scraped_at,
            updated_at          = :updated_at
        WHERE prtimes_company_id = :prtimes_company_id
    """

    now = datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")
    updated = error_count = 0
    try:
        for r in parse_results:
            if not r.success:
                error_count += 1
                continue
            conn.execute(sql, {
                "prtimes_company_id": r.prtimes_company_id,
                "company_name":       r.company_name,
                "industry":           r.industry,
                "prefecture":         r.prefecture,
                "address":            r.address,
                "phone_number":       r.phone_number,
                "representative":     r.representative,
                "listed":             r.listed,
                "capital":            r.capital,
                "established":         r.established,
                "company_description": r.company_description,
                "website_url":         r.website_url,
                "x_url":              r.x_url,
                "facebook_url":       r.facebook_url,
                "youtube_url":        r.youtube_url,
                "scraped_at":         now,
                "updated_at":         now,
            })
            updated += 1
        conn.commit()
        logger.info("企業 UPDATE 完了: 更新=%d件, エラー=%d件", updated, error_count)
    except Exception as e:
        conn.rollback()
        logger.error("企業 UPDATE エラー: %s", e)
    finally:
        conn.close()

    return updated, error_count
