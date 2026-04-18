# -*- coding: utf-8 -*-
"""
PR Times 記事ページ HTML クローラー

prtimes.db の prtimes_articles テーブルから html_path が未設定の記事を取得し、
記事ページの HTML をダウンロードしてローカルに保存する。

[保存先]
  C:/Temp/html/prtimes/articles/{YYYYMMDD}/{article_id}.html
  （YYYYMMDD はダウンロード実行日）

[設計方針]
  - requests + BeautifulSoup（記事ページは SSR のため Selenium 不要）
  - 再開対応: 既に html_path が設定済みの記事はスキップ
  - レート制限: wait_between_articles + random jitter
  - 失敗時は html_path を更新せず次の記事へ（次回バッチで再試行）
"""

import logging
import random
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

import requests

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.config import prtimes as _cfg

logger = logging.getLogger(__name__)

JST            = timezone(timedelta(hours=9))
TIMEOUT        = _cfg["timeout"]
RETRY_COUNT    = _cfg["retry_count"]
WAIT_ARTICLES  = _cfg["wait_between_articles"]
HTML_BASE_DIR  = Path(_cfg["html_dir"]) / "articles"
_USER_AGENT    = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

# ---------------------------------------------------------------------------
# データクラス
# ---------------------------------------------------------------------------

@dataclass
class ArticleHtmlResult:
    """記事 HTML 取得結果 1 件分"""
    article_id:  str
    article_url: str
    html_path:   str           # 保存先ファイルパス（文字列）
    success:     bool
    error:       Optional[str] = None


# ---------------------------------------------------------------------------
# 内部ヘルパー
# ---------------------------------------------------------------------------

def _fetch_html(url: str) -> Optional[bytes]:
    """記事ページの HTML を raw bytes で取得する。"""
    headers = {"User-Agent": _USER_AGENT}
    for attempt in range(1, RETRY_COUNT + 1):
        try:
            resp = requests.get(url, headers=headers, timeout=TIMEOUT)
            resp.raise_for_status()
            return resp.content
        except requests.RequestException as e:
            logger.warning("記事取得エラー (試行 %d/%d) %s: %s", attempt, RETRY_COUNT, url, e)
            if attempt < RETRY_COUNT:
                time.sleep(2 ** attempt + random.uniform(0, 1))
    return None


def _save_html(html: bytes, path: Path) -> None:
    """HTML バイト列をファイルに保存する。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(html)


def _mark_fetch_error(db_path: "str | Path", article_id: str) -> None:
    """取得失敗記事の html_path を 'FETCH_ERROR' に更新して再試行を防ぐ。"""
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            "UPDATE prtimes_articles SET html_path = 'FETCH_ERROR' WHERE article_id = ?",
            (article_id,),
        )
        conn.commit()
    finally:
        conn.close()


def _load_unscraped(
    db_path: "str | Path",
    limit: Optional[int] = None,
) -> list[tuple[str, str]]:
    """html_path が未設定の記事を DB から取得する。

    Returns:
        (article_id, article_url) のリスト
    """
    sql = "SELECT article_id, article_url FROM prtimes_articles WHERE html_path IS NULL"
    params: list = []
    if limit is not None:
        sql += " LIMIT ?"
        params.append(limit)

    conn = sqlite3.connect(str(db_path))
    try:
        rows = conn.execute(sql, params).fetchall()
    finally:
        conn.close()
    return rows


# ---------------------------------------------------------------------------
# メイン処理
# ---------------------------------------------------------------------------

def scrape_articles(
    db_path: "str | Path",
    limit: Optional[int] = None,
    mark_errors: bool = True,
) -> list[ArticleHtmlResult]:
    """未スクレイプ記事の HTML をダウンロードして保存する。

    Args:
        db_path: prtimes.db のパス
        limit:   最大取得件数（None で全件）

    Returns:
        ArticleHtmlResult のリスト
    """
    date_str  = datetime.now(JST).strftime("%Y%m%d")
    html_dir  = HTML_BASE_DIR / date_str

    rows = _load_unscraped(db_path, limit=limit)
    if not rows:
        logger.info("未スクレイプ記事なし")
        return []

    logger.info("記事 HTML 取得開始: %d 件 → %s", len(rows), html_dir)

    results: list[ArticleHtmlResult] = []
    saved = skipped = errors = 0

    for i, (article_id, url) in enumerate(rows, 1):
        save_path = html_dir / f"{article_id}.html"

        # ファイルが既に存在する場合はスキップ（手動再実行時の重複防止）
        if save_path.exists():
            logger.debug("[%d/%d] スキップ（ファイル存在）: %s", i, len(rows), article_id)
            results.append(ArticleHtmlResult(
                article_id  = article_id,
                article_url = url,
                html_path   = str(save_path),
                success     = True,
            ))
            skipped += 1
            continue

        html = _fetch_html(url)
        if html is None:
            logger.warning("[%d/%d] 取得失敗（スキップ）: %s", i, len(rows), url)
            if mark_errors:
                _mark_fetch_error(db_path, article_id)
            results.append(ArticleHtmlResult(
                article_id  = article_id,
                article_url = url,
                html_path   = "",
                success     = False,
                error       = "fetch_failed",
            ))
            errors += 1
        else:
            _save_html(html, save_path)
            results.append(ArticleHtmlResult(
                article_id  = article_id,
                article_url = url,
                html_path   = str(save_path),
                success     = True,
            ))
            saved += 1
            logger.debug("[%d/%d] 保存完了: %s", i, len(rows), save_path.name)

        if i % 50 == 0:
            logger.info("  進捗: %d / %d 件（保存: %d, スキップ: %d, エラー: %d）",
                        i, len(rows), saved, skipped, errors)

        # レート制限（最後の1件は不要）
        if i < len(rows):
            time.sleep(WAIT_ARTICLES + random.uniform(0, 1))

    logger.info("記事 HTML 取得完了: 保存=%d, スキップ=%d, エラー=%d", saved, skipped, errors)
    return results
