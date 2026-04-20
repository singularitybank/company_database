# -*- coding: utf-8 -*-
"""
ニュースRSSクローラー

対応フォーマット:
  - RSS 2.0 (NHK, 47news_local, Yahoo国内/地域)
  - RDF 1.0 (47news_national, 日経, 産経, 読売, 朝日)

[フォーマット別フィールド差異]
  RDF 1.0: dc:date (ISO 8601 + TZ) → feedparser の updated_parsed に格納
            description は基本空。URL は entry.id (rdf:about 属性)
  RSS 2.0: pubDate (RFC 2822)       → feedparser の published_parsed に格納
            description にテキストあり。image は media_thumbnail または enclosures
"""

import hashlib
import logging
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Optional

import feedparser

from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[5]))

logger = logging.getLogger(__name__)

JST = timezone(timedelta(hours=9))

# ---------------------------------------------------------------------------
# データクラス
# ---------------------------------------------------------------------------

@dataclass
class NewsRssEntry:
    """ニュースRSSエントリ 1件分"""
    article_id:   str
    source:       str            # ソースkey ('nhk', 'yahoo_domestic', ...)
    source_url:   str
    title:        str
    published_at: Optional[str]  # JST "YYYY-MM-DD HH:MM:SS"
    summary:      Optional[str]  # RSS 2.0のみ値あり、RDF 1.0はNULL
    image_url:    Optional[str]  # Yahoo / 47news_local のみ
    category:     str            # 'domestic' or 'local'
    fetched_at:   str


# ---------------------------------------------------------------------------
# 内部ヘルパー
# ---------------------------------------------------------------------------

def _make_article_id(url: str) -> str:
    return hashlib.sha256(url.encode()).hexdigest()[:16]


def _parse_published_at(entry) -> Optional[str]:
    """feedparser エントリから公開日時をJST文字列として取得する。

    RSS 2.0: published_parsed (pubDate)
    RDF 1.0: updated_parsed   (dc:date)
    """
    t = entry.get("published_parsed") or entry.get("updated_parsed")
    if t is None:
        return None
    try:
        dt_utc = datetime(*t[:6], tzinfo=timezone.utc)
        return dt_utc.astimezone(JST).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def _strip_html(text: Optional[str]) -> Optional[str]:
    """summary に含まれる HTML タグを除去する。"""
    if not text:
        return None
    cleaned = re.sub(r"<[^>]+>", "", text).strip()
    return cleaned if cleaned else None


def _extract_image_url(entry) -> Optional[str]:
    """エントリから画像URLを取得する（RSS 2.0のYahoo/47newsのみ存在）。"""
    thumbnails = getattr(entry, "media_thumbnail", None)
    if thumbnails and isinstance(thumbnails, list):
        url = thumbnails[0].get("url")
        if url:
            return url

    for enc in getattr(entry, "enclosures", []):
        mime = enc.get("type", "")
        if mime.startswith("image/"):
            url = enc.get("url")
            if url:
                return url

    return None


def _extract_url(entry) -> str:
    """エントリからURLを取得する。

    RSS 2.0: entry.link
    RDF 1.0: entry.id (rdf:about 属性) または entry.link
    """
    return entry.get("link") or entry.get("id", "")


# ---------------------------------------------------------------------------
# メイン処理
# ---------------------------------------------------------------------------

def fetch_rss(
    source_key: str,
    url: str,
    category: str,
    timeout: int = 30,
    retry_count: int = 3,
) -> list[NewsRssEntry]:
    """指定URLのRSSを取得してNewsRssEntryリストを返す。

    Args:
        source_key:   ソースkey ('nhk', 'yahoo_domestic', ...)
        url:          RSS/RDF フィードURL
        category:     'domestic' or 'local'
        timeout:      リクエストタイムアウト（秒）
        retry_count:  リトライ回数

    Returns:
        NewsRssEntry のリスト。取得失敗時は空リスト。
    """
    fetched_at = datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")
    logger.info("[%s] RSS取得開始: %s", source_key, url)

    feed = None
    for attempt in range(1, retry_count + 1):
        try:
            feed = feedparser.parse(url, request_headers={
                "User-Agent": "Mozilla/5.0 (compatible; CompanyDatabase/1.0)"
            })
            if feed.entries:
                break
            logger.warning("[%s] RSSエントリ 0件（試行 %d/%d）", source_key, attempt, retry_count)
        except Exception as e:
            logger.warning("[%s] RSS取得エラー（試行 %d/%d）: %s", source_key, attempt, retry_count, e)

    if not feed or not feed.entries:
        logger.error("[%s] RSS取得失敗（%d回試行）", source_key, retry_count)
        return []

    entries: list[NewsRssEntry] = []
    for e in feed.entries:
        article_url = _extract_url(e)
        if not article_url:
            continue

        title = e.get("title", "").strip()
        if not title:
            continue

        entry = NewsRssEntry(
            article_id   = _make_article_id(article_url),
            source       = source_key,
            source_url   = article_url,
            title        = title,
            published_at = _parse_published_at(e),
            summary      = _strip_html(e.get("summary")),
            image_url    = _extract_image_url(e),
            category     = category,
            fetched_at   = fetched_at,
        )
        entries.append(entry)

    logger.info("[%s] RSS取得完了: %d件", source_key, len(entries))
    return entries
