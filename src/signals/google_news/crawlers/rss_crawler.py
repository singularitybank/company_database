# -*- coding: utf-8 -*-
"""
Google News キーワード検索 RSS クローラー

URLパターン: https://news.google.com/rss/search?q=KEYWORD&hl=ja&gl=JP&ceid=JP:ja

[Google News URLについて]
  RSSの <link> は Google Newsリダイレクト URL (https://news.google.com/rss/articles/CBMi...)。
  requests.head() でリダイレクト追跡し、元記事URLを取得して保存する。
  article_id の生成にも解決済みURLを使用し、同一記事の重複登録を防ぐ。

[日付フィールド]
  RSS 2.0 形式。pubDate → feedparser の published_parsed (UTC time.struct_time) → JST変換。

[配信元メディア名]
  各エントリの <source> タグから取得（例: "NHK", "読売新聞オンライン"）。
  feedparser では entry.source.get('title') として取得可能。
"""

import hashlib
import logging
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Optional
from urllib.parse import urlencode

import feedparser
import requests

from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[5]))

logger = logging.getLogger(__name__)

JST = timezone(timedelta(hours=9))

# ---------------------------------------------------------------------------
# データクラス
# ---------------------------------------------------------------------------

@dataclass
class GoogleNewsEntry:
    """Google News RSSエントリ 1件分"""
    article_id:   str
    keyword_id:   int
    keyword:      str
    title:        str
    source_name:  Optional[str]  # 配信元メディア名（<source>タグ）
    link:         str            # 元記事URL（リダイレクト解決済み）
    published_at: Optional[str]  # JST "YYYY-MM-DD HH:MM:SS"
    summary:      Optional[str]
    fetched_at:   str


# ---------------------------------------------------------------------------
# 内部ヘルパー
# ---------------------------------------------------------------------------

def _make_article_id(url: str) -> str:
    return hashlib.sha256(url.encode()).hexdigest()[:16]


def _parse_published_at(entry) -> Optional[str]:
    t = entry.get("published_parsed") or entry.get("updated_parsed")
    if t is None:
        return None
    try:
        dt_utc = datetime(*t[:6], tzinfo=timezone.utc)
        return dt_utc.astimezone(JST).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def _strip_html(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    cleaned = re.sub(r"<[^>]+>", "", text).strip()
    return cleaned if cleaned else None


def _resolve_redirect(url: str, timeout: int = 10) -> str:
    """Google Newsリダイレクト URLを元記事URLに解決する。

    HEAD リクエストでリダイレクト先を追跡する。
    失敗時は元のURLをそのまま返す。
    """
    try:
        resp = requests.head(
            url,
            allow_redirects=True,
            timeout=timeout,
            headers={"User-Agent": "Mozilla/5.0 (compatible; CompanyDatabase/1.0)"},
        )
        final_url = resp.url
        # Google Newsのリダイレクト先がまだgoogleドメインの場合はそのまま返す
        if "google.com" in final_url:
            return url
        return final_url
    except Exception as e:
        logger.debug("リダイレクト解決失敗 (%s): %s", url[:60], e)
        return url


def _build_rss_url(base_url: str, keyword: str, params: dict) -> str:
    q_params = {**params, "q": keyword}
    return f"{base_url}?{urlencode(q_params)}"


# ---------------------------------------------------------------------------
# メイン処理
# ---------------------------------------------------------------------------

def fetch_by_keyword(
    keyword: str,
    keyword_id: int,
    base_url: str,
    params: dict,
    timeout: int = 30,
    retry_count: int = 3,
) -> list[GoogleNewsEntry]:
    """キーワードでGoogle News RSSを検索してエントリリストを返す。

    Args:
        keyword:      検索キーワード
        keyword_id:   google_news_keywords テーブルのPK
        base_url:     Google News RSS のベースURL
        params:       固定クエリパラメータ（hl, gl, ceid）
        timeout:      リクエストタイムアウト（秒）
        retry_count:  リトライ回数

    Returns:
        GoogleNewsEntry のリスト。取得失敗時は空リスト。
    """
    rss_url = _build_rss_url(base_url, keyword, params)
    fetched_at = datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")
    logger.info("[google_news] キーワード「%s」RSS取得開始", keyword)

    feed = None
    for attempt in range(1, retry_count + 1):
        try:
            feed = feedparser.parse(rss_url, request_headers={
                "User-Agent": "Mozilla/5.0 (compatible; CompanyDatabase/1.0)"
            })
            if feed.entries:
                break
            logger.warning("[google_news] エントリ 0件（試行 %d/%d）", attempt, retry_count)
        except Exception as e:
            logger.warning("[google_news] RSS取得エラー（試行 %d/%d）: %s", attempt, retry_count, e)

    if not feed or not feed.entries:
        logger.error("[google_news] キーワード「%s」RSS取得失敗（%d回試行）", keyword, retry_count)
        return []

    entries: list[GoogleNewsEntry] = []
    for e in feed.entries:
        raw_link = e.get("link") or e.get("id", "")
        if not raw_link:
            continue

        title = e.get("title", "").strip()
        if not title:
            continue

        resolved_link = _resolve_redirect(raw_link, timeout=min(timeout, 10))

        source_name: Optional[str] = None
        src = e.get("source")
        if isinstance(src, dict):
            source_name = src.get("title")

        entry = GoogleNewsEntry(
            article_id   = _make_article_id(resolved_link),
            keyword_id   = keyword_id,
            keyword      = keyword,
            title        = title,
            source_name  = source_name,
            link         = resolved_link,
            published_at = _parse_published_at(e),
            summary      = _strip_html(e.get("summary")),
            fetched_at   = fetched_at,
        )
        entries.append(entry)

        # リダイレクト解決に HTTP リクエストを使うため記事間に短いウェイト
        time.sleep(0.3)

    logger.info("[google_news] キーワード「%s」取得完了: %d件", keyword, len(entries))
    return entries
