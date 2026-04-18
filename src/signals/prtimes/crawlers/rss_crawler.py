# -*- coding: utf-8 -*-
"""
PR Times グローバル RSS クローラー

対象URL: https://prtimes.jp/index.rdf

[動作概要]
  1. feedparser でグローバル RSS を取得（raw bytes を直接渡してエンコード自動解決）
  2. 各エントリから article_id（URL ハッシュ）・prtimes_company_id・pr_number を抽出
  3. RssEntry のリストとして返す（DB への保存は loaders 層が行う）

[URL パターン]
  https://prtimes.jp/main/html/rd/p/{pr_number:09d}.{company_id:09d}.html
  例: /p/000000014.000096129.html → pr_number=14, company_id=96129
"""

import hashlib
import logging
import re
import time
import random
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import feedparser
import requests

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.config import prtimes as _cfg

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 定数
# ---------------------------------------------------------------------------

RSS_URL        = _cfg["rss_url"]
TIMEOUT        = _cfg["timeout"]
RETRY_COUNT    = _cfg["retry_count"]

_URL_PATTERN   = re.compile(r"/p/(\d+)\.(\d+)\.html")
_USER_AGENT    = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

# ---------------------------------------------------------------------------
# データクラス
# ---------------------------------------------------------------------------

@dataclass
class RssEntry:
    """RSS エントリ 1 件分"""
    article_id:         str            # URL の SHA-256 ハッシュ（先頭16文字）
    prtimes_company_id: Optional[int]  # URL から抽出
    pr_number:          Optional[int]  # URL から抽出（企業内の連番）
    title:              str
    article_url:        str
    company_name_rss:   Optional[str]  # dc_corp フィールド
    business_form:      Optional[str]  # business_form フィールド
    published_at:       Optional[str]  # ISO 8601 形式 "YYYY-MM-DD HH:MM:SS"
    summary:            Optional[str]
    source:             str = "global_rss"


# ---------------------------------------------------------------------------
# 内部ヘルパー
# ---------------------------------------------------------------------------

def _make_article_id(url: str) -> str:
    """URL の SHA-256 ハッシュ先頭 16 文字を article_id として返す。"""
    return hashlib.sha256(url.encode()).hexdigest()[:16]


def _extract_ids_from_url(url: str) -> tuple[Optional[int], Optional[int]]:
    """URL から (pr_number, company_id) を抽出して返す。

    パターンに一致しない場合は (None, None) を返す。
    """
    m = _URL_PATTERN.search(url)
    if m:
        return int(m.group(1)), int(m.group(2))
    return None, None


def _parsed_time_to_str(t) -> Optional[str]:
    """feedparser の updated_parsed（UTC の time.struct_time）を JST 文字列に変換する。

    feedparser は常に UTC に正規化して返すため、+9 時間して JST に変換する。
    """
    if t is None:
        return None
    try:
        from datetime import timezone, timedelta
        JST = timezone(timedelta(hours=9))
        dt_utc = datetime(*t[:6], tzinfo=timezone.utc)
        return dt_utc.astimezone(JST).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def _fetch_raw(url: str) -> Optional[bytes]:
    """指定 URL の生バイトを取得して返す。

    エンコード変換を行わず raw bytes を返すことで、feedparser 側の
    自動エンコード検出が正しく機能する（requests 経由で encoding を
    上書きすると文字化けが発生するため）。
    """
    headers = {"User-Agent": _USER_AGENT}
    for attempt in range(1, RETRY_COUNT + 1):
        try:
            resp = requests.get(url, headers=headers, timeout=TIMEOUT)
            resp.raise_for_status()
            return resp.content
        except requests.RequestException as e:
            logger.warning("RSS 取得エラー (試行 %d/%d): %s", attempt, RETRY_COUNT, e)
            if attempt < RETRY_COUNT:
                time.sleep(2 ** attempt + random.uniform(0, 1))
    logger.error("RSS 取得失敗（%d 回試行）: %s", RETRY_COUNT, url)
    return None


# ---------------------------------------------------------------------------
# メイン処理
# ---------------------------------------------------------------------------

def fetch_global_rss() -> list[RssEntry]:
    """グローバル RSS を取得してエントリリストを返す。

    Returns:
        RssEntry のリスト。取得失敗時は空リスト。
    """
    logger.info("グローバル RSS 取得開始: %s", RSS_URL)

    raw = _fetch_raw(RSS_URL)
    if raw is None:
        return []

    feed = feedparser.parse(raw)

    if feed.bozo and not feed.entries:
        logger.error("RSS パースエラー: %s", feed.bozo_exception)
        return []

    entries: list[RssEntry] = []
    for e in feed.entries:
        url = e.get("link") or e.get("id", "")
        if not url:
            continue

        pr_number, company_id = _extract_ids_from_url(url)

        entry = RssEntry(
            article_id         = _make_article_id(url),
            prtimes_company_id = company_id,
            pr_number          = pr_number,
            title              = e.get("title", ""),
            article_url        = url,
            company_name_rss   = e.get("dc_corp") or None,
            business_form      = e.get("business_form") or None,
            published_at       = _parsed_time_to_str(e.get("updated_parsed")),
            summary            = e.get("summary") or None,
        )
        entries.append(entry)

    logger.info("RSS 取得完了: %d 件", len(entries))
    return entries
