# -*- coding: utf-8 -*-
"""
東京商工リサーチ（TSR）倒産情報 RSS クローラー

対象URL: https://www.tsr-net.co.jp/rss/news_flash.xml

[動作概要]
  1. feedparser で RSS を取得
  2. 各エントリから case_id（URL SHA-256 先頭16文字）・メタ情報を抽出
  3. TsrRssEntry のリストとして返す（DB への保存は loaders 層が行う）

[URL パターン]
  https://www.tsr-net.co.jp/news/tsr/detail/{id}_1521.html

[日付フィールド]
  RSS の <dc:date> が feedparser の updated / updated_parsed にマッピングされる。
  updated_parsed は UTC の time.struct_time のため +9h して JST に変換する。
  TSR は秒まで含む（例: 2026-04-14T19:32:54+09:00）。
"""

import hashlib
import logging
import sys
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Optional

import feedparser

from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[5]))

from src.config import bankruptcy as _cfg

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 定数
# ---------------------------------------------------------------------------

RSS_URL     = _cfg["tsr_rss_url"]
TIMEOUT     = _cfg["timeout"]
RETRY_COUNT = _cfg["retry_count"]

JST = timezone(timedelta(hours=9))

# ---------------------------------------------------------------------------
# データクラス
# ---------------------------------------------------------------------------

@dataclass
class TsrRssEntry:
    """TSR RSS エントリ 1 件分"""
    case_id:       str            # 詳細URLのSHA-256先頭16文字
    source_url:    str            # 詳細ページURL
    company_name:  Optional[str]  # タイトルから抽出
    published_at:  Optional[str]  # JST "YYYY-MM-DD HH:MM:SS"
    summary:       Optional[str]  # RSS summary テキスト
    rss_fetched_at: str           # RSS取得日時（JST）


# ---------------------------------------------------------------------------
# 内部ヘルパー
# ---------------------------------------------------------------------------

def _make_case_id(url: str) -> str:
    return hashlib.sha256(url.encode()).hexdigest()[:16]


def _updated_parsed_to_jst(t) -> Optional[str]:
    """feedparser の updated_parsed（UTC の time.struct_time）を JST 文字列に変換する。"""
    if t is None:
        return None
    try:
        dt_utc = datetime(*t[:6], tzinfo=timezone.utc)
        return dt_utc.astimezone(JST).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def _strip_html(text: Optional[str]) -> Optional[str]:
    """feedparser summary に含まれる HTML タグを除去する。"""
    if not text:
        return None
    import re
    cleaned = re.sub(r"<[^>]+>", "", text).strip()
    return cleaned if cleaned else None


# ---------------------------------------------------------------------------
# メイン処理
# ---------------------------------------------------------------------------

def fetch_rss() -> list[TsrRssEntry]:
    """TSR RSS を取得してエントリリストを返す。

    Returns:
        TsrRssEntry のリスト。取得失敗時は空リスト。
    """
    fetched_at = datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")
    logger.info("TSR RSS 取得開始: %s", RSS_URL)

    feed = None
    for attempt in range(1, RETRY_COUNT + 1):
        try:
            feed = feedparser.parse(RSS_URL)
            if feed.entries:
                break
            logger.warning("TSR RSS エントリ 0 件（試行 %d/%d）", attempt, RETRY_COUNT)
        except Exception as e:
            logger.warning("TSR RSS 取得エラー（試行 %d/%d）: %s", attempt, RETRY_COUNT, e)

    if not feed or not feed.entries:
        logger.error("TSR RSS 取得失敗（%d 回試行）", RETRY_COUNT)
        return []

    entries: list[TsrRssEntry] = []
    for e in feed.entries:
        url = e.get("link") or e.get("id", "")
        if not url:
            continue

        entry = TsrRssEntry(
            case_id        = _make_case_id(url),
            source_url     = url,
            company_name   = e.get("title") or None,
            published_at   = _updated_parsed_to_jst(e.get("updated_parsed")),
            summary        = _strip_html(e.get("summary")),
            rss_fetched_at = fetched_at,
        )
        entries.append(entry)

    logger.info("TSR RSS 取得完了: %d 件", len(entries))
    return entries
