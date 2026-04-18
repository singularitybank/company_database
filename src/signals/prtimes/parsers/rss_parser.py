# -*- coding: utf-8 -*-
"""
PR Times RSS パーサー

prtimes_rss_crawler.fetch_global_rss() が返す RssEntry リストを受け取り、
DBローダーに渡せる ArticleRecord リストに変換する。

[責務]
  - フィールドの正規化・クリーニング（空白除去、空文字→None 統一）
  - fetched_at（取得日時）の付与
  - summary の HTML タグ除去
  - DB 挿入用 dict への変換（to_dict）
"""

import logging
import re
import sys
from dataclasses import dataclass, asdict
from datetime import datetime, timezone, timedelta
from typing import Optional

from bs4 import BeautifulSoup

sys.path.insert(0, str(__file__).replace("\\", "/").split("src/")[0])
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.crawlers.prtimes_rss_crawler import RssEntry

logger = logging.getLogger(__name__)

JST = timezone(timedelta(hours=9))

# ---------------------------------------------------------------------------
# データクラス
# ---------------------------------------------------------------------------

@dataclass
class ArticleRecord:
    """prtimes_articles テーブル 1 行分"""
    article_id:          str
    prtimes_company_id:  Optional[int]
    pr_number:           Optional[int]
    title:               str
    article_url:         str
    company_name_rss:    Optional[str]
    business_form:       Optional[str]
    published_at:        Optional[str]
    summary:             Optional[str]
    image_url:           Optional[str]  # 記事スクレイピング後に更新
    source:              str
    fetched_at:          str
    html_path:           Optional[str]  # 記事スクレイピング後に更新
    body_text:           Optional[str]  # 記事スクレイピング後に更新
    article_scraped_at:  Optional[str]  # 記事スクレイピング後に更新

    def to_dict(self) -> dict:
        return asdict(self)


# ---------------------------------------------------------------------------
# 内部ヘルパー
# ---------------------------------------------------------------------------

def _strip_or_none(value: Optional[str]) -> Optional[str]:
    """空白を除去し、空文字は None に統一する。"""
    if value is None:
        return None
    v = value.strip()
    return v if v else None


def _strip_html(text: Optional[str]) -> Optional[str]:
    """HTML タグを除去してプレーンテキストを返す。"""
    if not text:
        return None
    plain = BeautifulSoup(text, "html.parser").get_text(separator="\n", strip=True)
    return plain if plain else None


def _now_jst() -> str:
    """現在の JST 日時を 'YYYY-MM-DD HH:MM:SS' 形式で返す。"""
    return datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")


# ---------------------------------------------------------------------------
# メイン処理
# ---------------------------------------------------------------------------

def parse(entries: list[RssEntry]) -> list[ArticleRecord]:
    """RssEntry リストを ArticleRecord リストに変換する。

    Args:
        entries: prtimes_rss_crawler.fetch_global_rss() の戻り値

    Returns:
        ArticleRecord のリスト
    """
    fetched_at = _now_jst()
    records: list[ArticleRecord] = []

    for e in entries:
        title = _strip_or_none(e.title)
        if not title:
            logger.debug("タイトル空のエントリをスキップ: %s", e.article_url)
            continue

        record = ArticleRecord(
            article_id         = e.article_id,
            prtimes_company_id = e.prtimes_company_id,
            pr_number          = e.pr_number,
            title              = title,
            article_url        = e.article_url,
            company_name_rss   = _strip_or_none(e.company_name_rss),
            business_form      = _strip_or_none(e.business_form),
            published_at       = e.published_at,
            summary            = _strip_html(e.summary),
            image_url          = None,
            source             = e.source,
            fetched_at         = fetched_at,
            html_path          = None,
            body_text          = None,
            article_scraped_at = None,
        )
        records.append(record)

    logger.info("RSS パース完了: %d 件 → %d 件（スキップ: %d 件）",
                len(entries), len(records), len(entries) - len(records))
    return records
