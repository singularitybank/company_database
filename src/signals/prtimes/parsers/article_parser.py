# -*- coding: utf-8 -*-
"""
PR Times 記事ページ HTML パーサー

prtimes_article_crawler.scrape_articles() が保存した HTML ファイルを読み込み、
構造化データに変換して DB 更新用レコードを返す。

[抽出フィールド]
  image_url       : og:image メタタグ
  published_at    : <time datetime="..."> 属性（記事ページ値を正とする）
  body_text       : div[class*="press-release-body"] のプレーンテキスト

[設計方針]
  - HTML ファイルは raw bytes で読み込み（BeautifulSoup がエンコード自動検出）
  - フィールドが取得できない場合は None（空文字との区別）
  - article_scraped_at は parse() 呼び出し時の JST 現在時刻を付与
"""

import logging
import sys
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.crawlers.prtimes_article_crawler import ArticleHtmlResult

logger = logging.getLogger(__name__)

JST = timezone(timedelta(hours=9))

# ---------------------------------------------------------------------------
# データクラス
# ---------------------------------------------------------------------------

@dataclass
class ArticleParseResult:
    """記事パース結果 1 件分（DB UPDATE 用）"""
    article_id:         str
    html_path:          str            # 保存済み HTML ファイルパス
    image_url:          Optional[str]  # og:image
    published_at:       Optional[str]  # <time datetime="...">
    body_text:          Optional[str]  # 本文プレーンテキスト
    article_scraped_at: str            # パース実行時の JST 日時
    success:            bool
    error:              Optional[str] = None


# ---------------------------------------------------------------------------
# 内部ヘルパー
# ---------------------------------------------------------------------------

def _extract_image_url(soup: BeautifulSoup) -> Optional[str]:
    tag = soup.find("meta", property="og:image")
    if tag:
        url = tag.get("content", "").strip()
        return url if url else None
    return None


def _extract_published_at(soup: BeautifulSoup) -> Optional[str]:
    """<time datetime="YYYY-MM-DD HH:MM:SS"> から公開日時を取得する。"""
    tag = soup.find("time")
    if tag:
        val = tag.get("datetime", "").strip()
        return val if val else None
    return None


def _extract_body_text(soup: BeautifulSoup) -> Optional[str]:
    """class に 'press-release-body' を含む div から本文を取得する。

    PR Times はバージョン番号付きクラス名（例: press-release-body-v3-0-0）を
    使用するため前方一致でマッチする。
    """
    for div in soup.find_all("div", class_=True):
        classes = " ".join(div.get("class", []))
        if "press-release-body" in classes:
            text = div.get_text(separator="\n", strip=True)
            return text if text else None
    return None


# ---------------------------------------------------------------------------
# メイン処理
# ---------------------------------------------------------------------------

def parse(results: list[ArticleHtmlResult]) -> list[ArticleParseResult]:
    """ArticleHtmlResult リストを ArticleParseResult リストに変換する。

    取得失敗（success=False）の記事はスキップする。

    Args:
        results: prtimes_article_crawler.scrape_articles() の戻り値

    Returns:
        ArticleParseResult のリスト
    """
    scraped_at = datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")
    parsed: list[ArticleParseResult] = []
    errors = 0

    for r in results:
        if not r.success or not r.html_path:
            continue

        html_path = Path(r.html_path)
        if not html_path.exists():
            logger.warning("HTML ファイルが存在しません: %s", html_path)
            errors += 1
            continue

        try:
            soup = BeautifulSoup(html_path.read_bytes(), "html.parser")

            parsed.append(ArticleParseResult(
                article_id         = r.article_id,
                html_path          = r.html_path,
                image_url          = _extract_image_url(soup),
                published_at       = _extract_published_at(soup),
                body_text          = _extract_body_text(soup),
                article_scraped_at = scraped_at,
                success            = True,
            ))

        except Exception as e:
            logger.warning("パースエラー %s: %s", r.article_id, e)
            parsed.append(ArticleParseResult(
                article_id         = r.article_id,
                html_path          = r.html_path,
                image_url          = None,
                published_at       = None,
                body_text          = None,
                article_scraped_at = scraped_at,
                success            = False,
                error              = str(e),
            ))
            errors += 1

    ok = len(parsed) - errors
    logger.info("記事パース完了: 入力=%d件, 成功=%d件, エラー=%d件",
                len(results), ok, errors)
    return parsed
