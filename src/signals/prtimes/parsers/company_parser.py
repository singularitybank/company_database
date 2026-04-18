# -*- coding: utf-8 -*-
"""
PR Times 企業ページ HTML パーサー

保存済みの企業ページ HTML（Selenium で取得した page_source）から
企業情報を抽出する。

対象タグ: <aside aria-label="企業データ"> 内の dt/dd ペア

抽出フィールド:
  基本情報: industry, address, phone_number, representative, listed, capital, established, website_url
  詳細情報: x_url, facebook_url, youtube_url
"""

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from bs4 import BeautifulSoup

from src.signals.prtimes.crawlers.company_crawler import CompanyHtmlResult

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# データクラス
# ---------------------------------------------------------------------------

@dataclass
class CompanyParseResult:
    prtimes_company_id: int
    html_path:          str
    success:            bool
    error:              Optional[str] = None

    company_name:       Optional[str] = None
    industry:           Optional[str] = None
    prefecture:         Optional[str] = None
    address:            Optional[str] = None
    phone_number:       Optional[str] = None
    representative:     Optional[str] = None
    listed:             Optional[str] = None
    capital:            Optional[str] = None
    established:        Optional[str] = None
    website_url:        Optional[str] = None
    company_description: Optional[str] = None
    x_url:              Optional[str] = None
    facebook_url:       Optional[str] = None
    youtube_url:        Optional[str] = None


# ---------------------------------------------------------------------------
# 内部ヘルパー
# ---------------------------------------------------------------------------

_LABEL_MAP = {
    "業種":     "industry",
    "本社所在地": "address",
    "電話番号":  "phone_number",
    "代表者名":  "representative",
    "上場":     "listed",
    "資本金":   "capital",
    "設立":     "established",
    "URL":     "website_url",
    "X":       "x_url",
    "Facebook": "facebook_url",
    "YouTube":  "youtube_url",
}

_PREFECTURE_RE = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)


def _extract_prefecture(address: Optional[str]) -> Optional[str]:
    if not address:
        return None
    m = _PREFECTURE_RE.match(address)
    return m.group(1) if m else None


def _extract_company_name(soup) -> Optional[str]:
    h1 = soup.find("h1", class_=lambda c: c and "companyName" in c)
    if not h1:
        return None
    text = h1.get_text(strip=True)
    return text or None


def _extract_description(soup) -> Optional[str]:
    p = soup.find("p", class_=lambda c: c and "companyDescription" in c)
    if not p:
        return None
    text = p.get_text(strip=True)
    return text or None


def _parse_aside(aside) -> dict:
    result: dict = {}
    for dt in aside.find_all("dt"):
        label = dt.get_text(strip=True)
        field_name = _LABEL_MAP.get(label)
        if not field_name:
            continue
        dd = dt.find_next_sibling("dd")
        if not dd:
            continue

        # URL フィールドはリンクの href を優先
        if field_name in ("website_url", "x_url", "facebook_url", "youtube_url"):
            a = dd.find("a")
            value = a.get("href", "").strip() if a else dd.get_text(strip=True)
        else:
            value = " ".join(dd.get_text(" ", strip=True).split())

        if value and value != "-":
            result[field_name] = value

    return result


# ---------------------------------------------------------------------------
# メイン処理
# ---------------------------------------------------------------------------

def parse(results: list[CompanyHtmlResult]) -> list[CompanyParseResult]:
    """保存済み企業ページ HTML をパースして CompanyParseResult のリストを返す。"""
    parsed: list[CompanyParseResult] = []
    ok = ng = 0

    for r in results:
        if not r.success or not r.html_path:
            parsed.append(CompanyParseResult(
                prtimes_company_id=r.prtimes_company_id,
                html_path=r.html_path,
                success=False,
                error=r.error or "HTML取得失敗",
            ))
            ng += 1
            continue

        html_file = Path(r.html_path)
        if not html_file.exists():
            parsed.append(CompanyParseResult(
                prtimes_company_id=r.prtimes_company_id,
                html_path=r.html_path,
                success=False,
                error=f"ファイルが存在しません: {r.html_path}",
            ))
            ng += 1
            continue

        try:
            html = html_file.read_text(encoding="utf-8")
            soup = BeautifulSoup(html, "html.parser")

            aside = soup.find("aside", {"aria-label": "企業データ"})
            if not aside:
                raise ValueError("aside[aria-label='企業データ'] が見つかりません")

            fields = _parse_aside(aside)
            address = fields.get("address")

            parsed.append(CompanyParseResult(
                prtimes_company_id=r.prtimes_company_id,
                html_path=r.html_path,
                success=True,
                company_name=_extract_company_name(soup),
                industry=fields.get("industry"),
                prefecture=_extract_prefecture(address),
                address=address,
                phone_number=fields.get("phone_number"),
                representative=fields.get("representative"),
                listed=fields.get("listed"),
                capital=fields.get("capital"),
                established=fields.get("established"),
                company_description=_extract_description(soup),
                website_url=fields.get("website_url"),
                x_url=fields.get("x_url"),
                facebook_url=fields.get("facebook_url"),
                youtube_url=fields.get("youtube_url"),
            ))
            ok += 1
            logger.debug("パース完了: company_id=%d", r.prtimes_company_id)

        except Exception as e:
            logger.warning("パース失敗: company_id=%d: %s", r.prtimes_company_id, e)
            parsed.append(CompanyParseResult(
                prtimes_company_id=r.prtimes_company_id,
                html_path=r.html_path,
                success=False,
                error=str(e),
            ))
            ng += 1

    logger.info("企業ページパース完了: 成功=%d, 失敗=%d", ok, ng)
    return parsed
