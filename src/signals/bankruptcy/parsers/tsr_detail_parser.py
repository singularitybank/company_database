# -*- coding: utf-8 -*-
"""
TSR 倒産詳細ページ HTML パーサー

[抽出フィールド]
  company_name         : h1.title_data
  published_at         : div（classなし）の YYYY/MM/DD テキスト
  prefecture           : li.tag_prefecture > a
  industry             : li.tag_industry 1番目 > a
  business_description : li.tag_industry 2番目 > a（1つのみの場合は None）
  bankruptcy_type      : li.tag_procedure > a
  liabilities_text     : li.tag_debt > a（続報記事では存在しない場合あり）
  tsr_code             : div.entry_info_code から "TSRコード:(数字+)"
  corporate_number     : div.entry_info_code から "法人番号:(数字+)"
  body_capital_text    : ※フッターノート段落から "資本金(.+?)[）、]"
  body_capital_amount  : body_capital_text を万円単位の整数に変換
  body_address         : ※フッターノート段落から 法人番号の次の項目
  body_established     : ※フッターノート段落から "設立(.+?)[）、]"
  body_text            : main/article 内の p 要素（classなし）を連結したテキスト
  detail_scraped_at    : パース実行時の JST 現在時刻

[設計方針]
  - HTML ファイルは raw bytes で読み込み、utf-8 デコード後に BeautifulSoup へ渡す
  - フィールドが取得できない場合は None（空文字との区別）
  - li.tag_industry が 1 つのみの場合は industry に格納し business_description は None
"""

import logging
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).resolve().parents[5]))

logger = logging.getLogger(__name__)

JST = timezone(timedelta(hours=9))

# ---------------------------------------------------------------------------
# データクラス
# ---------------------------------------------------------------------------

@dataclass
class TsrDetailParseResult:
    """TSR 詳細ページパース結果 1 件分（DB UPDATE 用）"""
    case_id:              str
    company_name:         Optional[str]
    published_at:         Optional[str]
    prefecture:           Optional[str]
    industry:             Optional[str]
    business_description: Optional[str]
    bankruptcy_type:      Optional[str]
    liabilities_text:     Optional[str]
    tsr_code:             Optional[str]
    corporate_number:     Optional[str]
    body_capital_text:    Optional[str]
    body_capital_amount:  Optional[int]
    body_address:         Optional[str]
    body_established:     Optional[str]
    body_text:            Optional[str]
    html_path:            Optional[str]
    detail_scraped_at:    str
    success:              bool
    error:                Optional[str] = None


# ---------------------------------------------------------------------------
# 内部ヘルパー
# ---------------------------------------------------------------------------

_DATE_RE      = re.compile(r"^\d{4}/\d{2}/\d{2}$")
_TSR_CODE_RE  = re.compile(r"TSRコード[:：](\d+)")
_CORP_NUM_RE  = re.compile(r"法人番号[:：](\d+)")
_CAPITAL_RE   = re.compile(r"資本金([^）、]+)")
_ESTAB_RE     = re.compile(r"設立(\d{4}[^）、]+)")
_LIAB_NORM_RE = re.compile(r"[^\d億万円約]")

_FOOTNOTE_MARKER = "\u203b"  # ※

# 全角数字 → 半角変換テーブル
_ZEN_TO_HAN = str.maketrans("０１２３４５６７８９", "0123456789")


def _tag_text(container, class_name: str) -> Optional[str]:
    """li.{class_name} > a のテキストを返す。なければ None。"""
    li = container.find("li", class_=class_name)
    if li:
        a = li.find("a")
        return (a.get_text(strip=True) or None) if a else (li.get_text(strip=True) or None)
    return None


def _published_at(soup: BeautifulSoup) -> Optional[str]:
    """YYYY/MM/DD 形式のテキストを持つ div を探して ISO 形式に変換する。"""
    for div in soup.find_all("div"):
        if div.get("class"):
            continue
        txt = div.get_text(strip=True)
        if _DATE_RE.match(txt):
            return txt.replace("/", "-")
    return None


def _footnote_text(soup: BeautifulSoup) -> Optional[str]:
    """本文 p 要素内の ※ 以降のテキストを返す。
    ※ は本文末尾の <br> 区切り内に埋め込まれているため startswith ではなく find で検索する。
    """
    for p in soup.find_all("p"):
        if p.get("class"):
            continue
        txt = p.get_text(strip=True)
        idx = txt.find(_FOOTNOTE_MARKER)
        if idx >= 0:
            return txt[idx:]
    return None


def _parse_capital_amount(text: Optional[str]) -> Optional[int]:
    """「300万円」「1億2000万円」などを万円単位の整数に変換する。"""
    if not text:
        return None
    t = text.translate(_ZEN_TO_HAN).replace(",", "").replace("，", "")
    oku  = re.search(r"(\d+)億", t)
    man  = re.search(r"(\d+)万", t)
    if not oku and not man:
        return None
    return (int(oku.group(1)) * 10000 if oku else 0) + (int(man.group(1)) if man else 0)


def _extract_footnote_fields(footnote: str) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """※フッターノート段落から (資本金テキスト, 住所, 設立) を抽出する。

    例: ※　合同会社クリアースカイ（TSRコード:137254873、法人番号:9120003018366、
            京都市下京区西境町149、設立2020（令和2）年11月、資本金300万円）
    """
    capital    = None
    address    = None
    established = None

    cap_m = _CAPITAL_RE.search(footnote)
    if cap_m:
        capital = cap_m.group(1).strip().rstrip("）")

    est_m = _ESTAB_RE.search(footnote)
    if est_m:
        established = est_m.group(1).strip().rstrip("）")

    # 住所: 法人番号の次の「、」区切り項目（設立・資本金でないもの）
    corp_m = _CORP_NUM_RE.search(footnote)
    if corp_m:
        after_corp = footnote[corp_m.end():]
        parts = [p.strip().rstrip("）") for p in after_corp.split("、") if p.strip()]
        for part in parts:
            if part.startswith("設立") or part.startswith("資本金"):
                continue
            if re.search(r"[都道府県市区町村]", part):
                address = part
                break

    return capital, address, established


def _body_text(soup: BeautifulSoup) -> Optional[str]:
    """p 要素（classなし）のテキストを改行で連結して返す。※ 以降のフッターノートは除外する。"""
    paras = []
    for p in soup.find_all("p"):
        if p.get("class"):
            continue
        txt = p.get_text(strip=True)
        if not txt:
            continue
        idx = txt.find(_FOOTNOTE_MARKER)
        if idx >= 0:
            txt = txt[:idx].strip()
        if txt:
            paras.append(txt)
    return "\n".join(paras) if paras else None


# ---------------------------------------------------------------------------
# メイン処理
# ---------------------------------------------------------------------------

def parse(results: list) -> list[TsrDetailParseResult]:
    """DetailHtmlResult リストを TsrDetailParseResult リストに変換する。

    Args:
        results: tsr_detail_crawler.scrape() の戻り値

    Returns:
        TsrDetailParseResult のリスト
    """
    scraped_at = datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")
    parsed: list[TsrDetailParseResult] = []
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
            soup = BeautifulSoup(html_path.read_bytes().decode("utf-8"), "html.parser")

            # タグ類は div.entry_info にスコープ（関連記事の同名タグを拾わないため）
            entry_info = soup.find("div", class_="entry_info") or soup

            # 上段タグ群
            industry_tags = entry_info.find_all("li", class_="tag_industry")
            industry             = industry_tags[0].find("a").get_text(strip=True) if len(industry_tags) >= 1 else None
            business_description = industry_tags[1].find("a").get_text(strip=True) if len(industry_tags) >= 2 else None

            # entry_info_code（TSRコード / 法人番号）
            code_div  = entry_info.find("div", class_="entry_info_code")
            code_text = code_div.get_text(strip=True) if code_div else ""
            tsr_code_m  = _TSR_CODE_RE.search(code_text)
            corp_num_m  = _CORP_NUM_RE.search(code_text)

            # ※ フッターノート（本文末尾の <br> 区切り内に埋め込まれている）
            footnote = _footnote_text(soup)
            cap_text, address, established = _extract_footnote_fields(footnote) if footnote else (None, None, None)

            parsed.append(TsrDetailParseResult(
                case_id              = r.case_id,
                company_name         = soup.find("h1", class_="title_data").get_text(strip=True) if soup.find("h1", class_="title_data") else None,
                published_at         = _published_at(soup),
                prefecture           = _tag_text(entry_info, "tag_prefecture"),
                industry             = industry,
                business_description = business_description,
                bankruptcy_type      = _tag_text(entry_info, "tag_procedure"),
                liabilities_text     = _tag_text(entry_info, "tag_debt"),
                tsr_code             = tsr_code_m.group(1) if tsr_code_m else None,
                corporate_number     = corp_num_m.group(1) if corp_num_m else None,
                body_capital_text    = cap_text,
                body_capital_amount  = _parse_capital_amount(cap_text),
                body_address         = address,
                body_established     = established,
                body_text            = _body_text(soup),
                html_path            = r.html_path,
                detail_scraped_at    = scraped_at,
                success              = True,
            ))

        except Exception as e:
            logger.warning("パースエラー %s: %s", r.case_id, e)
            parsed.append(TsrDetailParseResult(
                case_id=r.case_id, company_name=None, published_at=None,
                prefecture=None, industry=None, business_description=None,
                bankruptcy_type=None, liabilities_text=None,
                tsr_code=None, corporate_number=None,
                body_capital_text=None, body_capital_amount=None,
                body_address=None, body_established=None,
                body_text=None, html_path=None, detail_scraped_at=scraped_at,
                success=False, error=str(e),
            ))
            errors += 1

    ok = len(parsed) - errors
    logger.info("TSR 詳細パース完了: 入力=%d件, 成功=%d件, エラー=%d件",
                len(results), ok, errors)
    return parsed
