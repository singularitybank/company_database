# -*- coding: utf-8 -*-
"""
TDB 倒産詳細ページ HTML パーサー

[抽出フィールド]
  company_name         : h1[class*="text-title-1-b"]
  published_at         : div[class*="text-sub"] の YYYY/MM/DD テキスト
  tdb_company_code     : p.whitespace-pre-wrap > span.md:hidden 1行目 "TDB企業コード:" 以降
  prefecture           : 同 2行目（[都道府県]で分割）
  city                 : 同 2行目（prefecture 以降の残り）
  business_description : 同 3行目
  bankruptcy_type      : 同 4行目
  liabilities_text     : 同 5行目
  body_capital_text    : main p（classなし）カッコ内の "資本金(.+?)、"
  body_address         : 同カッコ内の 資本金の次の項目
  body_representative  : 同カッコ内の "代表(社員)?(.+?)氏"
  body_employees       : 同カッコ内の "従業員(\\d+)名"（省略される場合あり）
  body_text            : main p（classなし）全段落を改行連結

[設計方針]
  - HTML ファイルは raw bytes で読み込み、utf-8 デコード後に BeautifulSoup へ渡す
  - フィールドが取得できない場合は None（空文字との区別）
  - カッコ内パターン: （資本金...、住所...、代表...氏[、従業員...名]）
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
class TdbDetailParseResult:
    """TDB 詳細ページパース結果 1 件分（DB UPDATE 用）"""
    case_id:              str
    company_name:         Optional[str]
    published_at:         Optional[str]
    tdb_company_code:     Optional[str]
    prefecture:           Optional[str]
    city:                 Optional[str]
    business_description: Optional[str]
    bankruptcy_type:      Optional[str]
    liabilities_text:     Optional[str]
    body_capital_text:    Optional[str]
    body_capital_amount:  Optional[int]
    body_address:         Optional[str]
    body_representative:  Optional[str]
    body_employees:       Optional[int]
    body_text:            Optional[str]
    html_path:            Optional[str]
    detail_scraped_at:    str
    success:              bool
    error:                Optional[str] = None


# ---------------------------------------------------------------------------
# 正規表現・定数
# ---------------------------------------------------------------------------

_DATE_RE      = re.compile(r"\d{4}/\d{2}/\d{2}")
_TDB_CODE_RE  = re.compile(r"TDB企業コード[:：](\S+)")
_PREF_RE      = re.compile(r"^(.+?[都道府県])(.*)")
_BODY_RE      = re.compile(
    r"（資本金(.+?)、(.+?)、代表(社員)?(.+?)氏(?:、従業員(\d+)名)?）"
)
_EMP_RE       = re.compile(r"従業員(\d+)名")

_ZEN_TO_HAN = str.maketrans("０１２３４５６７８９", "0123456789")


# ---------------------------------------------------------------------------
# 内部ヘルパー
# ---------------------------------------------------------------------------

def _parse_capital_amount(text: Optional[str]) -> Optional[int]:
    """「300万円」「1億2000万円」などを万円単位の整数に変換する。"""
    if not text:
        return None
    t = text.translate(_ZEN_TO_HAN).replace(",", "").replace("，", "")
    oku = re.search(r"(\d+)億", t)
    man = re.search(r"(\d+)万", t)
    if not oku and not man:
        return None
    return (int(oku.group(1)) * 10000 if oku else 0) + (int(man.group(1)) if man else 0)


def _header_lines(soup: BeautifulSoup) -> list[str]:
    """p.whitespace-pre-wrap > span.md:hidden のテキストを行リストで返す。"""
    pwp = soup.find("p", class_=lambda c: c and "whitespace-pre-wrap" in c)
    if not pwp:
        return []
    spans = pwp.find_all("span", class_=lambda c: c and "md:hidden" in c)
    if not spans:
        return []
    return [ln.strip() for ln in spans[0].get_text().split("\n") if ln.strip()]


def _split_pref_city(location: str) -> tuple[Optional[str], Optional[str]]:
    """「東京都台東区」→ ('東京都', '台東区')。都道府県のみの場合は city=None。"""
    m = _PREF_RE.match(location)
    if not m:
        return location or None, None
    pref = m.group(1) or None
    city = m.group(2).strip() or None
    return pref, city


def _published_at(soup: BeautifulSoup) -> Optional[str]:
    """YYYY/MM/DD テキストを持つ div[class*=text-sub] を探して ISO 形式に変換する。"""
    for div in soup.find_all("div"):
        cls = div.get("class") or []
        if not any("text-sub" in c for c in cls):
            continue
        txt = div.get_text(strip=True)
        m = _DATE_RE.search(txt)
        if m:
            return m.group(0).replace("/", "-")
    return None


def _body_fields(soup: BeautifulSoup) -> tuple[
    Optional[str], Optional[int], Optional[str], Optional[str], Optional[int]
]:
    """本文 p（classなし）カッコ内から (資本金テキスト, 資本金額, 住所, 代表者, 従業員数) を抽出する。"""
    main = soup.find("main")
    if not main:
        return None, None, None, None, None
    for p in main.find_all("p"):
        if p.get("class"):
            continue
        txt = p.get_text(strip=True)
        m = _BODY_RE.search(txt)
        if m:
            cap_text = m.group(1).strip()
            address  = m.group(2).strip()
            rep      = m.group(4).strip()
            emp_str  = m.group(5)
            return (
                cap_text,
                _parse_capital_amount(cap_text),
                address or None,
                rep or None,
                int(emp_str) if emp_str else None,
            )
    return None, None, None, None, None


def _body_text(soup: BeautifulSoup) -> Optional[str]:
    """main 内の p（classなし）全段落を改行連結して返す。"""
    main = soup.find("main")
    if not main:
        return None
    paras = [
        p.get_text(strip=True)
        for p in main.find_all("p")
        if not p.get("class") and p.get_text(strip=True)
    ]
    return "\n".join(paras) if paras else None


# ---------------------------------------------------------------------------
# メイン処理
# ---------------------------------------------------------------------------

def parse(results: list) -> list[TdbDetailParseResult]:
    """DetailHtmlResult リストを TdbDetailParseResult リストに変換する。

    Args:
        results: tdb_detail_crawler.scrape() の戻り値

    Returns:
        TdbDetailParseResult のリスト
    """
    scraped_at = datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")
    parsed: list[TdbDetailParseResult] = []
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

            # 上段ヘッダー（span テキストの行リスト）
            lines = _header_lines(soup)
            code_line = lines[0] if len(lines) >= 1 else ""
            loc_line  = lines[1] if len(lines) >= 2 else ""
            biz_line  = lines[2] if len(lines) >= 3 else None
            type_line = lines[3] if len(lines) >= 4 else None
            liab_line = lines[4] if len(lines) >= 5 else None

            code_m = _TDB_CODE_RE.search(code_line)
            tdb_company_code = code_m.group(1).strip() if code_m else None

            prefecture, city = _split_pref_city(loc_line) if loc_line else (None, None)

            # 本文カッコ内
            cap_text, cap_amount, address, rep, emp = _body_fields(soup)

            parsed.append(TdbDetailParseResult(
                case_id              = r.case_id,
                company_name         = (
                    soup.find("h1", class_=lambda c: c and "text-title-1-b" in c)
                    .get_text(strip=True)
                    if soup.find("h1", class_=lambda c: c and "text-title-1-b" in c)
                    else None
                ),
                published_at         = _published_at(soup),
                tdb_company_code     = tdb_company_code,
                prefecture           = prefecture,
                city                 = city,
                business_description = biz_line or None,
                bankruptcy_type      = type_line or None,
                liabilities_text     = liab_line or None,
                body_capital_text    = cap_text,
                body_capital_amount  = cap_amount,
                body_address         = address,
                body_representative  = rep,
                body_employees       = emp,
                body_text            = _body_text(soup),
                html_path            = r.html_path,
                detail_scraped_at    = scraped_at,
                success              = True,
            ))

        except Exception as e:
            logger.warning("パースエラー %s: %s", r.case_id, e)
            parsed.append(TdbDetailParseResult(
                case_id=r.case_id, company_name=None, published_at=None,
                tdb_company_code=None, prefecture=None, city=None,
                business_description=None, bankruptcy_type=None,
                liabilities_text=None, body_capital_text=None,
                body_capital_amount=None, body_address=None,
                body_representative=None, body_employees=None,
                body_text=None, html_path=None, detail_scraped_at=scraped_at,
                success=False, error=str(e),
            ))
            errors += 1

    ok = len(parsed) - errors
    logger.info("TDB 詳細パース完了: 入力=%d件, 成功=%d件, エラー=%d件",
                len(results), ok, errors)
    return parsed
