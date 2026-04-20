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
  bankruptcy_type      : 「負債」行の直前行（位置ではなく内容で識別）
  liabilities_text     : 「負債」で始まる行（位置ではなく内容で識別）
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
    case_id:                   str
    company_name:              Optional[str]
    published_at:              Optional[str]
    tdb_company_code:          Optional[str]
    prefecture:                Optional[str]
    city:                      Optional[str]
    business_description:      Optional[str]
    bankruptcy_type:           Optional[str]
    liabilities_text:          Optional[str]
    liabilities_amount:        Optional[int]
    is_followup:               bool
    former_name:               Optional[str]
    body_capital_text:         Optional[str]
    body_capital_amount:       Optional[int]
    body_address:              Optional[str]
    body_registered_address:   Optional[str]
    body_representative:       Optional[str]
    body_employees:            Optional[int]
    body_text:                 Optional[str]
    html_path:                 Optional[str]
    detail_scraped_at:         str
    success:                   bool
    error:                     Optional[str] = None


# ---------------------------------------------------------------------------
# 正規表現・定数
# ---------------------------------------------------------------------------

_DATE_RE      = re.compile(r"\d{4}/\d{2}/\d{2}")
_TDB_CODE_RE  = re.compile(r"TDB企業コード[:：](\S+)")
# Fix3: 否定先読みで「京都府」を「京都」で止めない（都道府県の後に[都道府県]が続く場合は延長）
_PREF_RE      = re.compile(r"^(.{2,5}?[都道府県](?![都道府県]))(.*)")

# グループ番号: former_name=1, cap=2, addr=3, rep=4, emp=5
# - 旧商号（オプション）: 「旧商号：（株）XXX、」― inner（）を含むため .+? を使用
# - 資本金（オプション）
# - 住所: [^（）]+? で括弧を跨がない
_BODY_RE = re.compile(
    r"（(?:旧商号[：:](.+?)、)?(?:資本金([^、）]+?)、)?([^（）]+?)、代表(?:社員)?(.+?)氏(?:ほか\d+名)?(?:、従業員(\d+)名)?）"
)

# 山括弧＜＞パターン: 「＜旧商号：...、資本金...、住所、代表...氏＞」
# グループ番号: former_name=1, cap=2, addr=3, rep=4, emp=5
_BODY_RE_ANGLE = re.compile(
    r"＜(?:旧商号[：:](.+?)、)?(?:資本金([^、＞]+?)、)?([^＜＞]+?)、代表(?:社員)?(.+?)氏(?:ほか\d+名)?(?:、従業員(\d+)名)?＞"
)

# 代表者なし（清算人・弁護士など）: 旧商号（オプション）＋資本金＋住所のみ抽出
# グループ番号: former_name=1, cap=2, addr=3
_BODY_RE_NO_REP = re.compile(
    r"（(?:旧商号[：:](.+?)、)?資本金([^、）]+?)、([^（）]+?)(?:、代表[^）]*)?）"
)

# 住所のみ（資本金・代表なし）: カッコ内5文字以上
_BODY_RE_ADDR_ONLY = re.compile(r"（([^（）]{5,})）")

# ヘッダー行に現れる記事区分ラベル（is_followup 判定用。列位置には影響しない）
_HEADER_LABELS = {"続報", "新報"}
_EMP_RE       = re.compile(r"従業員(\d+)名")
_BANKR_TYPE_RE = re.compile(r"破産|民事再生|会社更生|特別清算|自己破産|申請|開始決定|命令")

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

def _parse_liabilities_amount(text: Optional[str]) -> Optional[int]:
    """「約1億2000万円」「５７億円」などを万円単位の整数に変換する。"""
    return _parse_capital_amount(text)


def _split_registered_address(addr: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    """住所文字列を現住所と登記面住所に分離する。

    「XXX、登記面＝YYY」→ (XXX, YYY)
    「登記面＝YYY」     → (None, YYY)
    それ以外           → (addr, None)
    """
    if not addr:
        return None, None
    m = re.match(r"^(.+?)、登記面[＝=](.+)$", addr.strip())
    if m:
        return m.group(1).strip() or None, m.group(2).strip() or None
    m2 = re.match(r"^登記面[＝=](.+)$", addr.strip())
    if m2:
        return None, m2.group(1).strip() or None
    return addr.strip() or None, None


_NON_REP_PREFIX = re.compile(r"^(?:清算人|管財人|破産管財人|監督委員|民事再生監督委員)")


def _header_lines(soup: BeautifulSoup) -> tuple[list[str], bool]:
    """p.whitespace-pre-wrap > span.md:hidden のテキストを行リストで返す。

    Returns:
        (lines, is_followup)
        lines      : 全行（フィルタなし）。bankruptcy_type / liabilities は内容ベースで識別する
        is_followup: 「続報」ラベルが含まれていた場合 True
    """
    pwp = soup.find("p", class_=lambda c: c and "whitespace-pre-wrap" in c)
    if not pwp:
        return [], False
    spans = pwp.find_all("span", class_=lambda c: c and "md:hidden" in c)
    if not spans:
        return [], False
    lines = [ln.strip() for ln in spans[0].get_text().split("\n") if ln.strip()]
    is_followup = any(l in _HEADER_LABELS for l in lines)
    return lines, is_followup


def _find_type_and_liab(lines: list[str]) -> tuple[Optional[str], Optional[str]]:
    """ヘッダー行リストから bankruptcy_type と liabilities_text を内容ベースで抽出する。

    「負債」で始まる行を liabilities_text とし、その直前行を bankruptcy_type とする。
    これにより「今年最大の倒産」「続報」などの注目タグが何行あっても正しく識別できる。
    """
    liab_idx = next((i for i, l in enumerate(lines) if l.startswith("負債")), None)
    if liab_idx is None:
        # 負債行なし：lines[3:] を倒産キーワードでスキャンして bankruptcy_type を特定
        type_line = None
        for l in lines[3:]:
            if _BANKR_TYPE_RE.search(l):
                type_line = l
                break
        return type_line, None
    liab_line = lines[liab_idx]
    type_line = lines[liab_idx - 1] if liab_idx > 0 else None
    return type_line, liab_line


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
    Optional[str], Optional[int], Optional[str], Optional[str],
    Optional[str], Optional[int], Optional[str]
]:
    """本文 p（classなし）カッコ内から以下を抽出する。

    Returns:
        (cap_text, cap_amount, address, registered_address, rep, emp, former_name)

    パターン優先順:
      1. _BODY_RE       — 丸括弧（旧商号・資本金任意＋住所＋代表者氏）
      2. _BODY_RE_ANGLE — 山括弧＜＞（旧商号・資本金任意＋住所＋代表者氏）
      3. _BODY_RE_NO_REP — 丸括弧・代表者なし（資本金＋住所）
      4. _BODY_RE_ADDR_ONLY — 住所のみ（5文字以上）
    """
    main = soup.find("main")
    if not main:
        return None, None, None, None, None, None, None

    for p in main.find_all("p"):
        if p.get("class"):
            continue
        raw = p.get_text(strip=True)
        # 半角括弧を全角に正規化（入力ミスによる混在に対応）
        txt = raw.replace("(", "（").replace(")", "）")

        m = _BODY_RE.search(txt)
        if m:
            former_name = m.group(1).strip() if m.group(1) else None
            cap_text    = m.group(2).strip() if m.group(2) else None
            addr_raw    = m.group(3).strip()
            rep_raw     = m.group(4).strip()
            emp_str     = m.group(5)
            rep         = None if _NON_REP_PREFIX.match(rep_raw) else (rep_raw or None)
            address, registered = _split_registered_address(addr_raw)
            return (
                cap_text,
                _parse_capital_amount(cap_text),
                address,
                registered,
                rep,
                int(emp_str) if emp_str else None,
                former_name,
            )

        ma = _BODY_RE_ANGLE.search(txt)
        if ma:
            former_name = ma.group(1).strip() if ma.group(1) else None
            cap_text    = ma.group(2).strip() if ma.group(2) else None
            addr_raw    = ma.group(3).strip()
            rep_raw     = ma.group(4).strip()
            emp_str     = ma.group(5)
            rep         = None if _NON_REP_PREFIX.match(rep_raw) else (rep_raw or None)
            address, registered = _split_registered_address(addr_raw)
            return (
                cap_text,
                _parse_capital_amount(cap_text),
                address,
                registered,
                rep,
                int(emp_str) if emp_str else None,
                former_name,
            )

        m2 = _BODY_RE_NO_REP.search(txt)
        if m2:
            former_name = m2.group(1).strip() if m2.group(1) else None
            cap_text    = m2.group(2).strip() if m2.group(2) else None
            addr_raw    = m2.group(3).strip()
            address, registered = _split_registered_address(addr_raw)
            return (
                cap_text,
                _parse_capital_amount(cap_text),
                address,
                registered,
                None,
                None,
                former_name,
            )

        m3 = _BODY_RE_ADDR_ONLY.search(txt)
        if m3:
            address, registered = _split_registered_address(m3.group(1).strip())
            return None, None, address, registered, None, None, None

    return None, None, None, None, None, None, None


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
            lines, is_followup = _header_lines(soup)
            code_line = lines[0] if len(lines) >= 1 else ""
            loc_line  = lines[1] if len(lines) >= 2 else ""
            biz_line  = lines[2] if len(lines) >= 3 else None
            # Fix1: 注目タグによる列ずれ対策 — 内容ベースで識別
            type_line, liab_line = _find_type_and_liab(lines)

            code_m = _TDB_CODE_RE.search(code_line)
            tdb_company_code = code_m.group(1).strip() if code_m else None

            prefecture, city = _split_pref_city(loc_line) if loc_line else (None, None)

            # 本文カッコ内
            cap_text, cap_amount, address, registered_address, rep, emp, former_name = _body_fields(soup)

            parsed.append(TdbDetailParseResult(
                case_id                  = r.case_id,
                company_name             = (
                    soup.find("h1", class_=lambda c: c and "text-title-1-b" in c)
                    .get_text(strip=True)
                    if soup.find("h1", class_=lambda c: c and "text-title-1-b" in c)
                    else None
                ),
                published_at             = _published_at(soup),
                tdb_company_code         = tdb_company_code,
                prefecture               = prefecture,
                city                     = city,
                business_description     = biz_line or None,
                bankruptcy_type          = type_line or None,
                liabilities_text         = liab_line or None,
                liabilities_amount       = _parse_liabilities_amount(liab_line),
                is_followup              = is_followup,
                former_name              = former_name,
                body_capital_text        = cap_text,
                body_capital_amount      = cap_amount,
                body_address             = address,
                body_registered_address  = registered_address,
                body_representative      = rep,
                body_employees           = emp,
                body_text                = _body_text(soup),
                html_path                = r.html_path,
                detail_scraped_at        = scraped_at,
                success                  = True,
            ))

        except Exception as e:
            logger.warning("パースエラー %s: %s", r.case_id, e)
            parsed.append(TdbDetailParseResult(
                case_id=r.case_id, company_name=None, published_at=None,
                tdb_company_code=None, prefecture=None, city=None,
                business_description=None, bankruptcy_type=None,
                liabilities_text=None, liabilities_amount=None, is_followup=False,
                former_name=None, body_capital_text=None,
                body_capital_amount=None, body_address=None,
                body_registered_address=None,
                body_representative=None, body_employees=None,
                body_text=None, html_path=None, detail_scraped_at=scraped_at,
                success=False, error=str(e),
            ))
            errors += 1

    ok = len(parsed) - errors
    logger.info("TDB 詳細パース完了: 入力=%d件, 成功=%d件, エラー=%d件",
                len(results), ok, errors)
    return parsed
