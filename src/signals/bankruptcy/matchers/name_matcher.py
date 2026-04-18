# -*- coding: utf-8 -*-
"""
TDB↔TSR 倒産案件 名寄せモジュール

[スコア配点（最大100点）]
  社名類似度      : 60点  rapidfuzz token_sort_ratio（正規化後）
  都道府県一致    : 10点  完全一致
  市区町村一致    : 10点  TDB.city vs TSR NTA住所（companies.db 参照）
  詳細住所類似度  : 10点  TDB.body_address vs NTA住所（rapidfuzz partial_ratio）
  資本金一致      :  5点  TDB.body_capital_amount vs gBizInfo（±10%以内）
  代表者名一致    :  5点  TDB.body_representative vs NTA（rapidfuzz 80以上で満点）

[判定閾値]
  85点以上 → is_confirmed=1（確定マッチ）
  70〜84点 → is_confirmed=0（候補）
  70点未満 → 登録しない

[突合絞り込み]
  - TDB.published_at ±14日 に公開されたTSR案件のみ
  - 都道府県が異なる場合は除外
  - TSR案件が既にTDB案件に confirmed マッチ済みの場合はスキップ

[外部DB参照（読み取りのみ）]
  data/companies.db  → companies テーブル（NTA法人番号リスト、住所・代表者名）
  data/gbizinfo.db   → gbizinfo テーブル（資本金情報）
  bankruptcyDBへの書き込みは行わない（スコア算定時の参照のみ）
"""

import logging
import re
import sqlite3
import sys
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta, date
from pathlib import Path
from typing import Optional

from rapidfuzz import fuzz

sys.path.insert(0, str(Path(__file__).resolve().parents[5]))
from src.config import bankruptcy as _cfg

logger = logging.getLogger(__name__)

JST = timezone(timedelta(hours=9))

# ---------------------------------------------------------------------------
# 定数
# ---------------------------------------------------------------------------

MATCH_WINDOW_DAYS       = _cfg.get("match_window_days", 14)
THRESHOLD_CONFIRMED     = _cfg.get("match_threshold_confirmed", 85)
THRESHOLD_CANDIDATE     = _cfg.get("match_threshold_candidate", 70)

# 法人形態パターン（除去対象）
_CORP_TYPE_RE = re.compile(
    r"(株式会社|有限会社|合同会社|合名会社|合資会社|一般社団法人|一般財団法人"
    r"|公益社団法人|公益財団法人|医療法人|社会福祉法人|学校法人|NPO法人"
    r"|\(株\)|\(有\)|\(合\)|\（株\）|\（有\）|\（合\）)"
)
_BRACKETS_RE  = re.compile(r"[（）「」〔〕【】\(\)\[\]｛｝{}]")
_SPACE_RE     = re.compile(r"[\s\u3000\u2003]+")
_HIRA_RE      = re.compile(r"[\u3041-\u3096]")


# ---------------------------------------------------------------------------
# 社名正規化
# ---------------------------------------------------------------------------

def normalize_name(name: Optional[str]) -> str:
    """社名を比較用に正規化する。"""
    if not name:
        return ""
    # 全角→半角（数字・英字・記号）
    s = unicodedata.normalize("NFKC", name)
    # 法人形態除去
    s = _CORP_TYPE_RE.sub("", s)
    # カッコ除去
    s = _BRACKETS_RE.sub("", s)
    # ひらがな→カタカナ
    s = _HIRA_RE.sub(lambda m: chr(ord(m.group(0)) + 0x60), s)
    # スペース除去
    s = _SPACE_RE.sub("", s)
    # 大文字統一
    return s.upper()


# ---------------------------------------------------------------------------
# 外部DB参照
# ---------------------------------------------------------------------------

def _load_nta(companies_db: str, corporate_number: str) -> dict:
    """NTA法人情報を返す。なければ空dict。"""
    try:
        conn = sqlite3.connect(f"file:{companies_db}?mode=ro", uri=True)
        row = conn.execute(
            "SELECT address, representative_name FROM companies WHERE corporate_number = ?",
            (corporate_number,),
        ).fetchone()
        conn.close()
        if row:
            return {"address": row[0], "rep": row[1]}
    except Exception as e:
        logger.debug("NTA参照エラー: %s", e)
    return {}


def _load_gbiz(gbizinfo_db: str, corporate_number: str) -> dict:
    """gBizInfo資本金情報を返す。なければ空dict。"""
    try:
        conn = sqlite3.connect(f"file:{gbizinfo_db}?mode=ro", uri=True)
        row = conn.execute(
            "SELECT capital_amount FROM gbizinfo WHERE corporate_number = ?",
            (corporate_number,),
        ).fetchone()
        conn.close()
        if row and row[0] is not None:
            return {"capital_amount": row[0]}
    except Exception as e:
        logger.debug("gBizInfo参照エラー: %s", e)
    return {}


# ---------------------------------------------------------------------------
# スコア算定
# ---------------------------------------------------------------------------

@dataclass
class MatchScore:
    total:    float
    name:     float
    address:  float
    capital:  float
    rep:      float


def _score(
    tdb: dict,
    tsr: dict,
    nta: dict,
    gbiz: dict,
) -> MatchScore:
    """TDB案件 × TSR案件 の複合スコアを算定する。"""

    # --- 社名 (60点) ---
    name_score = fuzz.token_sort_ratio(
        normalize_name(tdb.get("company_name")),
        normalize_name(tsr.get("company_name")),
    ) * 0.60

    # --- 都道府県 (10点) ---
    pref_score = 10.0 if (
        tdb.get("prefecture") and tsr.get("prefecture")
        and tdb["prefecture"] == tsr["prefecture"]
    ) else 0.0

    # --- 市区町村 (10点) ---
    nta_address = nta.get("address", "") or ""
    city_score = 0.0
    tdb_city = tdb.get("city") or ""
    if tdb_city and tdb_city in nta_address:
        city_score = 10.0

    # --- 詳細住所類似度 (10点) ---
    tdb_addr = tdb.get("body_address") or ""
    addr_score = 0.0
    if tdb_addr and nta_address:
        addr_score = fuzz.partial_ratio(tdb_addr, nta_address) * 0.10

    # --- 資本金 (5点) ---
    cap_score = 0.0
    tdb_cap = tdb.get("body_capital_amount")
    gbiz_cap = gbiz.get("capital_amount")
    if tdb_cap and gbiz_cap and gbiz_cap > 0:
        ratio = abs(tdb_cap - gbiz_cap) / gbiz_cap
        if ratio <= 0.10:
            cap_score = 5.0

    # --- 代表者名 (5点) ---
    rep_score = 0.0
    tdb_rep = tdb.get("body_representative") or ""
    nta_rep  = nta.get("rep") or ""
    if tdb_rep and nta_rep:
        r = fuzz.ratio(tdb_rep, nta_rep)
        if r >= 80:
            rep_score = 5.0

    total = name_score + pref_score + city_score + addr_score + cap_score + rep_score

    return MatchScore(
        total=round(total, 2),
        name=round(name_score, 2),
        address=round(city_score + addr_score, 2),
        capital=round(cap_score, 2),
        rep=round(rep_score, 2),
    )


# ---------------------------------------------------------------------------
# 日付ウィンドウ
# ---------------------------------------------------------------------------

def _to_date(s: Optional[str]) -> Optional[date]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s[:10]).date()
    except Exception:
        return None


# ---------------------------------------------------------------------------
# メイン処理
# ---------------------------------------------------------------------------

@dataclass
class MatchResult:
    tdb_case_id:      str
    tsr_case_id:      str
    corporate_number: Optional[str]
    match_score:      float
    name_score:       float
    address_score:    float
    capital_score:    float
    rep_score:        float
    match_method:     str
    is_confirmed:     int


def run_matching(
    bankruptcy_conn: sqlite3.Connection,
    companies_db: Optional[str] = None,
    gbizinfo_db: Optional[str] = None,
) -> list[MatchResult]:
    """未マッチの TDB 案件 × TSR 案件を突合してスコアを算定し、閾値以上を登録する。

    Args:
        bankruptcy_conn : bankruptcy.db の接続（read/write）
        companies_db    : data/companies.db のパス（NTA参照用、None で住所/代表者スキップ）
        gbizinfo_db     : data/gbizinfo.db のパス（gBizInfo参照用、None で資本金スキップ）

    Returns:
        登録した MatchResult のリスト
    """
    matched_at = datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")

    # 未マッチ TDB 案件（bankruptcy_matches に is_confirmed=1 で登録されていないもの）
    tdb_rows = bankruptcy_conn.execute("""
        SELECT t.case_id, t.company_name, t.prefecture, t.city,
               t.body_address, t.body_capital_amount, t.body_representative,
               t.published_at
        FROM tdb_cases t
        LEFT JOIN bankruptcy_matches m
            ON t.case_id = m.tdb_case_id AND m.is_confirmed = 1
        WHERE m.match_id IS NULL
          AND t.detail_scraped_at IS NOT NULL
    """).fetchall()

    tdb_keys = ["case_id", "company_name", "prefecture", "city",
                "body_address", "body_capital_amount", "body_representative", "published_at"]
    tdb_list = [dict(zip(tdb_keys, r)) for r in tdb_rows]

    # 全 TSR 案件（detail_scraped_at IS NOT NULL のもの）
    tsr_rows = bankruptcy_conn.execute("""
        SELECT case_id, company_name, corporate_number, prefecture, published_at
        FROM tsr_cases
        WHERE detail_scraped_at IS NOT NULL
    """).fetchall()
    tsr_keys = ["case_id", "company_name", "corporate_number", "prefecture", "published_at"]
    tsr_list = [dict(zip(tsr_keys, r)) for r in tsr_rows]

    results: list[MatchResult] = []
    new_matches = 0

    for tdb in tdb_list:
        tdb_date = _to_date(tdb["published_at"])
        best: Optional[tuple[float, dict]] = None

        for tsr in tsr_list:
            # 日付ウィンドウ
            tsr_date = _to_date(tsr["published_at"])
            if tdb_date and tsr_date:
                if abs((tdb_date - tsr_date).days) > MATCH_WINDOW_DAYS:
                    continue

            # 都道府県フィルタ
            if tdb.get("prefecture") and tsr.get("prefecture"):
                if tdb["prefecture"] != tsr["prefecture"]:
                    continue

            # 外部DB参照
            corp_num = tsr.get("corporate_number") or ""
            nta  = _load_nta(companies_db, corp_num)  if companies_db and corp_num else {}
            gbiz = _load_gbiz(gbizinfo_db, corp_num) if gbizinfo_db and corp_num else {}

            sc = _score(tdb, tsr, nta, gbiz)

            if sc.total < THRESHOLD_CANDIDATE:
                continue

            if best is None or sc.total > best[0]:
                best = (sc.total, tsr, sc)

        if best is None:
            continue

        total, tsr, sc = best
        is_confirmed = 1 if total >= THRESHOLD_CONFIRMED else 0

        # 重複チェック: 既に同じペアが登録されている場合はスキップ
        existing = bankruptcy_conn.execute(
            "SELECT match_id FROM bankruptcy_matches WHERE tdb_case_id=? AND tsr_case_id=?",
            (tdb["case_id"], tsr["case_id"]),
        ).fetchone()
        if existing:
            continue

        bankruptcy_conn.execute("""
            INSERT INTO bankruptcy_matches
                (tdb_case_id, tsr_case_id, corporate_number,
                 match_score, name_score, address_score, capital_score, rep_score,
                 match_method, is_confirmed, matched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            tdb["case_id"], tsr["case_id"], tsr.get("corporate_number"),
            sc.total, sc.name, sc.address, sc.capital, sc.rep,
            "fuzzy_composite", is_confirmed, matched_at,
        ))
        new_matches += 1

        results.append(MatchResult(
            tdb_case_id      = tdb["case_id"],
            tsr_case_id      = tsr["case_id"],
            corporate_number = tsr.get("corporate_number"),
            match_score      = sc.total,
            name_score       = sc.name,
            address_score    = sc.address,
            capital_score    = sc.capital,
            rep_score        = sc.rep,
            match_method     = "fuzzy_composite",
            is_confirmed     = is_confirmed,
        ))

    bankruptcy_conn.commit()
    logger.info("名寄せ完了: TDB %d件 × TSR %d件 → マッチ %d件（確定=%d件）",
                len(tdb_list), len(tsr_list), new_matches,
                sum(1 for r in results if r.is_confirmed))
    return results
