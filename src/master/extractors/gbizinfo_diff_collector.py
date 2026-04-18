# -*- coding: utf-8 -*-
"""
gBizINFO 法人情報 差分API フェッチモジュール

[API仕様]
  エンドポイント: https://api.info.gbiz.go.jp/hojin/v2/hojin/updateInfo/{suffix}
  パラメータ:
    page          : ページ番号（1始まり）
    from          : 取得開始日 (YYYYMMDD)
    to            : 取得終了日 (YYYYMMDD)
    metadata_flg  : false（メタデータ取得不要）
  ヘッダー:
    X-hojinInfo-api-token : APIトークン

[対応データセット]
  dataset          suffix (updateInfo/...)   テーブル
  ─────────────────────────────────────────────────────
  kihonjoho        (なし)                    gbiz_companies
  todokedenintei   certification              gbiz_todokedenintei
  hyoshojoho       commendation               gbiz_commendation
  zaimujoho        finance                    gbiz_zaimu
  tokkyojoho       patent                     gbiz_patent
  chotatsujoho     procurement                gbiz_procurement
  hojokinjoho      subsidy                    gbiz_subsidy
  shokubajoho      workplace                  gbiz_workplace

[ページネーション]
  レスポンスの totalPage を見て、全ページ分をループ取得する。
"""

import logging
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.config import get_gbizinfo_api_token

# ---------------------------------------------------------------------------
# 定数
# ---------------------------------------------------------------------------

API_BASE_URL = "https://api.info.gbiz.go.jp/hojin/v2/hojin/updateInfo"
API_TIMEOUT  = 30   # HTTP タイムアウト（秒）

# dataset キー → URL サフィックス（空文字列はベースエンドポイント）
ENDPOINT_SUFFIX: dict[str, str] = {
    "kihonjoho":          "",
    "todokedeninteijoho": "certification",
    "hyoshojoho":         "commendation",
    "zaimujoho":          "finance",
    "tokkyojoho":         "patent",
    "chotatsujoho":       "procurement",
    "hojokinjoho":        "subsidy",
    "shokubajoho":        "workplace",
}

DATASET_KEYS = list(ENDPOINT_SUFFIX.keys())

# ---------------------------------------------------------------------------
# ロガー
# ---------------------------------------------------------------------------

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 内部ユーティリティ
# ---------------------------------------------------------------------------

def _list_to_str(v) -> "str | None":
    """配列 → パイプ区切り文字列。空・None は None を返す。"""
    if not v:
        return None
    return "|".join(str(x) for x in v)


def _fetch_page(
    token: str,
    suffix: str,
    from_date: str,
    to_date: str,
    page: int,
    retry: int = 3,
    backoff: float = 2.0,
) -> dict:
    """差分APIの1ページ分を取得し、JSONレスポンスを返す。"""
    url = f"{API_BASE_URL}/{suffix}" if suffix else API_BASE_URL
    params = {
        "page": page,
        "from": from_date,
        "to":   to_date,
        "metadata_flg": "false",
    }
    headers = {
        "accept": "application/json",
        "X-hojinInfo-api-token": token,
    }

    last_exc = None
    for attempt in range(retry):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=API_TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except (requests.RequestException, ValueError) as exc:
            last_exc = exc
            wait = backoff ** attempt
            logger.warning(
                "APIリクエスト失敗 (suffix=%r, page=%d, attempt=%d/%d): %s -> %.1f秒後にリトライ",
                suffix, page, attempt + 1, retry, exc, wait,
            )
            time.sleep(wait)

    raise RuntimeError(
        f"APIリクエストが {retry} 回失敗しました (suffix={suffix!r}, page={page}): {last_exc}"
    )


# ---------------------------------------------------------------------------
# データセット別ノーマライザー
# ---------------------------------------------------------------------------

def _norm_kihonjoho(h: dict) -> dict:
    """updateInfo (基本情報) レスポンス1件を正規化。"""
    return {
        "corporate_number":    h.get("corporate_number"),
        "name":                h.get("name"),
        "kana":                h.get("kana"),
        "name_en":             h.get("name_en"),
        "postal_code":         h.get("postal_code"),
        "location":            h.get("location"),
        "process":             h.get("process"),
        "status":              h.get("status"),
        "close_date":          h.get("close_date"),
        "close_cause":         h.get("close_cause"),
        "kind":                h.get("kind"),
        "representative_name": h.get("representative_name"),
        "capital_stock":       h.get("capital_stock"),
        "employee_number":     h.get("employee_number"),
        "company_size_male":   h.get("company_size_male"),
        "company_size_female": h.get("company_size_female"),
        "business_summary":    h.get("business_summary"),
        "company_url":         h.get("company_url"),
        "founding_year":       h.get("founding_year"),
        "date_of_establishment": h.get("date_of_establishment"),
        "qualification_grade": h.get("qualification_grade"),
        "update_date":         h.get("update_date"),
        "business_items":      _list_to_str(h.get("business_items")),
    }


def _norm_todokedeninteijoho(h: dict) -> dict:
    """updateInfo/certification レスポンス1件を正規化。

    APIフィールド → DBカラム:
      date_of_approval    → certification_date
      government_departments → issuer
      category            → department
    """
    # certificationが1レコード内にネストされている場合に対応
    cert = h.get("certification") or h
    return {
        "corporate_number":    h.get("corporate_number"),
        "name":                h.get("name"),
        "location":            h.get("location"),
        "certification_date":  cert.get("date_of_approval"),
        "title":               cert.get("title"),
        "target":              cert.get("target"),
        "issuer":              cert.get("government_departments"),
        "department":          cert.get("category"),
    }


def _norm_hyoshojoho(h: dict) -> dict:
    """updateInfo/commendation レスポンス1件を正規化。

    APIフィールド → DBカラム:
      date_of_commendation → certification_date
      category             → department
      government_departments → issuer
      note                 → remarks
    """
    comm = h.get("commendation") or h
    return {
        "corporate_number":  h.get("corporate_number"),
        "name":              h.get("name"),
        "location":          h.get("location"),
        "certification_date": comm.get("date_of_commendation"),
        "title":             comm.get("title"),
        "target":            comm.get("target"),
        "department":        comm.get("category"),
        "issuer":            comm.get("government_departments"),
        "remarks":           comm.get("note"),
    }


def _norm_chotatsujoho(h: dict) -> dict:
    """updateInfo/procurement レスポンス1件を正規化。

    APIフィールド → DBカラム:
      date_of_order       → order_date
      amount              → contract_price
      government_departments → organization_name
      note                → remarks
    """
    proc = h.get("procurement") or h
    return {
        "corporate_number":  h.get("corporate_number"),
        "name":              h.get("name"),
        "location":          h.get("location"),
        "order_date":        proc.get("date_of_order"),
        "title":             proc.get("title"),
        "contract_price":    proc.get("amount"),
        "organization_name": proc.get("government_departments"),
        "remarks":           proc.get("note"),
    }


def _norm_hojokinjoho(h: dict) -> dict:
    """updateInfo/subsidy レスポンス1件を正規化。

    APIフィールド → DBカラム:
      date_of_approval    → certification_date
      government_departments → issuer
    """
    sub = h.get("subsidy") or h
    return {
        "corporate_number":  h.get("corporate_number"),
        "name":              h.get("name"),
        "location":          h.get("location"),
        "certification_date": sub.get("date_of_approval"),
        "title":             sub.get("title"),
        "amount":            sub.get("amount"),
        "target":            sub.get("target"),
        "issuer":            sub.get("government_departments"),
    }


def _norm_tokkyojoho(h: dict) -> dict:
    """updateInfo/patent レスポンス1件を正規化。

    APIフィールド → DBカラム:
      classifications (配列) → 各種コードに展開
      url                  → document_fixed_address
    """
    pat = h.get("patent") or h

    # classifications は配列。特許/意匠/商標ごとに異なるコードが入る
    # 例: [{"fi_class": "...", "fi_class_ja": "..."}, {"f_term": "..."}, ...]
    classifications = pat.get("classifications") or []
    fi_code = fi_code_ja = f_term = design_code = design_code_ja = None
    trademark_code = trademark_code_ja = None

    for cls in classifications:
        if isinstance(cls, dict):
            fi_code       = fi_code       or cls.get("fi_classification_code")
            fi_code_ja    = fi_code_ja    or cls.get("fi_classification_code_ja")
            f_term        = f_term        or cls.get("f_term_theme_code")
            design_code   = design_code   or cls.get("design_new_classification_code")
            design_code_ja = design_code_ja or cls.get("design_new_classification_code_ja")
            trademark_code = trademark_code or cls.get("trademark_class_code")
            trademark_code_ja = trademark_code_ja or cls.get("trademark_class_code_ja")

    return {
        "corporate_number":              h.get("corporate_number"),
        "name":                          h.get("name"),
        "location":                      h.get("location"),
        "patent_type":                   pat.get("patent_type"),
        "registration_number":           pat.get("registration_number"),
        "application_date":              pat.get("application_date"),
        "fi_classification_code":        fi_code,
        "fi_classification_code_ja":     fi_code_ja,
        "f_term_theme_code":             f_term,
        "design_new_classification_code":    design_code,
        "design_new_classification_code_ja": design_code_ja,
        "trademark_class_code":          trademark_code,
        "trademark_class_code_ja":       trademark_code_ja,
        "title":                         pat.get("title"),
        "document_fixed_address":        pat.get("url"),
    }


def _norm_zaimujoho(h: dict) -> dict:
    """updateInfo/finance レスポンス1件を正規化。

    長いAPIフィールド名 → 短いDBカラム名に変換。
    major_shareholders 配列 → 大株主1〜5 に展開。
    """
    fin = h.get("finance") or h

    shareholders = fin.get("major_shareholders") or []
    sh: list[dict] = [s if isinstance(s, dict) else {} for s in shareholders]

    def _sh(i: int, key: str):
        return sh[i].get(key) if len(sh) > i else None

    return {
        "corporate_number":   h.get("corporate_number"),
        "name":               h.get("name"),
        "location":           h.get("location"),
        "accounting_standard": fin.get("accounting_standards"),
        "fiscal_period":      fin.get("fiscal_year_cover_page"),
        "term_number":        fin.get("management_index"),
        "net_sales":          fin.get("net_sales_summary_of_business_results"),
        "net_sales_unit":     fin.get("net_sales_summary_of_business_results_unit_ref"),
        "operating_revenue":  fin.get("operating_revenue1_summary_of_business_results"),
        "operating_revenue_unit": fin.get("operating_revenue1_summary_of_business_results_unit_ref"),
        "operating_income":   fin.get("operating_revenue2_summary_of_business_results"),
        "operating_income_unit": fin.get("operating_revenue2_summary_of_business_results_unit_ref"),
        "gross_operating_revenue": fin.get("gross_operating_revenue_summary_of_business_results"),
        "gross_operating_revenue_unit": fin.get("gross_operating_revenue_summary_of_business_results_unit_ref"),
        "ordinary_income":    fin.get("ordinary_income_summary_of_business_results"),
        "ordinary_income_unit": fin.get("ordinary_income_summary_of_business_results_unit_ref"),
        "net_insurance_premium": fin.get("net_premiums_written_summary_of_business_results_ins"),
        "net_insurance_premium_unit": fin.get("net_premiums_written_summary_of_business_results_ins_unit_ref"),
        "recurring_profit":   fin.get("ordinary_income_loss_summary_of_business_results"),
        "recurring_profit_unit": fin.get("ordinary_income_loss_summary_of_business_results_unit_ref"),
        "net_income":         fin.get("net_income_loss_summary_of_business_results"),
        "net_income_unit":    fin.get("net_income_loss_summary_of_business_results_unit_ref"),
        "capital_stock":      fin.get("capital_stock_summary_of_business_results"),
        "capital_stock_unit": fin.get("capital_stock_summary_of_business_results_unit_ref"),
        "net_assets":         fin.get("net_assets_summary_of_business_results"),
        "net_assets_unit":    fin.get("net_assets_summary_of_business_results_unit_ref"),
        "total_assets":       fin.get("total_assets_summary_of_business_results"),
        "total_assets_unit":  fin.get("total_assets_summary_of_business_results_unit_ref"),
        "employee_count":     fin.get("number_of_employees"),
        "employee_count_unit": fin.get("number_of_employees_unit_ref"),
        "major_shareholder_1":  _sh(0, "name_major_shareholders"),
        "shareholder_1_ratio":  _sh(0, "shareholding_ratio"),
        "major_shareholder_2":  _sh(1, "name_major_shareholders"),
        "shareholder_2_ratio":  _sh(1, "shareholding_ratio"),
        "major_shareholder_3":  _sh(2, "name_major_shareholders"),
        "shareholder_3_ratio":  _sh(2, "shareholding_ratio"),
        "major_shareholder_4":  _sh(3, "name_major_shareholders"),
        "shareholder_4_ratio":  _sh(3, "shareholding_ratio"),
        "major_shareholder_5":  _sh(4, "name_major_shareholders"),
        "shareholder_5_ratio":  _sh(4, "shareholding_ratio"),
    }


def _extract_avg_tenure(tenure_list, gender: str) -> "str | None":
    """average_continuous_service_years 配列から特定種別の値を取得する。

    APIは [{"type": "男性", "value": "..."}, ...] のような配列を返す想定。
    """
    if not tenure_list:
        return None
    for item in tenure_list:
        if isinstance(item, dict) and gender in str(item.get("type", "")):
            return item.get("value")
    return None


def _norm_shokubajoho(h: dict) -> dict:
    """updateInfo/workplace レスポンス1件を正規化。

    ネスト構造:
      workplace_info.base_infos            → 平均勤続年数・平均年齢等
      workplace_info.women_activity_infos  → 女性活躍情報
      workplace_info.compatibility_of_childcare_and_work → 育児休業情報
    """
    wp   = h.get("workplace_info") or h
    base = wp.get("base_infos") or wp
    women = wp.get("women_activity_infos") or wp
    child = wp.get("compatibility_of_childcare_and_work") or wp

    tenure_list = base.get("average_continuous_service_years") or []

    return {
        "corporate_number":         h.get("corporate_number"),
        "name":                     h.get("name"),
        "location":                 h.get("location"),
        "avg_tenure_range":         base.get("average_continuous_service_years_type"),
        "avg_tenure_male":          _extract_avg_tenure(tenure_list, "男性"),
        "avg_tenure_female":        _extract_avg_tenure(tenure_list, "女性"),
        "avg_tenure_fulltime":      _extract_avg_tenure(tenure_list, "正社員"),
        "avg_employee_age":         base.get("average_age"),
        "avg_overtime_hours":       base.get("month_average_predetermined_overtime_hours"),
        "female_worker_ratio_range": women.get("female_workers_proportion_type"),
        "female_worker_ratio":      women.get("female_workers_proportion"),
        "female_manager_count":     women.get("female_share_of_manager"),
        "total_manager_count":      women.get("gender_total_of_manager"),
        "female_executive_count":   women.get("female_share_of_officers"),
        "total_executive_count":    women.get("gender_total_of_officers"),
        "childcare_eligible_male":  child.get("number_of_paternity_leave"),
        "childcare_eligible_female": child.get("number_of_maternity_leave"),
        "childcare_takers_male":    child.get("paternity_leave_acquisition_num"),
        "childcare_takers_female":  child.get("maternity_leave_acquisition_num"),
    }


# データセット → ノーマライザー関数
_NORMALIZER = {
    "kihonjoho":          _norm_kihonjoho,
    "todokedeninteijoho": _norm_todokedeninteijoho,
    "hyoshojoho":         _norm_hyoshojoho,
    "chotatsujoho":       _norm_chotatsujoho,
    "hojokinjoho":        _norm_hojokinjoho,
    "tokkyojoho":         _norm_tokkyojoho,
    "zaimujoho":          _norm_zaimujoho,
    "shokubajoho":        _norm_shokubajoho,
}


# ---------------------------------------------------------------------------
# 公開インターフェース
# ---------------------------------------------------------------------------

def fetch_diff(
    dataset: str,
    from_date: "str | date",
    to_date: "str | date",
    wait_between_requests: float = 1.0,
) -> list[dict]:
    """gBizINFO 差分APIから指定データセット・期間の変更法人情報を全ページ取得する。

    Args:
        dataset:               データセットキー（DATASET_KEYS のいずれか）
        from_date:             取得開始日（YYYY-MM-DD または date オブジェクト）
        to_date:               取得終了日（YYYY-MM-DD または date オブジェクト）
        wait_between_requests: リクエスト間の待機秒数（サーバー負荷軽減）

    Returns:
        正規化済みの法人情報辞書リスト
    """
    if dataset not in ENDPOINT_SUFFIX:
        raise ValueError(f"不明なデータセット: {dataset!r}。有効値: {DATASET_KEYS}")

    # date → YYYYMMDD に変換
    if isinstance(from_date, date):
        from_ymd = from_date.strftime("%Y%m%d")
    else:
        from_ymd = from_date.replace("-", "")

    if isinstance(to_date, date):
        to_ymd = to_date.strftime("%Y%m%d")
    else:
        to_ymd = to_date.replace("-", "")

    suffix     = ENDPOINT_SUFFIX[dataset]
    normalizer = _NORMALIZER[dataset]
    token      = get_gbizinfo_api_token()
    all_records: list[dict] = []

    logger.info("gBizINFO 差分取得開始: dataset=%s, %s 〜 %s", dataset, from_ymd, to_ymd)

    # 1ページ目を取得して totalPage を確認
    data        = _fetch_page(token, suffix, from_ymd, to_ymd, page=1)
    total_count = int(data.get("totalCount") or 0)
    total_pages = int(data.get("totalPage") or 1)

    logger.info("  総件数: %d件 (%dページ)", total_count, total_pages)

    if total_count == 0:
        logger.info("  差分データなし。")
        return []

    hojins = data.get("hojin-infos") or []
    all_records.extend(normalizer(h) for h in hojins)
    logger.info("  ページ 1/%d 取得完了 (%d件)", total_pages, len(hojins))

    for page in range(2, total_pages + 1):
        time.sleep(wait_between_requests)
        data   = _fetch_page(token, suffix, from_ymd, to_ymd, page=page)
        hojins = data.get("hojin-infos") or []
        all_records.extend(normalizer(h) for h in hojins)
        logger.info("  ページ %d/%d 取得完了 (%d件)", page, total_pages, len(hojins))

    logger.info("gBizINFO 差分取得完了: dataset=%s, 合計 %d件", dataset, len(all_records))
    return all_records


# ---------------------------------------------------------------------------
# エントリーポイント（単体動作確認用）
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    from src.common.logging_setup import setup_logging
    setup_logging()

    parser = argparse.ArgumentParser(description="gBizINFO 差分APIフェッチ動作確認")
    parser.add_argument("--dataset", default="kihonjoho", choices=DATASET_KEYS)
    parser.add_argument(
        "--from-date", default=str(date.today() - timedelta(days=1)),
        help="取得開始日 (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--to-date", default=str(date.today()),
        help="取得終了日 (YYYY-MM-DD)"
    )
    args = parser.parse_args()

    records = fetch_diff(args.dataset, args.from_date, args.to_date)
    print(f"\n取得レコード数: {len(records)}")
    for r in records[:3]:
        print(r)
