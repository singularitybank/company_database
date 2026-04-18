# -*- coding: utf-8 -*-
"""
gbizinfo SQLite テーブル定義

構成:
  TABLE_CONFIGS  : Parquetキー → {table名, indexes, pk}
  META_CONFIGS   : Parquetキー → {table名}

型マッピング（PyArrow → SQLite）:
  large_string, string → TEXT
  int64, int32         → INTEGER
  double, float        → REAL
  その他               → TEXT
"""

import pyarrow as pa

# PyArrow 型 → SQLite 型
def pa_type_to_sqlite(pa_type: pa.DataType) -> str:
    if pa.types.is_large_string(pa_type) or pa.types.is_string(pa_type):
        return "TEXT"
    if pa.types.is_integer(pa_type):
        return "INTEGER"
    if pa.types.is_floating(pa_type):
        return "REAL"
    return "TEXT"


# ---------------------------------------------------------------------------
# コアテーブル設定
# ---------------------------------------------------------------------------

TABLE_CONFIGS: dict[str, dict] = {
    "kihonjoho": {
        "table":   "gbiz_companies",
        "pk":      "corporate_number",   # UNIQUE扱い（UPSERT対象）
        "indexes": ["kind", "prefecture_code", "close_date", "update_date"],
    },
    "tokkyojoho": {
        "table":   "gbiz_patent",
        "pk":      None,
        "indexes": ["corporate_number", "patent_type", "application_date"],
    },
    "hojokinjoho": {
        "table":   "gbiz_subsidy",
        "pk":      None,
        "indexes": ["corporate_number", "certification_date"],
    },
    "chotatsujoho": {
        "table":   "gbiz_procurement",
        "pk":      None,
        "indexes": ["corporate_number", "order_date"],
    },
    "hyoshojoho": {
        "table":   "gbiz_commendation",
        "pk":      None,
        "indexes": ["corporate_number"],
    },
    "shokubajoho": {
        "table":   "gbiz_workplace",
        "pk":      None,
        "indexes": ["corporate_number"],
    },
    "todokedeninteijoho": {
        "table":   "gbiz_certification",
        "pk":      None,
        "indexes": ["corporate_number", "certification_date"],
    },
    "zaimujoho": {
        "table":   "gbiz_financial",
        "pk":      None,
        "indexes": ["corporate_number", "fiscal_period"],
    },
    "kessanjoho": {
        "table":   "gbiz_financial_statement",
        "pk":      None,
        "indexes": ["corporate_number", "report_name", "kanpou_data_name"],
    },
}

# ---------------------------------------------------------------------------
# メタテーブル設定（全テーブル共通: corporate_number + metadata JSON）
# ---------------------------------------------------------------------------

META_CONFIGS: dict[str, dict] = {
    "kihonjoho":          {"table": "gbiz_companies_meta"},
    "tokkyojoho":         {"table": "gbiz_patent_meta"},
    "hojokinjoho":        {"table": "gbiz_subsidy_meta"},
    "chotatsujoho":       {"table": "gbiz_procurement_meta"},
    "hyoshojoho":         {"table": "gbiz_commendation_meta"},
    "shokubajoho":        {"table": "gbiz_workplace_meta"},
    "todokedeninteijoho": {"table": "gbiz_certification_meta"},
    "zaimujoho":          {"table": "gbiz_financial_meta"},
    "kessanjoho":         {"table": "gbiz_financial_statement_meta"},
}
