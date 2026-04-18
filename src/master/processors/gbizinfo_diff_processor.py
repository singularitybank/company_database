# -*- coding: utf-8 -*-
"""
gBizINFO 差分データ DB適用モジュール

[対象DB]
  data/gbizinfo.db

[データセット別処理方針]
  kihonjoho     : corporate_number を主キーとして新設/更新/スキップを判定
  その他 6種    : 複合ユニークキーで INSERT OR IGNORE（同一レコードの重複を防止）

[注意]
  gBizINFO の updateInfo API は住所を結合済み文字列 (location) で返すため、
  prefecture_name / prefecture_code / city_name / city_code / street_number は
  APIから取得できない。既存レコードの当該フィールドは保持する（上書きしない）。
"""

import logging
import sqlite3
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

# ---------------------------------------------------------------------------
# 定数
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parents[2]
DB_PATH   = REPO_ROOT / "data" / "gbizinfo.db"

# kihonjoho の変更検出フィールド
_KIHON_TRACKED = [
    "name", "kana", "name_en", "postal_code", "location",
    "process", "status", "close_date", "close_cause", "kind",
    "representative_name",
]

# kihonjoho のAPIカラム（corporate_number を含む）
_KIHON_COLS = [
    "corporate_number", "name", "kana", "name_en", "postal_code", "location",
    "process", "status", "close_date", "close_cause", "kind",
    "representative_name", "capital_stock", "employee_number",
    "company_size_male", "company_size_female", "business_summary",
    "company_url", "founding_year", "date_of_establishment",
    "qualification_grade", "update_date", "business_items",
]

# ---------------------------------------------------------------------------
# テーブル定義（DDL + ユニーク制約）
# ---------------------------------------------------------------------------

# テーブル名、DDL、ユニーク制約列のマッピング
_TABLE_CONFIG: dict[str, dict] = {
    "kihonjoho": {
        "table": "gbiz_companies",
        "ddl": """
            CREATE TABLE IF NOT EXISTS gbiz_companies (
                corporate_number      TEXT PRIMARY KEY,
                name                  TEXT,
                kana                  TEXT,
                name_en               TEXT,
                close_date            TEXT,
                close_cause           TEXT,
                location              TEXT,
                postal_code           TEXT,
                prefecture_name       TEXT,
                prefecture_code       TEXT,
                city_name             TEXT,
                city_code             TEXT,
                street_number         TEXT,
                kind                  TEXT,
                process               TEXT,
                correct               TEXT,
                status                TEXT,
                representative_name   TEXT,
                capital_stock         INTEGER,
                employee_number       INTEGER,
                company_size_male     INTEGER,
                company_size_female   INTEGER,
                business_summary      TEXT,
                company_url           TEXT,
                founding_year         TEXT,
                business_items        TEXT,
                date_of_establishment TEXT,
                qualification_grade   TEXT,
                business_category     TEXT,
                update_date           TEXT,
                loaded_at             TEXT
            )
        """,
        "indexes": [
            "CREATE INDEX IF NOT EXISTS idx_gbiz_companies_kind ON gbiz_companies (kind)",
            "CREATE INDEX IF NOT EXISTS idx_gbiz_companies_update_date ON gbiz_companies (update_date)",
        ],
        "unique_cols": None,  # PK のみ
    },
    "todokedeninteijoho": {
        "table": "gbiz_todokedenintei",
        "ddl": """
            CREATE TABLE IF NOT EXISTS gbiz_todokedenintei (
                corporate_number  TEXT,
                name              TEXT,
                location          TEXT,
                certification_date TEXT,
                title             TEXT,
                target            TEXT,
                department        TEXT,
                issuer            TEXT,
                loaded_at         TEXT
            )
        """,
        "indexes": [
            "CREATE INDEX IF NOT EXISTS idx_gbiz_todokede_cn ON gbiz_todokedenintei (corporate_number)",
        ],
        "unique_cols": ["corporate_number", "certification_date", "title"],
    },
    "hyoshojoho": {
        "table": "gbiz_commendation",
        "ddl": """
            CREATE TABLE IF NOT EXISTS gbiz_commendation (
                corporate_number  TEXT,
                name              TEXT,
                location          TEXT,
                certification_date TEXT,
                title             TEXT,
                target            TEXT,
                department        TEXT,
                issuer            TEXT,
                remarks           TEXT,
                loaded_at         TEXT
            )
        """,
        "indexes": [
            "CREATE INDEX IF NOT EXISTS idx_gbiz_commendation_cn ON gbiz_commendation (corporate_number)",
        ],
        "unique_cols": ["corporate_number", "certification_date", "title"],
    },
    "chotatsujoho": {
        "table": "gbiz_procurement",
        "ddl": """
            CREATE TABLE IF NOT EXISTS gbiz_procurement (
                corporate_number  TEXT,
                name              TEXT,
                location          TEXT,
                order_date        TEXT,
                title             TEXT,
                contract_price    TEXT,
                organization_name TEXT,
                remarks           TEXT,
                loaded_at         TEXT
            )
        """,
        "indexes": [
            "CREATE INDEX IF NOT EXISTS idx_gbiz_procurement_cn ON gbiz_procurement (corporate_number)",
        ],
        "unique_cols": ["corporate_number", "order_date", "title"],
    },
    "hojokinjoho": {
        "table": "gbiz_subsidy",
        "ddl": """
            CREATE TABLE IF NOT EXISTS gbiz_subsidy (
                corporate_number  TEXT,
                name              TEXT,
                location          TEXT,
                certification_date TEXT,
                title             TEXT,
                amount            TEXT,
                target            TEXT,
                issuer            TEXT,
                loaded_at         TEXT
            )
        """,
        "indexes": [
            "CREATE INDEX IF NOT EXISTS idx_gbiz_subsidy_cn ON gbiz_subsidy (corporate_number)",
        ],
        "unique_cols": ["corporate_number", "certification_date", "title"],
    },
    "tokkyojoho": {
        "table": "gbiz_patent",
        "ddl": """
            CREATE TABLE IF NOT EXISTS gbiz_patent (
                corporate_number              TEXT,
                name                          TEXT,
                location                      TEXT,
                patent_type                   TEXT,
                registration_number           TEXT,
                application_date              TEXT,
                fi_classification_code        TEXT,
                fi_classification_code_ja     TEXT,
                f_term_theme_code             TEXT,
                design_new_classification_code     TEXT,
                design_new_classification_code_ja  TEXT,
                trademark_class_code          TEXT,
                trademark_class_code_ja       TEXT,
                title                         TEXT,
                document_fixed_address        TEXT,
                loaded_at                     TEXT
            )
        """,
        "indexes": [
            "CREATE INDEX IF NOT EXISTS idx_gbiz_patent_cn ON gbiz_patent (corporate_number)",
            "CREATE INDEX IF NOT EXISTS idx_gbiz_patent_type ON gbiz_patent (patent_type)",
        ],
        "unique_cols": ["corporate_number", "registration_number"],
    },
    "zaimujoho": {
        "table": "gbiz_zaimu",
        "ddl": """
            CREATE TABLE IF NOT EXISTS gbiz_zaimu (
                corporate_number              TEXT,
                name                          TEXT,
                location                      TEXT,
                accounting_standard           TEXT,
                fiscal_period                 TEXT,
                term_number                   TEXT,
                net_sales                     TEXT,
                net_sales_unit                TEXT,
                operating_revenue             TEXT,
                operating_revenue_unit        TEXT,
                operating_income              TEXT,
                operating_income_unit         TEXT,
                gross_operating_revenue       TEXT,
                gross_operating_revenue_unit  TEXT,
                ordinary_income               TEXT,
                ordinary_income_unit          TEXT,
                net_insurance_premium         TEXT,
                net_insurance_premium_unit    TEXT,
                recurring_profit              TEXT,
                recurring_profit_unit         TEXT,
                net_income                    TEXT,
                net_income_unit               TEXT,
                capital_stock                 TEXT,
                capital_stock_unit            TEXT,
                net_assets                    TEXT,
                net_assets_unit               TEXT,
                total_assets                  TEXT,
                total_assets_unit             TEXT,
                employee_count                TEXT,
                employee_count_unit           TEXT,
                major_shareholder_1           TEXT,
                shareholder_1_ratio           TEXT,
                major_shareholder_2           TEXT,
                shareholder_2_ratio           TEXT,
                major_shareholder_3           TEXT,
                shareholder_3_ratio           TEXT,
                major_shareholder_4           TEXT,
                shareholder_4_ratio           TEXT,
                major_shareholder_5           TEXT,
                shareholder_5_ratio           TEXT,
                loaded_at                     TEXT
            )
        """,
        "indexes": [
            "CREATE INDEX IF NOT EXISTS idx_gbiz_zaimu_cn ON gbiz_zaimu (corporate_number)",
        ],
        "unique_cols": ["corporate_number", "fiscal_period", "accounting_standard"],
    },
    "shokubajoho": {
        "table": "gbiz_workplace",
        "ddl": """
            CREATE TABLE IF NOT EXISTS gbiz_workplace (
                corporate_number          TEXT,
                name                      TEXT,
                location                  TEXT,
                avg_tenure_range          TEXT,
                avg_tenure_male           TEXT,
                avg_tenure_female         TEXT,
                avg_tenure_fulltime       TEXT,
                avg_employee_age          TEXT,
                avg_overtime_hours        TEXT,
                female_worker_ratio_range TEXT,
                female_worker_ratio       TEXT,
                female_manager_count      TEXT,
                total_manager_count       TEXT,
                female_executive_count    TEXT,
                total_executive_count     TEXT,
                childcare_eligible_male   TEXT,
                childcare_eligible_female TEXT,
                childcare_takers_male     TEXT,
                childcare_takers_female   TEXT,
                loaded_at                 TEXT
            )
        """,
        "indexes": [
            "CREATE INDEX IF NOT EXISTS idx_gbiz_workplace_cn ON gbiz_workplace (corporate_number)",
        ],
        "unique_cols": ["corporate_number"],
    },
}

# ---------------------------------------------------------------------------
# ロガー
# ---------------------------------------------------------------------------

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 結果レポート
# ---------------------------------------------------------------------------

@dataclass
class GBizDiffResult:
    dataset:  str
    inserted: int = 0
    updated:  int = 0
    skipped:  int = 0
    errors: list[str] = field(default_factory=list)

    @property
    def total(self) -> int:
        return self.inserted + self.updated + self.skipped

    def summary(self) -> str:
        return (
            f"[{self.dataset}] "
            f"新設={self.inserted}, 更新={self.updated}, "
            f"変更なし={self.skipped}, エラー={len(self.errors)}"
        )


# ---------------------------------------------------------------------------
# 内部ユーティリティ
# ---------------------------------------------------------------------------

def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _open_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def _setup_table(conn: sqlite3.Connection, cfg: dict) -> None:
    """テーブルと付随インデックスを作成する。ユニーク制約も追加する。"""
    conn.execute(cfg["ddl"])
    for idx_sql in cfg.get("indexes") or []:
        conn.execute(idx_sql)

    # 複合ユニーク制約（kihonjoho 以外）
    unique_cols = cfg.get("unique_cols")
    if unique_cols:
        table = cfg["table"]
        idx_name = f"uq_{table}_{'_'.join(unique_cols)}"
        conn.execute(
            f"CREATE UNIQUE INDEX IF NOT EXISTS {idx_name} "
            f"ON {table} ({', '.join(unique_cols)})"
        )
    conn.commit()


def _get_columns(row: dict, exclude: "list[str] | None" = None) -> list[str]:
    """row のキーのうち、exclude に含まれないものをリストで返す。"""
    excl = set(exclude or [])
    return [k for k in row if k not in excl]


# ---------------------------------------------------------------------------
# kihonjoho 専用処理（差分検出 + UPDATE/INSERT）
# ---------------------------------------------------------------------------

def _fetch_existing_kihon(conn: sqlite3.Connection, corp_nums: list[str]) -> dict[str, dict]:
    if not corp_nums:
        return {}
    ph  = ",".join(["?"] * len(corp_nums))
    sql = f"SELECT {', '.join(_KIHON_COLS)} FROM gbiz_companies WHERE corporate_number IN ({ph})"
    cur = conn.execute(sql, corp_nums)
    cols = [d[0] for d in cur.description]
    return {row[0]: dict(zip(cols, row)) for row in cur.fetchall()}


def _detect_changes(existing: dict, incoming: dict) -> bool:
    for f in _KIHON_TRACKED:
        old = existing.get(f) or None
        new = incoming.get(f) or None
        if old != new:
            return True
    return False


def _apply_kihonjoho(
    conn: sqlite3.Connection,
    records: list[dict],
    batch_size: int,
    loaded_at: str,
) -> GBizDiffResult:
    result = GBizDiffResult(dataset="kihonjoho")

    for bs in range(0, len(records), batch_size):
        batch    = records[bs: bs + batch_size]
        corp_nums = [r["corporate_number"] for r in batch if r.get("corporate_number")]
        existing  = _fetch_existing_kihon(conn, corp_nums)
        cur       = conn.cursor()

        for row in batch:
            corp_num = row.get("corporate_number")
            if not corp_num:
                result.errors.append(f"corporate_number なし: {row}")
                continue

            try:
                if corp_num not in existing:
                    # INSERT
                    cols   = _KIHON_COLS + ["loaded_at"]
                    vals   = [row.get(c) for c in _KIHON_COLS] + [loaded_at]
                    ph     = ",".join(["?"] * len(cols))
                    cur.execute(
                        f"INSERT INTO gbiz_companies ({','.join(cols)}) VALUES ({ph})",
                        vals,
                    )
                    result.inserted += 1
                else:
                    if not _detect_changes(existing[corp_num], row):
                        result.skipped += 1
                        continue
                    # UPDATE（APIで取得できない住所系カラムは上書きしない）
                    update_cols = [c for c in _KIHON_COLS if c != "corporate_number"]
                    set_clause  = ", ".join(f"{c}=?" for c in update_cols)
                    vals = [row.get(c) for c in update_cols] + [loaded_at, corp_num]
                    cur.execute(
                        f"UPDATE gbiz_companies SET {set_clause}, loaded_at=? WHERE corporate_number=?",
                        vals,
                    )
                    result.updated += 1

            except sqlite3.Error as exc:
                logger.error("DB操作エラー (corporate_number=%s): %s", corp_num, exc)
                result.errors.append(f"{corp_num}: {exc}")

        conn.commit()
        logger.info("  [kihonjoho] バッチ %d〜%d 完了", bs + 1, min(bs + batch_size, len(records)))

    return result


# ---------------------------------------------------------------------------
# 汎用処理（INSERT OR IGNORE）
# ---------------------------------------------------------------------------

def _apply_generic(
    conn: sqlite3.Connection,
    dataset: str,
    records: list[dict],
    batch_size: int,
    loaded_at: str,
) -> GBizDiffResult:
    cfg    = _TABLE_CONFIG[dataset]
    table  = cfg["table"]
    result = GBizDiffResult(dataset=dataset)

    for bs in range(0, len(records), batch_size):
        batch = records[bs: bs + batch_size]
        cur   = conn.cursor()

        for row in batch:
            corp_num = row.get("corporate_number")
            if not corp_num:
                result.errors.append(f"corporate_number なし: {row}")
                continue

            try:
                cols = list(row.keys()) + ["loaded_at"]
                vals = list(row.values()) + [loaded_at]
                ph   = ",".join(["?"] * len(cols))
                cur.execute(
                    f"INSERT OR IGNORE INTO {table} ({','.join(cols)}) VALUES ({ph})",
                    vals,
                )
                if cur.rowcount > 0:
                    result.inserted += 1
                else:
                    result.skipped += 1

            except sqlite3.Error as exc:
                logger.error("DB操作エラー (dataset=%s, corporate_number=%s): %s", dataset, corp_num, exc)
                result.errors.append(f"{corp_num}: {exc}")

        conn.commit()
        logger.info("  [%s] バッチ %d〜%d 完了", dataset, bs + 1, min(bs + batch_size, len(records)))

    return result


# ---------------------------------------------------------------------------
# 公開インターフェース
# ---------------------------------------------------------------------------

def apply_diff(
    dataset: str,
    records: list[dict],
    db_path: "str | Path | None" = None,
    batch_size: int = 500,
) -> GBizDiffResult:
    """差分レコードリストを gbizinfo.db の対応テーブルに適用する。

    Args:
        dataset:    データセットキー（_TABLE_CONFIG のキーと一致）
        records:    gbizinfo_diff_collector.fetch_diff() の戻り値
        db_path:    SQLiteファイルパス（省略時は data/gbizinfo.db）
        batch_size: 一度に処理するレコード数

    Returns:
        GBizDiffResult（処理件数サマリー）
    """
    if dataset not in _TABLE_CONFIG:
        raise ValueError(f"不明なデータセット: {dataset!r}")

    db_path = Path(db_path) if db_path else DB_PATH
    conn    = _open_db(db_path)
    cfg     = _TABLE_CONFIG[dataset]

    _setup_table(conn, cfg)
    loaded_at = _now_utc()
    logger.info("gBizINFO 差分適用開始: dataset=%s, %d件 → %s", dataset, len(records), db_path)

    try:
        if dataset == "kihonjoho":
            result = _apply_kihonjoho(conn, records, batch_size, loaded_at)
        else:
            result = _apply_generic(conn, dataset, records, batch_size, loaded_at)
    finally:
        conn.close()

    logger.info("gBizINFO 差分適用完了: %s", result.summary())
    return result


# ---------------------------------------------------------------------------
# エントリーポイント（単体動作確認用）
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    from datetime import date, timedelta
    from src.common.logging_setup import setup_logging
    from src.master.extractors.gbizinfo_diff_collector import fetch_diff, DATASET_KEYS

    setup_logging()

    parser = argparse.ArgumentParser(description="gBizINFO 差分DB適用 動作確認")
    parser.add_argument("--dataset", default="kihonjoho", choices=DATASET_KEYS)
    parser.add_argument("--from-date", default=str(date.today() - timedelta(days=1)))
    parser.add_argument("--to-date",   default=str(date.today()))
    parser.add_argument("--db",        default=str(DB_PATH))
    args = parser.parse_args()

    records = fetch_diff(args.dataset, args.from_date, args.to_date)
    result  = apply_diff(args.dataset, records, args.db)
    print(f"\n結果: {result.summary()}")
