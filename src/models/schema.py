# -*- coding: utf-8 -*-
"""
データベーススキーマ定義 / 初期化

テーブル構成:
  companies       - 法人基本情報（国税庁全件ダウンロード由来）
  change_history  - 変更履歴（差分更新時に記録）
"""

import logging
import sqlite3
from pathlib import Path

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

DDL_COMPANIES = """
CREATE TABLE IF NOT EXISTS companies (
    corporate_number            TEXT PRIMARY KEY,
    name                        TEXT NOT NULL,
    furigana                    TEXT,
    kind                        TEXT,
    prefecture_name             TEXT,
    city_name                   TEXT,
    street_number               TEXT,
    prefecture_code             TEXT,
    city_code                   TEXT,
    post_code                   TEXT,
    close_date                  TEXT,
    close_cause                 TEXT,
    successor_corporate_number  TEXT,
    assignment_date             TEXT,
    update_date                 TEXT,
    process                     TEXT,
    hihyoji                     TEXT,
    sequence_number             TEXT,
    correct                     TEXT,
    change_date                 TEXT,
    name_image_id               TEXT,
    address_image_id            TEXT,
    address_outside             TEXT,
    address_outside_image_id    TEXT,
    change_cause                TEXT,
    latest                      TEXT,
    en_name                     TEXT,
    en_prefecture_name          TEXT,
    en_city_name                TEXT,
    en_address_outside          TEXT,
    loaded_at                   TEXT NOT NULL
);
"""

# 既存DBに新カラムを追加するマイグレーション定義（カラム名: 型定義）
_MIGRATION_COLUMNS: list[tuple[str, str]] = [
    ("sequence_number",          "TEXT"),
    ("correct",                  "TEXT"),
    ("change_date",              "TEXT"),
    ("name_image_id",            "TEXT"),
    ("address_image_id",         "TEXT"),
    ("address_outside",          "TEXT"),
    ("address_outside_image_id", "TEXT"),
    ("change_cause",             "TEXT"),
    ("latest",                   "TEXT"),
    ("en_name",                  "TEXT"),
    ("en_prefecture_name",       "TEXT"),
    ("en_city_name",             "TEXT"),
    ("en_address_outside",       "TEXT"),
]

DDL_CHANGE_HISTORY = """
CREATE TABLE IF NOT EXISTS change_history (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    corporate_number TEXT NOT NULL,
    field_name       TEXT NOT NULL,
    old_value        TEXT,
    new_value        TEXT,
    changed_at       TEXT NOT NULL
);
"""

DDL_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_companies_kind           ON companies (kind);",
    "CREATE INDEX IF NOT EXISTS idx_companies_prefecture     ON companies (prefecture_code);",
    "CREATE INDEX IF NOT EXISTS idx_companies_close_date     ON companies (close_date);",
    "CREATE INDEX IF NOT EXISTS idx_change_history_corp      ON change_history (corporate_number);",
]


# ---------------------------------------------------------------------------
# カラム定義（他モジュールから参照する）
# ---------------------------------------------------------------------------

# XML / CSV フィールド名（camelCase）→ DB カラム名（snake_case）
# ALL_COLUMNS（全30フィールド）に対応。corporate_number を先頭に固定する。
COLUMN_MAP: dict[str, str] = {
    "corporateNumber":          "corporate_number",
    "sequenceNumber":           "sequence_number",
    "process":                  "process",
    "correct":                  "correct",
    "updateDate":               "update_date",
    "changeDate":               "change_date",
    "name":                     "name",
    "nameImageId":              "name_image_id",
    "kind":                     "kind",
    "prefectureName":           "prefecture_name",
    "cityName":                 "city_name",
    "streetNumber":             "street_number",
    "addressImageId":           "address_image_id",
    "prefectureCode":           "prefecture_code",
    "cityCode":                 "city_code",
    "postCode":                 "post_code",
    "addressOutside":           "address_outside",
    "addressOutsideImageId":    "address_outside_image_id",
    "closeDate":                "close_date",
    "closeCause":               "close_cause",
    "successorCorporateNumber": "successor_corporate_number",
    "changeCause":              "change_cause",
    "assignmentDate":           "assignment_date",
    "latest":                   "latest",
    "enName":                   "en_name",
    "enPrefectureName":         "en_prefecture_name",
    "enCityName":               "en_city_name",
    "enAddressOutside":         "en_address_outside",
    "furigana":                 "furigana",
    "hihyoji":                  "hihyoji",
}

# companies テーブルの全カラム（loaded_at 含む）
COMPANY_COLUMNS: list[str] = list(COLUMN_MAP.values()) + ["loaded_at"]


# ---------------------------------------------------------------------------
# 初期化
# ---------------------------------------------------------------------------

def _migrate_companies(conn: sqlite3.Connection) -> None:
    """既存の companies テーブルに不足カラムを追加する。

    CREATE TABLE の IF NOT EXISTS では新カラムが追加されないため、
    ALTER TABLE で差分だけ追加する。新規DBでは何もしない。
    """
    existing = {row[1] for row in conn.execute("PRAGMA table_info(companies)")}
    for col_name, col_type in _MIGRATION_COLUMNS:
        if col_name not in existing:
            conn.execute(f"ALTER TABLE companies ADD COLUMN {col_name} {col_type}")
            logger.info("カラム追加: companies.%s", col_name)


def init_db(db_path: str | Path) -> sqlite3.Connection:
    """DBファイルを作成し、全テーブル・インデックスを初期化して接続を返す。

    既にテーブルが存在する場合は何もしない（IF NOT EXISTS）。
    既存DBには不足カラムをマイグレーションで追加する。
    """
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA foreign_keys = ON;")

    conn.execute(DDL_COMPANIES)
    conn.execute(DDL_CHANGE_HISTORY)
    for idx_ddl in DDL_INDEXES:
        conn.execute(idx_ddl)

    _migrate_companies(conn)
    conn.commit()

    logger.info("DB初期化完了: %s", db_path)
    return conn
