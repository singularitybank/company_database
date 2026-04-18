# -*- coding: utf-8 -*-
"""
国税庁 差分データ DB適用モジュール

[処理フロー]
  1. fetch_diff() が返した法人リストを受け取る
  2. companies テーブルの既存レコードと突き合わせ
  3. 変更種別ごとに処理:
       新設 (DBに存在しない)  → INSERT
       変更 (フィールドに差分) → UPDATE + change_history INSERT
       変更なし               → スキップ
       閉鎖 (process=21)      → close_date / close_cause を UPDATE

[変更検出対象フィールド]
  name, furigana, kind,
  prefecture_name, city_name, street_number, prefecture_code, city_code, post_code,
  close_date, close_cause, process
"""

import logging
import sqlite3
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.master.models.schema import COMPANY_COLUMNS, init_db

# ---------------------------------------------------------------------------
# 定数
# ---------------------------------------------------------------------------

# 変更を検出するフィールド（loaded_at / update_date は比較対象外）
TRACKED_FIELDS = [
    "name",
    "furigana",
    "kind",
    "prefecture_name",
    "city_name",
    "street_number",
    "prefecture_code",
    "city_code",
    "post_code",
    "close_date",
    "close_cause",
    "process",
]


# ---------------------------------------------------------------------------
# ロガー
# ---------------------------------------------------------------------------

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 結果レポート
# ---------------------------------------------------------------------------

@dataclass
class DiffResult:
    inserted: int = 0        # 新設
    updated: int = 0         # 変更あり
    closed: int = 0          # 閉鎖
    skipped: int = 0         # 変更なし
    errors: list[str] = field(default_factory=list)

    @property
    def total(self) -> int:
        return self.inserted + self.updated + self.skipped

    def summary(self) -> str:
        return (
            f"新設={self.inserted}, 更新={self.updated}, "
            f"閉鎖={self.closed}, 変更なし={self.skipped}, "
            f"エラー={len(self.errors)}"
        )


# ---------------------------------------------------------------------------
# 内部ユーティリティ
# ---------------------------------------------------------------------------

def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _fetch_existing(conn: sqlite3.Connection, corp_nums: list[str]) -> dict[str, dict]:
    """指定した法人番号リストの既存レコードを取得してdictで返す。

    Returns:
        {corporate_number: {field: value, ...}}
    """
    if not corp_nums:
        return {}

    placeholders = ",".join(["?"] * len(corp_nums))
    sql = f"SELECT {', '.join(COMPANY_COLUMNS)} FROM companies WHERE corporate_number IN ({placeholders})"
    cursor = conn.execute(sql, corp_nums)
    cols = [desc[0] for desc in cursor.description]
    return {
        row[0]: dict(zip(cols, row))
        for row in cursor.fetchall()
    }


def _insert_company(
    cursor: sqlite3.Cursor,
    row: dict,
    loaded_at: str,
) -> None:
    row = dict(row)
    row["loaded_at"] = loaded_at
    cols = [f for f in COMPANY_COLUMNS if f in row]
    values = [row.get(f) for f in cols]
    sql = f"INSERT INTO companies ({', '.join(cols)}) VALUES ({', '.join(['?'] * len(cols))})"
    cursor.execute(sql, values)


def _update_company(
    cursor: sqlite3.Cursor,
    row: dict,
    loaded_at: str,
) -> None:
    row = dict(row)
    row["loaded_at"] = loaded_at
    update_cols = [f for f in COMPANY_COLUMNS if f != "corporate_number"]
    set_clause = ", ".join(f"{c} = ?" for c in update_cols)
    values = [row.get(c) for c in update_cols] + [row["corporate_number"]]
    sql = f"UPDATE companies SET {set_clause} WHERE corporate_number = ?"
    cursor.execute(sql, values)


def _insert_change_history(
    cursor: sqlite3.Cursor,
    corporate_number: str,
    field_name: str,
    old_value: str | None,
    new_value: str | None,
    changed_at: str,
) -> None:
    cursor.execute(
        "INSERT INTO change_history (corporate_number, field_name, old_value, new_value, changed_at) VALUES (?, ?, ?, ?, ?)",
        (corporate_number, field_name, old_value, new_value, changed_at),
    )


def _detect_changes(existing: dict, incoming: dict) -> list[tuple[str, str | None, str | None]]:
    """TRACKED_FIELDS のうち変化したフィールドを返す。

    Returns:
        [(field_name, old_value, new_value), ...]
    """
    changes = []
    for f in TRACKED_FIELDS:
        old = existing.get(f)
        new = incoming.get(f)
        # None と "" は同一視
        old_norm = old if old else None
        new_norm = new if new else None
        if old_norm != new_norm:
            changes.append((f, old_norm, new_norm))
    return changes


# ---------------------------------------------------------------------------
# 公開インターフェース
# ---------------------------------------------------------------------------

def apply_diff(
    records: list[dict],
    db_path: str | Path,
    batch_size: int = 500,
) -> DiffResult:
    """差分レコードリストを companies テーブルに適用する。

    Args:
        records:    nta_diff_collector.fetch_diff() の戻り値
        db_path:    SQLiteファイルパス
        batch_size: 一度に処理するレコード数（メモリ節約）

    Returns:
        DiffResult（処理件数サマリー）
    """
    db_path = Path(db_path)
    conn = init_db(db_path)
    result = DiffResult()
    loaded_at = _now_utc()

    logger.info("差分適用開始: %d件", len(records))

    try:
        for batch_start in range(0, len(records), batch_size):
            batch = records[batch_start : batch_start + batch_size]
            corp_nums = [r["corporate_number"] for r in batch if r.get("corporate_number")]

            # 既存レコードをまとめて取得
            existing_map = _fetch_existing(conn, corp_nums)

            cursor = conn.cursor()
            for row in batch:
                corp_num = row.get("corporate_number")
                if not corp_num:
                    logger.warning("corporate_number が空のレコードをスキップ: %s", row)
                    result.errors.append(f"corporate_number なし: {row}")
                    continue

                try:
                    if corp_num not in existing_map:
                        # --- 新設 ---
                        _insert_company(cursor, row, loaded_at)
                        result.inserted += 1
                        logger.debug("新設: %s %s", corp_num, row.get("name"))

                    else:
                        # --- 既存レコードと比較 ---
                        existing = existing_map[corp_num]
                        changes = _detect_changes(existing, row)

                        if not changes:
                            result.skipped += 1
                            continue

                        # 変更あり → UPDATE
                        _update_company(cursor, row, loaded_at)
                        result.updated += 1

                        # 閉鎖フラグ判定
                        if row.get("process") == "21":
                            result.closed += 1

                        # 変更履歴を記録
                        changed_at = row.get("update_date") or loaded_at
                        for field_name, old_val, new_val in changes:
                            _insert_change_history(
                                cursor, corp_num, field_name, old_val, new_val, changed_at
                            )
                        logger.debug(
                            "更新: %s %s - 変更フィールド: %s",
                            corp_num, row.get("name"), [c[0] for c in changes],
                        )

                except sqlite3.Error as exc:
                    logger.error("DB操作エラー (corporate_number=%s): %s", corp_num, exc)
                    result.errors.append(f"{corp_num}: {exc}")

            conn.commit()
            logger.info(
                "  バッチ %d〜%d 完了",
                batch_start + 1,
                min(batch_start + batch_size, len(records)),
            )

    finally:
        conn.close()

    logger.info("差分適用完了: %s", result.summary())
    return result


# ---------------------------------------------------------------------------
# エントリーポイント（単体動作確認用）
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    from datetime import date, timedelta

    from src.common.logging_setup import setup_logging
    setup_logging()

    BASE_DIR = Path(__file__).resolve().parents[2]
    DB_PATH = BASE_DIR / "data" / "companies.db"

    parser = argparse.ArgumentParser(description="差分DB適用 動作確認")
    parser.add_argument("--from-date", default=str(date.today() - timedelta(days=1)))
    parser.add_argument("--to-date",   default=str(date.today()))
    parser.add_argument("--address",   default="13", help="都道府県コード")
    parser.add_argument("--db",        default=str(DB_PATH))
    args = parser.parse_args()

    # extractor を使って差分を取得してから適用
    from src.master.extractors.nta_diff_collector import fetch_diff
    records = fetch_diff(args.from_date, args.to_date, address_codes=[args.address])
    result = apply_diff(records, args.db)
    print(f"\n結果: {result.summary()}")
