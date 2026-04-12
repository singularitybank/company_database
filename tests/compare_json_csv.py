"""
gbizinfo JSON (Hojinjoho_xx.json) と CSV (Kihonjoho_UTF-8.csv) の比較テスト

比較対象フィールド（JSONキー → CSV列インデックス）:
    corporate_number  → [0]  法人番号
    name              → [1]  商号または名称
    kana              → [2]  商号または名称（カナ）
    name_en           → [3]  商号または名称（英字）
    close_date        → [4]  登記記録の閉鎖等年月日
    close_cause       → [5]  登記記録の閉鎖等の事由
    location          → [6]  登記住所（連結住所）
    postal_code       → [7]  郵便番号
    kind              → [13] 組織種別
    process           → [14] 処理区分
    status            → [16] 状態
    representative_name → [17] 代表者名称
    capital_stock     → [18] 資本金
    employee_number   → [19] 従業員数
    company_size_male → [20] 企業規模詳細(男性)
    company_size_female → [21] 企業規模詳細(女性)
    business_summary  → [22] 事業概要
    company_url       → [23] WebサイトURL
    founding_year     → [24] 創業年
    date_of_establishment → [26] 設立年月日
    qualification_grade → [27] 全省庁統一資格-資格等級
    update_date       → [29] 更新年月日

JSON固有フィールド（CSVに対応列なし）:
    aggregated_year, industry (array), subsidy, patent, commendation, procurement, certification

CSV固有フィールド（JSONに対応キーなし）:
    prefecture_name [8], prefecture_code [9], city_name [10],
    city_code [11], street_number [12], correct [15]
"""

import csv
import json
import sys
from collections import defaultdict
from pathlib import Path

# ---------------------------------------------------------------------------
# 設定
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).parent.parent
JSON_FILE = REPO_ROOT / "data/raw/gbizinfo/Hojinjoho_json/Hojinjoho_01.json"
CSV_FILE  = REPO_ROOT / "data/raw/gbizinfo/Kihonjoho_UTF-8.csv"

# JSONサンプル数（先頭N件）
JSON_SAMPLE_SIZE = 1000

# JSON key → CSV 列インデックス
FIELD_MAP: dict[str, int] = {
    "corporate_number":   0,
    "name":               1,
    "kana":               2,
    "name_en":            3,
    "close_date":         4,
    "close_cause":        5,
    "location":           6,
    "postal_code":        7,
    "kind":              13,
    "process":           14,
    "status":            16,
    "representative_name": 17,
    "capital_stock":     18,
    "employee_number":   19,
    "company_size_male": 20,
    "company_size_female": 21,
    "business_summary":  22,
    "company_url":       23,
    "founding_year":     24,
    "date_of_establishment": 26,
    "qualification_grade": 27,
    "update_date":       29,
}

# JSONにあってCSVにない（構造的差異）
JSON_ONLY_FIELDS = [
    "aggregated_year",
    "industry",        # 配列
    "business_items",  # 配列（CSVは|区切りで[25]に存在するが、JSON側は配列のため除外）
    "subsidy",         # 配列
    "patent",          # 配列
    "commendation",    # 配列
    "procurement",     # 配列
    "certification",   # 配列
]

# CSVにあってJSONにない（構造的差異）
CSV_ONLY_FIELDS = {
    8:  "prefecture_name",
    9:  "prefecture_code",
    10: "city_name",
    11: "city_code",
    12: "street_number",
    15: "correct",
}

# ---------------------------------------------------------------------------
# ヘルパー
# ---------------------------------------------------------------------------

def normalize(v: str | None) -> str:
    """比較用に値を正規化する（None/空文字/ハイフンの統一）"""
    if v is None:
        return ""
    s = str(v).strip()
    # gbizinfoのステータス "-" は空扱い（CSVは空欄）
    return "" if s == "-" else s


def load_json_sample(path: Path, n: int) -> list[dict]:
    with open(path, encoding="utf-8-sig") as f:
        records = json.load(f)
    return records[:n]


def build_csv_index(path: Path, target_ids: set[str]) -> dict[str, list[str]]:
    """target_ids に含まれる corporate_number の CSV行を返す。全件ストリーム読み。"""
    index: dict[str, list[str]] = {}
    with open(path, encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        next(reader)  # ヘッダースキップ
        for row in reader:
            if row and row[0] in target_ids:
                # 同一法人番号が複数行ある場合は最初の1件を使用
                if row[0] not in index:
                    index[row[0]] = row
            if len(index) == len(target_ids):
                break  # 全件ヒットしたら早期終了
    return index


# ---------------------------------------------------------------------------
# 比較ロジック
# ---------------------------------------------------------------------------

def compare_record(json_rec: dict, csv_row: list[str]) -> dict:
    """1レコードの比較結果を返す"""
    result = {
        "corporate_number": json_rec["corporate_number"],
        "matched": [],
        "mismatched": [],   # (field, json_val, csv_val)
        "json_null_csv_has": [],  # JSONがnullだがCSVに値あり
    }

    for field, col_idx in FIELD_MAP.items():
        if col_idx >= len(csv_row):
            continue
        json_val = normalize(json_rec.get(field))
        csv_val  = normalize(csv_row[col_idx])

        if json_val == csv_val:
            result["matched"].append(field)
        elif json_val == "" and csv_val != "":
            result["json_null_csv_has"].append((field, csv_val))
        else:
            result["mismatched"].append((field, json_val, csv_val))

    return result


# ---------------------------------------------------------------------------
# メイン
# ---------------------------------------------------------------------------

def main() -> None:
    print(f"JSON: {JSON_FILE}")
    print(f"CSV:  {CSV_FILE}")
    print(f"サンプル数: {JSON_SAMPLE_SIZE}件\n")

    # 1. JSONサンプルロード
    print("JSONロード中...")
    json_records = load_json_sample(JSON_FILE, JSON_SAMPLE_SIZE)
    target_ids = {r["corporate_number"] for r in json_records}
    print(f"  JSONレコード: {len(json_records)}件（ユニークID: {len(target_ids)}件）")

    # 2. CSV検索
    print("CSVをストリーム検索中（数分かかる場合があります）...")
    csv_index = build_csv_index(CSV_FILE, target_ids)
    print(f"  CSV一致件数: {len(csv_index)}件 / {len(target_ids)}件")

    csv_only_ids = target_ids - set(csv_index.keys())
    json_only_ids = set(csv_index.keys()) - target_ids
    print(f"  JSON側にのみ存在: {len(target_ids - set(csv_index.keys()))}件")
    print(f"  CSV側にのみ存在:  {len(json_only_ids)}件\n")

    # 3. フィールド比較
    field_match_counts    = defaultdict(int)
    field_mismatch_counts = defaultdict(int)
    field_json_null_counts = defaultdict(int)
    mismatch_examples: dict[str, list] = defaultdict(list)

    matched_records = 0
    for rec in json_records:
        corp_num = rec["corporate_number"]
        if corp_num not in csv_index:
            continue
        matched_records += 1
        result = compare_record(rec, csv_index[corp_num])
        for f in result["matched"]:
            field_match_counts[f] += 1
        for f, jv, cv in result["mismatched"]:
            field_mismatch_counts[f] += 1
            if len(mismatch_examples[f]) < 3:
                mismatch_examples[f].append({
                    "corporate_number": corp_num,
                    "json": jv,
                    "csv": cv,
                })
        for f, cv in result["json_null_csv_has"]:
            field_json_null_counts[f] += 1

    # 4. サマリー出力
    print("=" * 60)
    print(f"フィールド比較サマリー（比較対象: {matched_records}件）")
    print("=" * 60)
    print(f"{'フィールド':<25} {'一致':>6} {'不一致':>6} {'JSON null/CSV有':>14}")
    print("-" * 60)
    for field in FIELD_MAP:
        match   = field_match_counts[field]
        mismatch = field_mismatch_counts[field]
        jnull   = field_json_null_counts[field]
        marker = " !" if mismatch > 0 else ""
        print(f"{field:<25} {match:>6} {mismatch:>6} {jnull:>14}{marker}")

    # 5. 不一致フィールドの詳細
    if mismatch_examples:
        print("\n" + "=" * 60)
        print("不一致フィールドのサンプル（最大3件）")
        print("=" * 60)
        for field, examples in mismatch_examples.items():
            print(f"\n[{field}]")
            for ex in examples:
                print(f"  法人番号: {ex['corporate_number']}")
                print(f"    JSON: {ex['json']!r}")
                print(f"    CSV:  {ex['csv']!r}")

    # 6. JSON固有フィールドの統計
    print("\n" + "=" * 60)
    print("JSON固有フィールド（配列）の保有状況")
    print("=" * 60)
    array_stats: dict[str, dict] = {}
    for f in JSON_ONLY_FIELDS:
        counts = [len(r[f]) if isinstance(r.get(f), list) else (1 if r.get(f) is not None else 0)
                  for r in json_records[:matched_records]]
        non_empty = sum(1 for c in counts if c > 0)
        total_items = sum(counts)
        array_stats[f] = {"non_empty": non_empty, "total_items": total_items}
        if isinstance(json_records[0].get(f), list):
            max_items = max(counts) if counts else 0
            print(f"  {f:<20} 保有率: {non_empty/len(json_records)*100:5.1f}%  "
                  f"合計件数: {total_items:6}  最大: {max_items}")
        else:
            print(f"  {f:<20} 保有率: {non_empty/len(json_records)*100:5.1f}%")

    # 7. CSV固有フィールドの統計
    print("\n" + "=" * 60)
    print("CSV固有フィールド（分割住所・訂正区分）の非空率")
    print("=" * 60)
    for col_idx, field_name in CSV_ONLY_FIELDS.items():
        matched_rows = [csv_index[r["corporate_number"]]
                        for r in json_records if r["corporate_number"] in csv_index]
        non_empty = sum(1 for row in matched_rows
                        if col_idx < len(row) and normalize(row[col_idx]) != "")
        print(f"  [{col_idx:02d}] {field_name:<20} 非空率: {non_empty/len(matched_rows)*100:5.1f}%")

    print("\n完了")


if __name__ == "__main__":
    main()
