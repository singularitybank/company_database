# 職場情報総合サイト データ 仕様・運用ガイド

**対象データソース:** 職場情報総合サイト（shokuba.mhlw.go.jp）  
**取得方式:** 全件ダウンロードCSV（自動ダウンロード）  
**DB ファイル:** `data/shokuba.db`  
**更新戦略:** 差分なし。全件入れ替え（DROP → CREATE → INSERT）

---

## 目次

1. [処理フロー概要](#処理フロー概要)
2. [テーブル構成](#テーブル構成)
3. [実行方法](#実行方法)
4. [ファイル構成](#ファイル構成)
5. [DBスキーマ](#dbスキーマ)
6. [カラム定義](#カラム定義)
7. [CSV仕様メモ](#csv仕様メモ)

---

## 処理フロー概要

```
職場情報総合サイト
  └─ 全件ダウンロードCSV（ストリーミング取得）
        │ shokuba_downloader.py: download()
        ▼
  data/raw/shokuba/Shokubajoho_YYYYMMDD.csv
        │
        │ shokuba_to_parquet.py: convert()
        │   1. CSV ヘッダー（日本語）→ 英語カラム名に変換
        │   2. TABLE_RANGES で 8 テーブルに列分割
        │   3. 日付列を正規化
        ▼
  data/staging/shokuba/
    shokuba_basic.parquet
    shokuba_recruitment.parquet
    shokuba_work_hours.parquet
    shokuba_employment_system.parquet
    shokuba_female_workforce.parquet
    shokuba_childcare.parquet
    shokuba_career.parquet
    shokuba_certification.parquet
        │
        │ shokuba_to_sqlite.py: load_table()
        │   DROP TABLE → CREATE TABLE → INSERT
        ▼
  data/shokuba.db（8テーブル）
```

---

## テーブル構成

1つのCSV（最大636列）を内容ごとに8つのテーブルに分割して格納する。  
全テーブルに `corporate_number` を含む。

| テーブル名 | CSV列範囲 | 列数 | 内容 |
|---|---|---|---|
| `shokuba_basic` | 1〜39 | 39 | 基本情報・就業場所 |
| `shokuba_recruitment` | 40〜123 | 84 | 採用・定着・インターンシップ・社会保険 |
| `shokuba_work_hours` | 124〜258 | 135 | 継続勤務年数差異・残業・賃金差異・有給・テレワーク |
| `shokuba_employment_system` | 259〜390 | 132 | 多様な正社員制度・転換実績・定年制 |
| `shokuba_female_workforce` | 391〜418 | 28 | 女性労働者割合・管理職・役員 |
| `shokuba_childcare` | 419〜474 | 56 | 育児休業・両立支援 |
| `shokuba_career` | 475〜499 | 25 | キャリア開発・行動計画・自由記述 |
| `shokuba_certification` | 500〜636 | 137 | 認定・表彰・改善ポータルサイト |

---

## 実行方法

### 通常実行（ダウンロードから一括処理）

```bash
# STEP 1〜3 をすべて実行
python scripts/run_shokuba.py
```

### オプション

```bash
# ダウンロードをスキップして既存 CSV を使う
python scripts/run_shokuba.py --skip-download

# CSV を直接指定してダウンロードをスキップ
python scripts/run_shokuba.py --csv data/raw/shokuba/Shokubajoho_20260412.csv

# CSV → Parquet 変換もスキップして DB 投入のみ
python scripts/run_shokuba.py --skip-download --skip-convert

# 特定テーブルのみ DB 投入
python scripts/run_shokuba.py --skip-download --skip-convert --tables shokuba_basic shokuba_childcare

# DB パスを変更
python scripts/run_shokuba.py --db data/shokuba_test.db
```

### オプション一覧

| オプション | デフォルト | 説明 |
|---|---|---|
| `--skip-download` | なし | ダウンロードをスキップ。既存 CSV を使用 |
| `--skip-convert` | なし | CSV → Parquet 変換をスキップ。既存 Parquet を使用 |
| `--csv` | 自動検索 | 入力 CSV パス（省略時: `data/raw/shokuba/` の最新 `Shokubajoho_*.csv`） |
| `--db` | `data/shokuba.db` | SQLite ファイルパス |
| `--tables` | 全テーブル | DB 投入するテーブル名（複数スペース区切り） |

### 処理内容

| ステップ | モジュール / 関数 | 説明 |
|---|---|---|
| STEP 1: ダウンロード | `shokuba_downloader.download()` | サイトから CSV をストリーミング取得。1MB チャンクで保存 |
| STEP 2: CSV → Parquet | `shokuba_to_parquet.convert()` | 日本語ヘッダーを英語カラム名に変換し、8テーブルに列分割して Parquet 出力 |
| STEP 3: Parquet → DB | `shokuba_to_sqlite.load_table()` | 既存テーブルを DROP して再作成。50,000行バッチで INSERT |

> **注意:** STEP 3 は既存テーブルを `DROP TABLE` してから再作成する（フルリフレッシュ）。  
> 差分更新の仕組みはない。

### 個別モジュールの単体実行

```bash
# ダウンロードのみ
python src/downloaders/shokuba_downloader.py
python src/downloaders/shokuba_downloader.py --output data/raw/shokuba/Shokubajoho_20260412.csv

# CSV → Parquet 変換のみ
python src/converters/shokuba_to_parquet.py
python src/converters/shokuba_to_parquet.py --csv data/raw/shokuba/Shokubajoho_20260412.csv

# Parquet → SQLite 投入のみ（全テーブル）
python src/loaders/shokuba_to_sqlite.py

# 特定テーブルのみ
python src/loaders/shokuba_to_sqlite.py shokuba_basic shokuba_childcare
```

### ログ

実行ログは `logs/shokuba_YYYYMMDD.log` に保存される（ローカル時刻）。

---

## ファイル構成

```
src/
  downloaders/
    shokuba_downloader.py     サイトから CSV をダウンロード（download）
  converters/
    shokuba_schema.py         列定義（TABLE_RANGES / COLUMN_MAP / DATE_COLUMNS / TABLE_INDEXES）
    shokuba_to_parquet.py     CSV → 8テーブル Parquet 変換（convert）
  loaders/
    shokuba_to_sqlite.py      Parquet → SQLite 投入（load_table）

scripts/
  run_shokuba.py              一括処理エントリーポイント

data/
  raw/shokuba/                ダウンロード CSV 置き場
    Shokubajoho_YYYYMMDD.csv
  staging/shokuba/            中間 Parquet ファイル置き場
    shokuba_basic.parquet
    shokuba_recruitment.parquet
    shokuba_work_hours.parquet
    shokuba_employment_system.parquet
    shokuba_female_workforce.parquet
    shokuba_childcare.parquet
    shokuba_career.parquet
    shokuba_certification.parquet
  shokuba.db                  SQLite データベース

logs/
  shokuba_YYYYMMDD.log        実行ログ
```

---

## DBスキーマ

DDL は Parquet スキーマから自動生成される（`shokuba_to_sqlite._build_ddl()`）。  
全テーブルに `loaded_at TEXT`（UTC タイムスタンプ）が追加される。  
全テーブルに `corporate_number` の B-tree インデックスが作成される。

PyArrow 型 → SQLite 型のマッピング:

| PyArrow | SQLite |
|---|---|
| integer | `INTEGER` |
| floating | `REAL` |
| その他 | `TEXT` |

### テーブル別主要カラム

#### shokuba_basic（基本情報・就業場所）

| カラム名 | 元の日本語項目 |
|---|---|
| `corporate_number` | 法人番号 |
| `name` | 企業名 |
| `prefecture` | 都道府県 |
| `location` | 所在地 |
| `company_size` | 企業規模 |
| `company_size_detail` | 企業規模詳細 |
| `industry` | 業種 |
| `business_summary` | 事業概要 |
| `homepage_url` | 企業ホームページ |
| `recruitment_url` | 採用ページ |
| `founding_year` | 創業年 |
| `stock_code` | 証券コード |
| `market_segment` | 市場区分 |
| `hwis_fulltime` | HWIS 求人掲載（フルタイム） |
| `hwis_parttime` | HWIS 求人掲載（パートタイム） |
| `hwis_graduate` | HWIS 求人掲載（新卒・既卒） |
| `job_loc1_area1` 〜 `job_loc4_occupation` | 就業場所1〜4（エリア・求人区分・職種） |
| `registered_at` | 登録日時（日付正規化済み） |
| `updated_at` | 更新日時（日付正規化済み） |

#### shokuba_recruitment（採用・定着）

採用・定着状況（新卒・35歳未満）、再雇用実績、中途採用比率・定着率、  
インターンシップ実績、社会保険・退職金制度の有無など。

主なカラム: `new_grad_retention_male/female/total`, `new_grad_turnover_count`,  
`midcareer_hire_ratio`, `midcareer_retention_category*`

#### shokuba_work_hours（労働時間・賃金）

継続勤務年数差異、平均残業時間、賃金差異（男女・正規/非正規）、  
有給取得率、テレワーク実施状況など。

主なカラム: `avg_overtime_hours`, `paid_leave_rate`, `telework_*`

#### shokuba_employment_system（雇用制度）

多様な正社員制度（勤務地限定・職務限定・短時間正社員）、正規転換実績、定年制など。

#### shokuba_female_workforce（女性活躍）

女性労働者割合、女性管理職人数・比率、女性役員数など。  
gBizINFO の `shokubajoho` データセットと対応する項目群。

主なカラム: `female_worker_ratio`, `female_manager_count`, `total_manager_count`,  
`female_executive_count`, `total_executive_count`

#### shokuba_childcare（育児・両立支援）

育児休業取得状況（男女別）、育児短時間勤務、介護休業・介護短時間勤務など。

主なカラム: `childcare_eligible_male/female`, `childcare_takers_male/female`

#### shokuba_career（キャリア開発）

キャリアコンサルティング実施状況、自己啓発支援、行動計画、自由記述欄など。

#### shokuba_certification（認定・表彰）

くるみん認定、えるぼし認定、ユースエール認定、プラチナくるみん、  
各種表彰（テレワーク推進、キャリア支援、高年齢者活躍など）の取得状況・年度。

---

## CSV仕様メモ

### ダウンロード先

```
GET https://shokuba.mhlw.go.jp/shokuba/utilize/download010?lang=JA
```

認証不要。クリック直接ダウンロード方式。`requests` でストリーミング取得する（タイムアウト: 300秒）。

### ファイル仕様

| 項目 | 内容 |
|---|---|
| 文字コード | UTF-8 BOM 付き（`utf-8-sig`） |
| ヘッダー | 1行目に日本語カラム名 |
| 列数 | 最大636列（定義ファイルの番号は634番まで。番号なし2行あり） |
| 保存ファイル名 | `Shokubajoho_YYYYMMDD.csv`（日付は実行日） |

### 列名変換ルール

| 条件 | 変換後カラム名 |
|---|---|
| `COLUMN_MAP` に定義あり | 対応する英語 snake_case 名 |
| `COLUMN_MAP` に定義なし | `col_NNNN`（N は 1-indexed 列番号） |
| 英語名が重複する場合 | `{name}_dup{N}`（N は重複回数） |

### 日付カラム

`registered_at`（登録日時）と `updated_at`（更新日時）のみ日付正規化対象。  
`normalize_iso_date()` で ISO 8601 形式（`YYYY-MM-DD`）に統一される。
