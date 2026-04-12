# gBizINFO 法人情報データ 仕様・運用ガイド

**対象データソース:** gBizINFO（経済産業省 法人活動情報データベース）  
**取得方式:** 全件ダウンロードCSV（初回）/ 差分API（定期更新）  
**DB ファイル:** `data/gbizinfo.db`（NTAの `data/companies.db` とは別ファイル）

---

## 目次

1. [処理フロー概要](#処理フロー概要)
2. [データセット一覧](#データセット一覧)
3. [初回フルロード](#初回フルロード)
4. [定期差分更新](#定期差分更新)
5. [ファイル構成](#ファイル構成)
6. [DBスキーマ](#dbスキーマ)
7. [API仕様メモ](#api仕様メモ)

---

## 処理フロー概要

```
【初回フルロード】
gBizINFO サイト
  └─ 全件ダウンロードCSV (data/raw/gbizinfo/*.csv)
        │
        │ gbizinfo_to_parquet.py: convert()
        ▼
  Parquet (data/staging/gbizinfo/)
    {dataset}_core.parquet  コア列（英語カラム名）
    {dataset}_meta.parquet  メタデータ列（corporate_number + JSON blob）
        │
        │ gbizinfo_to_sqlite.py: load_dataset()
        ▼
  gbizinfo.db（各テーブルへ INSERT OR REPLACE）

【定期差分更新】
gBizINFO 差分API
        │
        │ gbizinfo_diff_collector.py: fetch_diff()
        ▼
  法人リスト（正規化済み辞書リスト）
        │
        │ gbizinfo_diff_processor.py: apply_diff()
        ▼
  gbizinfo.db（INSERT / UPDATE / INSERT OR IGNORE）
```

---

## データセット一覧

| データセットキー | 日本語名 | API サフィックス | DBテーブル（差分更新） | CSVファイル |
|---|---|---|---|---|
| `kihonjoho` | 基本情報 | *(なし)* | `gbiz_companies` | `Kihonjoho_UTF-8.csv` |
| `todokedeninteijoho` | 届出認定情報 | `certification` | `gbiz_todokedenintei` | `TodokedeNinteijoho_UTF-8.csv` |
| `hyoshojoho` | 表彰情報 | `commendation` | `gbiz_commendation` | `Hyoshojoho_UTF-8.csv` |
| `zaimujoho` | 財務情報 | `finance` | `gbiz_zaimu` | `Zaimujoho_UTF-8.csv` |
| `tokkyojoho` | 特許情報 | `patent` | `gbiz_patent` | `Tokkyojoho_UTF-8.csv` |
| `chotatsujoho` | 調達情報 | `procurement` | `gbiz_procurement` | `Chotatsujoho_UTF-8.csv` |
| `hojokinjoho` | 補助金情報 | `subsidy` | `gbiz_subsidy` | `Hojokinjoho_UTF-8.csv` |
| `shokubajoho` | 職場情報 | `workplace` | `Shokubajoho_UTF-8.csv` | `Shokubajoho_UTF-8.csv` |

---

## 初回フルロード

### 事前準備

1. gBizINFO サイトから各データセットのCSVをダウンロードして `data/raw/gbizinfo/` に配置する  
   ファイル名例: `Kihonjoho_UTF-8.csv`、`Tokkyojoho_UTF-8.csv` など
2. `config/.env` に `GBIZINFO_API_TOKEN` が設定済みであること

### 実行

```bash
# STEP 1: CSV → Parquet 変換（全データセット）
python src/converters/gbizinfo_to_parquet.py

# 特定データセットのみ変換
python src/converters/gbizinfo_to_parquet.py kihonjoho tokkyojoho

# STEP 2: Parquet → SQLite 投入（全データセット）
python src/loaders/gbizinfo_to_sqlite.py

# 特定データセットのみ投入
python src/loaders/gbizinfo_to_sqlite.py kihonjoho tokkyojoho
```

### 処理内容

| ステップ | モジュール / 関数 | 説明 |
|---|---|---|
| STEP 1: CSV → Parquet | `gbizinfo_to_parquet.convert()` | CSVをコア列とメタデータ列の2Parquetに分割して保存 |
| STEP 2: Parquet → DB | `gbizinfo_to_sqlite.load_dataset()` | コア・メタ各Parquetを対応テーブルにバッチ投入 |

#### Parquet の分割方式

各データセットのCSVは **コア列** と **メタデータ列** に分割される。

| ファイル | 内容 |
|---|---|
| `{dataset}_core.parquet` | 英語カラム名で定義されたコア列（`gbizinfo_schema.py` の `core_count` 列） |
| `{dataset}_meta.parquet` | `corporate_number` + 残り列を `{"日本語ヘッダー": "値", ...}` の JSON blob として格納 |

> **注意:** メタデータは null・空文字を省略した JSON 文字列として格納される。

---

## 定期差分更新

### 実行

```bash
# 全データセット、昨日分の差分を取得（通常運用）
python scripts/run_gbizinfo_diff.py

# 特定データセットのみ
python scripts/run_gbizinfo_diff.py --dataset kihonjoho hojokinjoho

# 期間を指定
python scripts/run_gbizinfo_diff.py --from 2026-04-01 --to 2026-04-10

# APIリクエスト間隔を変更（デフォルト: 1.0秒）
python scripts/run_gbizinfo_diff.py --wait 2.0

# DBパスを指定
python scripts/run_gbizinfo_diff.py --db /path/to/gbizinfo.db
```

### オプション一覧

| オプション | デフォルト | 説明 |
|---|---|---|
| `--from` | 昨日 | 取得開始日 (YYYY-MM-DD) |
| `--to` | 今日 | 取得終了日 (YYYY-MM-DD) |
| `--dataset` | 全件 | データセットキー（複数スペース区切り） |
| `--wait` | 1.0 | APIリクエスト間の待機秒数 |
| `--db` | data/gbizinfo.db | SQLiteファイルパス |

### 処理内容

| ステップ | 処理 | 説明 |
|---|---|---|
| 1. API取得 | `fetch_diff()` | データセットごとにAPIをリクエスト。`totalPage` に基づき全ページ取得 |
| 2. 正規化 | `_norm_*()` | データセット別ノーマライザーで辞書リストに変換 |
| 3. DB適用 | `apply_diff()` | `kihonjoho` は差分検出+UPDATE、その他は INSERT OR IGNORE |

### DB適用ロジック

```
kihonjoho の場合:
  corporate_number が...
    ├─ DBに存在しない  →  INSERT（新設）
    └─ DBに存在する    →  _KIHON_TRACKED を比較
          ├─ 変更あり  →  UPDATE
          └─ 変更なし  →  スキップ

その他データセットの場合:
  複合ユニーク制約 (UNIQUE INDEX) に基づき INSERT OR IGNORE
    ├─ 新規レコード  →  INSERT
    └─ 既存レコード  →  IGNORE（重複スキップ）
```

**kihonjoho の変更検出フィールド（`_KIHON_TRACKED`）:**

`name` / `kana` / `name_en` / `postal_code` / `location` / `process` / `status` /  
`close_date` / `close_cause` / `kind` / `representative_name`

> **注意:** gBizINFO の差分API は住所を結合済み文字列 (`location`) で返す。  
> `prefecture_name` / `prefecture_code` / `city_name` / `city_code` / `street_number` は  
> APIから取得できないため、更新時も既存値を保持する（上書きしない）。

### 戻り値

`apply_diff()` は `GBizDiffResult` オブジェクトを返す。

| フィールド | 説明 |
|---|---|
| `dataset` | データセットキー |
| `inserted` | 新規挿入件数 |
| `updated` | 更新件数（kihonjoho のみ） |
| `skipped` | 重複スキップ件数 |
| `errors` | エラーメッセージのリスト |

### ログ

実行ログは `logs/gbizinfo_diff_YYYYMMDD.log` に保存される（ローカル時刻）。

---

## ファイル構成

```
src/
  converters/
    gbizinfo_schema.py        全件CSVのカラム定義・型定義（DATASETS）
    gbizinfo_to_parquet.py    CSV → Parquet 変換（core + meta に分割）
  loaders/
    gbizinfo_db_schema.py     初回フルロード用テーブル定義（TABLE_CONFIGS / META_CONFIGS）
    gbizinfo_to_sqlite.py     Parquet → SQLite 投入
  extractors/
    gbizinfo_diff_collector.py  差分APIフェッチ・ページネーション・正規化（fetch_diff）
  processors/
    gbizinfo_diff_processor.py  差分DB適用（apply_diff / GBizDiffResult）

scripts/
  run_gbizinfo_diff.py        定期差分更新のエントリーポイント

config/
  .env                        GBIZINFO_API_TOKEN を設定（gitignore対象）

data/
  raw/gbizinfo/               全件ダウンロードCSV置き場
  staging/gbizinfo/           中間Parquetファイル置き場
    {dataset}_core.parquet
    {dataset}_meta.parquet
  gbizinfo.db                 SQLiteデータベース

logs/
  gbizinfo_diff_YYYYMMDD.log  差分更新実行ログ
```

---

## DBスキーマ

差分更新パスで使用されるテーブル定義（`gbizinfo_diff_processor.py` より）。

### gbiz_companies（基本情報）

`corporate_number` を主キーとし、差分更新で最新状態に上書きされる。

```sql
CREATE TABLE IF NOT EXISTS gbiz_companies (
    corporate_number      TEXT PRIMARY KEY,
    name                  TEXT,
    kana                  TEXT,
    name_en               TEXT,
    close_date            TEXT,
    close_cause           TEXT,
    location              TEXT,
    postal_code           TEXT,
    prefecture_name       TEXT,    -- CSVフルロード由来。差分APIでは更新されない
    prefecture_code       TEXT,    -- 同上
    city_name             TEXT,    -- 同上
    city_code             TEXT,    -- 同上
    street_number         TEXT,    -- 同上
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
    business_items        TEXT,    -- パイプ区切りリスト
    date_of_establishment TEXT,
    qualification_grade   TEXT,
    business_category     TEXT,
    update_date           TEXT,
    loaded_at             TEXT
);
```

インデックス: `kind`, `update_date`

### gbiz_todokedenintei（届出認定情報）

ユニーク制約: `(corporate_number, certification_date, title)`

```sql
CREATE TABLE IF NOT EXISTS gbiz_todokedenintei (
    corporate_number   TEXT,
    name               TEXT,
    location           TEXT,
    certification_date TEXT,
    title              TEXT,
    target             TEXT,
    department         TEXT,
    issuer             TEXT,
    loaded_at          TEXT
);
```

### gbiz_commendation（表彰情報）

ユニーク制約: `(corporate_number, certification_date, title)`

```sql
CREATE TABLE IF NOT EXISTS gbiz_commendation (
    corporate_number   TEXT,
    name               TEXT,
    location           TEXT,
    certification_date TEXT,
    title              TEXT,
    target             TEXT,
    department         TEXT,
    issuer             TEXT,
    remarks            TEXT,
    loaded_at          TEXT
);
```

### gbiz_procurement（調達情報）

ユニーク制約: `(corporate_number, order_date, title)`

```sql
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
);
```

### gbiz_subsidy（補助金情報）

ユニーク制約: `(corporate_number, certification_date, title)`

```sql
CREATE TABLE IF NOT EXISTS gbiz_subsidy (
    corporate_number   TEXT,
    name               TEXT,
    location           TEXT,
    certification_date TEXT,
    title              TEXT,
    amount             TEXT,
    target             TEXT,
    issuer             TEXT,
    loaded_at          TEXT
);
```

### gbiz_patent（特許情報）

ユニーク制約: `(corporate_number, registration_number)`

```sql
CREATE TABLE IF NOT EXISTS gbiz_patent (
    corporate_number                   TEXT,
    name                               TEXT,
    location                           TEXT,
    patent_type                        TEXT,    -- 特許/意匠/商標
    registration_number                TEXT,
    application_date                   TEXT,
    fi_classification_code             TEXT,
    fi_classification_code_ja          TEXT,
    f_term_theme_code                  TEXT,
    design_new_classification_code     TEXT,
    design_new_classification_code_ja  TEXT,
    trademark_class_code               TEXT,
    trademark_class_code_ja            TEXT,
    title                              TEXT,
    document_fixed_address             TEXT,
    loaded_at                          TEXT
);
```

インデックス: `corporate_number`, `patent_type`

### gbiz_zaimu（財務情報）

ユニーク制約: `(corporate_number, fiscal_period, accounting_standard)`

主な財務指標カラム: `net_sales`, `operating_revenue`, `operating_income`, `ordinary_income`,  
`recurring_profit`, `net_income`, `capital_stock`, `net_assets`, `total_assets`, `employee_count`,  
大株主1〜5 (`major_shareholder_1`〜`5`) + 持株比率 (`shareholder_1_ratio`〜`5_ratio`)

### gbiz_workplace（職場情報）

ユニーク制約: `(corporate_number)`

主なカラム: 平均継続勤務年数（男性・女性・正社員）、平均年齢、月平均所定外労働時間、  
女性労働者割合、女性管理職/役員人数、育児休業対象者・取得者数（男女別）

---

## API仕様メモ

### エンドポイント

```
GET https://api.info.gbiz.go.jp/hojin/v2/hojin/updateInfo/{suffix}
```

`suffix` は空文字（基本情報）または `certification` / `commendation` / `finance` /  
`patent` / `procurement` / `subsidy` / `workplace`。

### リクエスト

| パラメータ / ヘッダー | 必須 | 説明 |
|---|---|---|
| `X-hojinInfo-api-token` (header) | 必須 | APIトークン（`config/.env` の `GBIZINFO_API_TOKEN`） |
| `from` | 必須 | 取得開始日（**YYYYMMDD** 形式） |
| `to` | 必須 | 取得終了日（**YYYYMMDD** 形式） |
| `page` | 任意 | ページ番号（1始まり） |
| `metadata_flg` | 任意 | `false` を指定（メタデータ不要） |

> **注意:** NTA API の日付形式（`YYYY-MM-DD`）と異なり、gBizINFO は **`YYYYMMDD`** 形式。  
> `fetch_diff()` が `YYYY-MM-DD` または `date` オブジェクトを自動変換する。

### レスポンス構造

```json
{
  "totalCount": 1234,
  "totalPage": 3,
  "hojin-infos": [
    {
      "corporate_number": "...",
      "name": "...",
      ...
    }
  ]
}
```

### ページネーション

`totalPage > 1` の場合、`page=2, 3...` でループ取得する。  
接続エラー時は **指数バックオフ（最大3回）** で自動リトライする。

### データセット別ネスト構造

一部データセットは取得フィールドがネストされている。`fetch_diff()` 内のノーマライザーが展開する。

| データセット | ネストキー | 説明 |
|---|---|---|
| `todokedeninteijoho` | `certification` | 認定情報オブジェクト |
| `hyoshojoho` | `commendation` | 表彰情報オブジェクト |
| `chotatsujoho` | `procurement` | 調達情報オブジェクト |
| `hojokinjoho` | `subsidy` | 補助金情報オブジェクト |
| `tokkyojoho` | `patent` + `classifications[]` | 特許情報。分類コードは配列から展開 |
| `zaimujoho` | `finance` + `major_shareholders[]` | 財務情報。大株主は配列から1〜5に展開 |
| `shokubajoho` | `workplace_info.base_infos` ほか | 職場情報。複数ネスト階層 |
