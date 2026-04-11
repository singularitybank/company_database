# 国税庁 法人番号データ 仕様・運用ガイド

**対象データソース:** 国税庁 法人番号公表サイト  
**取得方式:** 全件ダウンロードCSV（初回）/ 差分API（定期更新）

---

## 目次

1. [処理フロー概要](#処理フロー概要)
2. [初回フルロード](#初回フルロード)
3. [定期差分更新](#定期差分更新)
4. [ファイル構成](#ファイル構成)
5. [DBスキーマ](#dbスキーマ)
6. [カラム定義](#カラム定義)
7. [コードテーブル](#コードテーブル)
8. [API仕様メモ](#api仕様メモ)

---

## 処理フロー概要

```
【初回フルロード】
国税庁サイト
  └─ 全件ダウンロードCSV (data/raw/*.csv)
        │
        │ nta_collector.py: convert_raw_to_staging()
        ▼
  Parquet (data/staging/nta_YYYYMMDD.parquet)
        │
        │ nta_collector.py: load_to_db()
        ▼
  companies テーブル（全件DELETE → INSERT）

【定期差分更新】
国税庁 差分API
        │
        │ nta_diff_collector.py: fetch_diff()
        ▼
  法人リスト (correct=0 & latest=1 のみ)
        │
        │ diff_processor.py: apply_diff()
        ▼
  companies テーブル（INSERT / UPDATE）
  change_history テーブル（変更履歴 INSERT）
```

---

## 初回フルロード

### 事前準備

1. 国税庁サイトから全件CSVをダウンロードして `data/raw/` に配置する  
   ファイル名例: `00_zenkoku_all_20260331.csv`
2. `config/.env` に `NTA_APPLICATION_ID` が設定済みであること

### 実行

```bash
# CSV → Parquet変換 → DB投入を一括実行
python src/collectors/nta_collector.py

# Parquet変換をスキップして既存のParquetを使う場合
python src/collectors/nta_collector.py --skip-staging
```

### 処理内容

| ステップ | 関数 | 説明 |
|---|---|---|
| CSV → Parquet | `convert_raw_to_staging()` | ヘッダーなしCSVに列名を付与し、Parquet形式で保存 |
| Parquet → DB | `load_to_db()` | companies テーブルを全件削除してから一括INSERT |

> **注意:** `load_to_db()` は既存データを全件削除してから投入する（フルリフレッシュ）。  
> 差分履歴（change_history）は削除されない。

---

## 定期差分更新

### 実行

```bash
# 昨日分の全国差分を取得してDBに適用（通常運用）
python scripts/run_nta_diff.py

# 期間を指定
python scripts/run_nta_diff.py --from 2026-04-01 --to 2026-04-10

# 都道府県を絞る（複数指定可）
python scripts/run_nta_diff.py --address 13
python scripts/run_nta_diff.py --address 13 14 27

# APIリクエスト間隔を変更（デフォルト: 1.0秒）
python scripts/run_nta_diff.py --wait 2.0

# DBパスを指定
python scripts/run_nta_diff.py --db /path/to/other.db
```

### オプション一覧

| オプション | デフォルト | 説明 |
|---|---|---|
| `--from` | 昨日 | 取得開始日 (YYYY-MM-DD) |
| `--to` | 今日 | 取得終了日 (YYYY-MM-DD) |
| `--address` | 全国 (01〜47+99) | 都道府県コード（複数スペース区切り） |
| `--wait` | 1.0 | APIリクエスト間の待機秒数 |
| `--db` | data/companies.db | SQLiteファイルパス |

### 処理内容

| ステップ | 処理 | 説明 |
|---|---|---|
| 1. API取得 | `fetch_diff()` | 都道府県コードごとにAPIをリクエスト。ページネーション自動処理 |
| 2. フィルタリング | `_parse_corporations()` | `correct=0 & latest=1` のレコードのみ抽出 |
| 3. 重複除去 | `_dedup_by_corporate_number()` | 同一法人番号が複数ある場合は `update_date` が最新のものを優先 |
| 4. DB適用 | `apply_diff()` | 新設・変更・閉鎖を判定してINSERT/UPDATE |
| 5. 変更履歴記録 | `_insert_change_history()` | 変更があったフィールドを `change_history` テーブルに記録 |

### DB適用ロジック

```
差分レコードの corporate_number が...
  ├─ DBに存在しない  →  INSERT（新設）
  └─ DBに存在する    →  TRACKED_FIELDS を比較
        ├─ 変更あり  →  UPDATE + change_history INSERT（変更・閉鎖）
        └─ 変更なし  →  スキップ
```

**変更履歴を記録するフィールド（TRACKED_FIELDS）:**

`name` / `furigana` / `kind` / `prefecture_name` / `city_name` / `street_number` /  
`prefecture_code` / `city_code` / `post_code` / `close_date` / `close_cause` / `process`

### ログ

実行ログは `logs/nta_diff_YYYYMMDD.log` に保存される（UTCではなくローカル時刻）。

---

## ファイル構成

```
src/
  collectors/
    nta_collector.py        全件CSVのParquet変換・DB投入
    nta_diff_collector.py   差分APIフェッチ・XML解析・ページネーション
  processors/
    diff_processor.py       差分検出・DB更新・変更履歴記録
  models/
    schema.py               DBスキーマ定義・COLUMN_MAP・init_db

scripts/
  run_nta_diff.py           定期差分更新のエントリーポイント

config/
  .env                      NTA_APPLICATION_ID を設定（gitignore対象）

data/
  raw/                      全件ダウンロードCSV置き場
  staging/                  中間Parquetファイル置き場
  companies.db              SQLiteデータベース

logs/
  nta_diff_YYYYMMDD.log     差分更新実行ログ
```

---

## DBスキーマ

### companies テーブル

法人の現在の最新状態を保持する。差分更新では常に最新状態に上書きされる。

```sql
CREATE TABLE companies (
    corporate_number            TEXT PRIMARY KEY,  -- 法人番号（13桁）
    name                        TEXT NOT NULL,     -- 商号又は名称
    furigana                    TEXT,
    kind                        TEXT,              -- 法人種別コード
    prefecture_name             TEXT,
    city_name                   TEXT,
    street_number               TEXT,
    prefecture_code             TEXT,
    city_code                   TEXT,
    post_code                   TEXT,
    close_date                  TEXT,              -- 閉鎖年月日
    close_cause                 TEXT,              -- 閉鎖事由コード
    successor_corporate_number  TEXT,
    assignment_date             TEXT,
    update_date                 TEXT,              -- 国税庁側の最終更新日
    process                     TEXT,              -- 処理区分コード
    hihyoji                     TEXT,              -- 検索対象除外フラグ
    sequence_number             TEXT,
    correct                     TEXT,              -- 訂正区分
    change_date                 TEXT,
    name_image_id               TEXT,
    address_image_id            TEXT,
    address_outside             TEXT,              -- 国外所在地
    address_outside_image_id    TEXT,
    change_cause                TEXT,              -- 変更事由の詳細
    latest                      TEXT,              -- 最新履歴フラグ
    en_name                     TEXT,
    en_prefecture_name          TEXT,
    en_city_name                TEXT,
    en_address_outside          TEXT,
    loaded_at                   TEXT NOT NULL      -- DB投入タイムスタンプ (UTC)
);
```

### change_history テーブル

差分更新で変更が検出されたフィールドの変更前後の値を追記する。レコードは削除されない。

```sql
CREATE TABLE change_history (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    corporate_number TEXT NOT NULL,
    field_name       TEXT NOT NULL,   -- 変更されたカラム名
    old_value        TEXT,
    new_value        TEXT,
    changed_at       TEXT NOT NULL    -- 国税庁側の update_date（なければ処理時刻）
);
```

### インデックス

| インデックス名 | カラム |
|---|---|
| `idx_companies_kind` | `kind` |
| `idx_companies_prefecture` | `prefecture_code` |
| `idx_companies_close_date` | `close_date` |
| `idx_change_history_corp` | `corporate_number` |

---

## カラム定義

CSV（camelCase）とDB（snake_case）のマッピングは `src/models/schema.py` の `COLUMN_MAP` で一元管理されている。

| CSV / XML フィールド名 | DB カラム名 | 説明 |
|---|---|---|
| `corporateNumber` | `corporate_number` | 法人番号（13桁）|
| `sequenceNumber` | `sequence_number` | 一連番号 |
| `process` | `process` | 処理区分コード |
| `correct` | `correct` | 訂正区分（0=正式, 1=訂正） |
| `updateDate` | `update_date` | 更新年月日 |
| `changeDate` | `change_date` | 変更年月日 |
| `name` | `name` | 商号又は名称 |
| `nameImageId` | `name_image_id` | 商号イメージID |
| `kind` | `kind` | 法人種別コード |
| `prefectureName` | `prefecture_name` | 都道府県 |
| `cityName` | `city_name` | 市区町村 |
| `streetNumber` | `street_number` | 丁目番地等 |
| `addressImageId` | `address_image_id` | 国内所在地イメージID |
| `prefectureCode` | `prefecture_code` | 都道府県コード（2桁） |
| `cityCode` | `city_code` | 市区町村コード |
| `postCode` | `post_code` | 郵便番号 |
| `addressOutside` | `address_outside` | 国外所在地 |
| `addressOutsideImageId` | `address_outside_image_id` | 国外所在地イメージID |
| `closeDate` | `close_date` | 閉鎖年月日 |
| `closeCause` | `close_cause` | 閉鎖事由コード |
| `successorCorporateNumber` | `successor_corporate_number` | 承継先法人番号 |
| `changeCause` | `change_cause` | 変更事由の詳細 |
| `assignmentDate` | `assignment_date` | 法人番号指定年月日 |
| `latest` | `latest` | 最新履歴フラグ（1=最新） |
| `enName` | `en_name` | 商号（英語） |
| `enPrefectureName` | `en_prefecture_name` | 都道府県（英語） |
| `enCityName` | `en_city_name` | 市区町村（英語） |
| `enAddressOutside` | `en_address_outside` | 国外所在地（英語） |
| `furigana` | `furigana` | フリガナ |
| `hihyoji` | `hihyoji` | 検索対象除外（0=表示, 1=除外） |
| *(内部付与)* | `loaded_at` | DB投入タイムスタンプ（UTC） |

---

## コードテーブル

### 処理区分 (`process`)

| コード | 意味 |
|---|---|
| `01` | 新規 |
| `11` | 商号又は名称の変更 |
| `12` | 国内所在地の変更 |
| `13` | 国外所在地の変更 |
| `21` | 登記記録の閉鎖等 |
| `22` | 登記記録の復活等 |
| `71` | 吸収合併 |
| `72` | 吸収合併無効 |
| `81` | 商号の登記の抹消 |
| `99` | 削除 |

### 法人種別 (`kind`)

| コード | 意味 |
|---|---|
| `101` | 国の機関 |
| `201` | 地方公共団体 |
| `301` | 株式会社 |
| `302` | 有限会社 |
| `303` | 合名会社 |
| `304` | 合資会社 |
| `305` | 合同会社 |
| `399` | その他の設立登記法人 |
| `401` | 外国会社等 |
| `499` | その他 |

### 閉鎖事由 (`close_cause`)

| コード | 意味 |
|---|---|
| `01` | 清算の結了等 |
| `11` | 合併による解散等 |
| `21` | 登記官による閉鎖 |
| `31` | その他の清算の結了等 |

---

## API仕様メモ

### エンドポイント

```
GET https://api.houjin-bangou.nta.go.jp/4/diff
```

### パラメータ

| パラメータ | 必須 | 説明 |
|---|---|---|
| `id` | 必須 | アプリケーションID（`config/.env` の `NTA_APPLICATION_ID`） |
| `from` | 必須 | 取得開始日（YYYY-MM-DD） |
| `to` | 必須 | 取得終了日（YYYY-MM-DD） |
| `type` | 必須 | `12` = XML形式（UTF-8） |
| `address` | 任意 | 都道府県コード（01〜47, 99=海外）。省略時は全国 |
| `divide` | 任意 | ページ番号（1〜）。省略時は1 |

### レスポンス構造

```xml
<corporations>
  <lastUpdateDate>2026-04-10</lastUpdateDate>
  <count>1421</count>        <!-- 総件数 -->
  <divideNumber>1</divideNumber>   <!-- 現在のページ番号 -->
  <divideSize>1</divideSize>       <!-- 総ページ数 -->
  <corporation>
    <corporateNumber>...</corporateNumber>
    <correct>0</correct>   <!-- 0=正式レコード, 1=訂正履歴 -->
    <latest>1</latest>     <!-- 1=この法人の最新状態 -->
    ...
  </corporation>
</corporations>
```

### 制約

- 1リクエストあたり最大 **2,000件**
- 2,000件を超える場合は `divideSize > 1` になるため、`divide=2, 3...` でページネーションが必要
- 取得対象を都道府県コードで絞ることで、1リクエストを2,000件以内に抑えることが多い
- 接続エラー時は **指数バックオフ（最大3回）** で自動リトライする

### レコードの取り扱い

差分APIは同一法人の複数の変更イベントをすべて返す。  
`correct=0`（正式）かつ `latest=1`（最新）のレコードのみを処理対象とする。

| `correct` | `latest` | 取り扱い |
|---|---|---|
| `0` | `1` | **処理対象**（現在の最新状態） |
| `0` | `0` | スキップ（古い変更イベント） |
| `1` | any | スキップ（訂正履歴） |
