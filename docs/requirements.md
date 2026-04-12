# 企業情報データベース構築プロジェクト 要件定義書

**バージョン:** 1.0  
**作成日:** 2026-04-07  
**担当:** Minseok Kang（個人開発）

---

## 1. プロジェクト概要

### 1.1 目的
国内公的機関が公開する企業情報を統合・蓄積し、独自の企業情報データベースを構築する。
データ収集の自動化により、新設・変更・閉鎖などの最新情報を継続的に維持・管理する。

### 1.2 スコープ
- **対象データソース**
  - 国税庁 法人番号公表サイト（法人番号・基本情報）
  - gBizInfo（経済産業省）（財務・特許・調達・認定情報）
  - 職場情報総合サイト・しょくばらぼ（厚生労働省）（労働環境・雇用情報）
  - ハローワークインターネットサービス（厚生労働省）（求人情報・日次）
- **対象企業:** 国内法人全般（法人番号を持つ法人）
- **開発規模:** 個人開発（一人）

### 1.3 開発方針
- シンプルさを優先し、段階的に機能を拡張する（MVP → 拡張）
- 公的APIが提供されている場合は優先的に利用し、スクレイピングは補完手段とする
- メンテナンス性を重視した設計（個人でも運用できる複雑度に留める）

---

## 2. システム構成

```
[データ収集層]
  ├── 国税庁 全件CSVダウンロード + 差分API
  ├── gBizInfo 全件CSVダウンロード + 差分API
  ├── しょくばらぼ 全件CSVダウンロード（一括）
  └── ハローワークインターネットサービス Seleniumクローリング（日次）

[データ処理層（Parquetステージング）]
  ├── 生データ（CSV/JSON/HTML）→ Parquet変換
  ├── 差分検出（新設・変更・閉鎖）
  └── Parquet → SQLite ロード

[データ格納層]
  ├── data/companies.db  （国税庁：法人基本情報・変更履歴）
  ├── data/gbizinfo.db   （gBizInfo：財務・特許・補助金・認定等）
  └── data/shokuba.db    （しょくばらぼ：労働環境情報）
  ※ ハローワーク求人情報は Parquet のみ蓄積（SQLite未導入）

[利用層]
  ├── Pythonスクリプトによる直接クエリ
  └── CSVエクスポート（未実装）
```

---

## 3. データ要件

### 3.1 収集するデータ項目

#### 基本情報（国税庁 法人番号公表サイト）
| フィールド名 | 説明 | 取得方法 |
|---|---|---|
| corporate_number | 法人番号（13桁） | API |
| name | 法人名（漢字） | API |
| name_kana | 法人名（フリガナ） | API |
| prefecture_name | 都道府県 | API |
| city_name | 市区町村 | API |
| street_number | 丁目番地 | API |
| postal_code | 郵便番号 | API |
| status | 登記記録の状態（現役/閉鎖等） | API |
| update_date | 最終更新日 | API |
| close_date | 閉鎖日 | API |
| close_cause | 閉鎖事由 | API |

#### 財務・企業情報（gBizInfo）
| フィールド名 | 説明 | 取得方法 |
|---|---|---|
| employee_number | 従業員数 | API |
| capital_amount | 資本金 | API |
| net_sales | 売上高 | API |
| business_category | 業種コード・業種名 | API |
| establishment_date | 設立年月日 | API |
| company_url | 企業URL | API |
| certification_info | 認定・表彰情報 | API |

#### 労働環境情報（しょくばらぼ）
全件CSVを8テーブルに分割してロードする（636列）。

| テーブル名 | 概要 | 列数目安 |
|---|---|---|
| shokuba_basic | 基本情報・就業場所 | 39列 |
| shokuba_recruitment | 採用・定着・インターン・社会保険 | 84列 |
| shokuba_work_hours | 残業・有給・テレワーク | 135列 |
| shokuba_employment_system | 多様な正社員制度・転換実績・定年制 | 132列 |
| shokuba_female_workforce | 女性労働者割合・管理職・役員 | 28列 |
| shokuba_childcare | 育児休業・両立支援 | 56列 |
| shokuba_career | キャリア開発・行動計画 | 25列 |
| shokuba_certification | 認定・表彰 | 137列 |

#### ハローワーク求人情報
| フィールド名 | 説明 | 取得方法 |
|---|---|---|
| job_number | 求人番号 | クローリング |
| company_name | 事業所名 | クローリング |
| corporate_number | 法人番号 | クローリング |
| job_title | 職種 | クローリング |
| employment_type | 雇用形態 | クローリング |
| wage / base_salary | 賃金・基本給 | クローリング |
| work_hours | 就業時間 | クローリング |
| annual_holidays | 年間休日数 | クローリング |
| （他210列超） | 求人票の全項目 | クローリング |

### 3.2 データ量見積もり
- 国内法人番号登録数：約600万件（アクティブ法人は約300万件）
- 初期構築時は対象を絞る（例：従業員数10名以上、または特定業種・地域）
- フルスキャンは段階的に実施

---

## 4. 機能要件

### 4.1 データ収集機能

#### FR-01: 法人番号APIによる基本情報取得
- 国税庁の法人番号公表サイトAPIを使い、法人番号・基本情報を取得する
- 一括ダウンロード（全件CSVファイル）とAPI検索の両方に対応する
- 取得結果をDBに格納する

#### FR-02: gBizInfo APIによる企業詳細情報取得
- gBizInfo RESTful APIを使い、法人番号をキーに財務・雇用情報を取得する
- APIレート制限（利用規約に準拠）を遵守するためのウェイト制御を実装する

#### FR-03: しょくばらぼ 全件CSVダウンロード
- しょくばらぼ（`https://shokuba.mhlw.go.jp`）の全件CSVを取得する
- Selenium（Microsoft Edge）でダウンロードし、Parquet変換後にSQLiteへロードする
- robots.txtおよびサイト利用規約を遵守する

#### FR-04a: ハローワーク求人情報収集（日次）
- ハローワークインターネットサービスから当日公開の求人情報をSeleniumでクロールする
- 処理フロー: 求人番号収集 → 詳細HTMLダウンロード → Parquet変換
- 実行単位: 日次（Windowsタスクスケジューラーで自動実行）
- 取得ファイル: `data/staging/hellowork_YYYYMMDD.parquet`

#### FR-04b: 定期自動更新
- Windowsタスクスケジューラーによる自動実行（APSchedulerは不採用）
- 更新頻度：
  - 法人基本情報（NTA差分）: 週1回
  - gBizInfo: 月1回
  - 職場情報（しょくばらぼ）: 月1回
  - ハローワーク求人情報: 毎日

### 4.2 差分検出・変更管理機能

#### FR-05: 変更検出
- 既存レコードと新規取得データを比較し、変更点（住所変更・名称変更など）を検出する
- 変更履歴テーブルに変更前後の値とタイムスタンプを記録する

#### FR-06: 新設・閉鎖の検出
- 新規登録法人を検出し「新設」フラグを付与する
- 閉鎖・解散法人を検出し「閉鎖」フラグを付与する
- これらのイベントをログファイルまたはDBテーブルに記録する

### 4.3 データ管理機能

#### FR-07: 重複排除・名寄せ
- 法人番号をプライマリキーとして重複を排除する
- 同一法人の複数データソースを法人番号で結合する

#### FR-08: データエクスポート
- 任意条件でのCSVエクスポート機能
- 都道府県・業種・従業員規模などでのフィルタリング

---

## 5. 非機能要件

### 5.1 パフォーマンス
- 1回の定期実行で差分データの取得・格納を2時間以内に完了する（初期段階）
- APIレートリミットに引っかからないよう、リクエスト間隔を適切に設定する

### 5.2 信頼性・エラーハンドリング
- ネットワークエラー・APIエラー時はリトライ（最大3回、指数バックオフ）
- エラーはログファイルに記録し、処理を継続する（部分失敗を許容）
- 実行ログは日付別ファイルで管理する

### 5.3 保守性
- 1ファイルの行数は400行以内を目安とし、モジュールを分割する
- 設定値（APIキー・スケジュール間隔など）は設定ファイル（`.env` または `config.yaml`）で管理する
- APIキー・認証情報はコードにハードコーディングしない

### 5.4 法令・倫理遵守
- 各サイトのrobots.txtおよび利用規約を遵守する
- スクレイピング時はアクセス間隔を設け、サーバー負荷を最小化する
- 収集したデータは個人利用・研究目的の範囲内で使用する

---

## 6. 技術スタック

| 分類 | 採用技術 | 理由 |
|---|---|---|
| 言語 | Python 3.13+ | データ処理・スクレイピングのエコシステムが充実 |
| DB | SQLite（初期）| 個人開発・ローカル運用に最適、依存なし |
| 中間ストア | Apache Parquet（PyArrow） | 型保持・高速な列指向フォーマット |
| ORM/クエリ | pandas + sqlite3 | 柔軟なデータ操作（SQLAlchemyは不採用） |
| API通信 | requests | HTTPクライアント |
| スクレイピング | Selenium（Microsoft Edge） | JavaScript依存サイト対応 |
| スケジューラー | Windowsタスクスケジューラー | 追加依存なし・OS標準機能（APSchedulerは不採用） |
| 設定管理 | python-dotenv | 環境変数・APIキー管理 |
| ログ | logging（標準ライブラリ） | 追加依存なし |
| バージョン管理 | Git / GitHub | ソース管理 |

---

## 7. ディレクトリ構成

```
company_database/
├── config/
│   ├── .env                        # APIキー等（gitignore対象）
│   └── config.yaml                 # 設定値（パス・タイミング等）
├── src/
│   ├── config.py                   # 設定の一元管理
│   ├── logging_setup.py            # ログ設定
│   ├── extractors/                 # 差分データ収集（NTA・gBizInfo）
│   ├── downloaders/                # 全件ダウンロード（しょくばらぼ）
│   ├── crawlers/                   # Seleniumクローラー（ハローワーク）
│   ├── parsers/                    # HTMLパーサー（ハローワーク）
│   ├── converters/                 # 生データ → Parquet 変換
│   ├── loaders/                    # Parquet → SQLite ロード
│   ├── models/                     # DBスキーマ定義（NTA用）
│   ├── processors/                 # 差分検出
│   └── utils/                      # 共通ユーティリティ
├── scripts/                        # 実行エントリーポイント
│   ├── run_nta.py                  # NTA 全件取込
│   ├── run_nta_diff.py             # NTA 差分更新
│   ├── run_gbizinfo_diff.py        # gBizInfo 差分更新
│   ├── run_shokuba.py              # しょくばらぼ 全件取込
│   └── run_hellowork.py            # ハローワーク 日次取得
├── data/
│   ├── companies.db                # SQLite: NTA法人基本情報
│   ├── gbizinfo.db                 # SQLite: gBizInfo詳細情報
│   ├── shokuba.db                  # SQLite: 職場情報
│   ├── raw/                        # 生データ（CSV/JSON）
│   │   ├── nta/
│   │   ├── gbizinfo/
│   │   └── shokuba/
│   └── staging/                    # Parquet中間ストア
│       ├── nta_YYYYMMDD.parquet
│       ├── gbizinfo_*.parquet
│       ├── shokuba_*.parquet
│       └── hellowork_YYYYMMDD.parquet
├── logs/                           # 実行ログ（日付別）
├── docs/
│   ├── requirements.md             # 本ドキュメント
│   ├── nta.md                      # NTA 詳細仕様
│   ├── gbizinfo.md                 # gBizInfo 詳細仕様
│   ├── shokuba.md                  # しょくばらぼ 詳細仕様
│   └── hellowork.md                # ハローワーク 詳細仕様
├── tests/
├── CLAUDE.md
└── requirements.txt
```

---

## 8. DBスキーマ（主要テーブル）

DBはデータソース単位で3ファイルに分離している。法人番号（`corporate_number`）を共通キーとして横断クエリが可能。統合DBへの移行は将来課題。

### data/companies.db（国税庁 法人基本情報）

#### companies
国税庁全件CSVの全30フィールドをそのまま格納する（フィールド名はAPIの camelCase を snake_case に変換）。

| 主要カラム | 説明 |
|---|---|
| `corporate_number` | 法人番号（13桁）PRIMARY KEY |
| `name` / `furigana` | 法人名・フリガナ |
| `kind` | 法人種別コード |
| `prefecture_name` / `city_name` / `street_number` | 所在地 |
| `close_date` / `close_cause` | 閉鎖日・閉鎖事由 |
| `update_date` | 最終更新日 |
| `en_name` / `en_prefecture_name` 等 | 英語表記 |
| `loaded_at` | DBロード日時 |

#### change_history
差分更新時に変更されたフィールドを記録する。

```sql
CREATE TABLE change_history (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    corporate_number TEXT NOT NULL,
    field_name       TEXT NOT NULL,
    old_value        TEXT,
    new_value        TEXT,
    changed_at       TEXT NOT NULL
);
```

---

### data/gbizinfo.db（gBizInfo 詳細情報）

Parquetスキーマから DDL を自動生成するため、カラム定義はスキーマファイルが正。

| テーブル名 | 内容 |
|---|---|
| `gbiz_companies` | 法人基本情報（`corporate_number` で UPSERT） |
| `gbiz_financial` | 財務情報 |
| `gbiz_financial_statement` | 決算情報 |
| `gbiz_patent` | 特許情報 |
| `gbiz_subsidy` | 補助金情報 |
| `gbiz_procurement` | 調達情報 |
| `gbiz_commendation` | 表彰情報 |
| `gbiz_workplace` | 職場情報（gBizInfo版） |
| `gbiz_certification` | 届出・認定情報 |
| `gbiz_*_meta` | 各テーブルのメタ情報（JSON） |

---

### data/shokuba.db（しょくばらぼ 職場情報）

全件CSV（636列）を8テーブルに分割してロード。スキーマはParquetから自動生成。全テーブルに `corporate_number` インデックスあり。更新戦略は全件入れ替え（差分APIなし）。

| テーブル名 | 内容 |
|---|---|
| `shokuba_basic` | 基本情報・就業場所 |
| `shokuba_recruitment` | 採用・定着・社会保険 |
| `shokuba_work_hours` | 残業・有給・テレワーク |
| `shokuba_employment_system` | 多様な正社員制度・定年制 |
| `shokuba_female_workforce` | 女性労働者・管理職割合 |
| `shokuba_childcare` | 育児休業・両立支援 |
| `shokuba_career` | キャリア開発・行動計画 |
| `shokuba_certification` | 認定・表彰 |

---

### ハローワーク求人情報（Parquetのみ）

日次クロールの結果は `data/staging/hellowork_YYYYMMDD.parquet` に蓄積する。SQLiteへのロードは未実装。`corporate_number` で companies.db と JOIN 可能。

---

## 9. 開発フェーズ計画

### Phase 1: 基盤構築（MVP）
- ✅ DBスキーマ設計・作成
- ✅ 国税庁 全件CSVによる基本情報取得・格納
- ✅ gBizInfo 全件CSVによる詳細情報取得・格納
- ✅ ログ機能の実装

### Phase 2: 自動化
- ✅ 差分検出・変更履歴記録の実装（NTA・gBizInfo）
- ✅ Windowsタスクスケジューラーによる自動実行
- ✅ エラーハンドリング・リトライ機能の実装

### Phase 3: データ拡充
- ✅ しょくばらぼ 全件CSV取込の実装
- ✅ ハローワーク求人情報 日次クロールの実装（追加データソース）
- [ ] ハローワーク求人情報の SQLite ロード
- [ ] CSVエクスポート機能の実装
- [ ] データクレンジングの強化

### Phase 4: 拡張（任意）
- [ ] DBをPostgreSQLへ移行
- [ ] 簡易WebUI（FastAPI + Jinja2）の追加
- [ ] 追加データソースの統合

---

## 10. リスクと対策

| リスク | 影響 | 対策 |
|---|---|---|
| APIの仕様変更・廃止 | データ取得不能 | 仕様変更検知ロジックの実装、定期確認 |
| スクレイピング対象サイトの構造変更 | スクレイピング失敗 | エラーログで即時検知、セレクタを設定ファイルで管理 |
| APIレートリミット超過 | アクセス制限 | リクエスト間隔制御、バッチ処理の分割 |
| ストレージ容量不足 | DB書き込み失敗 | 定期的な容量確認、不要な生データの削除 |
| 個人での継続運用が困難 | プロジェクト停滞 | シンプルな設計維持、自動化による運用負荷低減 |

---

## 11. 参照リソース

| リソース | URL |
|---|---|
| 国税庁 法人番号公表サイト API仕様 | https://www.houjin-bangou.nta.go.jp/webapi/ |
| gBizInfo API仕様 | https://info.gbiz.go.jp/api/index.html |
| 職場情報総合サイト（しょくばらぼ） | https://shokuba.mhlw.go.jp/ |
| gBizInfo 全件ダウンロード | https://info.gbiz.go.jp/hojin/downloadTop |
