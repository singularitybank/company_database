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
  ├── 国税庁API (法人番号公表API)
  ├── gBizInfo API
  ├── Selenium/requests スクレイピング（職場情報等）
  └── スケジューラー（自動更新）

[データ処理層]
  ├── 名寄せ・重複排除
  ├── データ正規化・クレンジング
  └── 差分検出（新設・変更・閉鎖）

[データ格納層]
  └── SQLiteデータベース（初期）→ PostgreSQL（将来拡張）

[利用層]
  ├── Pythonスクリプトによる直接クエリ
  └── CSVエクスポート
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

#### 労働環境情報（職場情報総合サイト）
| フィールド名 | 説明 | 取得方法 |
|---|---|---|
| avg_work_hours | 平均残業時間 | スクレイピング |
| paid_leave_rate | 有給取得率 | スクレイピング |
| female_manager_ratio | 女性管理職比率 | スクレイピング |
| new_graduate_retention | 新卒3年後定着率 | スクレイピング |

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

#### FR-03: 職場情報スクレイピング
- しょくばらぼ（`https://shokuba.mhlw.go.jp`）から労働環境情報を取得する
- Selenium（またはrequests + BeautifulSoup）で実装する
- robots.txtおよびサイト利用規約を遵守する

#### FR-04: 定期自動更新
- スケジューラー（APScheduler または Windowsタスクスケジューラー）による自動実行
- 更新頻度：
  - 基本情報（法人番号差分）: 週1回
  - gBizInfo: 月1回
  - 職場情報: 月1回

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
| ORM/クエリ | SQLAlchemy + pandas | 柔軟なデータ操作 |
| API通信 | requests / httpx | HTTPクライアント |
| スクレイピング | Selenium + BeautifulSoup | JavaScript依存サイト対応 |
| スケジューラー | APScheduler | Pythonのみで完結するスケジューリング |
| 設定管理 | python-dotenv | 環境変数・APIキー管理 |
| ログ | logging（標準ライブラリ） | 追加依存なし |
| バージョン管理 | Git / GitHub | ソース管理 |

---

## 7. ディレクトリ構成（案）

```
company_database/
├── config/
│   ├── .env                  # APIキー等（gitignore対象）
│   └── config.yaml           # 設定値
├── src/
│   ├── collectors/
│   │   ├── nta_collector.py       # 国税庁API
│   │   ├── gbizinfo_collector.py  # gBizInfo API
│   │   └── mhlw_scraper.py        # 職場情報スクレイピング
│   ├── processors/
│   │   ├── normalizer.py          # データ正規化
│   │   ├── deduplicator.py        # 重複排除
│   │   └── diff_detector.py       # 差分検出
│   ├── models/
│   │   └── schema.py              # DBスキーマ定義
│   ├── exporters/
│   │   └── csv_exporter.py        # CSVエクスポート
│   └── scheduler.py               # 定期実行スケジューラー
├── data/
│   ├── raw/                  # 取得した生データ（一時保存）
│   └── company.db            # SQLiteデータベース
├── logs/                     # 実行ログ
├── docs/
│   └── requirements.md       # 本ドキュメント
├── tests/                    # テストコード
├── requirements.txt
└── README.md
```

---

## 8. DBスキーマ（主要テーブル）

### companies（法人基本情報）
```sql
CREATE TABLE companies (
    corporate_number TEXT PRIMARY KEY,  -- 法人番号（13桁）
    name             TEXT NOT NULL,
    name_kana        TEXT,
    postal_code      TEXT,
    prefecture_name  TEXT,
    city_name        TEXT,
    street_number    TEXT,
    status           TEXT,              -- 'active' / 'closed'
    close_date       DATE,
    close_cause      TEXT,
    created_at       DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at       DATETIME DEFAULT CURRENT_TIMESTAMP
);
```

### company_details（gBizInfo詳細情報）
```sql
CREATE TABLE company_details (
    corporate_number  TEXT PRIMARY KEY REFERENCES companies(corporate_number),
    employee_number   INTEGER,
    capital_amount    BIGINT,
    net_sales         BIGINT,
    business_category TEXT,
    establishment_date DATE,
    company_url       TEXT,
    fetched_at        DATETIME DEFAULT CURRENT_TIMESTAMP
);
```

### workplace_info（職場情報）
```sql
CREATE TABLE workplace_info (
    corporate_number      TEXT PRIMARY KEY REFERENCES companies(corporate_number),
    avg_work_hours        REAL,
    paid_leave_rate       REAL,
    female_manager_ratio  REAL,
    new_graduate_retention REAL,
    fetched_at            DATETIME DEFAULT CURRENT_TIMESTAMP
);
```

### change_history（変更履歴）
```sql
CREATE TABLE change_history (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    corporate_number TEXT NOT NULL,
    field_name       TEXT NOT NULL,
    old_value        TEXT,
    new_value        TEXT,
    changed_at       DATETIME DEFAULT CURRENT_TIMESTAMP
);
```

---

## 9. 開発フェーズ計画

### Phase 1: 基盤構築（MVP）
- [ ] DBスキーマ設計・作成
- [ ] 国税庁API / 全件CSVによる基本情報取得・格納
- [ ] gBizInfo APIによる詳細情報取得・格納
- [ ] ログ機能の実装

### Phase 2: 自動化
- [ ] 差分検出・変更履歴記録の実装
- [ ] 定期スケジューラーの実装
- [ ] エラーハンドリング・リトライ機能の実装

### Phase 3: データ拡充
- [ ] 職場情報スクレイピングの実装
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
