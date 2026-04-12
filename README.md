# 企業情報データベース

国内公的機関が公開する企業情報を自動収集・統合する個人開発プロジェクト。
法人番号（`corporate_number`）を共通キーとして複数のデータソースを横断クエリできる。

---

## データソース

| データソース | 取得方式 | 更新頻度 | 格納先 |
|---|---|---|---|
| [国税庁 法人番号公表サイト](docs/nta.md) | 全件CSV（初回）/ 差分API（定期） | 週1回 | `data/companies.db` |
| [gBizInfo（経済産業省）](docs/gbizinfo.md) | 全件CSV（初回）/ 差分API（定期） | 月1回 | `data/gbizinfo.db` |
| [しょくばらぼ（厚生労働省）](docs/shokuba.md) | 全件CSV（自動ダウンロード） | 月1回 | `data/shokuba.db` |
| [ハローワークインターネットサービス](docs/hellowork.md) | Seleniumクローリング（日次） | 毎日 | `data/staging/hellowork_YYYYMMDD.parquet` |

---

## セットアップ

### 前提条件

- Python 3.13+
- Microsoft Edge（ハローワーククローラー用）
- conda 環境（推奨）

### 1. 依存パッケージのインストール

```bash
conda activate data
pip install -r requirements.txt
```

### 2. 設定ファイルの準備

```bash
cp config/.env.example config/.env   # .env.example が存在する場合
```

`config/.env` を編集して APIキーを設定する:

```
NTA_APPLICATION_ID=your_nta_app_id
GBIZINFO_API_TOKEN=your_gbizinfo_token
```

APIキーの取得先:
- NTA: [法人番号公表サイト Web-API 機能の利用申請](https://www.houjin-bangou.nta.go.jp/webapi/)
- gBizInfo: [gBizInfo API 申請](https://info.gbiz.go.jp/api/index.html)

### 3. ディレクトリの確認

初回実行前に以下のディレクトリが存在することを確認する（`data/` 以下は git 管理対象外）:

```
data/raw/nta/
data/raw/gbizinfo/
data/raw/shokuba/
data/staging/
```

---

## 実行方法

### 国税庁（NTA）

```bash
# 初回: 全件CSVをダウンロードして data/raw/nta/ に配置後
python scripts/run_nta.py

# 定期差分更新
python scripts/run_nta_diff.py
```

詳細は [docs/nta.md](docs/nta.md) を参照。

---

### gBizInfo

```bash
# 初回: 全件CSVを data/raw/gbizinfo/ に配置後
python scripts/run_gbizinfo_diff.py --full

# 定期差分更新
python scripts/run_gbizinfo_diff.py
```

詳細は [docs/gbizinfo.md](docs/gbizinfo.md) を参照。

---

### しょくばらぼ

```bash
# 全件取込（ダウンロード → Parquet変換 → SQLiteロード）
python scripts/run_shokuba.py
```

詳細は [docs/shokuba.md](docs/shokuba.md) を参照。

---

### ハローワーク（日次）

```bash
# 当日分（フルフロー）
python scripts/run_hellowork.py

# 日付指定
python scripts/run_hellowork.py --date 2026-04-10

# HTMLが取得済みでParquet変換のみ実行
python scripts/run_hellowork.py --date 2026-04-10 --skip-crawl
```

**Windowsタスクスケジューラーへの登録:** `scripts/run_hellowork.bat` から呼び出す（`--headless` 付き）。

詳細は [docs/hellowork.md](docs/hellowork.md) を参照。

---

## データ構造

### DB一覧

| ファイル | 主要テーブル | 法人数目安 |
|---|---|---|
| `data/companies.db` | `companies`, `change_history` | 約600万件 |
| `data/gbizinfo.db` | `gbiz_companies`, `gbiz_financial`, `gbiz_patent` 他 | 約300万件 |
| `data/shokuba.db` | `shokuba_basic`, `shokuba_work_hours` 他8テーブル | 約60万件 |

### データ結合例

```python
import sqlite3
import pandas as pd

# NTA基本情報 + gBizInfo財務情報を法人番号で結合
nta = sqlite3.connect("data/companies.db")
gbiz = sqlite3.connect("data/gbizinfo.db")

companies = pd.read_sql("SELECT corporate_number, name, prefecture_name FROM companies", nta)
financial = pd.read_sql("SELECT corporate_number, net_sales, employee_number FROM gbiz_companies", gbiz)

merged = companies.merge(financial, on="corporate_number", how="left")
```

```python
import pandas as pd

# ハローワーク求人情報（Parquet）+ NTA基本情報
hellowork = pd.read_parquet("data/staging/hellowork_20260412.parquet")
companies  = pd.read_sql("SELECT corporate_number, name FROM companies", nta_conn)

merged = hellowork.merge(companies, on="corporate_number", how="left")
```

---

## ディレクトリ構成

```
company_database/
├── config/
│   ├── .env            # APIキー（gitignore対象）
│   └── config.yaml     # パス・タイミング設定
├── src/
│   ├── config.py       # 設定の一元管理
│   ├── extractors/     # 差分データ収集（NTA・gBizInfo）
│   ├── downloaders/    # 全件ダウンロード（しょくばらぼ）
│   ├── crawlers/       # Seleniumクローラー（ハローワーク）
│   ├── parsers/        # HTMLパーサー（ハローワーク）
│   ├── converters/     # 生データ → Parquet変換
│   ├── loaders/        # Parquet → SQLiteロード
│   ├── models/         # DBスキーマ定義（NTA）
│   ├── processors/     # 差分検出
│   └── utils/          # 共通ユーティリティ
├── scripts/            # 実行エントリーポイント
├── data/
│   ├── companies.db
│   ├── gbizinfo.db
│   ├── shokuba.db
│   ├── raw/            # 生データ（CSV/JSON）
│   └── staging/        # Parquet中間ストア
├── logs/               # 実行ログ（日付別）
└── docs/               # 詳細仕様ドキュメント
```

---

## ドキュメント

| ドキュメント | 内容 |
|---|---|
| [docs/requirements.md](docs/requirements.md) | 要件定義・設計方針・フェーズ計画 |
| [docs/nta.md](docs/nta.md) | NTA詳細仕様・カラム定義・API仕様 |
| [docs/gbizinfo.md](docs/gbizinfo.md) | gBizInfo詳細仕様・テーブル構成 |
| [docs/shokuba.md](docs/shokuba.md) | しょくばらぼ詳細仕様・カラム定義 |
| [docs/hellowork.md](docs/hellowork.md) | ハローワーク詳細仕様・出力カラム一覧 |
