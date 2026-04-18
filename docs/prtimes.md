# PR Times 収集システム 実装ドキュメント

## 概要

PR TimesのグローバルRSSを1時間毎に収集し、プレスリリース記事・企業情報をDBに蓄積する。
記事本文は生HTMLファイルとパース済みテキストの両方を保持する。

---

## 全体処理フロー

```
[毎時00分] run_prtimes_rss.bat（1本のバッチで全フェーズを処理）

  STEP 1-3: RSS フェーズ
    グローバルRSS取得 → パース → 記事DB保存（INSERT OR IGNORE）
    ※ 新company_id は prtimes_companies に仮登録（company_name="未取得"）

  STEP 4-6: 記事スクレイピングフェーズ
    未取得記事HTML取得（requests）→ パース → DB更新

  STEP 7-9: 企業スクレイピングフェーズ
    scraped_at IS NULL の企業ページ取得（Selenium）→ パース → DB更新
    ※ 1時間分の新規企業のみ対象のため件数制限なし・IP制限リスク低
```

---

## ファイル構成

```
src/
├── crawlers/
│   ├── prtimes_rss_crawler.py      # RSS XML取得
│   ├── prtimes_article_crawler.py  # 記事ページHTML取得・保存
│   └── prtimes_company_crawler.py  # 企業ページHTML取得・保存（Selenium）
├── parsers/
│   ├── prtimes_rss_parser.py       # RSS XML → 構造化データ
│   ├── prtimes_article_parser.py   # 記事HTML → 構造化テキスト
│   └── prtimes_company_parser.py   # 企業HTML → 企業情報
├── loaders/
│   └── prtimes_db_loader.py        # 各パース結果 → prtimes.db
├── models/
│   └── prtimes_schema.py           # DDL定義・init_db()・マイグレーション
├── utils/
│   └── selenium_utils.py           # Edge WebDriver 共通初期化
scripts/
├── run_prtimes_rss.py              # Phase 1+2: 1時間毎RSS収集 + 記事スクレイピング
├── run_prtimes_rss.bat             # Task Scheduler用バッチ
└── run_prtimes_companies.py        # Phase 3: 企業情報収集（随時実行）
data/
└── prtimes.db                      # PR Times専用DB
```

### 生HTMLファイル保存先

```
C:\Temp\html\prtimes\
├── articles\
│   └── {YYYYMMDD}\
│       └── {article_id}.html       # 記事生HTML（SSR）
└── companies\
    └── {company_id}.html           # 企業ページ生HTML（SPA、Seleniumで取得）
```

---

## DB設計 (prtimes.db)

### `prtimes_companies` — PR Times企業マスタ

| カラム | 型 | 説明 |
|---|---|---|
| prtimes_company_id | INTEGER PK | PR TimesのID |
| company_name | TEXT NOT NULL | 企業名（RSS仮登録時は"未取得"、企業ページ取得後に更新） |
| company_name_kana | TEXT | フリガナ |
| industry | TEXT | 業種（企業ページ `aside[aria-label="企業データ"]` より） |
| prefecture | TEXT | 都道府県（addressから自動抽出） |
| address | TEXT | 本社所在地 |
| phone_number | TEXT | 電話番号 |
| representative | TEXT | 代表者名 |
| listed | TEXT | 上場区分（例: 東証プライム） |
| capital | TEXT | 資本金 |
| established | TEXT | 設立年月 |
| company_description | TEXT | 企業説明（`p[class*="companyDescription"]`） |
| website_url | TEXT | 企業サイトURL |
| x_url | TEXT | X（旧Twitter）URL |
| facebook_url | TEXT | Facebook URL |
| youtube_url | TEXT | YouTube URL |
| press_release_count | INTEGER | プレスリリース件数 |
| corporate_number | TEXT | 法人番号（名寄せ後に付与） |
| name_match_score | REAL | 名寄せ信頼度（0-1） |
| scraped_at | TEXT | 企業ページスクレイプ日時 |
| updated_at | TEXT NOT NULL | 更新日時 |

### `prtimes_articles` — プレスリリース記事

| カラム | 型 | 説明 |
|---|---|---|
| article_id | TEXT PK | URLのSHA-256先頭16文字 |
| prtimes_company_id | INTEGER FK | URLから抽出（`/p/{pr_num}.{company_id}.html`） |
| pr_number | INTEGER | 企業内のプレスリリース連番（URLから抽出） |
| title | TEXT | タイトル |
| article_url | TEXT | 記事URL |
| company_name_rss | TEXT | RSS `dc_corp` フィールドの企業名 |
| business_form | TEXT | RSS `business_form` フィールドの業種 |
| published_at | TEXT | 公開日時（JST、記事ページの `time[datetime]` より） |
| summary | TEXT | RSS由来の概要（HTMLタグ除去済み） |
| image_url | TEXT | OGP画像URL（記事ページ `og:image` より） |
| source | TEXT | `global_rss` |
| fetched_at | TEXT | RSS取得日時（JST） |
| html_path | TEXT | 生HTMLファイルパス |
| body_text | TEXT | パース済み本文テキスト |
| article_scraped_at | TEXT | 記事スクレイプ日時 |

### `rss_fetch_log` — RSS収集ログ

| カラム | 型 | 説明 |
|---|---|---|
| fetch_id | INTEGER PK AUTOINCREMENT | |
| fetched_at | TEXT | 取得日時（JST） |
| article_count | INTEGER | 取得件数 |
| new_count | INTEGER | 新規記事件数 |
| error | TEXT | エラー内容 |

---

## 技術スタック（実ページ調査結果）

| ページ種別 | 描画方式 | 取得方法 |
|---|---|---|
| グローバルRSS | XML（RDF形式） | feedparser（raw bytes渡し、エンコード自動検出） |
| 記事ページ | **SSR**（サーバーサイドレンダリング） | requests + BeautifulSoup |
| 企業詳細ページ | **SPA**（`<div id="root">` のみ） | **Selenium必須**（h1タグ出現待ち） |

### RSS時刻処理の注意点

feedparserの `updated_parsed` は **UTC** で返される。JST（+9h）に変換して保存。

```python
dt_utc = datetime(*t[:6], tzinfo=timezone.utc)
published_at = dt_utc.astimezone(JST).strftime("%Y-%m-%d %H:%M:%S")
```

### CSS Module クラス名の扱い

企業ページはCSS Modulesによりクラス名にハッシュが付く（例: `_companyName_1uyad_11`）。
セマンティックな部分（`companyName`, `companyDescription`）は安定しているため、
`class_=lambda c: c and "companyName" in c` のようなpartial matchで抽出する。

### PRTimesの空欄プレースホルダー

企業ページのdt/ddペアで値が存在しない場合、`-` が表示される。
パーサーで `value != "-"` を確認し、`None` として扱う。

---

## 各モジュールの責務

| ファイル | 責務 |
|---|---|
| `crawlers/prtimes_rss_crawler.py` | feedparser（raw bytes）でRSS取得、`RssEntry`リストを返す |
| `crawlers/prtimes_article_crawler.py` | requestsで記事HTML取得、`C:\Temp\html\prtimes\articles\{YYYYMMDD}\`に保存 |
| `crawlers/prtimes_company_crawler.py` | Seleniumで企業SPA取得、`C:\Temp\html\prtimes\companies\`に保存。h1タグ出現で描画完了判定 |
| `parsers/prtimes_rss_parser.py` | `RssEntry` → `ArticleRecord`。`article_id`はURL SHA-256先頭16文字 |
| `parsers/prtimes_article_parser.py` | BeautifulSoupで`og:image`・`time[datetime]`・本文テキストを抽出 |
| `parsers/prtimes_company_parser.py` | `aside[aria-label="企業データ"]`のdt/ddペアを解析。`h1[class*="companyName"]`で企業名、`p[class*="companyDescription"]`で企業説明を抽出 |
| `loaders/prtimes_db_loader.py` | `load()`: 記事INSERT + 企業仮登録。`update_articles()`: 記事UPDATE。`update_companies()`: 企業UPDATE |
| `models/prtimes_schema.py` | DDL + `init_db()` + `_migrate_companies()`（ALTER TABLEによる後付カラム追加） |
| `utils/selenium_utils.py` | Edge WebDriver共通初期化。ブラウザログ抑制（`--disable-logging`, `--log-file=nul`）。hellowork_crawlerと共用 |

---

## スケジュール設計

```
[毎時00分] run_prtimes_rss.bat
  → STEP 1-3: グローバルRSS取得 → パース → 記事DB保存
  → STEP 4-6: 未スクレイプ記事HTML取得 → パース → DB更新
  → STEP 7-9: scraped_at IS NULL の企業ページ取得 → パース → DB更新

[随時実行] python scripts/run_prtimes_companies.py  ← 初回一括取得・再処理用
  オプション:
    --parse-only    : スクレイピングせずパース・UPDATEのみ実行（再処理時）
    --no-headless   : ブラウザを表示して実行
```

---

## RSS情報

| 種別 | URL |
|---|---|
| グローバルRSS | `https://prtimes.jp/index.rdf` |
| 企業情報ページ | `https://prtimes.jp/main/html/searchrlp/company_id/{id}` |

---

## 依存ライブラリ

```
feedparser>=6.0     # RSS/Atom パース
beautifulsoup4      # HTML パース
selenium            # 企業ページSPA取得（Edge WebDriver）
requests            # 記事ページ取得
rapidfuzz>=3.0      # 法人番号名寄せ用（未実装）
```

---

## 実装済みステップ

| # | ファイル | 内容 | 状態 |
|---|---|---|---|
| 1 | `models/prtimes_schema.py` | DDL・init_db()・マイグレーション | ✅ 完了 |
| 2 | `crawlers/prtimes_rss_crawler.py` | グローバルRSS取得 | ✅ 完了 |
| 3 | `parsers/prtimes_rss_parser.py` | RSS XML解析 | ✅ 完了 |
| 4 | `loaders/prtimes_db_loader.py` | 記事・企業DB保存 | ✅ 完了 |
| 5 | `scripts/run_prtimes_rss.py` + `.bat` | 1時間毎バッチ | ✅ 完了 |
| 6 | `crawlers/prtimes_article_crawler.py` | 記事HTML取得・保存 | ✅ 完了 |
| 7 | `parsers/prtimes_article_parser.py` | 記事本文パース | ✅ 完了 |
| 8 | `crawlers/prtimes_company_crawler.py` | 企業ページ取得（Selenium） | ✅ 完了 |
| 9 | `parsers/prtimes_company_parser.py` | 企業情報パース | ✅ 完了 |
| 10 | `scripts/run_prtimes_companies.py` | 企業収集バッチ | ✅ 完了 |
| 11 | 法人番号名寄せ | rapidfuzzで企業名照合 | 未実装 |
