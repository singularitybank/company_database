# ニュース収集システム 実装ドキュメント

## 概要

国内ニュースRSSフィード（10ソース）およびGoogle Newsキーワード検索から、企業活動に関連する事故・事件・地域ニュースを収集してDBに蓄積する。

- **通常ニュース**: 10ソースのRSSを1時間ごとに取得 → `data/news.db`
- **Google Newsキーワード**: キーワードごとにRSSを取得 → `data/google_news.db`（別DB・別スクリプト）

---

## Part 1: 通常ニュース（`data/news.db`）

### 収集ソース

| key | 媒体 | カテゴリ | フォーマット | URL | description |
|-----|------|---------|------------|-----|-------------|
| nhk | NHK社会 | domestic | RSS 2.0 | https://www.nhk.or.jp/rss/news/cat1.xml | 要確認 |
| 47news_national | 47NEWS全国社会 | domestic | RDF 1.0 | https://assets.wor.jp/rss/rdf/ynnews/national.rdf | ❌ 空 |
| 47news_local | 47NEWS地域社会 | local | RSS 2.0 | https://assets.wor.jp/rss/rdf/ynlocalnews/national.rdf | ✅ あり |
| yahoo_domestic | Yahoo!国内 | domestic | RSS 2.0 | https://news.yahoo.co.jp/rss/categories/domestic.xml | ✅ あり |
| yahoo_local | Yahoo!地域 | local | RSS 2.0 | https://news.yahoo.co.jp/rss/categories/local.xml | ✅ あり |
| nikkei_society | 日経社会 | domestic | RDF 1.0 | https://assets.wor.jp/rss/rdf/nikkei/society.rdf | ❌ 空 |
| nikkei_local | 日経地域 | local | RDF 1.0 | https://assets.wor.jp/rss/rdf/nikkei/local.rdf | ❌ 空 |
| sankei_affairs | 産経社会 | domestic | RDF 1.0 | https://assets.wor.jp/rss/rdf/sankei/affairs.rdf | ❌ 空 |
| yomiuri_national | 読売社会 | domestic | RDF 1.0 | https://assets.wor.jp/rss/rdf/yomiuri/national.rdf | ❌ 空 |
| asahi_national | 朝日社会 | domestic | RDF 1.0 | https://www.asahi.com/rss/asahi/national.rdf | 要確認 |

**フィード別フィールド差異:**

| フォーマット | 日付フィールド | description | image_url |
|------------|--------------|-------------|-----------|
| RDF 1.0 | `dc:date` (ISO 8601 + TZ) | 基本的に空 | なし |
| RSS 2.0 | `pubDate` (RFC 2822) | テキストあり | `<image>` or `media_thumbnail` |

**NHK・朝日は外部からのアクセス制限の可能性あり。実装時に feedparser + requests (User-Agent設定) で動作確認が必要。**

---

### 全体処理フロー

```
[毎時] run_news_rss.py（夜間22:00〜07:00はスキップ、--forceで上書き）

  STEP 1: 夜間チェック
  STEP 2: DB初期化 (init_db)
  STEP 3: config.yaml からソースリスト読み込み
  STEP 4: ソースごとにループ（全10ソース）
    4-1: RSS取得 → NewsRssEntry リスト生成
    4-2: DB保存 (INSERT OR IGNORE)
    4-3: rss_fetch_log へ記録
    4-4: ソース間ウェイト（2秒）

引数:
  --force           夜間スキップ無効
  --source <key>    特定ソースのみ実行（デバッグ用）
```

---

### ファイル構成

```
src/signals/news/
├── __init__.py
├── models/
│   └── schema.py           # CREATE TABLE定数 + init_db()
├── crawlers/
│   └── rss_crawler.py      # NewsRssEntry dataclass + fetch_rss()
└── loaders/
    └── db_loader.py        # load_entries() + log_fetch()

scripts/
└── run_news_rss.py

data/news.db                # (実行時自動生成)
logs/news/                  # (実行時自動生成)
```

---

### DBスキーマ

```sql
CREATE TABLE IF NOT EXISTS news_articles (
    article_id    TEXT PRIMARY KEY,   -- SHA256(source_url)[:16]
    source        TEXT NOT NULL,      -- ソースkey ('nhk', 'yahoo_domestic', ...)
    source_url    TEXT NOT NULL,
    title         TEXT NOT NULL,
    published_at  TEXT,               -- JST "YYYY-MM-DD HH:MM:SS"
    summary       TEXT,               -- RSS 2.0のみ値あり、RDF 1.0はNULL
    image_url     TEXT,               -- Yahoo / 47news_local のみ
    category      TEXT,               -- 'domestic' or 'local'
    fetched_at    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS rss_fetch_log (
    fetch_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    source        TEXT NOT NULL,
    fetched_at    TEXT NOT NULL,
    article_count INTEGER DEFAULT 0,
    new_count     INTEGER DEFAULT 0,
    error         TEXT
);
```

---

### パーサー実装詳細（`rss_crawler.py`）

```python
@dataclass
class NewsRssEntry:
    article_id:   str           # SHA256(source_url)[:16]
    source:       str
    source_url:   str           # RDF: entry.id (rdf:about), RSS 2.0: entry.link
    title:        str
    published_at: Optional[str] # JST変換済み
    summary:      Optional[str] # RSS 2.0のみ実データあり
    image_url:    Optional[str] # entry.enclosures または entry.media_thumbnail
    category:     str
    fetched_at:   str
```

**日付パース（フォーマット差を吸収）:**
```python
def _parse_published_at(entry) -> Optional[str]:
    # entry.published_parsed (RSS 2.0 pubDate)
    # entry.updated_parsed   (RDF 1.0 dc:date のfallback)
    # どちらもtime.struct → JST変換（date_utils流用）
```

**重複排除:** `article_id = SHA256(source_url)[:16]` をPRIMARY KEYとし `INSERT OR IGNORE`

---

## Part 2: Google Newsキーワード収集（`data/google_news.db`）

### コンセプト

- URLパターン: `https://news.google.com/rss/search?q=KEYWORD&hl=ja&gl=JP&ceid=JP:ja`
- キーワードはDBで管理（追加・無効化が可能）
- 将来的にキーワードを増やしていく想定

### 全体処理フロー

```
[毎時] run_google_news.py

  # キーワード管理（随時実行）
  --add-keyword <kw>      google_news_keywords にINSERT
  --disable-keyword <kw>  is_active = 0 に更新

  # 定期実行
  STEP 1: 夜間チェック（--force で上書き）
  STEP 2: DB初期化
  STEP 3: active なキーワード一覧取得
  STEP 4: キーワードごとにループ
    4-1: RSS取得 (fetch_by_keyword)
    4-2: DB保存 (INSERT OR IGNORE)
    4-3: google_news_fetch_log へ記録
    4-4: キーワード間ウェイト（3秒）

引数:
  --force                 夜間スキップ無効
  --keyword <kw>          特定キーワードのみ実行
  --add-keyword <kw>      キーワード追加
  --disable-keyword <kw>  キーワード無効化
```

### ファイル構成

```
src/signals/google_news/
├── __init__.py
├── models/
│   └── schema.py           # 3テーブル + init_db()
├── crawlers/
│   └── rss_crawler.py      # GoogleNewsEntry dataclass + fetch_by_keyword()
└── loaders/
    └── db_loader.py        # load_entries() + keyword管理関数

scripts/
└── run_google_news.py

data/google_news.db         # (実行時自動生成)
logs/google_news/           # (実行時自動生成)
```

### DBスキーマ

```sql
CREATE TABLE IF NOT EXISTS google_news_keywords (
    keyword_id  INTEGER PRIMARY KEY AUTOINCREMENT,
    keyword     TEXT NOT NULL UNIQUE,
    is_active   INTEGER DEFAULT 1,      -- 0: 無効化
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS google_news_articles (
    article_id    TEXT PRIMARY KEY,   -- SHA256(link)[:16]
    keyword_id    INTEGER NOT NULL,
    keyword       TEXT NOT NULL,      -- 検索時のキーワード（非正規化で保持）
    title         TEXT NOT NULL,
    source_name   TEXT,               -- entry.source['title']（「NHK」「読売新聞」等）
    link          TEXT NOT NULL,      -- Google Newsリダイレクト URL
    published_at  TEXT,               -- JST ISO8601
    summary       TEXT,
    fetched_at    TEXT NOT NULL,
    FOREIGN KEY (keyword_id) REFERENCES google_news_keywords (keyword_id)
);

CREATE TABLE IF NOT EXISTS google_news_fetch_log (
    fetch_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    keyword       TEXT NOT NULL,
    fetched_at    TEXT NOT NULL,
    article_count INTEGER DEFAULT 0,
    new_count     INTEGER DEFAULT 0,
    error         TEXT
);
```

### Google News RSSフィールド

```python
@dataclass
class GoogleNewsEntry:
    article_id:   str           # SHA256(link)[:16]
    keyword_id:   int
    keyword:      str
    title:        str
    source_name:  Optional[str] # entry.source.get('title') — 配信元メディア名
    link:         str           # Google Newsリダイレクト URL
    published_at: Optional[str]
    summary:      Optional[str]
    fetched_at:   str
```

---

## `config/config.yaml` 追加内容

```yaml
news:
  db_path: "data/news.db"
  sources:
    - { key: nhk,              display_name: "NHK社会",       url: "https://www.nhk.or.jp/rss/news/cat1.xml",                   category: domestic }
    - { key: 47news_national,  display_name: "47NEWS全国社会", url: "https://assets.wor.jp/rss/rdf/ynnews/national.rdf",         category: domestic }
    - { key: 47news_local,     display_name: "47NEWS地域社会", url: "https://assets.wor.jp/rss/rdf/ynlocalnews/national.rdf",    category: local    }
    - { key: yahoo_domestic,   display_name: "Yahoo国内",      url: "https://news.yahoo.co.jp/rss/categories/domestic.xml",      category: domestic }
    - { key: yahoo_local,      display_name: "Yahoo地域",      url: "https://news.yahoo.co.jp/rss/categories/local.xml",         category: local    }
    - { key: nikkei_society,   display_name: "日経社会",       url: "https://assets.wor.jp/rss/rdf/nikkei/society.rdf",          category: domestic }
    - { key: nikkei_local,     display_name: "日経地域",       url: "https://assets.wor.jp/rss/rdf/nikkei/local.rdf",            category: local    }
    - { key: sankei_affairs,   display_name: "産経社会",       url: "https://assets.wor.jp/rss/rdf/sankei/affairs.rdf",          category: domestic }
    - { key: yomiuri_national, display_name: "読売社会",       url: "https://assets.wor.jp/rss/rdf/yomiuri/national.rdf",        category: domestic }
    - { key: asahi_national,   display_name: "朝日社会",       url: "https://www.asahi.com/rss/asahi/national.rdf",              category: domestic }
  night_skip:
    enabled: true
    start_hour: 22
    end_hour: 7
  request:
    timeout: 30
    retry_count: 3
    wait_between_sources: 2.0

google_news:
  db_path: "data/google_news.db"
  base_url: "https://news.google.com/rss/search"
  params:
    hl: "ja"
    gl: "JP"
    ceid: "JP:ja"
  night_skip:
    enabled: true
    start_hour: 22
    end_hour: 7
  request:
    timeout: 30
    retry_count: 3
    wait_between_keywords: 3.0
```

---

## 再利用する既存ユーティリティ

| ユーティリティ | パス | 用途 |
|---------------|------|------|
| `get_jst_now()` | `src/common/date_utils.py` | 現在時刻JST |
| `get_connection()` | `src/common/db_utils.py` | WAL/PRAGMA済みDB接続 |
| `setup_logger()` | `src/common/logging_setup.py` | 日付別ログ |
| config loader | `src/config.py` | YAML設定読み込み |

---

## 検証手順

```bash
# 単体ソース確認
python scripts/run_news_rss.py --source yahoo_domestic --force

# 全ソース実行
python scripts/run_news_rss.py --force

# DB確認
sqlite3 data/news.db "SELECT source, count(*) FROM news_articles GROUP BY source;"
sqlite3 data/news.db "SELECT * FROM rss_fetch_log ORDER BY fetched_at DESC LIMIT 20;"

# Google News: キーワード追加 & 実行
python scripts/run_google_news.py --add-keyword "工場火災"
python scripts/run_google_news.py --add-keyword "食中毒"
python scripts/run_google_news.py --force

# DB確認
sqlite3 data/google_news.db "SELECT keyword, count(*) FROM google_news_articles GROUP BY keyword;"
```

**重複排除確認:** 同スクリプトを2回実行し、2回目の `new_count=0` になることを確認する。
