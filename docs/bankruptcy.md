# 倒産情報データベース 実装ドキュメント

## 概要

帝国データバンク（TDB）と東京商工リサーチ（TSR）の倒産情報をRSS経由で収集し、詳細ページをスクレイピングしてDBに蓄積する。
TSRは法人番号を保持するため先に処理し、その後TDBとの名寄せにより法人番号を補完する。
倒産前企業情報（資本金・住所・代表者・従業員数）はTDB本文テキストおよびTSR `※` フッターノートから直接抽出してDBに保存する。
NTA・gBizInfoは**名寄せスコア算定時の参照のみ**に使用し、bankruptcyDBへの付加保存は行わない。

---

## データソース

| ソース | RSS URL | 詳細ページ | 更新頻度 |
|---|---|---|---|
| 帝国データバンク（TDB） | `https://www.tdb.co.jp/rss/jouhou.rdf` | `https://www.tdb.co.jp/report/bankruptcy/flash/{id}/` | 平日随時 |
| 東京商工リサーチ（TSR） | `https://www.tsr-net.co.jp/rss/news_flash.xml` | `https://www.tsr-net.co.jp/news/tsr/detail/{id}_1521.html` | 平日随時 |

---

## 各ページの構造（実ページ調査結果）

### RSS取得

| ソース | 取得方法 | 件数 | publishedフィールド | 備考 |
|---|---|---|---|---|
| TDB | `feedparser.parse(url)` | 40件 | `updated_parsed`（UTC）を JST 変換。TDBは時刻なし（`00:00:00+09:00`） | bozo=False |
| TSR | `feedparser.parse(url)` | 10件 | `updated_parsed`（UTC）を JST 変換。TSRは秒まで含む | bozo=True だが動作 |

`requests` ではSSL接続拒否されるため、RSS・詳細ページ共に `feedparser` 内部のHTTPクライアントを流用するか、URLはfeedparserで取得し詳細ページは `urllib.request` + `feedparser` User-Agentで取得する。

### TDB 詳細ページ（例: https://www.tdb.co.jp/report/bankruptcy/flash/5218/）

**レンダリング方式**: SSR（Remix フレームワーク。`window.__remixContext` が埋め込まれているが HTML はサーバーレンダリング済み）
**取得方法**: `urllib.request` + `User-Agent: feedparser/6.0.12 ...` + SSL検証なし
**エンコーディング**: UTF-8（`<meta charSet="utf-8"/>`）。`raw.decode('utf-8')` 後に BeautifulSoup へ渡す

**HTML要素マッピング**:

| 抽出フィールド | HTML要素 | 格納先カラム |
|---|---|---|
| 会社名 | `h1[class*="text-title-1-b"]` | `company_name` |
| 公開日 | `div[class*="text-sub"]` テキスト（`YYYY/MM/DD`形式） | `published_at` |
| TDB企業コード | `p.whitespace-pre-wrap > span.md:hidden` 1行目 `TDB企業コード:` 以降 | `tdb_company_code` |
| 都道府県・市区町村 | 同span 2行目（正規表現 `^(.+?[都道府県])(.*)` で分割） | `prefecture`, `city` |
| 事業内容 | 同span 3行目 | `business_description` |
| 申請種別 | 同span 4行目 | `bankruptcy_type` |
| 負債額テキスト | 同span 5行目 | `liabilities_text` |
| 資本金テキスト | `main p`（classなし）1段落目 カッコ内 `資本金` | `body_capital_text` |
| 詳細住所 | 同カッコ内（資本金の次の項目） | `body_address` |
| 代表者名 | 同カッコ内 `代表(社員)?(.+?)氏` | `body_representative` |
| 従業員数 | 同カッコ内 `従業員\d+名` | `body_employees` |
| 本文テキスト | `main p`（classなし）全段落 | `body_text` |

**本文カッコ内パターン例**:
```
「東京」　（株）The TCG（資本金100万円、台東区元浅草1-6-12、代表三村浩卯氏）は…
```
正規表現: `（資本金(.+?)、(.+?)、代表(社員)?(.+?)氏(?:、従業員(\d+)名)?）`
※ 資本金・住所・代表者の順序は固定。従業員は省略される場合あり。

### TSR 詳細ページ（実URLパターン: https://www.tsr-net.co.jp/news/tsr/detail/{id}_1521.html）

**レンダリング方式**: SSR（`#root` div なし、Selenium 不要）
**取得方法**: `urllib.request` + `User-Agent: feedparser/6.0.12 ...` + SSL検証なし
**エンコーディング**: UTF-8

**HTML要素マッピング**:

| 抽出フィールド | HTML要素 | 格納先カラム |
|---|---|---|
| 会社名 | `h1.title_data` | `company_name` |
| 公開日 | `div`（classなし）テキスト `YYYY/MM/DD` | `published_at` |
| 都道府県 | `li.tag_prefecture > a`（`div.entry_info` にスコープ） | `prefecture` |
| 業種 | `li.tag_industry` 1番目 `> a`（`div.entry_info` にスコープ） | `industry` |
| 事業内容 | `li.tag_industry` 2番目 `> a`（なければ `None`） | `business_description` |
| 申請種別 | `li.tag_procedure > a`（`div.entry_info` にスコープ） | `bankruptcy_type` |
| 負債額テキスト | `li.tag_debt > a`（`div.entry_info` にスコープ、**続報記事では存在しない場合あり**） | `liabilities_text` |
| TSRコード | `div.entry_info_code` テキストから `TSRコード[:：](\d+)` | `tsr_code` |
| 法人番号 | `div.entry_info_code` テキストから `法人番号[:：](\d+)` | `corporate_number` |
| 資本金テキスト | 本文 `p`（classなし）内の `※` 以降テキストから `資本金([^）、]+)` | `body_capital_text` |
| 詳細住所 | 同 `※` テキストから法人番号の次の `、` 区切り項目 | `body_address` |
| 設立年月 | 同 `※` テキストから `設立(\d{4}[^）、]+)` | `body_established` |
| 本文テキスト | `p`（classなし）全段落（`※` 以降を除外） | `body_text` |

**`※` フッターノートの実装上の注意**:

`※` は独立した `<p>` 要素ではなく、本文 `<p>` 要素の末尾に `<br>` 区切りで埋め込まれている。
`p.get_text(strip=True).startswith("※")` では検出できず、`txt.find("※")` で位置を特定する。

```
# 実際の DOM 構造
<p>
  本文テキスト...<br />
  本文テキスト...<br />
  <br />
  ※　（株）ＥＶモーターズ・ジャパン（TSRコード:131071165、法人番号:6290801025401、
      北九州市若松区向洋町22-1、設立2019（平成31）年4月、資本金41億1885万円）
</p>
```

**タグスコープについて**:

ページ下部の関連記事にも `li.tag_debt` 等が存在するため、`soup.find()` で全体検索すると誤ったタグを拾う。
すべてのタグ検索は `soup.find("div", class_="entry_info")` にスコープして行う。

**`※` フッターノート例**:
```
※　合同会社クリアースカイ（TSRコード:137254873、法人番号:9120003018366、京都市下京区西境町149、設立2020（令和2）年11月、資本金300万円）
```

---

## 全体処理フロー

```
[毎日 1回] run_bankruptcy.bat

  STEP 1: TSR RSS取得 → DB INSERT（INSERT OR IGNORE）
  STEP 2: TDB RSS取得 → DB INSERT（INSERT OR IGNORE）

  [--rss-only 指定時はここで終了]

  STEP 3: TSR詳細スクレイピング（detail_scraped_at IS NULL のみ）
  STEP 4: TSR詳細パース → DB UPDATE
          （法人番号・都道府県・申請種別・資本金・住所・設立年月 抽出）
  STEP 5: TDB詳細スクレイピング（detail_scraped_at IS NULL のみ）
  STEP 6: TDB詳細パース → DB UPDATE
          （TDB企業コード・都道府県・申請種別・資本金・住所・代表者・従業員数 抽出）

  STEP 7: 名寄せ（TDB↔TSR、確定マッチ未登録案件のみ）
          ※ スコア算定時に NTA（companies.db）・gBizInfo（gbizinfo.db）を参照（読み取りのみ）
          → 複合スコア算定 → 閾値以上でマッチ確定
          → bankruptcy_matches に登録

  [--no-match 指定時は STEP 7 をスキップ]
```

**未実装（将来タスク）**: `liabilities_text` → `liabilities_amount`（万円単位整数）への数値パース

---

## DB設計（`data/bankruptcy.db`）

### 設計方針

TDBとTSRは**別テーブルで独立して保存**する。
同一案件でも業種・申請種別・負債額の表記が異なる場合があるため、各ソースのデータをそのまま保持し、
`bankruptcy_matches` テーブルで紐付ける。法人番号はマッチテーブル経由でTDB案件に付与する。

### `tdb_cases` — TDB倒産案件

| カラム | 型 | 説明 |
|---|---|---|
| `case_id` | TEXT PK | 詳細URLのSHA-256先頭16文字 |
| `source_url` | TEXT NOT NULL | 詳細ページURL |
| `company_name` | TEXT | 社名（RSS `title` から抽出） |
| `tdb_company_code` | TEXT | TDB企業コード（上段ヘッダー） |
| `prefecture` | TEXT | 都道府県（上段ヘッダー） |
| `city` | TEXT | 市区町村（上段ヘッダー） |
| `business_description` | TEXT | 事業内容（上段ヘッダー） |
| `bankruptcy_type` | TEXT | 申請種別（上段ヘッダー） |
| `liabilities_text` | TEXT | 負債額テキスト（上段ヘッダー） |
| `liabilities_amount` | INTEGER | 負債額（万円単位、未実装） |
| `body_capital_text` | TEXT | 資本金テキスト（本文カッコ内） |
| `body_capital_amount` | INTEGER | 資本金（万円単位） |
| `body_address` | TEXT | 詳細住所（本文カッコ内） |
| `body_representative` | TEXT | 代表者名（本文カッコ内） |
| `body_employees` | INTEGER | 従業員数（本文カッコ内） |
| `published_at` | TEXT | 公開日（RSS `<dc:date>`） |
| `rss_fetched_at` | TEXT NOT NULL | RSS取得日時（JST） |
| `detail_scraped_at` | TEXT | 詳細スクレイピング日時（JST） |
| `html_path` | TEXT | 生HTMLファイルパス |
| `body_text` | TEXT | パース済み本文テキスト |

### `tsr_cases` — TSR倒産案件

| カラム | 型 | 説明 |
|---|---|---|
| `case_id` | TEXT PK | 詳細URLのSHA-256先頭16文字 |
| `source_url` | TEXT NOT NULL | 詳細ページURL |
| `company_name` | TEXT | 社名（RSS `title` から抽出） |
| `corporate_number` | TEXT | 法人番号（`div.entry_info_code`、13桁） |
| `tsr_code` | TEXT | TSRコード（`div.entry_info_code`） |
| `prefecture` | TEXT | 都道府県（`li.tag_prefecture`） |
| `industry` | TEXT | 業種（`li.tag_industry` 1番目） |
| `business_description` | TEXT | 事業内容（`li.tag_industry` 2番目） |
| `bankruptcy_type` | TEXT | 申請種別（`li.tag_procedure`） |
| `liabilities_text` | TEXT | 負債額テキスト（`li.tag_debt`） |
| `liabilities_amount` | INTEGER | 負債額（万円単位、未実装） |
| `body_capital_text` | TEXT | 資本金テキスト（`※` フッターノート） |
| `body_capital_amount` | INTEGER | 資本金（万円単位） |
| `body_address` | TEXT | 詳細住所（`※` フッターノート） |
| `body_established` | TEXT | 設立年月（`※` フッターノート） |
| `published_at` | TEXT | 公開日（RSS `<dc:date>`） |
| `rss_fetched_at` | TEXT NOT NULL | RSS取得日時（JST） |
| `detail_scraped_at` | TEXT | 詳細スクレイピング日時（JST） |
| `html_path` | TEXT | 生HTMLファイルパス |
| `body_text` | TEXT | パース済み本文テキスト（`※` 以降を除外） |

### `bankruptcy_matches` — 名寄せ結果

| カラム | 型 | 説明 |
|---|---|---|
| `match_id` | INTEGER PK AUTOINCREMENT | |
| `tdb_case_id` | TEXT FK | `tdb_cases.case_id` |
| `tsr_case_id` | TEXT FK | `tsr_cases.case_id` |
| `corporate_number` | TEXT | 法人番号（TSRから） |
| `match_score` | REAL | 総合スコア（0〜100） |
| `name_score` | REAL | 社名類似スコア |
| `address_score` | REAL | 住所一致スコア（市区町村10点 + 詳細住所10点） |
| `capital_score` | REAL | 資本金一致スコア |
| `rep_score` | REAL | 代表者名一致スコア |
| `match_method` | TEXT | `fuzzy_composite` / `manual` |
| `is_confirmed` | INTEGER | 1=確定, 0=候補（スコア閾値未満） |
| `matched_at` | TEXT | 名寄せ実行日時 |
| UNIQUE | | `(tdb_case_id, tsr_case_id)` |

### `rss_fetch_log` — RSS取得ログ

| カラム | 型 | 説明 |
|---|---|---|
| `fetch_id` | INTEGER PK AUTOINCREMENT | |
| `source` | TEXT | `tdb` / `tsr` |
| `fetched_at` | TEXT | 取得日時（JST） |
| `article_count` | INTEGER | 取得件数 |
| `new_count` | INTEGER | 新規件数（INSERT成功） |
| `error` | TEXT | エラー内容 |

---

## 名寄せ詳細ロジック

### 前処理（社名正規化）

```python
def normalize_name(name: str) -> str:
    # 1. unicodedata.normalize("NFKC") で全角→半角
    # 2. 法人形態を除去（株式会社・有限会社・合同会社・合名会社・合資会社・各種法人）
    # 3. （）「」〔〕【】を除去
    # 4. ひらがな→カタカナ（U+3041-U+3096 → +0x60）
    # 5. スペース・全角スペース・U+2003 を除去
    # 6. upper() で大文字統一
```

### スコア算定（最大 100点）

| 評価軸 | 重み | 算定方法 |
|---|---|---|
| 社名類似度 | 60点 | `rapidfuzz.fuzz.token_sort_ratio`（正規化後）× 0.60 |
| 都道府県一致 | 10点 | 完全一致で10点 |
| 市区町村一致 | 10点 | `tdb_cases.city` が NTA住所文字列に含まれるか |
| 詳細住所類似度 | 10点 | `tdb_cases.body_address` vs NTA住所（`partial_ratio`）× 0.10 |
| 資本金一致 | 5点 | `tdb_cases.body_capital_amount` vs gBizInfo 資本金（±10%以内で満点） |
| 代表者名一致 | 5点 | `tdb_cases.body_representative` vs NTA代表者名（`ratio` ≥ 80で満点） |

### 判定閾値

| スコア | 判定 | 処理 |
|---|---|---|
| 85点以上 | 確定マッチ | `is_confirmed=1` で登録 |
| 70〜84点 | 候補 | `is_confirmed=0` で登録（要目視確認） |
| 70点未満 | 非マッチ | 登録しない |

### 突合対象の絞り込み

- TDB案件の `published_at` の前後 **14日以内** に公開されたTSR案件のみ突合
- 都道府県が異なる場合は除外
- `is_confirmed=1` のマッチが既に登録済みのTDB案件はスキップ
- TSR案件は `detail_scraped_at IS NOT NULL`（詳細取得済み）のもののみ

### 外部DB参照（bankruptcy.dbへの書き込みなし）

```
名寄せ実行時:
  TSR案件の corporate_number
    → data/companies.db の companies テーブル → 住所・代表者名を取得
    → data/gbizinfo.db の gbizinfo テーブル → 資本金を取得
    → スコア算定の比較値として使用のみ（bankruptcy.db には書き込まない）
```

companies.db または gbizinfo.db が存在しない場合は該当スコア軸をスキップ（0点扱い）。

---

## ファイル構成

```
src/signals/bankruptcy/
├── __init__.py
├── crawlers/
│   ├── __init__.py
│   ├── tdb_rss_crawler.py        # TDB RSS取得・TdbRssEntry返却
│   ├── tsr_rss_crawler.py        # TSR RSS取得・TsrRssEntry返却
│   ├── tdb_detail_crawler.py     # TDB詳細ページHTML取得・保存
│   └── tsr_detail_crawler.py     # TSR詳細ページHTML取得・保存
├── parsers/
│   ├── __init__.py
│   ├── tdb_detail_parser.py      # HTML → 上段ヘッダー＋本文カッコ内抽出
│   └── tsr_detail_parser.py      # HTML → 上段タグ＋法人番号＋※フッターノート抽出
├── matchers/
│   ├── __init__.py
│   └── name_matcher.py           # TDB↔TSR複合スコア名寄せ・bankruptcy_matches登録
├── loaders/
│   ├── __init__.py
│   └── db_loader.py              # INSERT/UPDATE・未スクレイピング取得・ログ記録
└── models/
    ├── __init__.py
    └── schema.py                  # DDL定義・init_db()・マイグレーション

scripts/
├── run_bankruptcy.py             # 全フロー実行（STEP 1〜7）
├── run_bankruptcy.bat            # タスクスケジューラー用
└── run_bankruptcy_match.py       # 名寄せのみ再実行

data/
└── bankruptcy.db                 # 倒産情報専用DB（実運用ファイル）

logs/
└── bankruptcy/                   # 実行ログ（YYYYMMDD 付きファイル名）
```

### 生HTMLファイル保存先

```
C:\Temp\html\bankruptcy\
├── tdb\
│   └── {YYYYMMDD}\
│       └── {case_id}.html         # TDB詳細ページHTML
└── tsr\
    └── {YYYYMMDD}\
        └── {case_id}.html         # TSR詳細ページHTML
```

---

## 技術スタック（実ページ調査済み）

| ページ種別 | 描画方式 | 取得方法 | 確認状況 |
|---|---|---|---|
| TDB RSS | RDF/XML (UTF-8) | `feedparser.parse(url)` 直接呼び出し | ✅ 確認済み |
| TDB詳細ページ | **SSR** (Remix) | `urllib.request` + feedparser UA + SSL検証なし | ✅ 確認済み |
| TSR RSS | RSS XML (UTF-8) | `feedparser.parse(url)` 直接呼び出し | ✅ 確認済み |
| TSR詳細ページ | **SSR** | `urllib.request` + feedparser UA + SSL検証なし | ✅ 確認済み |

**Selenium は不要**（両ページとも SSR 確認済み）。

### 共通HTTPクライアント仕様

```python
FEEDPARSER_UA = "feedparser/6.0.12 +https://github.com/kurtmckee/feedparser/"

import ssl, urllib.request

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

req = urllib.request.Request(url, headers={"User-Agent": FEEDPARSER_UA})
with urllib.request.urlopen(req, timeout=30, context=ctx) as resp:
    raw = resp.read()

# BeautifulSoupには必ずデコード済み文字列を渡す
soup = BeautifulSoup(raw.decode("utf-8"), "html.parser")
```

---

## 実行方法

```bash
# 通常実行（全フロー: RSS取得 → スクレイピング → パース → 名寄せ）
python scripts/run_bankruptcy.py

# RSS保存のみ（スクレイピング・名寄せをスキップ）
python scripts/run_bankruptcy.py --rss-only

# 名寄せをスキップ（スクレイピング・パースのみ実行）
python scripts/run_bankruptcy.py --no-match

# 名寄せのみ再実行（スクレイピング済みデータに対して）
python scripts/run_bankruptcy_match.py
```

**推奨実行頻度**: 毎日1回（平日更新が多いが土日分も翌日蓄積される）

---

## 懸念点・対応方針

### 1. robots.txt・利用規約の確認
- **TDB**: `https://www.tdb.co.jp/robots.txt` を確認。クロール間隔 3秒以上を厳守
- **TSR**: `https://www.tsr-net.co.jp/robots.txt` を確認。同様に 3秒以上
- 商用利用でなく個人研究目的として利用

### 2. 負債額テキストの表記揺れ
- 全角数字（「５７億円」）・「約」「総額」等の接頭辞が混在
- **現状**: `_parse_capital_amount()` と同様のロジックでパースして `liabilities_amount` カラムに保存（対応済み）
- **将来対応**: なし

### 3. TDB本文カッコ内パターンの揺れ
- 従業員数は省略される場合がある（`body_employees = None`）
- **対応済み**: 正規表現 `(?:、従業員(\d+)名)?` でオプション扱い

### 4. 同名別会社の誤マッチ
- 都道府県不一致の場合は突合対象から除外
- 住所・資本金のいずれかが一致しない場合は `is_confirmed=0`（候補止まり）

### 5. RSS の過去データ非包含
- TDB・TSR の RSS は最新数十件のみ（全履歴なし）
- **対応済み**: `INSERT OR IGNORE` による重複排除。初回実行以降は差分のみ取得

---

## 依存ライブラリ

```
feedparser>=6.0      # RSS/RDF パース
beautifulsoup4       # HTML パース
rapidfuzz>=3.0       # 社名あいまい一致スコアリング
```

`requests` は TDB・TSR ともに SSL 接続拒否されるため使用しない。
詳細ページ取得は標準ライブラリの `urllib.request` + feedparser User-Agent で対応済み。
Selenium は両サイトとも SSR 確認済みのため不要。

---

## 実装ステップ

| # | ファイル | 内容 | 状態 |
|---|---|---|---|
| 1 | `models/schema.py` | DDL（tdb_cases / tsr_cases / bankruptcy_matches / rss_fetch_log）・init_db()・マイグレーション | ✅ 完了 |
| 2 | `crawlers/tsr_rss_crawler.py` | TSR RSS取得・TsrRssEntry返却 | ✅ 完了 |
| 3 | ~~`parsers/tsr_rss_parser.py`~~ | RSSパースはcrawlerに統合済み | ~~省略~~ |
| 4 | `crawlers/tsr_detail_crawler.py` | TSR詳細ページHTML取得・保存 | ✅ 完了 |
| 5 | `parsers/tsr_detail_parser.py` | TSR上段タグ・法人番号・`※`フッターノート解析 | ✅ 完了 |
| 6 | `crawlers/tdb_rss_crawler.py` | TDB RSS取得・TdbRssEntry返却 | ✅ 完了 |
| 7 | ~~`parsers/tdb_rss_parser.py`~~ | RSSパースはcrawlerに統合済み | ~~省略~~ |
| 8 | `crawlers/tdb_detail_crawler.py` | TDB詳細ページHTML取得・保存 | ✅ 完了 |
| 9 | `parsers/tdb_detail_parser.py` | TDB上段ヘッダー解析・本文カッコ内抽出 | ✅ 完了 |
| 10 | `loaders/db_loader.py` | INSERT/UPDATE・未スクレイピング取得・RSSログ記録 | ✅ 完了 |
| 11 | `matchers/name_matcher.py` | 複合スコア名寄せ・bankruptcy_matches登録 | ✅ 完了 |
| 12 | `scripts/run_bankruptcy.py` + `.bat` | 全フロー実行（STEP 1〜7）・タスクスケジューラー登録 | ✅ 完了 |
| 13 | `scripts/run_bankruptcy_match.py` | 名寄せ単独実行 | ✅ 完了 |
| 14 | TSR/TDBページのレンダリング方式確認 | 実ページ取得・Selenium要否判定 | ✅ 完了 |
| 15 | `liabilities_amount` 数値パース | `liabilities_text` → 万円単位整数 | ✅ 完了 |
