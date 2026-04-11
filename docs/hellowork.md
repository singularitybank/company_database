# ハローワーク求人情報 収集・管理ガイド

**対象サイト:** ハローワークインターネットサービス  
**取得方法:** Selenium（Microsoft Edge）によるスクレイピング  
**最終更新:** 2026-04-11

---

## 1. 概要

ハローワーク（公共職業安定所）が公開する求人情報を毎日自動収集し、Parquetファイルとして蓄積する。

### 処理フロー

```
STEP 1: 求人番号収集
  ハローワーク検索画面 → 求人番号リスト（jobnumbers_YYYYMMDD.csv）

STEP 2: 詳細HTMLダウンロード
  求人番号リスト → 詳細ページHTML → C:\Temp\html\YYYYMMDD\{job_number}.html

STEP 3: Parquet変換
  HTMLファイル群 → data/staging/hellowork_YYYYMMDD.parquet
```

---

## 2. ディレクトリ構成

```
company_database/
├── config/
│   └── config.yaml              # パス・タイミング設定（★変更はここで）
├── data/
│   └── staging/
│       └── hellowork_YYYYMMDD.parquet   # 解析済みデータ（主ストア）
├── logs/
│   └── hellowork_YYYYMMDD.log   # 実行ログ（日付別）
├── scripts/
│   ├── run_hellowork.py         # バッチエントリーポイント
│   └── run_hellowork.bat        # タスクスケジューラ用バッチ
└── src/
    ├── crawlers/
    │   └── hellowork_crawler.py # Seleniumクローラー
    └── parsers/
        └── hellowork_parser.py  # HTMLパーサー → Parquet変換
```

### 生HTMLの保存場所

```
C:\Temp\html\
└── 20260410\          # 日付ごとのディレクトリ
    ├── 01010-12345678.html
    ├── 01010-12345679.html
    └── ...
```

> **注意:** Cドライブの容量節約のため、定期的に外付けHDDへ手動移動する。  
> 移動後も Parquet ファイルが手元にあれば分析に支障はない。  
> パス変更は `config/config.yaml` の `html_dir` を修正すること。

---

## 3. 設定（config/config.yaml）

```yaml
hellowork:
  html_dir: "C:/Temp/html"           # 生HTML保存先
  staging_dir: "data/staging"        # Parquet保存先（プロジェクト相対パス）
  temp_dir: "C:/Temp"                # 求人番号収集時の一時CSV置き場
  output_dir: "..."                  # 求人番号CSV最終保存先（OneDrive）
  wait_between_pages: 2.0            # ページ間待機（秒）
  wait_between_details: 1.0          # 詳細取得間待機（秒）
  timeout: 15                        # Seleniumタイムアウト（秒）
  headless: false                    # ヘッドレスモード（タスクスケジューラ時は true）
```

---

## 4. 実行方法

### 手動実行

```bash
# 環境を有効化
conda activate data

# 通常（当日分・フルフロー）
python scripts/run_hellowork.py

# 日付指定
python scripts/run_hellowork.py --date 2026-04-10

# ヘッドレスモード（ブラウザウィンドウ非表示）
python scripts/run_hellowork.py --headless

# HTMLが既にある場合のParquet変換のみ
python scripts/run_hellowork.py --date 2026-04-10 --skip-crawl
```

### 引数一覧

| 引数 | デフォルト | 説明 |
|---|---|---|
| `--date YYYY-MM-DD` | 当日 | 処理対象の日付 |
| `--headless` | なし | ブラウザを非表示で起動 |
| `--skip-crawl` | なし | クロールをスキップしParquet変換のみ実行 |

---

## 5. タスクスケジューラへの登録手順

1. **タスクスケジューラを開く**  
   スタートメニュー →「タスクスケジューラ」

2. **基本タスクの作成**  
   右ペイン →「基本タスクの作成」

3. **設定内容**

   | 項目 | 値 |
   |---|---|
   | 名前 | `ハローワーク 日次取得` |
   | トリガー | 毎日（例：07:00） |
   | 操作 | プログラムの開始 |
   | プログラム | `C:\Users\singu\github\company_database\scripts\run_hellowork.bat` |
   | 作業フォルダ | `C:\Users\singu\github\company_database` |

4. **動作確認**  
   タスク右クリック →「実行」→ `logs/hellowork_YYYYMMDD.log` でSTEP1〜3の完了を確認

> **headlessモードについて**  
> バッチファイル（`run_hellowork.bat`）には `--headless` を指定済み。  
> bot検知でエラーになる場合はバッチファイルから `--headless` を削除する（タスクスケジューラ実行時もブラウザウィンドウが表示される）。

---

## 6. 外付けHDDへのHTML移動手順

Parquet変換が完了した日付のHTMLは外付けHDDに移動できる。

```
移動元: C:\Temp\html\YYYYMMDD\
移動先: (外付けHDD)\hellowork\html\YYYYMMDD\
```

移動後、`config.yaml` の `html_dir` を外付けHDDのパスに変更すれば  
`--skip-crawl` での再変換も可能。

---

## 7. 出力データ仕様（Parquet）

### ファイル

| 項目 | 内容 |
|---|---|
| パス | `data/staging/hellowork_YYYYMMDD.parquet` |
| 行数 | 当日の求人数（例：約5,000〜6,000件/日） |
| 列数 | 219列（217フィールド + `fetched_date` + `source_file`） |
| エンジン | pyarrow |

### 読み込み例

```python
import pandas as pd

df = pd.read_parquet("data/staging/hellowork_20260410.parquet")
print(df.shape)          # (6262, 219)
print(df.columns.tolist())

# 法人番号でNTA企業情報と結合
import sqlite3
conn = sqlite3.connect("data/companies.db")
companies = pd.read_sql("SELECT * FROM companies", conn)
merged = df.merge(companies, on="corporate_number", how="left")
```

### カラム一覧

#### メタ情報

| カラム名 | 説明 |
|---|---|
| `fetched_date` | クロール日（YYYYMMDD） |
| `source_file` | 元HTMLファイル名 |

#### 求人基本情報

| カラム名 | 日本語名 | HTML ID |
|---|---|---|
| `job_number` | 求人番号 | ID_kjNo |
| `received_date` | 受付年月日 | ID_uktkYmd |
| `expiry_date` | 受付期限日 | ID_shkiKigenHi |
| `hello_work_office` | 受理安定所 | ID_juriAtsh |
| `job_type_kbn` | 求人区分 | ID_kjKbn |
| `industry` | 産業分類 | ID_sngBrui |
| `online_application` | オンライン自主応募 | ID_onlinJishuOboUktkKahi |
| `try_employed` | 在職者採用希望 | ID_tryKoyoKibo |

#### 事業所情報

| カラム名 | 日本語名 | HTML ID |
|---|---|---|
| `establishment_number` | 事業所番号 | ID_jgshNo |
| `company_name_kana` | 事業所名カナ | ID_jgshMeiKana |
| `company_name` | 事業所名 | ID_jgshMei |
| `company_zip` | 事業所郵便番号 | ID_szciYbn |
| `company_address` | 事業所所在地 | ID_szci |
| `corporate_number` | 法人番号 | ID_hoNinNo |
| `representative_title` | 代表者役職 | ID_yshk |
| `representative_name` | 代表者名 | ID_dhshaMei |
| `established_year` | 設立年 | ID_setsuritsuNen |
| `capital` | 資本金 | ID_shkn |
| `employees_total` | 従業員数（企業全体） | ID_jgisKigyoZentai |
| `employees_workplace` | 従業員数（就業場所） | ID_jgisShgBs |
| `employees_female` | うち女性 | ID_jgisUchiJosei |
| `employees_part` | うちパート | ID_jgisUchiPart |
| `labor_union` | 労働組合 | ID_rodoKumiai |
| `business_description` | 事業内容 | ID_jigyoNy |
| `company_feature` | 会社の特長 | ID_kaishaNoTokucho |
| `company_notes` | 事業所特記事項 | ID_jgshTkjk |
| `work_regulations_full` | フルタイム就業規則 | ID_fltmShgKisoku |
| `work_regulations_part` | パート就業規則 | ID_partShgKisoku |
| `corporate_pension` | 企業年金 | ID_kigyoNenkin |
| `welfare_detail` | 福利厚生内容 | ID_fukuriKoseiNoNy |
| `company_url` | 企業URL | ID_hp |
| `inquiry_contact1` | 問い合わせ担当者1 | ID_kksh1 |
| `inquiry_contact2` | 問い合わせ担当者2 | ID_kksh2 |
| `inquiry_contact3` | 問い合わせ担当者3 | ID_kksh3 |
| `pr_logo1` | PRロゴ1（src属性） | ID_prLogo1 |
| `pr_logo2` | PRロゴ2（src属性） | ID_prLogo2 |
| `pr_logo3` | PRロゴ3（src属性） | ID_prLogo3 |

#### 仕事内容

| カラム名 | 日本語名 | HTML ID |
|---|---|---|
| `job_title` | 職種 | ID_sksu |
| `job_description` | 仕事内容 | ID_shigotoNy |
| `employment_type` | 雇用形態 | ID_koyoKeitai |
| `employment_type_other` | 雇用形態（その他） | ID_koyoKeitaiSsinIgaiNoMeisho |
| `contract_period` | 雇用期間 | ID_koyoKikan |
| `contract_period_count` | 雇用期間数 | ID_koyoKikanSu |
| `contract_period_end` | 契約期間終了日 | ID_koyoKikanYMD |
| `contract_renewal` | 契約更新の可能性 | ID_koyoKikanKeiyakuKsnNoKnsi |
| `contract_renewal_condition` | 契約更新条件 | ID_koyoKikanKeiyakuKsnNoJkn |
| `regular_conversion_record` | 正社員転換実績有無 | ID_koyoKeitaiSsinNoUmu |
| `regular_conversion_count` | 正社員転換実績数 | ID_koyoKeitaiSsinJisseki |
| `foreign_worker_record` | 外国人雇用実績 | ID_gkjnKoyoJisseki |
| `workplace_access` | 就業場所アクセス | ID_shgBs |
| `workplace_access_note` | 就業場所特記事項 | ID_shgBsTkjk |
| `workplace_zip` | 就業場所郵便番号 | ID_shgBsYubinNo |
| `workplace_address` | 就業場所住所 | ID_shgBsJusho |
| `nearest_station` | 最寄り駅 | ID_shgBsMyorEki |
| `commute_transport` | 交通手段 | ID_shgBsKotsuShudan |
| `commute_time` | 所要時間 | ID_shgBsShyoJn |
| `smoking_policy` | 喫煙対策 | ID_shgBsKitsuTsak |
| `smoking_policy_note` | 喫煙対策特記 | ID_shgBsKitsuTsakTkjk |
| `transfer` | 転勤の可能性 | ID_tenkinNoKnsi |
| `transfer_area` | 転勤可能エリア | ID_tenkinNoKnsiTenkinHanni |
| `uij_turn` | UIJターン歓迎 | ID_uIJTurn |
| `mycar_commute` | マイカー通勤 | ID_mycarTskn |
| `parking_available` | 駐車場有無 | ID_mycarTsknChushaUmu |
| `age_note` | 年齢（不問等） | ID_nenrei |
| `age_restriction` | 年齢制限 | ID_nenreiSegn |
| `age_restriction_range` | 年齢制限範囲 | ID_nenreiSegnHanni |
| `age_restriction_reason_type` | 年齢制限該当事由 | ID_nenreiSegnGaitoJiyu |
| `age_restriction_reason` | 年齢制限の理由 | ID_nenreiSegnNoRy |
| `education` | 学歴 | ID_grki |
| `education_detail` | 学歴詳細 | ID_grkiIjo |
| `education_subject` | 学科・専攻 | ID_grkiSnkNi |
| `experience_required` | 必要な経験 | ID_hynaKiknt |
| `experience_detail` | 経験詳細 | ID_hynaKikntShsi |
| `pc_skills` | PC要件 | ID_hynaPc |
| `license_necessity` | 免許不問等 | ID_hynaMenkyoSkku |
| `license_required` | 普通免許 | ID_FutsuMenkyo |
| `license_other_necessity` | その他免許必須/尚可 | ID_MenkyoSkkuSel |
| `license_other_name` | その他免許名 | ID_MenkyoSkkuMeisho |
| `license_other_input` | 免許資格入力 | ID_MenkyoSkkuNyuryoku |
| `license_additional` | その他免許資格（追加） | ID_sNtaMenkyoSkku |
| `trial_period` | 試用期間 | ID_trialKikan |
| `trial_period_duration` | 試用期間の長さ | ID_trialKikanKikan |
| `trial_period_condition` | 試用期間中の条件 | ID_trialKikanChuuNoRodoJkn |
| `trial_period_condition_detail` | 試用期間中の条件詳細 | ID_trialKikanChuuNoRodoJknNoNy |
| `dispatch_flag` | 派遣・請負 | ID_hakenUkeoiToShgKeitai |
| `dispatch_number` | 派遣許可番号 | ID_hakenUkeoiToRdsha |
| `special_work_regulations` | 特別な事情 | ID_tkbsNaJijo |
| `hiring_reason` | 募集理由 | ID_boshuRy |
| `hiring_reason_other` | 募集理由（その他） | ID_sntaNoBoshuRy |
| `workplace_count` | 就業場所数 | ID_shtnKssu |
| `workplace_1` | 就業場所1 | ID_shtn1 |
| `workplace_2` | 就業場所2 | ID_shtn2 |
| `workplace_3` | 就業場所3 | ID_shtn3 |
| `workplace_current_location` | 在勤者対象 | ID_shgBsZaiKinmu |
| `workplace_condition_note` | 就業場所条件特記 | ID_tkjShstTkjk |
| `job_type_url` | 職業解説URL | ID_shokusyuKaisetsuURL |
| `special_condition1` | 特別な条件1 | ID_stsk1 |
| `special_condition2` | 特別な条件2 | ID_stsk2 |
| `special_condition3` | 特別な条件3 | ID_stsk3 |
| `recruitment_support` | 採用支援内容 | ID_rrtShienNy |
| `job_change_form` | 就職定着型求人 | ID_kigyoZiskKataJobUmu |

#### 賃金・手当

| カラム名 | 日本語名 | HTML ID |
|---|---|---|
| `wage` | 賃金 | ID_chgn |
| `base_salary` | 基本給 | ID_khky |
| `wage_type` | 賃金形態 | ID_chgnKeitaiToKbn |
| `wage_type_other` | 賃金形態（その他） | ID_chgnKeitaiTo |
| `wage_type_note` | 賃金形態特記 | ID_chgnKeitaiToSntaNy |
| `fixed_allowance` | 定額的に支払われる手当 | ID_tgktNiShwrTat |
| `fixed_allowance_rate` | 定額的手当支給率 | ID_sokkgSkrt |
| `fixed_overtime_flag` | 固定残業代有無 | ID_koteiZngyKbn |
| `fixed_overtime_amount` | 固定残業代金額 | ID_koteiZngy |
| `fixed_overtime_detail` | 固定残業代詳細 | ID_koteiZngyTkjk |
| `other_allowances` | その他手当 | ID_sntaTatFukiJk |
| `commute_allowance` | 通勤手当 | ID_tsknTat |
| `commute_allowance_unit` | 通勤手当単位 | ID_tsknTatTsuki |
| `commute_allowance_amount` | 通勤手当金額 | ID_tsknTatKingaku |
| `wage_closing_day` | 賃金締切日 | ID_chgnSkbi |
| `wage_closing_day_other` | 賃金締切日（その他） | ID_chgnSkbiMitk |
| `wage_closing_day_extra` | 賃金締切日特記 | ID_chgnSkbiSntaNoSkbi |
| `wage_payment_type` | 賃金支払日（種別） | ID_chgnSrbi |
| `wage_payment_month` | 賃金支払月 | ID_chgnSrbiTsuki |
| `wage_payment_day` | 賃金支払日 | ID_chgnSrbiHi |
| `wage_payment_extra` | 賃金支払日特記 | ID_chgnSrbiSnta |
| `pay_raise` | 昇給 | ID_shokyuSd |
| `pay_raise_record` | 昇給前年度実績 | ID_shokyuMaeNendoJisseki |
| `bonus` | 賞与 | ID_shoyoSdNoUmu |
| `bonus_record` | 賞与前年度実績有無 | ID_shoyoMaeNendoUmu |
| `bonus_times` | 賞与回数 | ID_shoyoMaeNendKaisu |
| `bonus_amount` | 賞与金額 | ID_shoyoKingaku |
| `annual_income1` | 年収例1 | ID_nensho1 |
| `annual_income1_years` | 年収例1経験年数 | ID_nensho1Nen |
| `annual_income2` | 年収例2 | ID_nensho2 |
| `annual_income2_years` | 年収例2経験年数 | ID_nensho2Nen |
| `annual_income3` | 年収例3 | ID_nensho3 |
| `annual_income3_years` | 年収例3経験年数 | ID_nensho3Nen |

#### 労働時間

| カラム名 | 日本語名 | HTML ID |
|---|---|---|
| `work_hours` | 就業時間1 | ID_shgJn1 |
| `work_hours_shift` | 就業時間（交替制） | ID_shgJn |
| `work_hours_2` | 就業時間2 | ID_shgJn2 |
| `work_hours_3` | 就業時間3 | ID_shgJn3 |
| `work_hours_or` | 就業時間（その他） | ID_shgJnOr |
| `work_hours_note` | 就業時間備考 | ID_shgJiknTkjk |
| `flexible_work_unit` | 変形労働時間単位 | ID_henkeiRdTani |
| `weekly_work_days` | 就労日数 | ID_shuRdNisu |
| `weekly_work_days_negotiable` | 就労日数相談可 | ID_shuRdNisuSodanKa |
| `overtime_flag` | 時間外労働 | ID_jkgiRodoJn |
| `overtime_hours_monthly` | 月平均残業時間 | ID_thkinJkgiRodoJn |
| `avg_work_days_monthly` | 月平均労働日数 | ID_thkinRodoNissu |
| `article36_agreement` | 36協定 | ID_sanrokuKyotei |
| `break_time` | 休憩時間 | ID_kyukeiJn |
| `break_room` | 休憩室 | ID_kyukeiShitsu |
| `weekly_holiday` | 週休2日制 | ID_shukFtskSei |
| `annual_holidays` | 年間休日数 | ID_nenkanKjsu |
| `holiday_type` | 休日等 | ID_kyjs |
| `holiday_detail` | 休日その他詳細 | ID_kyjsSnta |
| `paid_leave_days` | 年次有給休暇 | ID_nenjiYukyu |

#### その他の労働条件

| カラム名 | 日本語名 | HTML ID |
|---|---|---|
| `social_insurance` | 加入保険 | ID_knyHoken |
| `social_insurance_extra` | 社会保険設備等追記 | ID_sgshaShstSetsubiTo |
| `retirement_fund_union` | 退職金共済 | ID_tskinKsi |
| `retirement_fund_system` | 退職金制度 | ID_tskinSd |
| `retirement_fund_years` | 退職金勤続年数 | ID_tskinSdKinzokuNensu |
| `retirement_age_system` | 定年制 | ID_tnsei |
| `retirement_age` | 定年年齢 | ID_tnseiTeinenNenrei |
| `rehire_system` | 再雇用制度 | ID_saiKoyoSd |
| `rehire_age_limit` | 再雇用上限年齢 | ID_saiKoyoSdJgnNenrei |
| `work_extension_system` | 勤務延長制度 | ID_kmec |
| `work_extension_age_limit` | 勤務延長上限年齢 | ID_kmecJgnNenrei |
| `housing` | 社宅・寮 | ID_nkj |
| `housing_note` | 社宅・寮特記 | ID_nkjTkjk |
| `childcare_facility` | 利用可能な託児施設 | ID_riyoKanoTkjShst |
| `childcare_leave_record` | 育児休業取得実績 | ID_ikujiKyugyoStkJisseki |
| `nursing_care_leave_record` | 介護休業取得実績 | ID_kaigoKyugyoStkJisseki |
| `nursing_leave_record` | 看護休暇取得実績 | ID_kangoKyukaStkJisseki |
| `performance_pay_system` | 職務給制度 | ID_shokumuKyuSd |
| `performance_pay_detail` | 職務給制度内容 | ID_shokumuKyuSdNoNy |
| `rehire_other_system` | 福職制度 | ID_fukushokuSd |
| `rehire_other_detail` | 福職制度内容 | ID_fukushokuSdNoNy |
| `training_system` | 訓練制度内容 | ID_knsSdNy |
| `training_for_non_regular` | 訓練制度（非正規） | ID_knsSdNoSsinIgaiNoRiyo |
| `barrier_free` | バリアフリー | ID_barrierFree |
| `elevator` | エレベーター | ID_elevator |
| `handrail_stairs` | 階段手すり | ID_kaidanTesuri |
| `wheelchair_access` | 建物内車椅子移動 | ID_tatemonoKrmIsuIdo |
| `display_facility` | 点字・展示設備 | ID_tenjiSetsubi |
| `handrail_installation` | 手すり設置 | ID_tesuriSechi |
| `latitude1` | 緯度（事業所） | ID_latitudemap1 |
| `longitude1` | 経度（事業所） | ID_longitudemap1 |
| `latitude2` | 緯度（就業場所） | ID_latitudemap2 |
| `longitude2` | 経度（就業場所） | ID_longitudemap2 |

#### 選考情報

| カラム名 | 日本語名 | HTML ID |
|---|---|---|
| `hiring_count` | 採用人数 | ID_saiyoNinsu |
| `selection_method` | 選考方法 | ID_selectHoho |
| `document_screening_result` | 書類選考結果通知 | ID_shoruiSelectKekka |
| `interview_result_timing` | 面接選考結果 | ID_mensetsuSelectKekka |
| `result_notification_timing` | 選考結果通知タイミング | ID_selectKekkaTsuch |
| `result_notification_method` | 通知方法 | ID_ksshEnoTsuchiHoho |
| `selection_schedule` | 選考日時等 | ID_selectNichijiTo |
| `selection_schedule_note` | 面接日時 | ID_sntaNoSelectNichijiTo |
| `selection_venue_zip` | 選考場所郵便番号 | ID_selectBsYubinNo |
| `selection_venue_address` | 選考場所住所 | ID_selectBsJusho |
| `selection_venue_station` | 選考場所最寄り駅 | ID_selectBsMyorEki |
| `selection_venue_transport` | 選考場所交通手段 | ID_selectBsMyorEkiKotsuShudan |
| `selection_venue_time` | 選考場所所要時間 | ID_selectBsShyoJn |
| `selection_notes` | 選考特記事項 | ID_selectTkjk |
| `application_documents` | 応募書類 | ID_oboShoruitou |
| `application_submission_method` | 応募書類送付方法 | ID_oboShoruiNoSofuHoho |
| `application_doc_return` | 応募書類返戻 | ID_obohen |
| `other_doc_submission_method` | その他書類送付方法 | ID_sntaNoSofuHoho |
| `other_required_docs` | その他応募書類 | ID_sntaNoOboShorui |
| `postal_submission_zip` | 郵送先郵便番号 | ID_yusoNoSofuBsYubinNo |
| `postal_submission_address` | 郵送先住所 | ID_yusoNoSofuBsJusho |
| `contact_department` | 担当部署 | ID_ttsYkm |
| `contact_person` | 担当者名 | ID_ttsTts |
| `contact_person_kana` | 担当者名カナ | ID_ttsTtsKana |
| `contact_phone` | 担当電話番号 | ID_ttsTel |
| `contact_fax` | FAX番号 | ID_ttsFax |
| `contact_extension` | 内線 | ID_ttsNaisen |
| `contact_email` | 担当メール | ID_ttsEmail |

#### 特記事項

| カラム名 | 日本語名 | HTML ID |
|---|---|---|
| `special_notes` | 求人に関する特記事項 | ID_kjTkjk |
| `company_pr` | 求人・事業所PR | ID_jgshKaraNoMsg |

---

## 8. トラブルシューティング

### クロールが止まる・途中で終了する

`--skip-crawl` で再実行するとHTMLは既存ファイルをスキップして途中から再開できる。

```bash
python scripts/run_hellowork.py --date 2026-04-10 --skip-crawl
```

### Seleniumのタイムアウトエラーが多い

`config.yaml` の `timeout` を大きくする（例：`30`）。

### bot検知で求人が取得できない

`config.yaml` の `wait_between_pages` / `wait_between_details` を大きくする（例：`3.0` / `2.0`）。

### HTMLファイルは増えているがParquetが古い

`--skip-crawl` で Parquet のみ再生成する。

```bash
python scripts/run_hellowork.py --date 2026-04-10 --skip-crawl
```

### ログの確認

```
logs/hellowork_YYYYMMDD.log
```

STEP 1〜3 の完了・件数・所要時間が記録されている。エラーは `[ERROR]` または `[WARNING]` で検索。
