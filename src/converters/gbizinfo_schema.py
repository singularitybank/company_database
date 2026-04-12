# -*- coding: utf-8 -*-
"""
gbizinfo 各CSV のスキーマ定義

構造:
  DATASETS[key] = {
      "csv_file"    : CSVファイル名（data/raw/gbizinfo/ 以下）
      "core_count"  : コア列数（先頭 N 列）
      "core_columns": [英語カラム名リスト（コア列順）]
      "date_columns": 日付型に変換するカラム名セット
      "int_columns" : 整数型に変換するカラム名セット
  }

  メタデータ列はすべて JSON blob として meta Parquet に格納する。
  コア列は core Parquet に英語カラム名で格納する。
"""

DATASETS: dict[str, dict] = {

    # ------------------------------------------------------------------
    # 基本情報（5,816,681件 / 96列 = core 30 + meta 66）
    # ------------------------------------------------------------------
    "kihonjoho": {
        "csv_file": "Kihonjoho_UTF-8.csv",
        "core_count": 30,
        "core_columns": [
            "corporate_number",          # 01 法人番号
            "name",                      # 02 商号または名称
            "kana",                      # 03 商号または名称（カナ）
            "name_en",                   # 04 商号または名称（英字）
            "close_date",                # 05 登記記録の閉鎖等年月日
            "close_cause",               # 06 登記記録の閉鎖等の事由（コード値）
            "location",                  # 07 登記住所（連結）
            "postal_code",               # 08 郵便番号
            "prefecture_name",           # 09 都道府県
            "prefecture_code",           # 10 都道府県コード
            "city_name",                 # 11 市区町村（郡）
            "city_code",                 # 12 市区町村コード
            "street_number",             # 13 番地以下
            "kind",                      # 14 組織種別（コード値）
            "process",                   # 15 処理区分（コード値）
            "correct",                   # 16 訂正区分
            "status",                    # 17 状態
            "representative_name",       # 18 代表者名称
            "capital_stock",             # 19 資本金
            "employee_number",           # 20 従業員数
            "company_size_male",         # 21 企業規模詳細（男性）
            "company_size_female",       # 22 企業規模詳細（女性）
            "business_summary",          # 23 事業概要
            "company_url",               # 24 WebサイトURL
            "founding_year",             # 25 創業年
            "business_items",            # 26 事業種目（|区切り）
            "date_of_establishment",     # 27 設立年月日
            "qualification_grade",       # 28 全省庁統一資格-資格等級
            "business_category",         # 29 全省庁統一資格-営業品目
            "update_date",               # 30 更新年月日
        ],
        "date_columns": {"close_date", "date_of_establishment", "update_date"},
        "int_columns":  {"capital_stock", "employee_number",
                         "company_size_male", "company_size_female"},
    },

    # ------------------------------------------------------------------
    # 調達情報（8 core + 6 meta）
    # ------------------------------------------------------------------
    "chotatsujoho": {
        "csv_file": "Chotatsujoho_UTF-8.csv",
        "core_count": 8,
        "core_columns": [
            "corporate_number",   # 01 法人番号
            "name",               # 02 商号または名称
            "location",           # 03 登記住所
            "order_date",         # 04 受注日
            "title",              # 05 件名
            "contract_price",     # 06 落札価格
            "organization_name",  # 07 組織名
            "remarks",            # 08 備考
        ],
        "date_columns": {"order_date"},
        "int_columns":  {"contract_price"},
    },

    # ------------------------------------------------------------------
    # 補助金情報（8 core + 6 meta）
    # ------------------------------------------------------------------
    "hojokinjoho": {
        "csv_file": "Hojokinjoho_UTF-8.csv",
        "core_count": 8,
        "core_columns": [
            "corporate_number",   # 01 法人番号
            "name",               # 02 商号または名称
            "location",           # 03 登記住所
            "certification_date", # 04 証明日
            "title",              # 05 名称
            "amount",             # 06 金額
            "target",             # 07 対象
            "issuer",             # 08 発行元
        ],
        "date_columns": {"certification_date"},
        "int_columns":  {"amount"},
    },

    # ------------------------------------------------------------------
    # 表彰情報（9 core + 6 meta）
    # ------------------------------------------------------------------
    "hyoshojoho": {
        "csv_file": "Hyoshojoho_UTF-8.csv",
        "core_count": 9,
        "core_columns": [
            "corporate_number",   # 01 法人番号
            "name",               # 02 商号または名称
            "location",           # 03 登記住所
            "certification_date", # 04 証明日
            "title",              # 05 名称
            "target",             # 06 対象
            "department",         # 07 部門
            "issuer",             # 08 発行元
            "remarks",            # 09 備考
        ],
        "date_columns": {"certification_date"},
        "int_columns":  set(),
    },

    # ------------------------------------------------------------------
    # 職場情報（19 core + 6 meta）
    # ------------------------------------------------------------------
    "shokubajoho": {
        "csv_file": "Shokubajoho_UTF-8.csv",
        "core_count": 19,
        "core_columns": [
            "corporate_number",              # 01 法人番号
            "name",                          # 02 商号または名称
            "location",                      # 03 登記住所
            "avg_tenure_range",              # 04 平均継続勤務年数-範囲
            "avg_tenure_male",               # 05 平均継続勤務年数-男性
            "avg_tenure_female",             # 06 平均継続勤務年数-女性
            "avg_tenure_fulltime",           # 07 正社員の平均継続勤務年数
            "avg_employee_age",              # 08 従業員の平均年齢
            "avg_overtime_hours",            # 09 月平均所定外労働時間
            "female_worker_ratio_range",     # 10 労働者に占める女性労働者の割合-範囲
            "female_worker_ratio",           # 11 労働者に占める女性労働者の割合
            "female_manager_count",          # 12 女性管理職人数
            "total_manager_count",           # 13 管理職全体人数（男女計）
            "female_executive_count",        # 14 女性役員人数
            "total_executive_count",         # 15 役員全体人数（男女計）
            "childcare_eligible_male",       # 16 育児休業対象者数（男性）
            "childcare_eligible_female",     # 17 育児休業対象者数（女性）
            "childcare_takers_male",         # 18 育児休業取得者数（男性）
            "childcare_takers_female",       # 19 育児休業取得者数（女性）
        ],
        "date_columns": set(),
        "int_columns":  set(),
    },

    # ------------------------------------------------------------------
    # 届出認定情報（8 core + 6 meta）
    # ------------------------------------------------------------------
    "todokedeninteijoho": {
        "csv_file": "TodokedeNinteijoho_UTF-8.csv",
        "core_count": 8,
        "core_columns": [
            "corporate_number",   # 01 法人番号
            "name",               # 02 商号または名称
            "location",           # 03 登記住所
            "certification_date", # 04 証明日
            "title",              # 05 名称
            "target",             # 06 対象
            "department",         # 07 部門
            "issuer",             # 08 発行元
        ],
        "date_columns": {"certification_date"},
        "int_columns":  set(),
    },

    # ------------------------------------------------------------------
    # 特許情報（15 core + 6 meta）
    # ------------------------------------------------------------------
    "tokkyojoho": {
        "csv_file": "Tokkyojoho_UTF-8.csv",
        "core_count": 15,
        "core_columns": [
            "corporate_number",              # 01 法人番号
            "name",                          # 02 商号または名称
            "location",                      # 03 登記住所
            "patent_type",                   # 04 特許/意匠/商標
            "registration_number",           # 05 登録番号
            "application_date",              # 06 出願年月日
            "fi_classification_code",        # 07 特許_FI分類_コード値
            "fi_classification_code_ja",     # 08 特許_FI分類_コード値（日本語）
            "f_term_theme_code",             # 09 特許_Fターム-テーマコード
            "design_new_classification_code",    # 10 意匠_意匠新分類_コード値
            "design_new_classification_code_ja", # 11 意匠_意匠新分類_コード値（日本語）
            "trademark_class_code",          # 12 商標_類_コード値
            "trademark_class_code_ja",       # 13 商標_類_コード値（日本語）
            "title",                         # 14 発明の名称(等)/意匠に係る物品/表示用商標
            "document_fixed_address",        # 15 文献固定アドレス
        ],
        "date_columns": {"application_date"},
        "int_columns":  set(),
    },

    # ------------------------------------------------------------------
    # 財務情報（40 core + 6 meta）
    # ------------------------------------------------------------------
    "zaimujoho": {
        "csv_file": "Zaimujoho_UTF-8.csv",
        "core_count": 40,
        "core_columns": [
            "corporate_number",           # 01 法人番号
            "name",                       # 02 商号または名称
            "location",                   # 03 登記住所
            "accounting_standard",        # 04 会計基準
            "fiscal_period",              # 05 事業年度
            "term_number",                # 06 回次
            "net_sales",                  # 07 売上高
            "net_sales_unit",             # 08 売上高（単位）
            "operating_revenue",          # 09 営業収益
            "operating_revenue_unit",     # 10 営業収益（単位）
            "operating_income",           # 11 営業収入
            "operating_income_unit",      # 12 営業収入（単位）
            "gross_operating_revenue",    # 13 営業総収入
            "gross_operating_revenue_unit", # 14 営業総収入（単位）
            "ordinary_income",            # 15 経常収益
            "ordinary_income_unit",       # 16 経常収益（単位）
            "net_insurance_premium",      # 17 正味収入保険料
            "net_insurance_premium_unit", # 18 正味収入保険料（単位）
            "recurring_profit",           # 19 経常利益又は経常損失（△）
            "recurring_profit_unit",      # 20 経常利益又は経常損失（△）(単位)
            "net_income",                 # 21 当期純利益又は当期純損失（△）
            "net_income_unit",            # 22 当期純利益又は当期純損失（△）(単位)
            "capital_stock",              # 23 資本金
            "capital_stock_unit",         # 24 資本金(単位)
            "net_assets",                 # 25 純資産額
            "net_assets_unit",            # 26 純資産額(単位)
            "total_assets",               # 27 総資産額
            "total_assets_unit",          # 28 総資産額(単位)
            "employee_count",             # 29 従業員数
            "employee_count_unit",        # 30 従業員数(単位)
            "major_shareholder_1",        # 31 大株主1
            "shareholder_1_ratio",        # 32 発行済株式総数に対する所有株式数の割合1
            "major_shareholder_2",        # 33 大株主2
            "shareholder_2_ratio",        # 34 割合2
            "major_shareholder_3",        # 35 大株主3
            "shareholder_3_ratio",        # 36 割合3
            "major_shareholder_4",        # 37 大株主4
            "shareholder_4_ratio",        # 38 割合4
            "major_shareholder_5",        # 39 大株主5
            "shareholder_5_ratio",        # 40 割合5
        ],
        "date_columns": set(),
        "int_columns":  set(),
    },
}
