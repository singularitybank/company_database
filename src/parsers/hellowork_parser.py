# -*- coding: utf-8 -*-
"""
ハローワーク 求人詳細ページ HTMLパーサー

scrape_details() が保存したHTMLファイルを読み込み、
構造化されたデータ（dict）に変換する。

[設計方針]
  - 全フィールドを id付きdivから直接取得（th/tdのテーブル走査は使わない）
  - 同一IDが複数回出現する場合は末尾（詳細部）の値を使用
  - 値が存在しない場合は None を返す（空文字との区別）
"""

import logging
import sys
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional

import pandas as pd
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.config import hellowork as _cfg

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# データクラス
# ---------------------------------------------------------------------------

@dataclass
class HellworkJob:
    """求人詳細1件分の構造化データ"""

    # ── 求人基本情報 ──────────────────────────────────────────────────────────
    job_number:                     Optional[str] = None  # 求人番号              ID_kjNo
    received_date:                  Optional[str] = None  # 受付年月日            ID_uktkYmd
    expiry_date:                    Optional[str] = None  # 受付期限日            ID_shkiKigenHi
    hello_work_office:              Optional[str] = None  # 受理安定所            ID_juriAtsh
    job_type_kbn:                   Optional[str] = None  # 求人区分              ID_kjKbn
    industry:                       Optional[str] = None  # 産業分類              ID_sngBrui
    online_application:             Optional[str] = None  # オンライン自主応募    ID_onlinJishuOboUktkKahi
    try_employed:                   Optional[str] = None  # 在職者採用希望        ID_tryKoyoKibo

    # ── 事業所情報 ────────────────────────────────────────────────────────────
    establishment_number:           Optional[str] = None  # 事業所番号            ID_jgshNo
    company_name_kana:              Optional[str] = None  # 事業所名カナ          ID_jgshMeiKana
    company_name:                   Optional[str] = None  # 事業所名              ID_jgshMei
    company_zip:                    Optional[str] = None  # 事業所郵便番号        ID_szciYbn
    company_address:                Optional[str] = None  # 事業所所在地          ID_szci
    corporate_number:               Optional[str] = None  # 法人番号              ID_hoNinNo
    representative_title:           Optional[str] = None  # 代表者役職            ID_yshk
    representative_name:            Optional[str] = None  # 代表者名              ID_dhshaMei
    established_year:               Optional[str] = None  # 設立年                ID_setsuritsuNen
    capital:                        Optional[str] = None  # 資本金                ID_shkn
    employees_total:                Optional[str] = None  # 従業員数（企業全体）  ID_jgisKigyoZentai
    employees_workplace:            Optional[str] = None  # 従業員数（就業場所）  ID_jgisShgBs
    employees_female:               Optional[str] = None  # うち女性              ID_jgisUchiJosei
    employees_part:                 Optional[str] = None  # うちパート            ID_jgisUchiPart
    labor_union:                    Optional[str] = None  # 労働組合              ID_rodoKumiai
    business_description:           Optional[str] = None  # 事業内容              ID_jigyoNy
    company_feature:                Optional[str] = None  # 会社の特長            ID_kaishaNoTokucho
    company_notes:                  Optional[str] = None  # 事業所特記事項        ID_jgshTkjk
    work_regulations_full:          Optional[str] = None  # フルタイム就業規則    ID_fltmShgKisoku
    work_regulations_part:          Optional[str] = None  # パート就業規則        ID_partShgKisoku
    corporate_pension:              Optional[str] = None  # 企業年金              ID_kigyoNenkin
    welfare_detail:                 Optional[str] = None  # 福利厚生内容          ID_fukuriKoseiNoNy

    # ── 仕事内容 ──────────────────────────────────────────────────────────────
    job_title:                      Optional[str] = None  # 職種                  ID_sksu
    job_description:                Optional[str] = None  # 仕事内容              ID_shigotoNy
    employment_type:                Optional[str] = None  # 雇用形態              ID_koyoKeitai
    employment_type_other:          Optional[str] = None  # 雇用形態（その他）    ID_koyoKeitaiSsinIgaiNoMeisho
    contract_period:                Optional[str] = None  # 雇用期間              ID_koyoKikan
    contract_period_count:          Optional[str] = None  # 雇用期間数            ID_koyoKikanSu
    contract_period_end:            Optional[str] = None  # 契約期間終了日        ID_koyoKikanYMD
    contract_renewal:               Optional[str] = None  # 契約更新の可能性      ID_koyoKikanKeiyakuKsnNoKnsi
    contract_renewal_condition:     Optional[str] = None  # 契約更新条件          ID_koyoKikanKeiyakuKsnNoJkn
    regular_conversion_record:      Optional[str] = None  # 正社員転換実績有無    ID_koyoKeitaiSsinNoUmu
    regular_conversion_count:       Optional[str] = None  # 正社員転換実績数      ID_koyoKeitaiSsinJisseki
    foreign_worker_record:          Optional[str] = None  # 外国人雇用実績        ID_gkjnKoyoJisseki
    workplace_access:               Optional[str] = None  # 就業場所アクセス      ID_shgBs
    workplace_access_note:          Optional[str] = None  # 就業場所特記事項      ID_shgBsTkjk
    workplace_zip:                  Optional[str] = None  # 就業場所郵便番号      ID_shgBsYubinNo
    workplace_address:              Optional[str] = None  # 就業場所住所          ID_shgBsJusho
    nearest_station:                Optional[str] = None  # 最寄り駅              ID_shgBsMyorEki
    commute_transport:              Optional[str] = None  # 交通手段              ID_shgBsKotsuShudan
    commute_time:                   Optional[str] = None  # 所要時間              ID_shgBsShyoJn
    smoking_policy:                 Optional[str] = None  # 喫煙対策              ID_shgBsKitsuTsak
    smoking_policy_note:            Optional[str] = None  # 喫煙対策特記          ID_shgBsKitsuTsakTkjk
    transfer:                       Optional[str] = None  # 転勤の可能性          ID_tenkinNoKnsi
    transfer_area:                  Optional[str] = None  # 転勤可能エリア        ID_tenkinNoKnsiTenkinHanni
    uij_turn:                       Optional[str] = None  # UIJターン歓迎         ID_uIJTurn
    mycar_commute:                  Optional[str] = None  # マイカー通勤          ID_mycarTskn
    parking_available:              Optional[str] = None  # 駐車場有無            ID_mycarTsknChushaUmu
    age_note:                       Optional[str] = None  # 年齢（不問等）        ID_nenrei
    age_restriction:                Optional[str] = None  # 年齢制限              ID_nenreiSegn
    age_restriction_range:          Optional[str] = None  # 年齢制限範囲          ID_nenreiSegnHanni
    age_restriction_reason_type:    Optional[str] = None  # 年齢制限該当事由      ID_nenreiSegnGaitoJiyu
    age_restriction_reason:         Optional[str] = None  # 年齢制限の理由        ID_nenreiSegnNoRy
    education:                      Optional[str] = None  # 学歴                  ID_grki
    education_detail:               Optional[str] = None  # 学歴詳細              ID_grkiIjo
    education_subject:              Optional[str] = None  # 学科・専攻            ID_grkiSnkNi
    experience_required:            Optional[str] = None  # 必要な経験            ID_hynaKiknt
    experience_detail:              Optional[str] = None  # 経験詳細              ID_hynaKikntShsi
    pc_skills:                      Optional[str] = None  # PC要件                ID_hynaPc
    license_necessity:              Optional[str] = None  # 免許不問等            ID_hynaMenkyoSkku
    license_required:               Optional[str] = None  # 普通免許              ID_FutsuMenkyo
    license_other_necessity:        Optional[str] = None  # その他免許必須/尚可   ID_MenkyoSkkuSel
    license_other_name:             Optional[str] = None  # その他免許名          ID_MenkyoSkkuMeisho
    license_additional:             Optional[str] = None  # その他免許資格（追加）ID_sNtaMenkyoSkku
    trial_period:                   Optional[str] = None  # 試用期間              ID_trialKikan
    trial_period_duration:          Optional[str] = None  # 試用期間の長さ        ID_trialKikanKikan
    trial_period_condition:         Optional[str] = None  # 試用期間中の条件      ID_trialKikanChuuNoRodoJkn
    trial_period_condition_detail:  Optional[str] = None  # 試用期間中の条件詳細  ID_trialKikanChuuNoRodoJknNoNy
    dispatch_flag:                  Optional[str] = None  # 派遣・請負            ID_hakenUkeoiToShgKeitai
    dispatch_number:                Optional[str] = None  # 派遣許可番号          ID_hakenUkeoiToRdsha
    special_work_regulations:       Optional[str] = None  # 特別な事情            ID_tkbsNaJijo
    hiring_reason:                  Optional[str] = None  # 募集理由              ID_boshuRy
    hiring_reason_other:            Optional[str] = None  # 募集理由（その他）    ID_sntaNoBoshuRy
    workplace_count:                Optional[str] = None  # 就業場所数            ID_shtnKssu
    workplace_1:                    Optional[str] = None  # 就業場所1             ID_shtn1
    workplace_2:                    Optional[str] = None  # 就業場所2             ID_shtn2
    workplace_3:                    Optional[str] = None  # 就業場所3             ID_shtn3

    # ── 賃金・手当 ────────────────────────────────────────────────────────────
    wage:                           Optional[str] = None  # 賃金                  ID_chgn
    base_salary:                    Optional[str] = None  # 基本給                ID_khky
    wage_type:                      Optional[str] = None  # 賃金形態              ID_chgnKeitaiToKbn
    wage_type_other:                Optional[str] = None  # 賃金形態（その他）    ID_chgnKeitaiTo
    wage_type_note:                 Optional[str] = None  # 賃金形態特記          ID_chgnKeitaiToSntaNy
    fixed_allowance:                Optional[str] = None  # 定額的に支払われる手当 ID_tgktNiShwrTat
    fixed_allowance_rate:           Optional[str] = None  # 定額的手当支給率      ID_sokkgSkrt
    fixed_overtime_flag:            Optional[str] = None  # 固定残業代有無        ID_koteiZngyKbn
    fixed_overtime_amount:          Optional[str] = None  # 固定残業代金額        ID_koteiZngy
    fixed_overtime_detail:          Optional[str] = None  # 固定残業代詳細        ID_koteiZngyTkjk
    other_allowances:               Optional[str] = None  # その他手当            ID_sntaTatFukiJk
    commute_allowance:              Optional[str] = None  # 通勤手当              ID_tsknTat
    commute_allowance_unit:         Optional[str] = None  # 通勤手当単位          ID_tsknTatTsuki
    commute_allowance_amount:       Optional[str] = None  # 通勤手当金額          ID_tsknTatKingaku
    wage_closing_day:               Optional[str] = None  # 賃金締切日            ID_chgnSkbi
    wage_closing_day_other:         Optional[str] = None  # 賃金締切日（その他）  ID_chgnSkbiMitk
    wage_payment_type:              Optional[str] = None  # 賃金支払日（種別）    ID_chgnSrbi
    wage_payment_month:             Optional[str] = None  # 賃金支払月            ID_chgnSrbiTsuki
    wage_payment_day:               Optional[str] = None  # 賃金支払日            ID_chgnSrbiHi
    pay_raise:                      Optional[str] = None  # 昇給                  ID_shokyuSd
    pay_raise_record:               Optional[str] = None  # 昇給前年度実績        ID_shokyuMaeNendoJisseki
    bonus:                          Optional[str] = None  # 賞与                  ID_shoyoSdNoUmu
    bonus_record:                   Optional[str] = None  # 賞与前年度実績有無    ID_shoyoMaeNendoUmu
    bonus_times:                    Optional[str] = None  # 賞与回数              ID_shoyoMaeNendKaisu
    bonus_amount:                   Optional[str] = None  # 賞与金額              ID_shoyoKingaku

    # ── 労働時間 ──────────────────────────────────────────────────────────────
    work_hours:                     Optional[str] = None  # 就業時間1             ID_shgJn1
    work_hours_shift:               Optional[str] = None  # 就業時間（交替制）    ID_shgJn
    work_hours_2:                   Optional[str] = None  # 就業時間2             ID_shgJn2
    work_hours_3:                   Optional[str] = None  # 就業時間3             ID_shgJn3
    work_hours_or:                  Optional[str] = None  # 就業時間（その他）    ID_shgJnOr
    work_hours_note:                Optional[str] = None  # 就業時間備考          ID_shgJiknTkjk
    flexible_work_unit:             Optional[str] = None  # 変形労働時間単位      ID_henkeiRdTani
    weekly_work_days:               Optional[str] = None  # 就労日数              ID_shuRdNisu
    weekly_work_days_negotiable:    Optional[str] = None  # 就労日数相談可        ID_shuRdNisuSodanKa
    overtime_flag:                  Optional[str] = None  # 時間外労働            ID_jkgiRodoJn
    overtime_hours_monthly:         Optional[str] = None  # 月平均残業時間        ID_thkinJkgiRodoJn
    avg_work_days_monthly:          Optional[str] = None  # 月平均労働日数        ID_thkinRodoNissu
    article36_agreement:            Optional[str] = None  # 36協定                ID_sanrokuKyotei
    break_time:                     Optional[str] = None  # 休憩時間              ID_kyukeiJn
    break_room:                     Optional[str] = None  # 休憩室                ID_kyukeiShitsu
    weekly_holiday:                 Optional[str] = None  # 週休2日制             ID_shukFtskSei
    annual_holidays:                Optional[str] = None  # 年間休日数            ID_nenkanKjsu
    holiday_type:                   Optional[str] = None  # 休日等                ID_kyjs
    holiday_detail:                 Optional[str] = None  # 休日その他詳細        ID_kyjsSnta
    paid_leave_days:                Optional[str] = None  # 年次有給休暇          ID_nenjiYukyu

    # ── その他の労働条件 ──────────────────────────────────────────────────────
    social_insurance:               Optional[str] = None  # 加入保険              ID_knyHoken
    retirement_fund_union:          Optional[str] = None  # 退職金共済            ID_tskinKsi
    retirement_fund_system:         Optional[str] = None  # 退職金制度            ID_tskinSd
    retirement_fund_years:          Optional[str] = None  # 退職金勤続年数        ID_tskinSdKinzokuNensu
    retirement_age_system:          Optional[str] = None  # 定年制                ID_tnsei
    retirement_age:                 Optional[str] = None  # 定年年齢              ID_tnseiTeinenNenrei
    rehire_system:                  Optional[str] = None  # 再雇用制度            ID_saiKoyoSd
    rehire_age_limit:               Optional[str] = None  # 再雇用上限年齢        ID_saiKoyoSdJgnNenrei
    work_extension_system:          Optional[str] = None  # 勤務延長制度          ID_kmec
    work_extension_age_limit:       Optional[str] = None  # 勤務延長上限年齢      ID_kmecJgnNenrei
    housing:                        Optional[str] = None  # 社宅・寮              ID_nkj
    housing_note:                   Optional[str] = None  # 社宅・寮特記          ID_nkjTkjk
    childcare_facility:             Optional[str] = None  # 利用可能な託児施設    ID_riyoKanoTkjShst
    childcare_leave_record:         Optional[str] = None  # 育児休業取得実績      ID_ikujiKyugyoStkJisseki
    nursing_care_leave_record:      Optional[str] = None  # 介護休業取得実績      ID_kaigoKyugyoStkJisseki
    nursing_leave_record:           Optional[str] = None  # 看護休暇取得実績      ID_kangoKyukaStkJisseki
    performance_pay_system:         Optional[str] = None  # 職務給制度            ID_shokumuKyuSd
    performance_pay_detail:         Optional[str] = None  # 職務給制度内容        ID_shokumuKyuSdNoNy
    rehire_other_system:            Optional[str] = None  # 福職制度              ID_fukushokuSd
    rehire_other_detail:            Optional[str] = None  # 福職制度内容          ID_fukushokuSdNoNy
    training_system:                Optional[str] = None  # 訓練制度内容          ID_knsSdNy
    training_for_non_regular:       Optional[str] = None  # 訓練制度（非正規）    ID_knsSdNoSsinIgaiNoRiyo

    # ── 選考情報 ──────────────────────────────────────────────────────────────
    hiring_count:                   Optional[str] = None  # 採用人数              ID_saiyoNinsu
    selection_method:               Optional[str] = None  # 選考方法              ID_selectHoho
    document_screening_result:      Optional[str] = None  # 書類選考結果通知      ID_shoruiSelectKekka
    interview_result_timing:        Optional[str] = None  # 面接選考結果          ID_mensetsuSelectKekka
    result_notification_timing:     Optional[str] = None  # 選考結果通知タイミング ID_selectKekkaTsuch
    result_notification_method:     Optional[str] = None  # 通知方法              ID_ksshEnoTsuchiHoho
    selection_schedule:             Optional[str] = None  # 選考日時等            ID_selectNichijiTo
    selection_schedule_note:        Optional[str] = None  # 面接日時              ID_sntaNoSelectNichijiTo
    selection_venue_zip:            Optional[str] = None  # 選考場所郵便番号      ID_selectBsYubinNo
    selection_venue_address:        Optional[str] = None  # 選考場所住所          ID_selectBsJusho
    selection_venue_station:        Optional[str] = None  # 選考場所最寄り駅      ID_selectBsMyorEki
    selection_venue_transport:      Optional[str] = None  # 選考場所交通手段      ID_selectBsMyorEkiKotsuShudan
    selection_venue_time:           Optional[str] = None  # 選考場所所要時間      ID_selectBsShyoJn
    selection_notes:                Optional[str] = None  # 選考特記事項          ID_selectTkjk
    application_documents:          Optional[str] = None  # 応募書類              ID_oboShoruitou
    application_submission_method:  Optional[str] = None  # 応募書類送付方法      ID_oboShoruiNoSofuHoho
    application_doc_return:         Optional[str] = None  # 応募書類返戻          ID_obohen
    other_doc_submission_method:    Optional[str] = None  # その他書類送付方法    ID_sntaNoSofuHoho
    other_required_docs:            Optional[str] = None  # その他応募書類        ID_sntaNoOboShorui
    postal_submission_zip:          Optional[str] = None  # 郵送先郵便番号        ID_yusoNoSofuBsYubinNo
    postal_submission_address:      Optional[str] = None  # 郵送先住所            ID_yusoNoSofuBsJusho
    contact_department:             Optional[str] = None  # 担当部署              ID_ttsYkm
    contact_person:                 Optional[str] = None  # 担当者名              ID_ttsTts
    contact_person_kana:            Optional[str] = None  # 担当者名カナ          ID_ttsTtsKana
    contact_phone:                  Optional[str] = None  # 担当電話番号          ID_ttsTel
    contact_fax:                    Optional[str] = None  # FAX番号               ID_ttsFax
    contact_extension:              Optional[str] = None  # 内線                  ID_ttsNaisen
    contact_email:                  Optional[str] = None  # 担当メール            ID_ttsEmail

    # ── 特記事項 ──────────────────────────────────────────────────────────────
    special_notes:                  Optional[str] = None  # 求人に関する特記事項  ID_kjTkjk
    company_pr:                     Optional[str] = None  # 求人・事業所PR        ID_jgshKaraNoMsg

    # ── 追加：事業所情報 ──────────────────────────────────────────────────────
    company_url:                    Optional[str] = None  # 企業URL               ID_hp
    inquiry_contact1:               Optional[str] = None  # 問い合わせ担当者1     ID_kksh1
    inquiry_contact2:               Optional[str] = None  # 問い合わせ担当者2     ID_kksh2
    inquiry_contact3:               Optional[str] = None  # 問い合わせ担当者3     ID_kksh3

    # ── 追加：仕事内容 ────────────────────────────────────────────────────────
    workplace_current_location:     Optional[str] = None  # 在勤者対象            ID_shgBsZaiKinmu
    workplace_condition_note:       Optional[str] = None  # 就業場所条件特記      ID_tkjShstTkjk
    job_type_url:                   Optional[str] = None  # 職業解説URL           ID_shokusyuKaisetsuURL (href)
    license_other_input:            Optional[str] = None  # 免許資格入力          ID_MenkyoSkkuNyuryoku
    special_condition1:             Optional[str] = None  # 特別な条件1           ID_stsk1
    special_condition2:             Optional[str] = None  # 特別な条件2           ID_stsk2
    special_condition3:             Optional[str] = None  # 特別な条件3           ID_stsk3
    recruitment_support:            Optional[str] = None  # 採用支援内容          ID_rrtShienNy
    job_change_form:                Optional[str] = None  # 就職定着型求人        ID_kigyoZiskKataJobUmu

    # ── 追加：賃金・手当 ──────────────────────────────────────────────────────
    wage_closing_day_extra:         Optional[str] = None  # 賃金締切日特記        ID_chgnSkbiSntaNoSkbi
    wage_payment_extra:             Optional[str] = None  # 賃金支払日特記        ID_chgnSrbiSnta
    annual_income1:                 Optional[str] = None  # 年収例1               ID_nensho1
    annual_income1_years:           Optional[str] = None  # 年収例1経験年数       ID_nensho1Nen
    annual_income2:                 Optional[str] = None  # 年収例2               ID_nensho2
    annual_income2_years:           Optional[str] = None  # 年収例2経験年数       ID_nensho2Nen
    annual_income3:                 Optional[str] = None  # 年収例3               ID_nensho3
    annual_income3_years:           Optional[str] = None  # 年収例3経験年数       ID_nensho3Nen

    # ── 追加：その他の労働条件 ────────────────────────────────────────────────
    social_insurance_extra:         Optional[str] = None  # 社会保険設備等追記    ID_sgshaShstSetsubiTo
    barrier_free:                   Optional[str] = None  # バリアフリー          ID_barrierFree
    elevator:                       Optional[str] = None  # エレベーター          ID_elevator
    handrail_stairs:                Optional[str] = None  # 階段手すり            ID_kaidanTesuri
    wheelchair_access:              Optional[str] = None  # 建物内車椅子移動      ID_tatemonoKrmIsuIdo
    display_facility:               Optional[str] = None  # 点字・展示設備        ID_tenjiSetsubi
    handrail_installation:          Optional[str] = None  # 手すり設置            ID_tesuriSechi

    # ── 追加：画像・座標 ──────────────────────────────────────────────────────
    pr_logo1:                       Optional[str] = None  # PRロゴ1 src           ID_prLogo1
    pr_logo2:                       Optional[str] = None  # PRロゴ2 src           ID_prLogo2
    pr_logo3:                       Optional[str] = None  # PRロゴ3 src           ID_prLogo3
    latitude1:                      Optional[str] = None  # 緯度（事業所）        ID_latitudemap1
    longitude1:                     Optional[str] = None  # 経度（事業所）        ID_longitudemap1
    latitude2:                      Optional[str] = None  # 緯度（就業場所）      ID_latitudemap2
    longitude2:                     Optional[str] = None  # 経度（就業場所）      ID_longitudemap2

    # ── メタ情報 ──────────────────────────────────────────────────────────────
    source_file:                    Optional[str] = None  # 元HTMLファイル名


# ---------------------------------------------------------------------------
# パーサー
# ---------------------------------------------------------------------------

# 同一IDがページ内に複数回出現するため、末尾（詳細部）を使うべきIDの集合
_TAKE_LAST = {
    "ID_sksu", "ID_shigotoNy", "ID_kjKbn", "ID_koyoKeitai",
    "ID_shgBsYubinNo", "ID_shgBsJusho", "ID_jgshMei",
    "ID_selectHoho", "ID_onlinJishuOboUktkKahi", "ID_mycarTskn",
    "ID_tsknTat", "ID_shokusyuKaisetsuURL",
}


def _get(soup: BeautifulSoup, div_id: str) -> Optional[str]:
    """指定IDのdivからテキストを取得する。

    _TAKE_LAST に含まれるIDは末尾のdivを使用する。
    値が空（空白のみ）の場合は None を返す。
    """
    tags = soup.find_all("div", id=div_id)
    if not tags:
        return None
    tag = tags[-1] if div_id in _TAKE_LAST else tags[0]
    text = tag.get_text(" ", strip=True)
    return text if text else None


def _get_attr(soup: BeautifulSoup, elem_id: str, attr: str) -> Optional[str]:
    """任意要素のHTML属性値を取得する（input[value], a[href], img[src] 等）。"""
    tags = soup.find_all(id=elem_id)
    if not tags:
        return None
    val = tags[-1 if elem_id in _TAKE_LAST else 0].get(attr, "")
    return val if val else None


def parse_html(html_path: str | Path) -> HellworkJob:
    """HTMLファイルを読み込み HellworkJob に変換して返す。"""
    html_path = Path(html_path)

    with open(html_path, encoding="utf-8") as f:
        soup = BeautifulSoup(f, "html.parser")

    job = HellworkJob(source_file=html_path.name)

    # 求人基本情報
    job.job_number          = _get(soup, "ID_kjNo")
    job.received_date       = _get(soup, "ID_uktkYmd")
    job.expiry_date         = _get(soup, "ID_shkiKigenHi")
    job.hello_work_office   = _get(soup, "ID_juriAtsh")
    job.job_type_kbn        = _get(soup, "ID_kjKbn")
    job.industry            = _get(soup, "ID_sngBrui")
    job.online_application  = _get(soup, "ID_onlinJishuOboUktkKahi")
    job.try_employed        = _get(soup, "ID_tryKoyoKibo")

    # 事業所情報
    job.establishment_number  = _get(soup, "ID_jgshNo")
    job.company_name_kana     = _get(soup, "ID_jgshMeiKana")
    job.company_name          = _get(soup, "ID_jgshMei")
    job.company_zip           = _get(soup, "ID_szciYbn")
    job.company_address       = _get(soup, "ID_szci")
    job.corporate_number      = _get(soup, "ID_hoNinNo")
    job.representative_title  = _get(soup, "ID_yshk")
    job.representative_name   = _get(soup, "ID_dhshaMei")
    job.established_year      = _get(soup, "ID_setsuritsuNen")
    job.capital               = _get(soup, "ID_shkn")
    job.employees_total       = _get(soup, "ID_jgisKigyoZentai")
    job.employees_workplace   = _get(soup, "ID_jgisShgBs")
    job.employees_female      = _get(soup, "ID_jgisUchiJosei")
    job.employees_part        = _get(soup, "ID_jgisUchiPart")
    job.labor_union           = _get(soup, "ID_rodoKumiai")
    job.business_description  = _get(soup, "ID_jigyoNy")
    job.company_feature       = _get(soup, "ID_kaishaNoTokucho")
    job.company_notes         = _get(soup, "ID_jgshTkjk")
    job.work_regulations_full = _get(soup, "ID_fltmShgKisoku")
    job.work_regulations_part = _get(soup, "ID_partShgKisoku")
    job.corporate_pension     = _get(soup, "ID_kigyoNenkin")
    job.welfare_detail        = _get(soup, "ID_fukuriKoseiNoNy")

    # 仕事内容
    job.job_title                     = _get(soup, "ID_sksu")
    job.job_description               = _get(soup, "ID_shigotoNy")
    job.employment_type               = _get(soup, "ID_koyoKeitai")
    job.employment_type_other         = _get(soup, "ID_koyoKeitaiSsinIgaiNoMeisho")
    job.contract_period               = _get(soup, "ID_koyoKikan")
    job.contract_period_count         = _get(soup, "ID_koyoKikanSu")
    job.contract_period_end           = _get(soup, "ID_koyoKikanYMD")
    job.contract_renewal              = _get(soup, "ID_koyoKikanKeiyakuKsnNoKnsi")
    job.contract_renewal_condition    = _get(soup, "ID_koyoKikanKeiyakuKsnNoJkn")
    job.regular_conversion_record     = _get(soup, "ID_koyoKeitaiSsinNoUmu")
    job.regular_conversion_count      = _get(soup, "ID_koyoKeitaiSsinJisseki")
    job.foreign_worker_record         = _get(soup, "ID_gkjnKoyoJisseki")
    job.workplace_access              = _get(soup, "ID_shgBs")
    job.workplace_access_note         = _get(soup, "ID_shgBsTkjk")
    job.workplace_zip                 = _get(soup, "ID_shgBsYubinNo")
    job.workplace_address             = _get(soup, "ID_shgBsJusho")
    job.nearest_station               = _get(soup, "ID_shgBsMyorEki")
    job.commute_transport             = _get(soup, "ID_shgBsKotsuShudan")
    job.commute_time                  = _get(soup, "ID_shgBsShyoJn")
    job.smoking_policy                = _get(soup, "ID_shgBsKitsuTsak")
    job.smoking_policy_note           = _get(soup, "ID_shgBsKitsuTsakTkjk")
    job.transfer                      = _get(soup, "ID_tenkinNoKnsi")
    job.transfer_area                 = _get(soup, "ID_tenkinNoKnsiTenkinHanni")
    job.uij_turn                      = _get(soup, "ID_uIJTurn")
    job.mycar_commute                 = _get(soup, "ID_mycarTskn")
    job.parking_available             = _get(soup, "ID_mycarTsknChushaUmu")
    job.age_note                      = _get(soup, "ID_nenrei")
    job.age_restriction               = _get(soup, "ID_nenreiSegn")
    job.age_restriction_range         = _get(soup, "ID_nenreiSegnHanni")
    job.age_restriction_reason_type   = _get(soup, "ID_nenreiSegnGaitoJiyu")
    job.age_restriction_reason        = _get(soup, "ID_nenreiSegnNoRy")
    job.education                     = _get(soup, "ID_grki")
    job.education_detail              = _get(soup, "ID_grkiIjo")
    job.education_subject             = _get(soup, "ID_grkiSnkNi")
    job.experience_required           = _get(soup, "ID_hynaKiknt")
    job.experience_detail             = _get(soup, "ID_hynaKikntShsi")
    job.pc_skills                     = _get(soup, "ID_hynaPc")
    job.license_necessity             = _get(soup, "ID_hynaMenkyoSkku")
    job.license_required              = _get(soup, "ID_FutsuMenkyo")
    job.license_other_necessity       = _get(soup, "ID_MenkyoSkkuSel")
    job.license_other_name            = _get(soup, "ID_MenkyoSkkuMeisho")
    job.license_additional            = _get(soup, "ID_sNtaMenkyoSkku")
    job.trial_period                  = _get(soup, "ID_trialKikan")
    job.trial_period_duration         = _get(soup, "ID_trialKikanKikan")
    job.trial_period_condition        = _get(soup, "ID_trialKikanChuuNoRodoJkn")
    job.trial_period_condition_detail = _get(soup, "ID_trialKikanChuuNoRodoJknNoNy")
    job.dispatch_flag                 = _get(soup, "ID_hakenUkeoiToShgKeitai")
    job.dispatch_number               = _get(soup, "ID_hakenUkeoiToRdsha")
    job.special_work_regulations      = _get(soup, "ID_tkbsNaJijo")
    job.hiring_reason                 = _get(soup, "ID_boshuRy")
    job.hiring_reason_other           = _get(soup, "ID_sntaNoBoshuRy")
    job.workplace_count               = _get(soup, "ID_shtnKssu")
    job.workplace_1                   = _get(soup, "ID_shtn1")
    job.workplace_2                   = _get(soup, "ID_shtn2")
    job.workplace_3                   = _get(soup, "ID_shtn3")

    # 賃金・手当
    job.wage                    = _get(soup, "ID_chgn")
    job.base_salary             = _get(soup, "ID_khky")
    job.wage_type               = _get(soup, "ID_chgnKeitaiToKbn")
    job.wage_type_other         = _get(soup, "ID_chgnKeitaiTo")
    job.wage_type_note          = _get(soup, "ID_chgnKeitaiToSntaNy")
    job.fixed_allowance         = _get(soup, "ID_tgktNiShwrTat")
    job.fixed_allowance_rate    = _get(soup, "ID_sokkgSkrt")
    job.fixed_overtime_flag     = _get(soup, "ID_koteiZngyKbn")
    job.fixed_overtime_amount   = _get(soup, "ID_koteiZngy")
    job.fixed_overtime_detail   = _get(soup, "ID_koteiZngyTkjk")
    job.other_allowances        = _get(soup, "ID_sntaTatFukiJk")
    job.commute_allowance       = _get(soup, "ID_tsknTat")
    job.commute_allowance_unit  = _get(soup, "ID_tsknTatTsuki")
    job.commute_allowance_amount= _get(soup, "ID_tsknTatKingaku")
    job.wage_closing_day        = _get(soup, "ID_chgnSkbi")
    job.wage_closing_day_other  = _get(soup, "ID_chgnSkbiMitk")
    job.wage_payment_type       = _get(soup, "ID_chgnSrbi")
    job.wage_payment_month      = _get(soup, "ID_chgnSrbiTsuki")
    job.wage_payment_day        = _get(soup, "ID_chgnSrbiHi")
    job.pay_raise               = _get(soup, "ID_shokyuSd")
    job.pay_raise_record        = _get(soup, "ID_shokyuMaeNendoJisseki")
    job.bonus                   = _get(soup, "ID_shoyoSdNoUmu")
    job.bonus_record            = _get(soup, "ID_shoyoMaeNendoUmu")
    job.bonus_times             = _get(soup, "ID_shoyoMaeNendKaisu")
    job.bonus_amount            = _get(soup, "ID_shoyoKingaku")

    # 労働時間
    job.work_hours                  = _get(soup, "ID_shgJn1")
    job.work_hours_shift            = _get(soup, "ID_shgJn")
    job.work_hours_2                = _get(soup, "ID_shgJn2")
    job.work_hours_3                = _get(soup, "ID_shgJn3")
    job.work_hours_or               = _get(soup, "ID_shgJnOr")
    job.work_hours_note             = _get(soup, "ID_shgJiknTkjk")
    job.flexible_work_unit          = _get(soup, "ID_henkeiRdTani")
    job.weekly_work_days            = _get(soup, "ID_shuRdNisu")
    job.weekly_work_days_negotiable = _get(soup, "ID_shuRdNisuSodanKa")
    job.overtime_flag               = _get(soup, "ID_jkgiRodoJn")
    job.overtime_hours_monthly      = _get(soup, "ID_thkinJkgiRodoJn")
    job.avg_work_days_monthly       = _get(soup, "ID_thkinRodoNissu")
    job.article36_agreement         = _get(soup, "ID_sanrokuKyotei")
    job.break_time                  = _get(soup, "ID_kyukeiJn")
    job.break_room                  = _get(soup, "ID_kyukeiShitsu")
    job.weekly_holiday              = _get(soup, "ID_shukFtskSei")
    job.annual_holidays             = _get(soup, "ID_nenkanKjsu")
    job.holiday_type                = _get(soup, "ID_kyjs")
    job.holiday_detail              = _get(soup, "ID_kyjsSnta")
    job.paid_leave_days             = _get(soup, "ID_nenjiYukyu")

    # その他の労働条件
    job.social_insurance          = _get(soup, "ID_knyHoken")
    job.retirement_fund_union     = _get(soup, "ID_tskinKsi")
    job.retirement_fund_system    = _get(soup, "ID_tskinSd")
    job.retirement_fund_years     = _get(soup, "ID_tskinSdKinzokuNensu")
    job.retirement_age_system     = _get(soup, "ID_tnsei")
    job.retirement_age            = _get(soup, "ID_tnseiTeinenNenrei")
    job.rehire_system             = _get(soup, "ID_saiKoyoSd")
    job.rehire_age_limit          = _get(soup, "ID_saiKoyoSdJgnNenrei")
    job.work_extension_system     = _get(soup, "ID_kmec")
    job.work_extension_age_limit  = _get(soup, "ID_kmecJgnNenrei")
    job.housing                   = _get(soup, "ID_nkj")
    job.housing_note              = _get(soup, "ID_nkjTkjk")
    job.childcare_facility        = _get(soup, "ID_riyoKanoTkjShst")
    job.childcare_leave_record    = _get(soup, "ID_ikujiKyugyoStkJisseki")
    job.nursing_care_leave_record = _get(soup, "ID_kaigoKyugyoStkJisseki")
    job.nursing_leave_record      = _get(soup, "ID_kangoKyukaStkJisseki")
    job.performance_pay_system    = _get(soup, "ID_shokumuKyuSd")
    job.performance_pay_detail    = _get(soup, "ID_shokumuKyuSdNoNy")
    job.rehire_other_system       = _get(soup, "ID_fukushokuSd")
    job.rehire_other_detail       = _get(soup, "ID_fukushokuSdNoNy")
    job.training_system           = _get(soup, "ID_knsSdNy")
    job.training_for_non_regular  = _get(soup, "ID_knsSdNoSsinIgaiNoRiyo")

    # 選考情報
    job.hiring_count                  = _get(soup, "ID_saiyoNinsu")
    job.selection_method              = _get(soup, "ID_selectHoho")
    job.document_screening_result     = _get(soup, "ID_shoruiSelectKekka")
    job.interview_result_timing       = _get(soup, "ID_mensetsuSelectKekka")
    job.result_notification_timing    = _get(soup, "ID_selectKekkaTsuch")
    job.result_notification_method    = _get(soup, "ID_ksshEnoTsuchiHoho")
    job.selection_schedule            = _get(soup, "ID_selectNichijiTo")
    job.selection_schedule_note       = _get(soup, "ID_sntaNoSelectNichijiTo")
    job.selection_venue_zip           = _get(soup, "ID_selectBsYubinNo")
    job.selection_venue_address       = _get(soup, "ID_selectBsJusho")
    job.selection_venue_station       = _get(soup, "ID_selectBsMyorEki")
    job.selection_venue_transport     = _get(soup, "ID_selectBsMyorEkiKotsuShudan")
    job.selection_venue_time          = _get(soup, "ID_selectBsShyoJn")
    job.selection_notes               = _get(soup, "ID_selectTkjk")
    job.application_documents         = _get(soup, "ID_oboShoruitou")
    job.application_submission_method = _get(soup, "ID_oboShoruiNoSofuHoho")
    job.application_doc_return        = _get(soup, "ID_obohen")
    job.other_doc_submission_method   = _get(soup, "ID_sntaNoSofuHoho")
    job.other_required_docs           = _get(soup, "ID_sntaNoOboShorui")
    job.postal_submission_zip         = _get(soup, "ID_yusoNoSofuBsYubinNo")
    job.postal_submission_address     = _get(soup, "ID_yusoNoSofuBsJusho")
    job.contact_department            = _get(soup, "ID_ttsYkm")
    job.contact_person                = _get(soup, "ID_ttsTts")
    job.contact_person_kana           = _get(soup, "ID_ttsTtsKana")
    job.contact_phone                 = _get(soup, "ID_ttsTel")
    job.contact_fax                   = _get(soup, "ID_ttsFax")
    job.contact_extension             = _get(soup, "ID_ttsNaisen")
    job.contact_email                 = _get(soup, "ID_ttsEmail")

    # 特記事項
    job.special_notes = _get(soup, "ID_kjTkjk")
    job.company_pr    = _get(soup, "ID_jgshKaraNoMsg")

    # 追加：事業所情報
    job.company_url       = _get_attr(soup, "ID_hp", "href")
    job.inquiry_contact1  = _get(soup, "ID_kksh1")
    job.inquiry_contact2  = _get(soup, "ID_kksh2")
    job.inquiry_contact3  = _get(soup, "ID_kksh3")

    # 追加：仕事内容
    job.workplace_current_location = _get(soup, "ID_shgBsZaiKinmu")
    job.workplace_condition_note   = _get(soup, "ID_tkjShstTkjk")
    job.job_type_url               = _get_attr(soup, "ID_shokusyuKaisetsuURL", "href")
    job.license_other_input        = _get(soup, "ID_MenkyoSkkuNyuryoku")
    job.special_condition1         = _get(soup, "ID_stsk1")
    job.special_condition2         = _get(soup, "ID_stsk2")
    job.special_condition3         = _get(soup, "ID_stsk3")
    job.recruitment_support        = _get(soup, "ID_rrtShienNy")
    job.job_change_form            = _get(soup, "ID_kigyoZiskKataJobUmu")

    # 追加：賃金・手当
    job.wage_closing_day_extra = _get(soup, "ID_chgnSkbiSntaNoSkbi")
    job.wage_payment_extra     = _get(soup, "ID_chgnSrbiSnta")
    job.annual_income1         = _get(soup, "ID_nensho1")
    job.annual_income1_years   = _get(soup, "ID_nensho1Nen")
    job.annual_income2         = _get(soup, "ID_nensho2")
    job.annual_income2_years   = _get(soup, "ID_nensho2Nen")
    job.annual_income3         = _get(soup, "ID_nensho3")
    job.annual_income3_years   = _get(soup, "ID_nensho3Nen")

    # 追加：その他の労働条件
    job.social_insurance_extra = _get(soup, "ID_sgshaShstSetsubiTo")
    job.barrier_free           = _get(soup, "ID_barrierFree")
    job.elevator               = _get(soup, "ID_elevator")
    job.handrail_stairs        = _get(soup, "ID_kaidanTesuri")
    job.wheelchair_access      = _get(soup, "ID_tatemonoKrmIsuIdo")
    job.display_facility       = _get(soup, "ID_tenjiSetsubi")
    job.handrail_installation  = _get(soup, "ID_tesuriSechi")

    # 追加：画像・座標
    job.pr_logo1    = _get_attr(soup, "ID_prLogo1", "src")
    job.pr_logo2    = _get_attr(soup, "ID_prLogo2", "src")
    job.pr_logo3    = _get_attr(soup, "ID_prLogo3", "src")
    job.latitude1   = _get_attr(soup, "ID_latitudemap1", "value")
    job.longitude1  = _get_attr(soup, "ID_longitudemap1", "value")
    job.latitude2   = _get_attr(soup, "ID_latitudemap2", "value")
    job.longitude2  = _get_attr(soup, "ID_longitudemap2", "value")

    return job


# ---------------------------------------------------------------------------
# バッチ処理
# ---------------------------------------------------------------------------

def parse_directory(html_dir: str | Path) -> list[HellworkJob]:
    """ディレクトリ内のHTMLファイルを全件パースしてリストで返す。"""
    html_dir = Path(html_dir)
    files = sorted(html_dir.glob("*.html"))
    logger.info("パース開始: %d件 (%s)", len(files), html_dir)

    jobs = []
    errors = 0
    for i, f in enumerate(files, 1):
        try:
            jobs.append(parse_html(f))
        except Exception as e:
            logger.warning("[%d/%d] パースエラー (%s): %s", i, len(files), f.name, e)
            errors += 1

    logger.info("パース完了: 成功=%d, エラー=%d", len(jobs), errors)
    return jobs


def parse_to_parquet(
    html_dir: str | Path,
    staging_dir: str | Path,
) -> Path:
    """HTMLディレクトリをパースしてParquetファイルに出力する。

    ファイル名は html_dir の末尾ディレクトリ名（YYYYMMDD）から決定する。
    例: html_dir=C:/Temp/html/20260410 → staging_dir/hellowork_20260410.parquet

    Args:
        html_dir:    HTMLファイルが格納されたディレクトリ（末尾がYYYYMMDD）
        staging_dir: Parquet出力先ディレクトリ

    Returns:
        出力したParquetファイルのPath
    """
    html_dir = Path(html_dir)
    staging_dir = Path(staging_dir)
    staging_dir.mkdir(parents=True, exist_ok=True)

    date_str = html_dir.name  # 例: "20260410"
    out_path = staging_dir / f"hellowork_{date_str}.parquet"

    jobs = parse_directory(html_dir)
    if not jobs:
        logger.warning("パース結果が0件です: %s", html_dir)
        return out_path

    df = pd.DataFrame([asdict(j) for j in jobs])
    df["fetched_date"] = date_str
    df.to_parquet(out_path, index=False, engine="pyarrow")

    logger.info("Parquet書き込み完了: %s (%d件)", out_path.name, len(df))
    return out_path


# ---------------------------------------------------------------------------
# エントリーポイント（動作確認用）
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    import datetime
    import io
    import sys

    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="ハローワーク HTML → Parquet変換")
    parser.add_argument(
        "--date",
        type=lambda s: datetime.datetime.strptime(s, "%Y-%m-%d").date(),
        default=datetime.date.today(),
        help="処理対象の日付（YYYY-MM-DD形式、デフォルト: 当日）",
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="data/sample ディレクトリでカバレッジ確認のみ実行（Parquet出力なし）",
    )
    args = parser.parse_args()

    if args.sample:
        # 動作確認モード：data/sample を対象にカバレッジを表示
        target_dir = Path(__file__).resolve().parents[2] / "data" / "sample"
        jobs = parse_directory(target_dir)
        total = len(jobs)
        coverage = {}
        for job in jobs:
            for k, v in asdict(job).items():
                if k == "source_file":
                    continue
                coverage[k] = coverage.get(k, 0) + (1 if v is not None else 0)
        print(f"\n=== フィールドカバレッジ（{total}件中） ===")
        for field_name, count in coverage.items():
            bar = "#" * (count * 20 // max(total, 1))
            print(f"  {field_name:40s} {count:3d}/{total}  {bar}")
    else:
        # 通常モード：config の html_dir/{YYYYMMDD} を読んで staging_dir に Parquet 出力
        date_str = args.date.strftime("%Y%m%d")
        html_dir = Path(_cfg["html_dir"]) / date_str
        staging_dir = Path(__file__).resolve().parents[2] / _cfg["staging_dir"]
        parse_to_parquet(html_dir, staging_dir)
