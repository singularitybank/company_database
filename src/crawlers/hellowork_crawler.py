# -*- coding: utf-8 -*-
"""
ハローワークインターネットサービス 求人情報クローラー

対象サイト: https://www.hellowork.mhlw.go.jp/
ブラウザ:  Microsoft Edge (Selenium 4.x)

[動作概要]
  1. 求人検索画面を開く
  2. キーワード・都道府県・雇用形態を指定して検索
  3. 検索結果を一覧から求人番号のみ取得 → 求人番号リストをCSVに保存
  4. 求人番号リストを用いて詳細ページの順にクロール
  5. 求人情報をcsvに保存

[利用規約]
  ハローワークインターネットサービスの利用規約に従い、
  アクセス間隔を設けて過度な負荷をかけないよう制御しています。
"""
import os
import pandas as pd
import datetime
import logging
import time
import random
from dataclasses import dataclass, field, asdict
from typing import Optional
from pathlib import Path
import json
import glob
import yaml

from selenium import webdriver
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
    ElementClickInterceptedException,
)
from selenium.webdriver.common.by import By
from selenium.webdriver.edge.options import Options
from selenium.webdriver.edge.service import Service
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 設定読み込み
# ---------------------------------------------------------------------------

def _load_config() -> dict:
    """config/config.yaml の hellowork セクションを返す。"""
    config_path = Path(__file__).resolve().parents[2] / "config" / "config.yaml"
    with open(config_path, encoding="utf-8") as f:
        return yaml.safe_load(f)["hellowork"]

_cfg = _load_config()

# ---------------------------------------------------------------------------
# 定数
# ---------------------------------------------------------------------------

TEMP_CSV_DIR    = _cfg["temp_dir"]
OUTPUT_CSV_DIR  = _cfg["output_dir"]
BASE_URL = "https://www.hellowork.mhlw.go.jp/kensaku/GECA110010.do?action=initDisp&screenId=GECA110010"
PAGE_URL = "https://www.hellowork.mhlw.go.jp/kensaku/GECA110010.do?screenId=GECA110010&action=dispDetailBtn&kJNo={{job_number}}&kJKbn={{kyujintype}}"

# アクセス間隔の基準値（秒）- 実際の待機時にランダム幅を加算する
WAIT_BETWEEN_PAGES   = _cfg["wait_between_pages"]
WAIT_BETWEEN_DETAILS = _cfg["wait_between_details"]
DEFAULT_TIMEOUT      = _cfg["timeout"]

# MAPPING
MAPPING_KIND = {
    "一般求人": 1,
    "新卒・既卒求人": 2,
    "季節求人": 3,
    "出稼ぎ求人": 4,
    "障害のある方のための求人": 5
}

MAPPING_DISTRICT = {
    "北海道・東北": ["北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県"],
    "関東": ["茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県"],
    "甲信越・北陸": ["新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県"],
    "東海": ["岐阜県", "静岡県", "愛知県", "三重県"],
    "近畿": ["滋賀県", "京都府", "大阪府", "兵庫県", "奈良県", "和歌山県"],
    "中国": ["鳥取県", "島根県", "岡山県", "広島県", "山口県"],
    "四国": ["徳島県", "香川県", "愛媛県", "高知県"],
    "九州・沖縄": ["福岡県", "佐賀県", "長崎県", "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県"]
}

MAPPING_PREFECTURE = {
    "北海道": 1, "青森県": 2, "岩手県": 3, "宮城県": 4, "秋田県": 5, "山形県": 6, "福島県": 7,
    "茨城県": 8, "栃木県": 9, "群馬県": 10, "埼玉県": 11, "千葉県": 12, "東京都": 13, "神奈川県": 14,
    "新潟県": 15, "富山県": 16, "石川県": 17, "福井県": 18, "山梨県": 19, "長野県": 20,
    "岐阜県": 21, "静岡県": 22, "愛知県": 23, "三重県": 24,
    "滋賀県": 25, "京都府": 26, "大阪府": 27, "兵庫県": 28, "奈良県": 29, "和歌山県": 30,
    "鳥取県": 31, "島根県": 32, "岡山県": 33, "広島県": 34, "山口県": 35,
    "徳島県": 36, "香川県": 37, "愛媛県": 38, "高知県": 39,
    "福岡県": 40, "佐賀県": 41, "長崎県": 42, "熊本県": 43, "大分県": 44, "宮崎県": 45, "鹿児島県": 46, "沖縄県": 47
}
# ---------------------------------------------------------------------------
# ドライバー初期化
# ---------------------------------------------------------------------------

def build_driver(headless: bool = _cfg["headless"]) -> webdriver.Edge:
    """Edgeドライバーを初期化して返す。

    Args:
        headless: Trueにするとブラウザウィンドウを表示しない

    Returns:
        初期化済みの Edge WebDriver インスタンス
    """
    options = Options()
    if headless:
        options.add_argument("--headless")

    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-software-rasterizer")  # GPU overlay エラー抑制
    options.add_argument("--log-level=3")                   # ERROR以上のみ（Chromiumログ抑制）
    options.add_argument("--window-size=1280,900")
    options.add_argument("--lang=ja")
    # ボット検出対策: 自動化フラグを軽減
    options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
    options.add_experimental_option("useAutomationExtension", False)

    # Selenium 4.6+ の selenium-manager が msedgedriver を自動管理
    # service_log_path=os.devnull でドライバー自身のログも抑制
    service = Service(log_output=os.devnull)

    driver = webdriver.Edge(service=service, options=options)
    driver.implicitly_wait(5)
    logger.info("Edge ドライバー起動完了 (headless=%s)", headless)
    return driver


# ---------------------------------------------------------------------------
# ページ操作ヘルパー
# ---------------------------------------------------------------------------

def _wait_for(driver: webdriver.Edge, by: str, value: str, timeout: int = DEFAULT_TIMEOUT):
    """指定要素が表示されるまで待機して返す。"""
    return WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((by, value))
    )


def _safe_text(driver: webdriver.Edge, by: str, value: str, default: str = "") -> str:
    """要素のテキストを安全に取得する。見つからない場合は default を返す。"""
    try:
        return driver.find_element(by, value).text.strip()
    except NoSuchElementException:
        return default


# ---------------------------------------------------------------------------
# データクラス
# ---------------------------------------------------------------------------

@dataclass
class JobListing:
    """求人情報1件分"""
    job_number: str = ""           # 求人番号
    company_name: str = ""         # 会社名
    job_title: str = ""            # 職種
    employment_type: str = ""      # 雇用形態（正社員・パートなど）
    prefecture: str = ""           # 就業場所（都道府県）
    city: str = ""                 # 就業場所（市区町村）
    wage: str = ""                 # 賃金
    work_hours: str = ""           # 就業時間
    holiday: str = ""              # 休日
    expiry_date: str = ""          # 求人有効期限
    detail_url: str = ""           # 詳細ページURL
    raw_address: str = ""          # 住所（生テキスト）


# ---------------------------------------------------------------------------
# 検索・クロール
# ---------------------------------------------------------------------------

def filter_kyujintype(driver: webdriver.Edge, kyujintype_code: int):
    # 求人種別選択
    try:
        _wait_for(driver, By.ID, "ID_LkjKbnRadioBtn{:d}".format(kyujintype_code))
        kjKbn_btn = driver.find_element(By.ID, "ID_LkjKbnRadioBtn{:d}".format(kyujintype_code))
        kjKbn_btn.click()
        time.sleep(1)
        return True
    except NoSuchElementException:
        logger.warning("求人種別ボタンが見つかりません")
        return False

def filter_prefecture(driver: webdriver.Edge, district_no: int, prefecture_code: int):
    # 都道府県選択popupを開いて、指定の都道府県にチェックを入れる
    try:
        # 都道府県選択popup
        _wait_for(driver, By.ID, "ID_todohukenHiddenAccoBtn")
        todohuken_btn = driver.find_element(By.ID, "ID_todohukenHiddenAccoBtn")
        todohuken_btn.click()
        # エリア選択
        _wait_for(driver, By.CLASS_NAME, "prefecture")
        todohuken_modal = driver.find_elements(By.CLASS_NAME, "prefecture")
        district_btn = todohuken_modal[district_no].find_element(By.TAG_NAME, "li")
        district_btn.click()
        # 都道府県選択
        _wait_for(driver, By.NAME, "skCheck{:02d}".format(prefecture_code))
        todohuken_checkbox = district_btn.find_element(By.NAME, "skCheck{:02d}".format(prefecture_code))
        todohuken_checkbox.click()
        # 確定
        driver.find_element(By.ID, "ID_ok4").click()
        time.sleep(1)
        return True
    except NoSuchElementException:
        logger.warning("都道府県選択の要素が見つかりません")
        return False

def scrape_jobnumber(driver: webdriver.Edge, target_date: datetime.date, kyujintype: int) -> list[dict]:
    """一覧ページから対象日付の求人番号を収集する。

    Returns:
        {"job_number": str, "kyujintype": int} のリスト
    """
    l_jobnumber = []
    page_no = 1
    loop = True
    while loop:
        _wait_for(driver, By.CLASS_NAME, "samari_tyousei")
        result = driver.find_element(By.CLASS_NAME, "samari_tyousei")
        tables = result.find_elements(By.CLASS_NAME, "kyujin")
        for table in tables:
            job_date = table.find_element(By.CLASS_NAME, "kyujin_head").find_element(By.XPATH, "td/div/div[2]/div[1]/div[1]/div[1]/div[2]").text # '：2026年4月10日'
            job_date = job_date.replace("：", "").strip() # '2026年4月10日'
            job_date = datetime.datetime.strptime(job_date, "%Y年%m月%d日").date() # datetime.date(2026, 4, 10)
            # 求人情報は新しい順に並んでいるため、求人日付が対象日より古い場合はループを抜ける
            if job_date < target_date:
                loop = False
                break
            elif job_date > target_date:
                continue
            job_number = table.find_element(By.CLASS_NAME, "kyujin_body").find_element(By.XPATH, "td/div/div[2]/table/tbody/tr[4]/td[2]/div/div").text
            l_jobnumber.append({"job_number": job_number, "kyujintype": kyujintype})
        logger.info(f"page {page_no} done, {len(l_jobnumber)} job numbers found")
        if not loop:
            break
        # 次のページへ
        _wait_for(driver, By.NAME, "fwListNaviBtnNext")
        next_btn = driver.find_element(By.NAME, "fwListNaviBtnNext")
        try:
            next_btn.click()
            time.sleep(WAIT_BETWEEN_PAGES + random.uniform(0, 1))
            page_no += 1
        except:
            break
    return l_jobnumber

def search(
    driver: webdriver.Edge,
    kyujintype: str,
    district_no: int,
    prefecture_code: int,
    target_date: datetime.date,
) -> list:
    """求人検索を実行する。

    Args:
        driver:          Edge WebDriver
        kyujintype:      求人種別コード（例: 1=一般求人）
        district_no:     エリア区分番号（例: 0=北海道・東北、1=関東、2=甲信越・北陸、3=東海、4=近畿、5=中国、6=四国、7=九州・沖縄）
        prefecture_code: 都道府県コード（例: 13=東京都）
        target_date:     取得対象の日付

    Returns:
        検索結果ページへの遷移に成功したら True
    """
    if kyujintype == 1:
        logger.info("検索開始: type=%r, district=%r, prefecture=%r", kyujintype, district_no, prefecture_code)
    driver.get(BASE_URL)

    # 求人種別選択
    if not filter_kyujintype(driver, kyujintype):
        return False

    # 都道府県選択
    if kyujintype == 1: # 一般求人のみ都道府県で絞り込み
        if not filter_prefecture(driver, district_no, prefecture_code):
            return False

    # 検索
    try:
        search_btn = WebDriverWait(driver, DEFAULT_TIMEOUT).until(
            EC.element_to_be_clickable((By.ID, "ID_searchBtn"))
        )
        search_btn.click()
        time.sleep(WAIT_BETWEEN_PAGES)
    except (NoSuchElementException, ElementClickInterceptedException, TimeoutException):
        logger.error("検索ボタンが見つかりません")
        return False

    # 表示件数を50件に変更
    try:
        _wait_for(driver, By.ID, "ID_fwListNaviDispTop")
        select_display = driver.find_element(By.ID, "ID_fwListNaviDispTop")
        select = Select(select_display)
        select.select_by_visible_text("50件")
    except NoSuchElementException:
        logger.warning("表示件数セレクトボックスが見つかりません")
        return False
    
    # 求人番号を抽出（対象日付以外は除外）
    l_jobnumbers = scrape_jobnumber(driver, target_date, kyujintype)

    # 一時的にCSVとして保存（job_number + kyujintype）
    if l_jobnumbers:
        df = pd.DataFrame(l_jobnumbers)
        if kyujintype == 1:
            df.to_csv(os.path.join(TEMP_CSV_DIR, "jobnumber_{}_{}_{:02d}.csv".format(kyujintype, district_no, prefecture_code)), index=False)
        else:
            df.to_csv(os.path.join(TEMP_CSV_DIR, "jobnumber_{}.csv".format(kyujintype)), index=False)
    logger.info("検索実行完了: %d 件の求人番号を取得", len(l_jobnumbers))
    return l_jobnumbers


def _parse_list_page(driver: webdriver.Edge) -> list[dict]:
    """現在の検索結果一覧ページから求人情報（概要）を抽出する。"""
    items = []
    # 求人カードのセレクタ（サイト構造に応じて調整が必要な場合あり）
    cards = driver.find_elements(By.CSS_SELECTOR, "div.result-list-item, tr.kyujin-row, .job-item")

    for card in cards:
        item = {}
        # 求人番号
        try:
            item["job_number"] = card.find_element(
                By.CSS_SELECTOR, ".job-number, [class*='jobno'], td.kyujin-no"
            ).text.strip()
        except NoSuchElementException:
            item["job_number"] = ""

        # 会社名
        try:
            item["company_name"] = card.find_element(
                By.CSS_SELECTOR, ".company-name, [class*='company'], td.kaishamei"
            ).text.strip()
        except NoSuchElementException:
            item["company_name"] = ""

        # 詳細ページへのリンク
        try:
            link = card.find_element(By.CSS_SELECTOR, "a[href*='zaiken']")
            item["detail_url"] = link.get_attribute("href")
        except NoSuchElementException:
            item["detail_url"] = ""

        if item.get("job_number") or item.get("company_name"):
            items.append(item)

    logger.info("  一覧ページ抽出: %d件", len(items))
    return items


def _parse_detail_page(driver: webdriver.Edge) -> JobListing:
    """現在の詳細ページから求人情報を抽出する。"""
    job = JobListing()
    job.detail_url = driver.current_url

    # 求人票の各項目を <th>ラベル : <td>値 の形式から取得する汎用関数
    def get_field(label: str) -> str:
        try:
            ths = driver.find_elements(By.TAG_NAME, "th")
            for th in ths:
                if label in th.text:
                    td = th.find_element(By.XPATH, "following-sibling::td[1]")
                    return td.text.strip()
        except NoSuchElementException:
            pass
        return ""

    job.job_number    = get_field("求人番号")
    job.company_name  = get_field("会社名") or get_field("事業所名")
    job.job_title     = get_field("職種")   or get_field("求人職種")
    job.employment_type = get_field("雇用形態")
    job.wage          = get_field("賃金")   or get_field("基本給")
    job.work_hours    = get_field("就業時間")
    job.holiday       = get_field("休日")   or get_field("休暇")
    job.expiry_date   = get_field("求人有効期限") or get_field("有効期限")
    job.raw_address   = get_field("就業場所") or get_field("勤務地")

    logger.debug("詳細取得: %s / %s", job.job_number, job.company_name)
    return job

# ---------------------------------------------------------------------------
# メインクロール処理
# ---------------------------------------------------------------------------

def crawl(
    driver: webdriver.Edge,
    target_date: datetime.date,
) -> pd.DataFrame:
    """検索〜ページング〜（詳細取得）を一括で行いJobListingのリストを返す。

    Args:
        driver:      Edge WebDriver
        target_date: 取得対象の日付（デフォルトは当日）

    Returns:
        求人番号のDataFrame
    """
    today = target_date.strftime("%Y%m%d")  # YYYYMMDDに変換
    l_kind = list(MAPPING_KIND.keys())
    l_district = list(MAPPING_DISTRICT.keys())

    # 求人番号のリストを蓄積（job_number + kyujintype のdictリスト）
    l_jobnumbers = []
    for i, kind in zip(range(1, len(l_kind) + 1), l_kind):
        logger.info("開始 - 求人種別: %s", kind)
        if kind == "一般求人":
            for j, district in zip(range(0, len(l_district)), l_district):
                l_prefecture = MAPPING_DISTRICT[district]
                for prefecture in l_prefecture:
                    k = MAPPING_PREFECTURE[prefecture]
                    if os.path.exists(os.path.join(TEMP_CSV_DIR, "jobnumber_{}_{}_{:02d}.csv".format(i, j, k))):
                        logger.info("スキップ - 既にCSVが存在: %s, %s, %s", kind, district, prefecture)
                        try:
                            df = pd.read_csv(os.path.join(TEMP_CSV_DIR, "jobnumber_{}_{}_{:02d}.csv".format(i, j, k)))
                            l_jobnumbers.extend(df.to_dict("records"))
                        except pd.errors.EmptyDataError:
                            logger.info("CSVが空（検索結果ゼロ）: %s, %s, %s", kind, district, prefecture)
                        continue
                    logger.info("開始 - 地域: %s, 都道府県: %s", district, prefecture)
                    l_jobnumber = search(driver, kyujintype=i, district_no=j, prefecture_code=k, target_date=target_date)
                    l_jobnumbers.extend(l_jobnumber)
        else:
            if os.path.exists(os.path.join(TEMP_CSV_DIR, "jobnumber_{}.csv".format(i))):
                logger.info("スキップ - 既にCSVが存在: %s", kind)
                try:
                    df = pd.read_csv(os.path.join(TEMP_CSV_DIR, "jobnumber_{}.csv".format(i)))
                    l_jobnumbers.extend(df.to_dict("records"))
                except pd.errors.EmptyDataError:
                    logger.info("CSVが空（検索結果ゼロ）: %s", kind)
                continue
            # 一般求人以外は都道府県絞り込みなし（district_no/prefecture_code は未使用）
            l_jobnumber = search(driver, kyujintype=i, district_no=0, prefecture_code=0, target_date=target_date)
            l_jobnumbers.extend(l_jobnumber)
    # 重複を除いて求人番号のリストを作成&CSVに保存（job_number + kyujintype）
    df = pd.DataFrame(l_jobnumbers).drop_duplicates(subset="job_number").reset_index(drop=True)
    df.to_csv(f"{OUTPUT_CSV_DIR}/jobnumbers_{today}.csv", index=False)
    logger.info("求人番号の収集完了: %d 件の求人番号を取得（重複除外後）", len(df))

    # 一時的に保存したCSVを削除
    l_temp_csv_files = glob.glob(os.path.join(TEMP_CSV_DIR, "jobnumber_*.csv"))
    for temp_csv in l_temp_csv_files:
        os.remove(temp_csv)
    logger.info("一時CSVファイルを削除: %d 件", len(l_temp_csv_files))

    logger.info("クロール完了: 合計 %d 件取得", len(l_jobnumbers))
    return df

def scrape_details(
    driver: webdriver.Edge,
    df: pd.DataFrame,
    target_date: datetime.date,
) -> None:
    """求人番号リストをもとに詳細ページのHTMLをローカルに保存する。

    保存先:
        {OUTPUT_CSV_DIR}/html/{YYYYMMDD}/{job_number}.html

    再開対応:
        既に同名HTMLが存在する求人はスキップするため、
        途中で中断しても続きから再開できる。

    Args:
        driver:      Edge WebDriver
        df:          crawl() が返す DataFrame（job_number・kyujintype 列を持つ）
        target_date: 取得対象の日付（保存ディレクトリ名に使用）
    """
    date_str = target_date.strftime("%Y%m%d")
    html_dir = os.path.join(_cfg["html_dir"], date_str)
    os.makedirs(html_dir, exist_ok=True)

    total = len(df)
    skipped = 0
    saved = 0
    errors = 0

    logger.info("詳細ページ保存開始: %d 件 → %s", total, html_dir)

    for i, row in enumerate(df.itertuples(index=False), 1):
        job_number = str(row.job_number)
        kyujintype = int(row.kyujintype)
        save_path = os.path.join(html_dir, f"{job_number}.html")

        # 既存ファイルはスキップ（再開対応）
        if os.path.exists(save_path):
            skipped += 1
            continue
        if kyujintype != 5:
            url = (
                PAGE_URL
                .replace("{{job_number}}", job_number)
                .replace("{{kyujintype}}", str(kyujintype))
            )
        else:
            # 障害のある方のための求人は kyujintype=1 で固定
            url = (
                PAGE_URL
                .replace("{{job_number}}", job_number)
                .replace("{{kyujintype}}", "1")
            )

        try:
            driver.get(url)
            _wait_for(driver, By.TAG_NAME, "body")
            html = driver.page_source
            with open(save_path, "w", encoding="utf-8") as f:
                f.write(html)
            saved += 1
            time.sleep(WAIT_BETWEEN_DETAILS + random.uniform(0, 1))
        except TimeoutException:
            logger.warning("[%d/%d] タイムアウト: %s", i, total, job_number)
            errors += 1
        except Exception as e:
            logger.warning("[%d/%d] 取得エラー (%s): %s", i, total, job_number, e)
            errors += 1

        if i % 100 == 0:
            logger.info("  進捗: %d / %d 件（保存: %d, スキップ: %d, エラー: %d）", i, total, saved, skipped, errors)

    logger.info("詳細ページ保存完了: 保存=%d, スキップ=%d, エラー=%d", saved, skipped, errors)

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main(target_date: datetime.date):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logger.info("対象日付: %s", target_date)

    driver = build_driver()
    try:
        df = crawl(driver, target_date)
        scrape_details(driver, df, target_date)
    finally:
        driver.quit()
        logger.info("ドライバー終了")

# ---------------------------------------------------------------------------
# エントリーポイント
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="ハローワーク求人情報クローラー")
    parser.add_argument(
        "--date",
        type=lambda s: datetime.datetime.strptime(s, "%Y-%m-%d").date(),
        default=datetime.date.today(),
        help="取得対象の日付（YYYY-MM-DD形式、デフォルト: 当日）",
    )
    args = parser.parse_args()

    main(args.date)