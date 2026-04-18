# -*- coding: utf-8 -*-
"""
PR Times 企業ページ HTML クローラー（Selenium）

企業ページは SPA（JavaScript レンダリング）のため Selenium が必要。

対象URL: https://prtimes.jp/main/html/searchrlp/company_id/{company_id}

[動作概要]
  1. prtimes_companies テーブルから未スクレイプ企業（scraped_at IS NULL）を取得
  2. Selenium で各企業ページをレンダリング
  3. page_source を C:/Temp/html/prtimes/companies/{company_id}.html に保存
  4. CompanyHtmlResult のリストを返す

[保存先]
  C:/Temp/html/prtimes/companies/{company_id}.html
"""

import logging
import random
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.config import prtimes as _cfg
from src.common.selenium_utils import build_driver

logger = logging.getLogger(__name__)

JST             = timezone(timedelta(hours=9))
TIMEOUT         = _cfg["timeout"]
WAIT_COMPANIES  = _cfg["wait_between_companies"]
HTML_COMPANIES  = Path(_cfg["html_dir"]) / "companies"
COMPANY_URL     = "https://prtimes.jp/main/html/searchrlp/company_id/{company_id}"

# ---------------------------------------------------------------------------
# データクラス
# ---------------------------------------------------------------------------

@dataclass
class CompanyHtmlResult:
    """企業ページ HTML 取得結果 1 件分"""
    prtimes_company_id: int
    html_path:          str
    success:            bool
    error:              Optional[str] = None


# ---------------------------------------------------------------------------
# 内部ヘルパー
# ---------------------------------------------------------------------------

def _load_unscraped(
    db_path: "str | Path",
    limit: Optional[int] = None,
) -> list[int]:
    """scraped_at が未設定の company_id を DB から取得する。"""
    sql = "SELECT prtimes_company_id FROM prtimes_companies WHERE scraped_at IS NULL ORDER BY prtimes_company_id"
    params: list = []
    if limit is not None:
        sql += " LIMIT ?"
        params.append(limit)

    conn = sqlite3.connect(str(db_path))
    try:
        rows = conn.execute(sql, params).fetchall()
    finally:
        conn.close()
    return [r[0] for r in rows]


def _wait_for_content(driver, timeout: int) -> bool:
    """h1 が表示されるまで待機する（SPA レンダリング完了の代替判定）。"""
    try:
        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.TAG_NAME, "h1"))
        )
        return True
    except Exception:
        return False


# ---------------------------------------------------------------------------
# メイン処理
# ---------------------------------------------------------------------------

def scrape_companies(
    db_path: "str | Path",
    limit: Optional[int] = None,
    headless: bool = True,
) -> list[CompanyHtmlResult]:
    """未スクレイプ企業ページの HTML をダウンロードして保存する。

    Args:
        db_path:  prtimes.db のパス
        limit:    最大取得件数（None で全件）
        headless: ブラウザをヘッドレスモードで起動するか

    Returns:
        CompanyHtmlResult のリスト
    """
    HTML_COMPANIES.mkdir(parents=True, exist_ok=True)

    company_ids = _load_unscraped(db_path, limit=limit)
    if not company_ids:
        logger.info("未スクレイプ企業なし")
        return []

    logger.info("企業ページ取得開始: %d 件 → %s", len(company_ids), HTML_COMPANIES)

    driver = build_driver(headless=headless)
    results: list[CompanyHtmlResult] = []
    saved = skipped = errors = 0

    try:
        for i, company_id in enumerate(company_ids, 1):
            save_path = HTML_COMPANIES / f"{company_id}.html"

            # ファイルが既に存在する場合はスキップ
            if save_path.exists():
                logger.debug("[%d/%d] スキップ（ファイル存在）: company_id=%d",
                             i, len(company_ids), company_id)
                results.append(CompanyHtmlResult(
                    prtimes_company_id = company_id,
                    html_path          = str(save_path),
                    success            = True,
                ))
                skipped += 1
                continue

            url = COMPANY_URL.format(company_id=company_id)
            try:
                driver.get(url)
                rendered = _wait_for_content(driver, TIMEOUT)

                if not rendered:
                    raise TimeoutError(f"h1 が {TIMEOUT}秒以内に表示されませんでした")

                # 追加で 1 秒待機（非同期コンテンツの安定化）
                time.sleep(1)

                save_path.write_text(driver.page_source, encoding="utf-8")
                results.append(CompanyHtmlResult(
                    prtimes_company_id = company_id,
                    html_path          = str(save_path),
                    success            = True,
                ))
                saved += 1
                logger.debug("[%d/%d] 保存完了: company_id=%d", i, len(company_ids), company_id)

            except Exception as e:
                logger.warning("[%d/%d] 取得失敗 company_id=%d: %s",
                               i, len(company_ids), company_id, e)
                results.append(CompanyHtmlResult(
                    prtimes_company_id = company_id,
                    html_path          = "",
                    success            = False,
                    error              = str(e),
                ))
                errors += 1

            if i % 20 == 0:
                logger.info("  進捗: %d / %d 件（保存: %d, スキップ: %d, エラー: %d）",
                            i, len(company_ids), saved, skipped, errors)

            # レート制限
            if i < len(company_ids):
                time.sleep(WAIT_COMPANIES + random.uniform(0, 1))

    finally:
        driver.quit()
        logger.info("ドライバー終了")

    logger.info("企業ページ取得完了: 保存=%d, スキップ=%d, エラー=%d",
                saved, skipped, errors)
    return results
