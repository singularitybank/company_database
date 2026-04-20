# -*- coding: utf-8 -*-
"""
帝国データバンク（TDB）倒産詳細ページ クローラー

[動作概要]
  1. tdb_cases テーブルから detail_scraped_at IS NULL のレコードを取得
  2. curl_cffi（Chrome TLS フィンガープリント）で取得を試みる
  3. 全リトライ失敗時は undetected_chromedriver による Selenium にフォールバック
  4. 生 HTML を {html_dir}/tdb/{case_id}.html に保存
  5. DetailHtmlResult のリストを返す（パースは parsers 層が行う）

[取得仕様]
  - HTTP   : curl_cffi chrome146 impersonate + Cookie 確立（トップページ訪問）
  - 待機   : random.uniform(wait_between_requests, wait_between_requests × 2.5) 秒
  - fallback: undetected_chromedriver（headless は config で制御）
"""

import logging
import random
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[5]))
from src.config import bankruptcy as _cfg
from src.common.requests_utils import (
    DEFAULT_BROWSER_HEADERS,
    NotFoundError,
    create_session,
    fetch,
)

logger = logging.getLogger(__name__)

# 404 など「存在しないURL」を表すセンチネル（接続エラーの None と区別するため）
_NOT_FOUND = object()

# ---------------------------------------------------------------------------
# 定数
# ---------------------------------------------------------------------------

HTML_BASE_DIR = Path(_cfg["html_dir"]) / "tdb"
TIMEOUT       = _cfg["timeout"]
RETRY_COUNT   = _cfg["retry_count"]
WAIT          = _cfg["wait_between_requests"]
HEADLESS      = _cfg.get("headless", False)

_DETAIL_HEADERS = {
    **DEFAULT_BROWSER_HEADERS,
    "Referer": "https://www.tdb.co.jp/",
    "Sec-Fetch-Site": "same-origin",
}

# ---------------------------------------------------------------------------
# データクラス
# ---------------------------------------------------------------------------

@dataclass
class DetailHtmlResult:
    """詳細ページ取得結果 1 件分"""
    case_id:    str
    source_url: str
    html_path:  Optional[str]
    success:    bool
    error:      Optional[str] = None


# ---------------------------------------------------------------------------
# 内部ヘルパー
# ---------------------------------------------------------------------------

def _fetch_http(session, url: str):
    """curl_cffi セッションで取得。

    Returns:
        bytes       : 取得成功
        _NOT_FOUND  : 404（ページが存在しない）
        None        : 接続エラー等（Selenium fallback が有効なら試みる価値あり）
    """
    try:
        return fetch(session, url, headers=_DETAIL_HEADERS,
                     retry_count=RETRY_COUNT, timeout=TIMEOUT, wait=WAIT)
    except NotFoundError:
        return _NOT_FOUND


def _fetch_selenium(url: str, driver_ref: list) -> Optional[bytes]:
    """Selenium（undetected_chromedriver）で取得。失敗時は None。

    driver_ref[0] にドライバーインスタンスを保持して再利用する。
    """
    try:
        import undetected_chromedriver as uc
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.common.by import By
    except ImportError:
        logger.error("undetected_chromedriver がインストールされていません")
        return None

    try:
        if driver_ref[0] is None:
            logger.info("Selenium ドライバー初期化")
            options = uc.ChromeOptions()
            if HEADLESS:
                options.add_argument("--headless=new")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            driver_ref[0] = uc.Chrome(options=options)

        driver = driver_ref[0]
        driver.get(url)
        WebDriverWait(driver, TIMEOUT).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        return driver.page_source.encode("utf-8")
    except Exception as e:
        logger.warning("Selenium 取得エラー %s: %s", url, e)
        return None


def _fetch_raw(session, driver_ref: list, url: str, selenium_fallback: bool = True) -> Optional[bytes]:
    """HTTP を試み、接続エラー時に Selenium へフォールバック。

    404（_NOT_FOUND）の場合は Selenium を試みず None を返す。
    """
    raw = _fetch_http(session, url)
    if isinstance(raw, bytes):
        return raw
    if raw is _NOT_FOUND:
        logger.debug("404 Not Found: %s", url)
        return None
    # raw is None = 接続エラー → Selenium fallback
    if not selenium_fallback:
        return None
    logger.info("HTTP 失敗（接続エラー）→ Selenium fallback: %s", url)
    return _fetch_selenium(url, driver_ref)


def _save_html(raw: bytes, case_id: str) -> str:
    """生 HTML をファイルに保存してパスを返す。"""
    HTML_BASE_DIR.mkdir(parents=True, exist_ok=True)
    path = HTML_BASE_DIR / f"{case_id}.html"
    path.write_bytes(raw)
    return str(path)


# ---------------------------------------------------------------------------
# メイン処理
# ---------------------------------------------------------------------------

def scrape(entries: list, selenium_fallback: bool = True) -> list[DetailHtmlResult]:
    """TDB 詳細ページを取得・保存して結果リストを返す。

    Args:
        entries:           case_id と source_url を持つオブジェクトのリスト
        selenium_fallback: False にすると HTTP 失敗時に Selenium を使わない

    Returns:
        DetailHtmlResult のリスト
    """
    results: list[DetailHtmlResult] = []
    session    = create_session(timeout=TIMEOUT)
    driver_ref = [None]  # Selenium ドライバーの遅延初期化コンテナ

    try:
        for i, entry in enumerate(entries):
            case_id    = entry.case_id
            source_url = entry.source_url

            logger.info("[%d/%d] TDB 詳細取得: %s", i + 1, len(entries), source_url)

            raw = _fetch_raw(session, driver_ref, source_url, selenium_fallback)
            if raw is None:
                results.append(DetailHtmlResult(
                    case_id    = case_id,
                    source_url = source_url,
                    html_path  = None,
                    success    = False,
                    error      = "fetch failed",
                ))
            else:
                html_path = _save_html(raw, case_id)
                results.append(DetailHtmlResult(
                    case_id    = case_id,
                    source_url = source_url,
                    html_path  = html_path,
                    success    = True,
                ))
                logger.debug("保存: %s", html_path)

            if i < len(entries) - 1:
                time.sleep(random.uniform(WAIT, WAIT * 2.5))

    finally:
        if driver_ref[0] is not None:
            driver_ref[0].quit()
            # undetected_chromedriver の __del__ が quit() を再呼び出しして
            # インタープリター終了時に OSError になるのを防ぐ
            driver_ref[0].quit = lambda: None
            logger.debug("Selenium ドライバー終了")

    ok  = sum(1 for r in results if r.success)
    err = len(results) - ok
    logger.info("TDB 詳細取得完了: 成功=%d件, エラー=%d件", ok, err)
    return results
