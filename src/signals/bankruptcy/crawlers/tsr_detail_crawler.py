# -*- coding: utf-8 -*-
"""
東京商工リサーチ（TSR）倒産詳細ページ クローラー

[動作概要]
  1. tsr_cases テーブルから detail_scraped_at IS NULL のレコードを取得
  2. 各詳細ページを urllib.request + feedparser User-Agent で取得
  3. 生 HTML を C:/Temp/html/bankruptcy/tsr/{YYYYMMDD}/{case_id}.html に保存
  4. DetailHtmlResult のリストを返す（パースは parsers 層が行う）

[取得仕様]
  - User-Agent : feedparser/6.0.12 ...（requests は SSL 接続拒否される）
  - SSL 検証   : 無効（tsr-net.co.jp は証明書エラーになる環境がある）
  - エンコード : UTF-8（<meta charset="utf-8"> 宣言済み）
  - リトライ   : config の retry_count 回
  - 待機       : wait_between_requests + random(0,1) 秒
"""

import logging
import random
import ssl
import sys
import time
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[5]))
from src.config import bankruptcy as _cfg

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 定数
# ---------------------------------------------------------------------------

FEEDPARSER_UA  = "feedparser/6.0.12 +https://github.com/kurtmckee/feedparser/"
HTML_BASE_DIR  = Path(_cfg["html_dir"]) / "tsr"
TIMEOUT        = _cfg["timeout"]
RETRY_COUNT    = _cfg["retry_count"]
WAIT           = _cfg["wait_between_requests"]

JST = timezone(timedelta(hours=9))

_SSL_CTX = ssl.create_default_context()
_SSL_CTX.check_hostname = False
_SSL_CTX.verify_mode    = ssl.CERT_NONE

# ---------------------------------------------------------------------------
# データクラス
# ---------------------------------------------------------------------------

@dataclass
class DetailHtmlResult:
    """詳細ページ取得結果 1 件分"""
    case_id:   str
    source_url: str
    html_path: Optional[str]   # 保存先パス（取得失敗時は None）
    success:   bool
    error:     Optional[str] = None


# ---------------------------------------------------------------------------
# 内部ヘルパー
# ---------------------------------------------------------------------------

def _fetch_raw(url: str) -> Optional[bytes]:
    """URL の生バイトを取得して返す。失敗時は None。"""
    req = urllib.request.Request(url, headers={"User-Agent": FEEDPARSER_UA})
    for attempt in range(1, RETRY_COUNT + 1):
        try:
            with urllib.request.urlopen(req, timeout=TIMEOUT, context=_SSL_CTX) as resp:
                return resp.read()
        except Exception as e:
            logger.warning("取得エラー（試行 %d/%d）%s: %s", attempt, RETRY_COUNT, url, e)
            if attempt < RETRY_COUNT:
                time.sleep(2 ** attempt + random.uniform(0, 1))
    logger.error("取得失敗（%d 回試行）: %s", RETRY_COUNT, url)
    return None


def _save_html(raw: bytes, case_id: str) -> str:
    """生 HTML をファイルに保存してパスを返す。"""
    date_str = datetime.now(JST).strftime("%Y%m%d")
    save_dir = HTML_BASE_DIR / date_str
    save_dir.mkdir(parents=True, exist_ok=True)
    path = save_dir / f"{case_id}.html"
    path.write_bytes(raw)
    return str(path)


# ---------------------------------------------------------------------------
# メイン処理
# ---------------------------------------------------------------------------

def scrape(entries: list) -> list[DetailHtmlResult]:
    """TSR 詳細ページを取得・保存して結果リストを返す。

    Args:
        entries: case_id と source_url を持つオブジェクトのリスト
                 （TsrRssEntry または (case_id, source_url) の名前付き任意オブジェクト）

    Returns:
        DetailHtmlResult のリスト
    """
    results: list[DetailHtmlResult] = []

    for i, entry in enumerate(entries):
        case_id    = entry.case_id
        source_url = entry.source_url

        logger.info("[%d/%d] TSR 詳細取得: %s", i + 1, len(entries), source_url)

        raw = _fetch_raw(source_url)
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
            time.sleep(WAIT + random.uniform(0, 1))

    ok  = sum(1 for r in results if r.success)
    err = len(results) - ok
    logger.info("TSR 詳細取得完了: 成功=%d件, エラー=%d件", ok, err)
    return results
