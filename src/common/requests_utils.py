# -*- coding: utf-8 -*-
"""
汎用 HTTP ユーティリティ（curl_cffi ベース）

curl_cffi の Chrome TLS フィンガープリントを使い、ボット検知を回避する。
各スクレイパーから create_session / fetch を呼び出して使う。
"""

import logging
import random
import time
from typing import Optional


class NotFoundError(Exception):
    """サーバーが not_found_codes（デフォルト 404）を返したときに raise される。

    接続エラーやその他の HTTP エラーとは異なり、リトライしても無意味なため
    fetch() は即座にこの例外を raise して呼び出し元に通知する。
    """

from curl_cffi import requests as curl_requests

logger = logging.getLogger(__name__)

DEFAULT_IMPERSONATE = "chrome146"

DEFAULT_BROWSER_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
}


def create_session(
    warm_up_url: Optional[str] = None,
    impersonate: str = DEFAULT_IMPERSONATE,
    timeout: int = 30,
) -> curl_requests.Session:
    """Chrome TLS フィンガープリントのセッションを生成する。

    warm_up_url を指定するとトップページを訪問して Cookie を確立する。
    訪問失敗時もセッション自体は返す。
    """
    session = curl_requests.Session(impersonate=impersonate)
    if warm_up_url:
        try:
            session.get(warm_up_url, headers=DEFAULT_BROWSER_HEADERS, verify=False, timeout=timeout)
            logger.debug("ウォームアップ完了: %s", warm_up_url)
        except Exception as e:
            logger.warning("ウォームアップ失敗（続行）: %s: %s", warm_up_url, e)
    return session


def fetch(
    session: curl_requests.Session,
    url: str,
    headers: Optional[dict] = None,
    retry_count: int = 3,
    timeout: int = 30,
    wait: float = 5.0,
    not_found_codes: tuple[int, ...] = (404,),
) -> Optional[bytes]:
    """URL の生バイトを取得して返す。全リトライ失敗時は None。

    バックオフ: wait * 2^attempt + random(0, wait) 秒

    not_found_codes に含まれるステータスコードを受け取った場合は
    リトライせず即座に NotFoundError を raise する。
    """
    _headers = headers or DEFAULT_BROWSER_HEADERS
    for attempt in range(1, retry_count + 1):
        try:
            resp = session.get(url, headers=_headers, verify=False, timeout=timeout)
            if resp.status_code in not_found_codes:
                logger.debug("Not Found (%d): %s", resp.status_code, url)
                raise NotFoundError(f"{resp.status_code} {url}")
            resp.raise_for_status()
            return resp.content
        except NotFoundError:
            raise
        except Exception as e:
            logger.warning("取得エラー（試行 %d/%d）%s: %s", attempt, retry_count, url, e)
            if attempt < retry_count:
                time.sleep(wait * (2 ** attempt) + random.uniform(0, wait))
    logger.error("取得失敗（%d 回試行）: %s", retry_count, url)
    return None
