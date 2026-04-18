# -*- coding: utf-8 -*-
"""
Selenium 共通ユーティリティ

Edge WebDriver の初期化設定を一元管理する。
各クローラーはこのモジュールから build_driver() を import して使用する。

使い方:
  from src.utils.selenium_utils import build_driver
  driver = build_driver(headless=True)
"""

import logging
import os
import subprocess
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.edge.options import Options
from selenium.webdriver.edge.service import Service

logger = logging.getLogger(__name__)


def build_driver(headless: bool = True) -> webdriver.Edge:
    """Edge WebDriver を初期化して返す。

    タスクスケジューラ実行を想定しデフォルトは headless=True。
    手動実行時やデバッグ時は headless=False を渡す。

    Args:
        headless: True にするとブラウザウィンドウを表示しない

    Returns:
        初期化済みの Edge WebDriver インスタンス
    """
    options = Options()
    if headless:
        options.add_argument("--headless")

    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-gpu-sandbox")
    options.add_argument("--disable-software-rasterizer")
    options.add_argument("--disable-logging")               # Chromium内部ログ無効化
    options.add_argument("--log-level=3")                   # FATAL のみ
    options.add_argument("--log-file=nul")                  # Windowsのnullデバイスへ
    options.add_argument("--window-size=1280,900")
    options.add_argument("--lang=ja")
    # ボット検出対策: 自動化フラグを軽減
    options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
    options.add_experimental_option("useAutomationExtension", False)

    service = Service(log_output=subprocess.DEVNULL)

    driver = webdriver.Edge(service=service, options=options)
    driver.implicitly_wait(5)
    logger.info("Edge ドライバー起動完了 (headless=%s)", headless)
    return driver
