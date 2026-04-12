# -*- coding: utf-8 -*-
"""
プロジェクト共通設定モジュール

全モジュールはこのモジュールから設定値を取得する。
config/config.yaml と config/.env を一元管理する。

使い方:
  from src.config import hellowork, get_nta_app_id, DATA_DIR

  cfg = hellowork          # dict: config.yaml の hellowork セクション
  app_id = get_nta_app_id()  # NTA_APPLICATION_ID 環境変数の値
"""
import os
from pathlib import Path

import yaml
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# プロジェクトルートとファイルパス定義
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[1]
_CONFIG_PATH = PROJECT_ROOT / "config" / "config.yaml"
_ENV_PATH    = PROJECT_ROOT / "config" / ".env"

# .env をインポート時に読み込む（NTA_APPLICATION_ID 等）
load_dotenv(_ENV_PATH)

# ---------------------------------------------------------------------------
# YAML 設定読み込み
# ---------------------------------------------------------------------------

def _load_yaml() -> dict:
    with open(_CONFIG_PATH, encoding="utf-8") as f:
        return yaml.safe_load(f)

_yaml_cfg = _load_yaml()

# ---------------------------------------------------------------------------
# ハローワーク設定
# ---------------------------------------------------------------------------

hellowork: dict = _yaml_cfg["hellowork"]

# ---------------------------------------------------------------------------
# NTA 設定
# ---------------------------------------------------------------------------

def get_nta_app_id() -> str:
    """NTA アプリケーション ID を環境変数から取得する。

    Returns:
        NTA_APPLICATION_ID の値

    Raises:
        EnvironmentError: 環境変数が未設定の場合
    """
    app_id = os.getenv("NTA_APPLICATION_ID")
    if not app_id:
        raise EnvironmentError(
            "環境変数 NTA_APPLICATION_ID が設定されていません。config/.env を確認してください。"
        )
    return app_id


# ---------------------------------------------------------------------------
# gBizINFO 設定
# ---------------------------------------------------------------------------

def get_gbizinfo_api_token() -> str:
    """gBizINFO API トークンを環境変数から取得する。

    Returns:
        GBIZINFO_API_TOKEN の値

    Raises:
        EnvironmentError: 環境変数が未設定の場合
    """
    token = os.getenv("GBIZINFO_API_TOKEN")
    if not token:
        raise EnvironmentError(
            "環境変数 GBIZINFO_API_TOKEN が設定されていません。config/.env を確認してください。"
        )
    return token

# ---------------------------------------------------------------------------
# 共通パス定義
# ---------------------------------------------------------------------------

DATA_DIR    = PROJECT_ROOT / "data"
RAW_DIR     = DATA_DIR / "raw"
STAGING_DIR = DATA_DIR / "staging"
LOGS_DIR    = PROJECT_ROOT / "logs"
