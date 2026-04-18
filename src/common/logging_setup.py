# -*- coding: utf-8 -*-
"""
ログ設定一元管理モジュール

各スクリプト・モジュールはこのモジュールの setup_logging() を呼び出してロギングを初期化する。
モジュールレベルでの logging.basicConfig() 呼び出しはここに集約される。

使い方:
  from src.logging_setup import setup_logging

  # コンソールのみ
  setup_logging()

  # コンソール + 日付付きファイル（例: logs/nta_diff_20260412.log）
  setup_logging(log_dir="logs", filename_prefix="nta_diff")

  # ファイル名を完全指定（例: logs/hellowork_20260410.log）
  setup_logging(log_dir="logs", log_filename="hellowork_20260410")
"""
import logging
import sys
from datetime import date
from pathlib import Path


def setup_logging(
    log_dir: "Path | str | None" = None,
    filename_prefix: str = "app",
    log_filename: "str | None" = None,
    level: int = logging.INFO,
) -> None:
    """ルートロガーにコンソール（＋オプションでファイル）ハンドラを設定する。

    既にハンドラが設定されている場合は何もしない（二重設定防止）。

    Args:
        log_dir:         ログファイルの出力先ディレクトリ。None の場合はコンソールのみ
        filename_prefix: ログファイル名のプレフィックス。
                         例: "nta_diff" → "nta_diff_20260412.log"
                         log_filename が指定された場合は無視される
        log_filename:    ファイル名を完全指定（拡張子なし）。
                         例: "hellowork_20260410" → "hellowork_20260410.log"
        level:           ログレベル（デフォルト: INFO）
    """
    root = logging.getLogger()
    if root.handlers:
        return  # 既に設定済み（二重設定防止）

    fmt     = "%(asctime)s [%(levelname)s] %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    handlers: list[logging.Handler] = []

    # コンソールハンドラ（Windows CP932 端末でも文字化けしないよう UTF-8 強制）
    try:
        console_stream = open(
            sys.stdout.fileno(), mode="w", encoding="utf-8",
            errors="replace", closefd=False,
        )
        handlers.append(logging.StreamHandler(console_stream))
    except Exception:
        handlers.append(logging.StreamHandler(sys.stdout))

    # ファイルハンドラ（log_dir が指定された場合のみ）
    if log_dir is not None:
        log_dir = Path(log_dir)
        log_dir.mkdir(parents=True, exist_ok=True)
        if log_filename is not None:
            log_file = log_dir / f"{log_filename}.log"
        else:
            log_file = log_dir / f"{filename_prefix}_{date.today().strftime('%Y%m%d')}.log"
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))

    logging.basicConfig(level=level, format=fmt, datefmt=datefmt, handlers=handlers)
