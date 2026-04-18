# -*- coding: utf-8 -*-
"""
職場情報総合サイト 全件ダウンロード

URL: https://shokuba.mhlw.go.jp/shokuba/utilize/download010?lang=JA
  → クリックで CSV ファイルが直接ダウンロードされる

使い方:
  python src/downloaders/shokuba_downloader.py
  python src/downloaders/shokuba_downloader.py --output data/raw/shokuba/Shokubajoho_20260412.csv
"""

import argparse
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

import requests

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

DOWNLOAD_URL = "https://shokuba.mhlw.go.jp/shokuba/utilize/download010?lang=JA"
DEFAULT_RAW_DIR = REPO_ROOT / "data" / "raw" / "shokuba"
CHUNK_SIZE = 1024 * 1024   # 1 MB ストリームチャンク
TIMEOUT    = 300            # 接続タイムアウト秒数（大容量ファイルのため長めに設定）

log = logging.getLogger(__name__)


def download(output_path: Path | None = None, timeout: int = TIMEOUT) -> Path:
    """
    職場情報総合サイトから CSV ファイルをダウンロードする。

    Args:
        output_path: 保存先ファイルパス。None の場合は日付付きファイル名を自動生成。
        timeout: HTTP タイムアウト秒数。

    Returns:
        ダウンロードしたファイルのパス。

    Raises:
        requests.HTTPError: HTTP エラーが発生した場合。
        OSError: ファイルの書き込みに失敗した場合。
    """
    if output_path is None:
        date_str = datetime.now().strftime("%Y%m%d")
        DEFAULT_RAW_DIR.mkdir(parents=True, exist_ok=True)
        output_path = DEFAULT_RAW_DIR / f"Shokubajoho_{date_str}.csv"
    else:
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

    log.info("ダウンロード開始: %s", DOWNLOAD_URL)
    log.info("保存先: %s", output_path)

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/csv,application/octet-stream,*/*",
        "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
        "Referer": "https://shokuba.mhlw.go.jp/",
    }

    start = time.time()
    downloaded_bytes = 0

    with requests.get(
        DOWNLOAD_URL,
        headers=headers,
        stream=True,
        timeout=timeout,
        allow_redirects=True,
    ) as resp:
        resp.raise_for_status()

        # Content-Type を確認してログ出力
        content_type = resp.headers.get("Content-Type", "unknown")
        content_length = resp.headers.get("Content-Length")
        log.info("Content-Type: %s", content_type)
        if content_length:
            log.info("ファイルサイズ: %.1f MB", int(content_length) / 1024 / 1024)

        with open(output_path, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                if chunk:
                    fh.write(chunk)
                    downloaded_bytes += len(chunk)

    elapsed = time.time() - start
    size_mb = downloaded_bytes / 1024 / 1024
    log.info("ダウンロード完了: %.1f MB / %.1f秒", size_mb, elapsed)

    return output_path


def main() -> None:
    from src.common.logging_setup import setup_logging
    setup_logging(filename_prefix="shokuba")

    parser = argparse.ArgumentParser(
        description="職場情報総合サイト 全件ダウンロード"
    )
    parser.add_argument(
        "--output", "-o",
        type=Path,
        default=None,
        help="保存先ファイルパス（省略時: data/raw/shokuba/Shokubajoho_YYYYMMDD.csv）",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=TIMEOUT,
        help=f"HTTP タイムアウト秒数（デフォルト: {TIMEOUT}）",
    )
    args = parser.parse_args()

    try:
        path = download(output_path=args.output, timeout=args.timeout)
        log.info("保存完了: %s", path)
    except requests.HTTPError as e:
        log.error("HTTP エラー: %s", e)
        sys.exit(1)
    except OSError as e:
        log.error("ファイル書き込みエラー: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
