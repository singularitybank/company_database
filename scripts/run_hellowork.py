# -*- coding: utf-8 -*-
"""
ハローワーク 日次バッチ処理 エントリーポイント

[処理フロー]
  STEP 1: 求人番号収集      crawl()
  STEP 2: 詳細HTML保存      scrape_details()
  STEP 3: Parquet変換       parse_to_parquet()

[実行方法]
  # 通常（当日分）
  python scripts/run_hellowork.py

  # 日付指定
  python scripts/run_hellowork.py --date 2026-04-10

  # クロールをスキップしてParquet変換のみ
  python scripts/run_hellowork.py --date 2026-04-10 --skip-crawl

[タスクスケジューラ]
  scripts/run_hellowork.bat から呼び出す
"""
import argparse
import datetime
import logging
import sys
import time
from pathlib import Path

# プロジェクトルートを sys.path に追加（src 配下のモジュールを import できるようにする）
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from crawlers.hellowork_crawler import build_driver, crawl, scrape_details
from parsers.hellowork_parser import _load_config, parse_to_parquet


# ---------------------------------------------------------------------------
# ログ設定
# ---------------------------------------------------------------------------

def _setup_logging(date_str: str) -> None:
    """コンソール＋日付別ファイルの二重ログを設定する。"""
    log_dir = PROJECT_ROOT / "logs"
    log_dir.mkdir(exist_ok=True)
    log_path = log_dir / f"hellowork_{date_str}.log"

    fmt = "%(asctime)s [%(levelname)s] %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"

    # Windows の CP932 端末でも文字化けしないよう errors="replace"
    console_handler = logging.StreamHandler(
        stream=open(sys.stdout.fileno(), mode="w", encoding="utf-8",
                    errors="replace", closefd=False)
    )
    console_handler.setFormatter(logging.Formatter(fmt, datefmt))

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(logging.Formatter(fmt, datefmt))

    logging.basicConfig(level=logging.INFO, handlers=[console_handler, file_handler])


# ---------------------------------------------------------------------------
# メイン処理
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description="ハローワーク 日次バッチ処理")
    parser.add_argument(
        "--date",
        type=lambda s: datetime.datetime.strptime(s, "%Y-%m-%d").date(),
        default=datetime.date.today(),
        help="処理対象の日付（YYYY-MM-DD形式、デフォルト: 当日）",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="ブラウザをヘッドレスモードで起動する（タスクスケジューラ実行時に指定）",
    )
    parser.add_argument(
        "--skip-crawl",
        action="store_true",
        help="クロールをスキップし、既存HTMLのParquet変換のみ実行する",
    )
    args = parser.parse_args()

    date_str = args.date.strftime("%Y%m%d")
    _setup_logging(date_str)
    logger = logging.getLogger(__name__)

    logger.info("=" * 60)
    logger.info("ハローワーク 日次バッチ 開始  対象日付: %s", args.date)
    logger.info("=" * 60)

    cfg = _load_config()
    html_dir = Path(cfg["html_dir"]) / date_str
    staging_dir = PROJECT_ROOT / cfg["staging_dir"]
    batch_start = time.time()

    # ── STEP 1 & 2: クロール ─────────────────────────────────────────────────
    if args.skip_crawl:
        logger.info("[STEP 1-2] --skip-crawl 指定のためクロールをスキップ")
    else:
        logger.info("[STEP 1/3] 求人番号収集 開始")
        driver = None
        try:
            driver = build_driver(headless=args.headless)

            step_start = time.time()
            df = crawl(driver, args.date)
            logger.info(
                "[STEP 1/3] 求人番号収集完了: %d件 (%.1f分)",
                len(df), (time.time() - step_start) / 60,
            )

            logger.info("[STEP 2/3] 詳細HTMLダウンロード 開始")
            step_start = time.time()
            scrape_details(driver, df, args.date)
            logger.info(
                "[STEP 2/3] 詳細HTMLダウンロード完了 (%.1f分)",
                (time.time() - step_start) / 60,
            )

        except Exception:
            logger.exception("[STEP 1-2] クロール中に予期しないエラーが発生しました")
            return 1
        finally:
            if driver:
                driver.quit()
                logger.info("ブラウザ終了")

    # ── STEP 3: Parquet変換 ──────────────────────────────────────────────────
    logger.info("[STEP 3/3] Parquet変換 開始: %s", html_dir)

    if not html_dir.exists():
        logger.error("[STEP 3/3] HTMLディレクトリが存在しません: %s", html_dir)
        return 1

    try:
        step_start = time.time()
        out_path = parse_to_parquet(html_dir, staging_dir)
        logger.info(
            "[STEP 3/3] Parquet変換完了: %s (%.1f分)",
            out_path.name, (time.time() - step_start) / 60,
        )
    except Exception:
        logger.exception("[STEP 3/3] Parquet変換中にエラーが発生しました")
        return 1

    elapsed = time.time() - batch_start
    logger.info("=" * 60)
    logger.info("ハローワーク 日次バッチ 正常終了  所要時間: %.1f分", elapsed / 60)
    logger.info("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
