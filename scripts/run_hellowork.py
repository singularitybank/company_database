# -*- coding: utf-8 -*-
"""
ハローワーク 日次バッチ処理 エントリーポイント

[処理フロー]
  STEP 1: 求人番号収集      crawl()
  STEP 2: 詳細HTML保存      scrape_details()
  STEP 3: Parquet変換       parse_to_parquet()
  STEP 4: DB投入            load_parquet() → hellowork.db

[実行方法]
  # 通常（当日分）
  python scripts/run_hellowork.py

  # 日付指定
  python scripts/run_hellowork.py --date 2026-04-10

  # クロールをスキップしてParquet変換のみ
  python scripts/run_hellowork.py --date 2026-04-10 --skip-crawl

  # 求人番号収集(STEP1)をスキップして詳細HTMLダウンロードから開始
  python scripts/run_hellowork.py --date 2026-04-10 --skip-step1

[タスクスケジューラ]
  scripts/run_hellowork.bat から呼び出す
"""
import argparse
import datetime
import logging
import sys
import time
from pathlib import Path

import pandas as pd

# プロジェクトルートを sys.path に追加（src 配下のモジュールを import できるようにする）
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.config import hellowork as _cfg, DATA_DIR
from src.signals.hellowork.crawler import build_driver, crawl, scrape_details
from src.common.logging_setup import setup_logging
from src.signals.hellowork.parser import parse_to_parquet
from src.signals.hellowork.models.schema import init_db
from src.signals.hellowork.loaders.db_loader import load_parquet
from src.common.db_utils import configure_for_bulk_load


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
    parser.add_argument(
        "--skip-step1",
        action="store_true",
        help="求人番号収集(STEP1)をスキップし、保存済みCSVから詳細HTMLダウンロードを開始する",
    )
    args = parser.parse_args()

    date_str = args.date.strftime("%Y%m%d")
    setup_logging(PROJECT_ROOT / "logs" / "hellowork", log_filename=f"hellowork_{date_str}")
    logger = logging.getLogger(__name__)

    logger.info("=" * 60)
    logger.info("ハローワーク 日次バッチ 開始  対象日付: %s", args.date)
    logger.info("=" * 60)

    html_dir = Path(_cfg["html_dir"]) / date_str
    staging_dir = PROJECT_ROOT / _cfg["staging_dir"]
    batch_start = time.time()

    # ── STEP 1 & 2: クロール ─────────────────────────────────────────────────
    if args.skip_crawl:
        logger.info("[STEP 1-2] --skip-crawl 指定のためクロールをスキップ")
    elif args.skip_step1:
        logger.info("[STEP 1] --skip-step1 指定のため求人番号収集をスキップ")
        jobnumbers_csv = Path(_cfg["output_dir"]) / f"jobnumbers_{date_str}.csv"
        if not jobnumbers_csv.exists():
            logger.error("[STEP 1] 求人番号CSVが見つかりません: %s", jobnumbers_csv)
            return 1
        df = pd.read_csv(jobnumbers_csv)
        logger.info("[STEP 1] 求人番号CSV読み込み完了: %d件 (%s)", len(df), jobnumbers_csv.name)

        logger.info("[STEP 2/4] 詳細HTMLダウンロード 開始")
        step_start = time.time()
        try:
            scrape_details(df, args.date)
        except Exception:
            logger.exception("[STEP 2] 詳細HTMLダウンロード中に予期しないエラーが発生しました")
            return 1
        logger.info(
            "[STEP 2/4] 詳細HTMLダウンロード完了 (%.1f分)",
            (time.time() - step_start) / 60,
        )
    else:
        logger.info("[STEP 1/4] 求人番号収集 開始")
        driver = None
        try:
            driver = build_driver(headless=args.headless)

            step_start = time.time()
            df = crawl(driver, args.date)
            logger.info(
                "[STEP 1/4] 求人番号収集完了: %d件 (%.1f分)",
                len(df), (time.time() - step_start) / 60,
            )

        except Exception:
            logger.exception("[STEP 1] クロール中に予期しないエラーが発生しました")
            return 1
        finally:
            if driver:
                driver.quit()
                logger.info("ブラウザ終了")

        logger.info("[STEP 2/4] 詳細HTMLダウンロード 開始")
        step_start = time.time()
        try:
            scrape_details(df, args.date)
        except Exception:
            logger.exception("[STEP 2] 詳細HTMLダウンロード中に予期しないエラーが発生しました")
            return 1
        logger.info(
            "[STEP 2/4] 詳細HTMLダウンロード完了 (%.1f分)",
            (time.time() - step_start) / 60,
        )

    # ── STEP 3: Parquet変換 ──────────────────────────────────────────────────
    logger.info("[STEP 3/4] Parquet変換 開始: %s", html_dir)

    if not html_dir.exists():
        logger.error("[STEP 3/4] HTMLディレクトリが存在しません: %s", html_dir)
        return 1

    try:
        step_start = time.time()
        out_path = parse_to_parquet(html_dir, staging_dir)
        logger.info(
            "[STEP 3/4] Parquet変換完了: %s (%.1f分)",
            out_path.name, (time.time() - step_start) / 60,
        )
    except Exception:
        logger.exception("[STEP 3/4] Parquet変換中にエラーが発生しました")
        return 1

    # ── STEP 4: DB投入 ───────────────────────────────────────────────────────
    logger.info("[STEP 4/4] DB投入 開始: %s", out_path.name)
    try:
        step_start = time.time()
        db_path = DATA_DIR / "hellowork.db"
        conn = init_db(db_path)
        configure_for_bulk_load(conn)
        count = load_parquet(conn, out_path)
        conn.close()
        logger.info(
            "[STEP 4/4] DB投入完了: %d件 (%.1f分)",
            count, (time.time() - step_start) / 60,
        )
    except Exception:
        logger.exception("[STEP 4/4] DB投入中にエラーが発生しました")
        return 1

    elapsed = time.time() - batch_start
    logger.info("=" * 60)
    logger.info("ハローワーク 日次バッチ 正常終了  所要時間: %.1f分", elapsed / 60)
    logger.info("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
