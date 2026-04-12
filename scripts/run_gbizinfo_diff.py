# -*- coding: utf-8 -*-
"""
gBizINFO 差分データ 取得・DB適用スクリプト

使い方:
  # 全データセット、昨日分の差分を取得
  python scripts/run_gbizinfo_diff.py

  # 特定データセットのみ
  python scripts/run_gbizinfo_diff.py --dataset kihonjoho hojokinjoho

  # 期間指定
  python scripts/run_gbizinfo_diff.py --from 2026-04-01 --to 2026-04-10

  # DBパスを明示
  python scripts/run_gbizinfo_diff.py --db /path/to/gbizinfo.db

対応データセット:
  kihonjoho          基本情報         (updateInfo)
  todokedeninteijoho 届出認定情報     (updateInfo/certification)
  hyoshojoho         表彰情報         (updateInfo/commendation)
  zaimujoho          財務情報         (updateInfo/finance)
  tokkyojoho         特許情報         (updateInfo/patent)
  chotatsujoho       調達情報         (updateInfo/procurement)
  hojokinjoho        補助金情報       (updateInfo/subsidy)
  shokubajoho        職場情報         (updateInfo/workplace)
"""

import argparse
import logging
import sys
from datetime import date, timedelta
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from src.extractors.gbizinfo_diff_collector import DATASET_KEYS, fetch_diff
from src.logging_setup import setup_logging
from src.processors.gbizinfo_diff_processor import apply_diff


# ---------------------------------------------------------------------------
# メイン
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="gBizINFO 差分データ取得・DB適用")
    parser.add_argument(
        "--from", dest="from_date",
        default=str(date.today() - timedelta(days=1)),
        help="取得開始日 (YYYY-MM-DD)。デフォルト: 昨日",
    )
    parser.add_argument(
        "--to", dest="to_date",
        default=str(date.today()),
        help="取得終了日 (YYYY-MM-DD)。デフォルト: 今日",
    )
    parser.add_argument(
        "--dataset", dest="datasets", nargs="+",
        default=DATASET_KEYS,
        choices=DATASET_KEYS,
        metavar="DATASET",
        help=f"処理するデータセット。複数指定可。デフォルト: 全件 ({', '.join(DATASET_KEYS)})",
    )
    parser.add_argument(
        "--db",
        default=str(BASE_DIR / "data" / "gbizinfo.db"),
        help="SQLiteファイルパス（デフォルト: data/gbizinfo.db）",
    )
    parser.add_argument(
        "--wait", type=float, default=1.0,
        help="APIリクエスト間の待機秒数 (デフォルト: 1.0秒)",
    )
    args = parser.parse_args()

    setup_logging(BASE_DIR / "logs", filename_prefix="gbizinfo_diff")
    logger = logging.getLogger(__name__)

    logger.info("=" * 60)
    logger.info("gBizINFO 差分更新 開始")
    logger.info("  期間      : %s 〜 %s", args.from_date, args.to_date)
    logger.info("  データセット: %s", args.datasets)
    logger.info("  DB        : %s", args.db)
    logger.info("=" * 60)

    overall_errors = 0

    for dataset in args.datasets:
        logger.info("─" * 40)
        logger.info("▶ %s", dataset)

        # 1. 差分データ取得
        try:
            records = fetch_diff(
                dataset=dataset,
                from_date=args.from_date,
                to_date=args.to_date,
                wait_between_requests=args.wait,
            )
        except Exception as exc:
            logger.error("[%s] 差分データ取得に失敗しました: %s", dataset, exc)
            overall_errors += 1
            continue

        if not records:
            logger.info("[%s] 差分データなし。スキップ。", dataset)
            continue

        # 2. DB適用
        try:
            result = apply_diff(dataset, records, db_path=args.db)
        except Exception as exc:
            logger.error("[%s] DB適用に失敗しました: %s", dataset, exc)
            overall_errors += 1
            continue

        logger.info("  %s", result.summary())
        if result.errors:
            logger.warning("  エラーあり (%d件):", len(result.errors))
            for err in result.errors[:5]:
                logger.warning("    %s", err)
        overall_errors += len(result.errors)

    logger.info("=" * 60)
    status = "完了" if overall_errors == 0 else f"完了（エラー {overall_errors} 件）"
    logger.info("gBizINFO 差分更新 %s", status)
    logger.info("=" * 60)

    if overall_errors > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
