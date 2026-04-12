# -*- coding: utf-8 -*-
"""
国税庁 差分データ 取得・DB適用スクリプト

使い方:
  python scripts/run_nta_diff.py                        # 昨日分の全国差分を取得
  python scripts/run_nta_diff.py --from 2026-04-01      # 指定日から今日まで
  python scripts/run_nta_diff.py --from 2026-04-01 --to 2026-04-10
  python scripts/run_nta_diff.py --address 13           # 東京都のみ
  python scripts/run_nta_diff.py --address 13 14        # 複数都道府県
"""

import argparse
import logging
import sys
from datetime import date, timedelta
from pathlib import Path

# プロジェクトルートをパスに追加
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from src.extractors.nta_diff_collector import ALL_ADDRESS_CODES, fetch_diff
from src.logging_setup import setup_logging
from src.processors.diff_processor import apply_diff


# ---------------------------------------------------------------------------
# メイン
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="国税庁 差分データ取得・DB適用")
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
        "--address", dest="address", nargs="+", default=None,
        metavar="CODE",
        help="都道府県コード (例: 13 14 27)。省略時は全国 (01〜47+99)",
    )
    parser.add_argument(
        "--db",
        default=str(BASE_DIR / "data" / "companies.db"),
        help="SQLiteファイルパス",
    )
    parser.add_argument(
        "--wait", type=float, default=1.0,
        help="APIリクエスト間の待機秒数 (デフォルト: 1.0秒)",
    )
    args = parser.parse_args()

    setup_logging(BASE_DIR / "logs", filename_prefix="nta_diff")
    logger = logging.getLogger(__name__)

    address_codes = args.address if args.address else ALL_ADDRESS_CODES

    logger.info("=" * 60)
    logger.info("国税庁 差分更新 開始")
    logger.info("  期間    : %s 〜 %s", args.from_date, args.to_date)
    logger.info("  対象    : %d都道府県", len(address_codes))
    logger.info("  DB      : %s", args.db)
    logger.info("=" * 60)

    # 1. 差分データ取得
    try:
        records = fetch_diff(
            from_date=args.from_date,
            to_date=args.to_date,
            address_codes=address_codes,
            wait_between_requests=args.wait,
        )
    except Exception as exc:
        logger.error("差分データ取得に失敗しました: %s", exc)
        sys.exit(1)

    if not records:
        logger.info("差分データなし。処理を終了します。")
        return

    # 2. DB適用
    try:
        result = apply_diff(records, db_path=args.db)
    except Exception as exc:
        logger.error("DB適用に失敗しました: %s", exc)
        sys.exit(1)

    logger.info("=" * 60)
    logger.info("国税庁 差分更新 完了")
    logger.info("  %s", result.summary())
    if result.errors:
        logger.warning("  エラーあり (%d件):", len(result.errors))
        for err in result.errors[:10]:
            logger.warning("    %s", err)
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
