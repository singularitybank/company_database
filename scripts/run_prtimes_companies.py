# -*- coding: utf-8 -*-
"""
PR Times 企業ページ スクレイピング バッチ

[実行フロー]
  STEP 1: prtimes_companies から scraped_at IS NULL の企業を取得してHTMLを保存
  STEP 2: 保存済みHTMLをパースして企業情報を抽出
  STEP 3: パース結果を prtimes_companies テーブルに UPDATE

[オプション]
  --limit N     : 取得件数上限（省略時=全件）
  --headless    : ブラウザをヘッドレスで起動（デフォルト）
  --no-headless : ブラウザを表示する
  --parse-only  : スクレイピングをスキップしてパース・UPDATE のみ実行
"""

import argparse
import logging
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from src.config import prtimes as _cfg
from src.signals.prtimes.crawlers.company_crawler import scrape_companies, CompanyHtmlResult
from src.signals.prtimes.parsers.company_parser import parse
from src.signals.prtimes.loaders.db_loader import update_companies, init_db
from src.signals.prtimes.models.schema import init_db as _init_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

DB_PATH = REPO_ROOT / _cfg["db_path"]
HTML_COMPANIES = Path(_cfg["html_dir"]) / "companies"


def _load_existing_html_results(db_path: Path) -> list[CompanyHtmlResult]:
    """HTMLが保存済みの全企業の CompanyHtmlResult を生成する（scraped_at 不問）。"""
    import sqlite3
    conn = sqlite3.connect(str(db_path))
    rows = conn.execute(
        "SELECT prtimes_company_id FROM prtimes_companies ORDER BY prtimes_company_id"
    ).fetchall()
    conn.close()

    results = []
    for (cid,) in rows:
        html_file = HTML_COMPANIES / f"{cid}.html"
        if html_file.exists():
            results.append(CompanyHtmlResult(
                prtimes_company_id=cid,
                html_path=str(html_file),
                success=True,
            ))
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="PR Times 企業ページ スクレイピング バッチ")
    parser.add_argument("--limit",       type=int, default=None)
    parser.add_argument("--headless",    action="store_true",  default=True)
    parser.add_argument("--no-headless", action="store_false", dest="headless")
    parser.add_argument("--parse-only",  action="store_true",  default=False)
    args = parser.parse_args()

    _init_db(DB_PATH)

    # ------------------------------------------------------------------
    if not args.parse_only:
        logger.info("=== STEP 1: 企業ページ HTML 取得 ===")
        html_results = scrape_companies(
            db_path=DB_PATH,
            limit=args.limit,
            headless=args.headless,
        )
    else:
        logger.info("=== STEP 1: スキップ（--parse-only）===")
        html_results = _load_existing_html_results(DB_PATH)

    if not html_results:
        logger.info("処理対象なし。終了します。")
        return

    # ------------------------------------------------------------------
    logger.info("=== STEP 2: HTML パース (%d 件) ===", len(html_results))
    parse_results = parse(html_results)

    ok  = sum(1 for r in parse_results if r.success)
    ng  = sum(1 for r in parse_results if not r.success)
    logger.info("パース結果: 成功=%d件, 失敗=%d件", ok, ng)

    # ------------------------------------------------------------------
    logger.info("=== STEP 3: DB UPDATE ===")
    updated, errors = update_companies(parse_results, db_path=DB_PATH)
    logger.info("UPDATE 完了: 更新=%d件, エラー=%d件", updated, errors)

    logger.info("=== バッチ完了 ===")


if __name__ == "__main__":
    main()
