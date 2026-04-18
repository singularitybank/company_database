# -*- coding: utf-8 -*-
"""
国税庁 法人番号公表サイト 差分API フェッチモジュール

[API仕様]
  エンドポイント: https://api.houjin-bangou.nta.go.jp/4/diff
  パラメータ:
    id      : アプリケーションID
    from    : 取得開始日 (YYYY-MM-DD)
    to      : 取得終了日 (YYYY-MM-DD)
    type    : 12 = XML形式 (UTF-8)
    address : 都道府県コード (01〜47, 99=海外)。省略時は全国
    divide  : ページ番号 (divideSize > 1 の場合に使用)

[制約]
  - 1リクエストの最大取得件数: 2,000件
  - count > 2,000 の場合は address 別に分割して取得する

[戻り値]
  correct=0 かつ latest=1 のレコードのみを返す（各法人の現在の最新状態）
"""

import logging
import sys
import time
import xml.etree.ElementTree as ET
from datetime import date, timedelta
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.config import get_nta_app_id
from src.models.schema import COLUMN_MAP

# ---------------------------------------------------------------------------
# 定数
# ---------------------------------------------------------------------------

API_BASE_URL = "https://api.houjin-bangou.nta.go.jp/4/diff"
MAX_RECORDS_PER_PAGE = 2000
API_TIMEOUT = 30  # HTTP タイムアウト（秒）

# 都道府県コード (01〜47 + 99:海外)
ALL_ADDRESS_CODES = [f"{i:02d}" for i in range(1, 48)] + ["99"]

# ---------------------------------------------------------------------------
# ロガー
# ---------------------------------------------------------------------------

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 内部ユーティリティ
# ---------------------------------------------------------------------------

def _fetch_page(
    app_id: str,
    from_date: str,
    to_date: str,
    address: str,
    divide: int = 1,
    retry: int = 3,
    backoff: float = 2.0,
) -> ET.Element:
    """差分APIの1ページ分を取得し、XMLルート要素を返す。

    Args:
        app_id:    アプリケーションID
        from_date: 取得開始日 (YYYY-MM-DD)
        to_date:   取得終了日 (YYYY-MM-DD)
        address:   都道府県コード
        divide:    ページ番号
        retry:     最大リトライ回数
        backoff:   リトライ間隔の基数（秒）。指数バックオフ

    Returns:
        XMLのルート要素 (<corporations>)
    """
    params = {
        "id": app_id,
        "from": from_date,
        "to": to_date,
        "type": "12",
        "address": address,
        "divide": divide,
    }

    last_exc = None
    for attempt in range(retry):
        try:
            resp = requests.get(API_BASE_URL, params=params, timeout=API_TIMEOUT)
            resp.raise_for_status()
            root = ET.fromstring(resp.content)
            return root
        except (requests.RequestException, ET.ParseError) as exc:
            last_exc = exc
            wait = backoff ** attempt
            logger.warning(
                "APIリクエスト失敗 (address=%s, divide=%d, attempt=%d/%d): %s -> %.1f秒後にリトライ",
                address, divide, attempt + 1, retry, exc, wait,
            )
            time.sleep(wait)

    raise RuntimeError(
        f"APIリクエストが {retry} 回失敗しました (address={address}, divide={divide}): {last_exc}"
    )


def _parse_corporations(root: ET.Element) -> list[dict]:
    """<corporations> ルートから各法人レコードを辞書リストに変換する。

    correct=0 かつ latest=1 のレコードのみ返す。
    """
    records = []
    for corp in root.findall("corporation"):
        correct = corp.findtext("correct")
        latest = corp.findtext("latest")

        # 訂正レコード・古い履歴はスキップ
        if correct != "0" or latest != "1":
            continue

        row = {}
        for xml_key, db_key in COLUMN_MAP.items():
            val = corp.findtext(xml_key)
            row[db_key] = val if val else None

        records.append(row)
    return records


# ---------------------------------------------------------------------------
# 公開インターフェース
# ---------------------------------------------------------------------------

def fetch_diff(
    from_date: "str | date",
    to_date: "str | date",
    address_codes: "list[str] | None" = None,
    wait_between_requests: float = 1.0,
) -> list[dict]:
    """差分APIから指定期間の変更法人を取得する。

    1アドレスあたりの件数が 2,000 件を超える場合は自動でページネーションを行う。
    都道府県コード別に分割してリクエストする。

    Args:
        from_date:             取得開始日
        to_date:               取得終了日
        address_codes:         都道府県コードのリスト。None の場合は全都道府県+海外
        wait_between_requests: リクエスト間の待機秒数（サーバー負荷軽減）

    Returns:
        法人情報の辞書リスト（correct=0 & latest=1 のみ）
    """
    if isinstance(from_date, date):
        from_date = from_date.strftime("%Y-%m-%d")
    if isinstance(to_date, date):
        to_date = to_date.strftime("%Y-%m-%d")

    app_id = get_nta_app_id()
    targets = address_codes if address_codes is not None else ALL_ADDRESS_CODES
    all_records: list[dict] = []

    logger.info("差分取得開始: %s 〜 %s (アドレス数: %d)", from_date, to_date, len(targets))

    for address in targets:
        # まず1ページ目を取得して総件数・ページ数を確認
        root = _fetch_page(app_id, from_date, to_date, address, divide=1)
        count = int(root.findtext("count") or 0)
        divide_size = int(root.findtext("divideSize") or 1)

        logger.info("  address=%s: %d件 (%dページ)", address, count, divide_size)

        if count == 0:
            time.sleep(wait_between_requests)
            continue

        # 1ページ目のレコードを追加
        all_records.extend(_parse_corporations(root))
        time.sleep(wait_between_requests)

        # 2ページ目以降
        for page in range(2, divide_size + 1):
            root = _fetch_page(app_id, from_date, to_date, address, divide=page)
            all_records.extend(_parse_corporations(root))
            logger.info("    ページ %d/%d 取得完了", page, divide_size)
            time.sleep(wait_between_requests)

    # 同期間内で同一法人に複数の変更があった場合、updateDate が最新のものを優先
    deduped = _dedup_by_corporate_number(all_records)

    logger.info(
        "差分取得完了: 総レコード=%d件 → 重複除去後=%d件",
        len(all_records), len(deduped),
    )
    return deduped


def _dedup_by_corporate_number(records: list[dict]) -> list[dict]:
    """同一 corporate_number が複数ある場合、updateDate が最新のものだけ残す。

    都道府県コードをまたいで取得した場合や、短期間内に複数変更があった場合に対応。
    """
    latest: dict[str, dict] = {}
    for row in records:
        corp_num = row["corporate_number"]
        if corp_num is None:
            continue
        if corp_num not in latest:
            latest[corp_num] = row
        else:
            existing_date = latest[corp_num].get("update_date") or ""
            new_date = row.get("update_date") or ""
            if new_date > existing_date:
                latest[corp_num] = row
    return list(latest.values())


# ---------------------------------------------------------------------------
# エントリーポイント（単体動作確認用）
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    from src.logging_setup import setup_logging
    setup_logging()

    parser = argparse.ArgumentParser(description="国税庁 差分APIフェッチ動作確認")
    parser.add_argument("--from-date", default=str(date.today() - timedelta(days=1)), help="取得開始日 (YYYY-MM-DD)")
    parser.add_argument("--to-date",   default=str(date.today()),                     help="取得終了日 (YYYY-MM-DD)")
    parser.add_argument("--address",   default="13",                                   help="都道府県コード (例: 13=東京)")
    args = parser.parse_args()

    records = fetch_diff(args.from_date, args.to_date, address_codes=[args.address])
    print(f"\n取得レコード数: {len(records)}")
    for r in records[:3]:
        print(r)
