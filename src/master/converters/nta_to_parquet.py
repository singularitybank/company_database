# -*- coding: utf-8 -*-
"""
国税庁 法人番号公表サイト 全件ダウンロードCSV → Parquet 変換モジュール

[入力]  data/raw/nta/     - ヘッダーなし・30カラムのCSV（月末全件ダウンロード）
[出力]  data/staging/     - ヘッダー付き Parquet（nta_YYYYMMDD.parquet）

[カラム定義の根拠]
  CSVは no.7(sequenceNumber) 〜 no.36(hihyoji) の30フィールドが並ぶ

[使い方（単体実行）]
  python src/converters/nta_to_parquet.py               # 最新CSVを変換
  python src/converters/nta_to_parquet.py --csv path/to/file.csv
"""
import argparse
import csv
import logging
import re
import sys
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

# ---------------------------------------------------------------------------
# 定数
# ---------------------------------------------------------------------------

# CSV全カラム（no.7〜36、download=trueの順）
ALL_COLUMNS = [
    "sequenceNumber",           # 一連番号
    "corporateNumber",          # 法人番号
    "process",                  # 処理区分
    "correct",                  # 訂正区分
    "updateDate",               # 更新年月日
    "changeDate",               # 変更年月日
    "name",                     # 商号又は名称
    "nameImageId",              # 商号イメージID
    "kind",                     # 法人種別
    "prefectureName",           # 都道府県
    "cityName",                 # 市区町村
    "streetNumber",             # 丁目番地等
    "addressImageId",           # 国内所在地イメージID
    "prefectureCode",           # 都道府県コード
    "cityCode",                 # 市区町村コード
    "postCode",                 # 郵便番号
    "addressOutside",           # 国外所在地
    "addressOutsideImageId",    # 国外所在地イメージID
    "closeDate",                # 閉鎖年月日
    "closeCause",               # 閉鎖事由
    "successorCorporateNumber", # 承継先法人番号
    "changeCause",              # 変更事由の詳細
    "assignmentDate",           # 法人番号指定年月日
    "latest",                   # 最新履歴
    "enName",                   # 商号（英語表記）
    "enPrefectureName",         # 都道府県（英語）
    "enCityName",               # 市区町村丁目（英語）
    "enAddressOutside",         # 国外所在地（英語）
    "furigana",                 # フリガナ
    "hihyoji",                  # 検索対象除外
]

# チャンクサイズ（行数）。メモリ使用量の調整に使用
CHUNK_SIZE = 200_000

# 処理区分コードの意味
PROCESS_CODE = {
    "01": "新規",
    "11": "商号又は名称の変更",
    "12": "国内所在地の変更",
    "13": "国外所在地の変更",
    "21": "登記記録の閉鎖等",
    "22": "登記記録の復活等",
    "71": "吸収合併",
    "72": "吸収合併無効",
    "81": "商号の登記の抹消",
    "99": "削除",
}

# 法人種別コードの意味
KIND_CODE = {
    "101": "国の機関",
    "201": "地方公共団体",
    "301": "株式会社",
    "302": "有限会社",
    "303": "合名会社",
    "304": "合資会社",
    "305": "合同会社",
    "399": "その他の設立登記法人",
    "401": "外国会社等",
    "499": "その他",
}

# 閉鎖事由コードの意味
CLOSE_CAUSE_CODE = {
    "01": "清算の結了等",
    "11": "合併による解散等",
    "21": "登記官による閉鎖",
    "31": "その他の清算の結了等",
}

# ---------------------------------------------------------------------------
# ロガー
# ---------------------------------------------------------------------------

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# ユーティリティ
# ---------------------------------------------------------------------------

def _extract_date_from_filename(filepath: Path) -> str:
    """ファイル名から YYYYMMDD 形式の日付を抽出する。

    例: 00_zenkoku_all_20260331.csv → "20260331"
    見つからない場合はファイルのstem全体を返す。
    """
    match = re.search(r"(\d{8})", filepath.stem)
    return match.group(1) if match else filepath.stem


# ---------------------------------------------------------------------------
# 変換処理
# ---------------------------------------------------------------------------

def convert_raw_to_staging(
    raw_path: "str | Path",
    staging_dir: "str | Path",
    chunk_size: int = CHUNK_SIZE,
) -> Path:
    """全件ダウンロードCSVを読み込み、Parquetファイルに変換して保存する。

    Args:
        raw_path:    入力CSVファイルパス（ヘッダーなし）
        staging_dir: 出力先ディレクトリ
        chunk_size:  チャンクサイズ（行数）。メモリ使用量の調整に使用

    Returns:
        出力 Parquet ファイルの Path
    """
    raw_path = Path(raw_path)
    staging_dir = Path(staging_dir)
    staging_dir.mkdir(parents=True, exist_ok=True)

    date_str = _extract_date_from_filename(raw_path)
    out_path = staging_dir / f"nta_{date_str}.parquet"

    logger.info("変換開始: %s → %s", raw_path.name, out_path.name)
    logger.info("チャンクサイズ: %d行", chunk_size)

    chunks = []
    total_rows = 0

    reader = pd.read_csv(
        raw_path,
        header=None,
        names=ALL_COLUMNS,
        encoding="utf-8",
        dtype=str,          # 全カラムをstrで読み込む（法人番号の先頭ゼロ保持）
        chunksize=chunk_size,
    )

    for i, chunk in enumerate(reader):
        chunks.append(chunk)
        total_rows += len(chunk)
        if (i + 1) % 5 == 0:
            logger.info("  読み込み済み: %d行", total_rows)

    logger.info("全チャンク読み込み完了: %d行", total_rows)

    df = pd.concat(chunks, ignore_index=True)
    df.to_parquet(out_path, index=False, engine="pyarrow")

    logger.info("Parquet書き込み完了: %s", out_path)
    return out_path


def load_staging(staging_path: "str | Path") -> pd.DataFrame:
    """Staging の Parquet ファイルを読み込んで返す。"""
    return pd.read_parquet(staging_path, engine="pyarrow")


def summarize(df: pd.DataFrame) -> None:
    """データの概要をログに出力する（動作確認用）。"""
    logger.info("=== データ概要 ===")
    logger.info("総件数: %d", len(df))
    logger.info("カラム: %s", df.columns.tolist())

    kind_counts = (
        df["kind"]
        .map(KIND_CODE)
        .fillna("不明")
        .value_counts()
    )
    logger.info("法人種別:\n%s", kind_counts.to_string())

    closed = df["closeDate"].notna() & (df["closeDate"] != "")
    logger.info("閉鎖法人数: %d", closed.sum())
    logger.info("アクティブ法人数: %d", (~closed).sum())


# ---------------------------------------------------------------------------
# エントリーポイント（CSV → Parquet 変換のみ）
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    from src.common.logging_setup import setup_logging
    setup_logging()

    parser = argparse.ArgumentParser(description="国税庁CSV → Parquet 変換")
    parser.add_argument("--csv", dest="csv_path", default=None,
                        help="入力CSVファイルパス。省略時は data/raw/nta/ の最新ファイルを使用")
    parser.add_argument("--staging-dir", default=str(REPO_ROOT / "data" / "staging"),
                        help="Parquet出力先ディレクトリ")
    args = parser.parse_args()

    if args.csv_path:
        csv_path = Path(args.csv_path)
    else:
        raw_dir = REPO_ROOT / "data" / "raw" / "nta"
        csv_files = sorted(raw_dir.glob("*.csv"))
        if not csv_files:
            logger.error("rawディレクトリにCSVファイルが見つかりません: %s", raw_dir)
            raise SystemExit(1)
        csv_path = csv_files[-1]
        logger.info("対象ファイル: %s", csv_path.name)

    parquet_path = convert_raw_to_staging(csv_path, args.staging_dir)
    df = load_staging(parquet_path)
    summarize(df)
