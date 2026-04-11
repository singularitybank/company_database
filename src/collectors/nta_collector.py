# -*- coding: utf-8 -*-
"""
国税庁 法人番号公表サイト 全件ダウンロードCSV 前処理モジュール

[入力]  data/raw/      - ヘッダーなし・30カラムのCSV（月末全件ダウンロード）
[中間]  data/staging/  - ヘッダー付き・21カラムのParquet
[出力]  data/companies.db（companiesテーブル）

[カラム定義の根拠]
  definition_rawdata_nta.json の "download": true の項目を順番に対応
  CSVはno.7(sequenceNumber)〜no.36(hihyoji)の30フィールドが並ぶ
"""
import argparse
import logging
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.models.schema import COLUMN_MAP, init_db

# ---------------------------------------------------------------------------
# 定数
# ---------------------------------------------------------------------------

# CSV全カラム（no.7〜36、download=trueの順）
ALL_COLUMNS = [
    "sequenceNumber",          # 一連番号
    "corporateNumber",         # 法人番号
    "process",                 # 処理区分
    "correct",                 # 訂正区分
    "updateDate",              # 更新年月日
    "changeDate",              # 変更年月日
    "name",                    # 商号又は名称
    "nameImageId",             # 商号イメージID
    "kind",                    # 法人種別
    "prefectureName",          # 都道府県
    "cityName",                # 市区町村
    "streetNumber",            # 丁目番地等
    "addressImageId",          # 国内所在地イメージID
    "prefectureCode",          # 都道府県コード
    "cityCode",                # 市区町村コード
    "postCode",                # 郵便番号
    "addressOutside",          # 国外所在地
    "addressOutsideImageId",   # 国外所在地イメージID
    "closeDate",               # 閉鎖年月日
    "closeCause",              # 閉鎖事由
    "successorCorporateNumber",# 承継先法人番号
    "changeCause",             # 変更事由の詳細
    "assignmentDate",          # 法人番号指定年月日
    "latest",                  # 最新履歴
    "enName",                  # 商号（英語表記）
    "enPrefectureName",        # 都道府県（英語）
    "enCityName",              # 市区町村丁目（英語）
    "enAddressOutside",        # 国外所在地（英語）
    "furigana",                # フリガナ
    "hihyoji",                 # 検索対象除外
]

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
# ロガー設定
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# ユーティリティ
# ---------------------------------------------------------------------------

def _extract_date_from_filename(filepath: Path) -> str:
    """ファイル名からYYYYMMDD形式の日付を抽出する。
    例: 00_zenkoku_all_20260331.csv → 20260331
    見つからない場合はファイルのstem全体を返す。
    """
    match = re.search(r"(\d{8})", filepath.stem)
    return match.group(1) if match else filepath.stem


# ---------------------------------------------------------------------------
# メイン処理
# ---------------------------------------------------------------------------

def convert_raw_to_staging(
    raw_path: str | Path,
    staging_dir: str | Path,
    chunk_size: int = 200_000,
) -> Path:
    """全件ダウンロードCSVを読み込み、Parquetファイルに変換して保存する。

    Args:
        raw_path:    入力CSVファイルパス（ヘッダーなし）
        staging_dir: 出力先ディレクトリ
        chunk_size:  チャンクサイズ（行数）。メモリ使用量の調整に使用

    Returns:
        出力ParquetファイルのPath
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


def load_staging(staging_path: str | Path) -> pd.DataFrame:
    """StagingのParquetファイルを読み込んで返す。"""
    return pd.read_parquet(staging_path, engine="pyarrow")


# ---------------------------------------------------------------------------
# DB投入
# ---------------------------------------------------------------------------

def load_to_db(
    staging_path: str | Path,
    db_path: str | Path,
    chunk_size: int = 50_000,
) -> int:
    """StagingのParquetをSQLite companiesテーブルに一括投入する。

    既存テーブルは全件置き換える（フルリフレッシュ）。
    差分更新は diff_detector モジュールで別途行う。

    Args:
        staging_path: 入力Parquetファイルパス
        db_path:      SQLiteファイルパス
        chunk_size:   to_sql のチャンクサイズ

    Returns:
        投入した行数
    """
    staging_path = Path(staging_path)
    db_path = Path(db_path)

    logger.info("DB投入開始: %s → %s", staging_path.name, db_path.name)

    conn = init_db(db_path)

    # パフォーマンス設定（バルクロード用）
    conn.execute("PRAGMA synchronous = OFF;")
    conn.execute("PRAGMA cache_size = -65536;")  # 64MB

    df = pd.read_parquet(staging_path, engine="pyarrow")

    # カラム名をDB形式に変換（Parquetに存在するカラムのみ選択）
    available = [k for k in COLUMN_MAP if k in df.columns]
    df = df[available].rename(columns={k: COLUMN_MAP[k] for k in available})

    # NaN → None（SQLite の NULL として扱う）
    df = df.where(df.notna(), other=None)

    # 投入タイムスタンプ付与
    loaded_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    df["loaded_at"] = loaded_at

    # 既存データを全削除してから投入（フルリフレッシュ）
    conn.execute("DELETE FROM companies;")
    conn.commit()

    cols = df.columns.tolist()
    placeholders = ", ".join(["?"] * len(cols))
    insert_sql = f"INSERT INTO companies ({', '.join(cols)}) VALUES ({placeholders})"

    total = 0
    cursor = conn.cursor()
    for start in range(0, len(df), chunk_size):
        batch = df.iloc[start : start + chunk_size]
        cursor.executemany(insert_sql, batch.itertuples(index=False, name=None))
        conn.commit()
        total += len(batch)
        logger.info("  投入済み: %d / %d行", total, len(df))

    # インデックス再構築
    conn.execute("ANALYZE;")
    conn.commit()

    # 同期設定を戻す
    conn.execute("PRAGMA synchronous = NORMAL;")
    conn.commit()
    conn.close()

    logger.info("DB投入完了: %d行", total)
    return total


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
# エントリーポイント
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="国税庁データ前処理・DB投入")
    parser.add_argument(
        "--skip-staging",
        action="store_true",
        help="staging変換をスキップし、既存Parquetを使用する",
    )
    args = parser.parse_args()

    BASE_DIR = Path(__file__).resolve().parents[2]  # プロジェクトルート
    RAW_DIR = BASE_DIR / "data" / "raw"
    STAGING_DIR = BASE_DIR / "data" / "staging"
    DB_PATH = BASE_DIR / "data" / "companies.db"

    if args.skip_staging:
        parquet_files = sorted(STAGING_DIR.glob("nta_*.parquet"))
        if not parquet_files:
            logger.error("stagingにParquetファイルが見つかりません: %s", STAGING_DIR)
            raise SystemExit(1)
        parquet_path = parquet_files[-1]
        logger.info("既存Parquetを使用: %s", parquet_path.name)
    else:
        csv_files = sorted(RAW_DIR.glob("*.csv"))
        if not csv_files:
            logger.error("rawディレクトリにCSVファイルが見つかりません: %s", RAW_DIR)
            raise SystemExit(1)
        latest_csv = csv_files[-1]
        logger.info("対象ファイル: %s", latest_csv.name)
        parquet_path = convert_raw_to_staging(latest_csv, STAGING_DIR)

    df = load_staging(parquet_path)
    summarize(df)

    load_to_db(parquet_path, DB_PATH)
