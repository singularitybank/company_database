# -*- coding: utf-8 -*-
"""
gbizinfo 決算情報 XML → Parquet 変換スクリプト

入力 : data/raw/gbizinfo/Kessanjoho/*.xml  (22,969ファイル)
出力 : data/staging/gbizinfo/
         kessanjoho_core.parquet  - 明細レベル（1行 = 1勘定科目）
         kessanjoho_meta.parquet  - ドキュメントレベル（1行 = 1XMLファイル）

XML構造:
  FinancialInformation
  ├── KanpouPostedInformation
  │   ├── DataName, Status, IssueDate, Classification, Number, Page
  ├── CorporateInformation
  │   ├── Period, Release, CompanyName, Unit, CorporateNumber
  ├── Report (複数)
  │   └── ReportName[@表名]
  │       └── BsPlDate[@日付]
  │           └── Division[@部] (複数)
  │               └── Meisai (複数)
  │                   ├── Subject
  │                   └── Amount
  └── Metadata
      ├── KeyField, DataQuality, Source, ImportFrequency
      ├── LastAcquisitionDate, LastUpdateDate

使い方:
  python src/converters/kessanjoho_to_parquet.py
"""

import json
import logging
import sys
from pathlib import Path
from xml.etree import ElementTree as ET

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

REPO_ROOT   = Path(__file__).resolve().parents[2]
XML_DIR     = REPO_ROOT / "data/raw/gbizinfo/Kessanjoho"
STAGING_DIR = REPO_ROOT / "data/staging/gbizinfo"
COMPRESSION = "snappy"
BATCH_SIZE  = 500       # Parquetへの書き出しバッチ（行数ではなくファイル数）

sys.path.insert(0, str(REPO_ROOT))
from src.common.date_utils import normalize_jp_date

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 1ファイルのパース
# ---------------------------------------------------------------------------

def parse_xml(filepath: Path) -> tuple[list[dict], dict | None]:
    """
    XMLを解析して (core_rows, meta_row) を返す。
    core_rows: 明細レベルの行リスト
    meta_row : メタデータ行（1ファイル1行）
    """
    try:
        tree = ET.parse(filepath)
    except ET.ParseError as e:
        log.warning("XMLパースエラー: %s (%s)", filepath.name, e)
        return [], None

    root = tree.getroot()
    filename = filepath.name

    # ---- ファイル名から corporate_number を取得 ----
    corporate_number = filename.split("_")[0]

    # ---- 官報掲載情報 ----
    kanpou = root.find("KanpouPostedInformation")
    def _text(el, tag):
        node = el.find(tag) if el is not None else None
        return (node.text or "").strip() or None if node is not None else None

    kanpou_data_name      = _text(kanpou, "DataName")
    kanpou_status         = _text(kanpou, "Status")
    kanpou_issue_date     = normalize_jp_date(_text(kanpou, "IssueDate"))
    kanpou_classification = _text(kanpou, "Classification")
    kanpou_number         = _text(kanpou, "Number")
    kanpou_page           = _text(kanpou, "Page")

    # ---- 法人情報 ----
    corp = root.find("CorporateInformation")
    period            = _text(corp, "Period")
    release_date      = normalize_jp_date(_text(corp, "Release"))
    company_name      = _text(corp, "CompanyName")
    unit              = _text(corp, "Unit")
    corporate_number  = _text(corp, "CorporateNumber") or corporate_number

    # ---- 明細行の生成 ----
    core_rows: list[dict] = []
    for report in root.findall("Report"):
        for report_name_el in report:
            report_name = report_name_el.attrib.get("表名", "").strip() or None
            for bs_pl_date_el in report_name_el:
                bs_pl_date = bs_pl_date_el.attrib.get("日付", "").strip() or None
                for division_el in bs_pl_date_el:
                    division = division_el.attrib.get("部", "").strip() or None
                    for meisai in division_el.findall("Meisai"):
                        subject = _text(meisai, "Subject")
                        amount  = _text(meisai, "Amount")
                        core_rows.append({
                            "corporate_number":      corporate_number,
                            "company_name":          company_name,
                            "period":                period,
                            "release_date":          release_date,
                            "unit":                  unit,
                            "kanpou_data_name":      kanpou_data_name,
                            "kanpou_status":         kanpou_status,
                            "kanpou_issue_date":     kanpou_issue_date,
                            "kanpou_classification": kanpou_classification,
                            "kanpou_number":         kanpou_number,
                            "kanpou_page":           kanpou_page,
                            "report_name":           report_name,
                            "bs_pl_date":            bs_pl_date,
                            "division":              division,
                            "subject":               subject,
                            "amount":                amount,
                            "source_filename":       filename,
                        })

    # ---- メタデータ ----
    meta_el = root.find("Metadata")
    meta_row = None
    if meta_el is not None:
        meta_dict = {
            child.tag: (child.text or "").strip()
            for child in meta_el
            if (child.text or "").strip()
        }
        meta_row = {
            "corporate_number": corporate_number,
            "kanpou_data_name": kanpou_data_name,
            "metadata":         json.dumps(meta_dict, ensure_ascii=False),
        }

    return core_rows, meta_row


# ---------------------------------------------------------------------------
# メイン
# ---------------------------------------------------------------------------

def main() -> None:
    from src.common.logging_setup import setup_logging
    setup_logging()

    xml_files = sorted(XML_DIR.glob("*.xml"))
    total = len(xml_files)
    log.info("XMLファイル数: %d", total)

    STAGING_DIR.mkdir(parents=True, exist_ok=True)
    core_out = STAGING_DIR / "kessanjoho_core.parquet"
    meta_out = STAGING_DIR / "kessanjoho_meta.parquet"

    core_writer: pq.ParquetWriter | None = None
    meta_writer: pq.ParquetWriter | None = None

    core_buffer: list[dict] = []
    meta_buffer: list[dict] = []
    total_core_rows = 0
    total_meta_rows = 0

    import time
    start = time.time()

    for i, xml_path in enumerate(xml_files, 1):
        core_rows, meta_row = parse_xml(xml_path)
        core_buffer.extend(core_rows)
        if meta_row:
            meta_buffer.append(meta_row)

        # バッチ書き出し
        if i % BATCH_SIZE == 0 or i == total:
            if core_buffer:
                core_df = pd.DataFrame(core_buffer)
                if core_writer is None:
                    core_schema = pa.Schema.from_pandas(core_df)
                    core_writer = pq.ParquetWriter(core_out, core_schema, compression=COMPRESSION)
                core_writer.write_table(pa.Table.from_pandas(core_df, schema=core_schema))
                total_core_rows += len(core_buffer)
                core_buffer = []

            if meta_buffer:
                meta_df = pd.DataFrame(meta_buffer)
                if meta_writer is None:
                    meta_schema = pa.Schema.from_pandas(meta_df)
                    meta_writer = pq.ParquetWriter(meta_out, meta_schema, compression=COMPRESSION)
                meta_writer.write_table(pa.Table.from_pandas(meta_df, schema=meta_schema))
                total_meta_rows += len(meta_buffer)
                meta_buffer = []

            elapsed = time.time() - start
            log.info("  %5d / %d ファイル処理済 | 明細 %d行 | %.1fs",
                     i, total, total_core_rows, elapsed)

    if core_writer:
        core_writer.close()
    if meta_writer:
        meta_writer.close()

    elapsed = time.time() - start
    core_mb = core_out.stat().st_size / 1024 / 1024
    meta_mb = meta_out.stat().st_size / 1024 / 1024
    log.info("完了: 明細 %d行 | メタ %d行 | %.1fs | core %.1fMB | meta %.1fMB",
             total_core_rows, total_meta_rows, elapsed, core_mb, meta_mb)


if __name__ == "__main__":
    main()
