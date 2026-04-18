#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Hellowork 詳細ページ requests vs Selenium 検証スクリプト

- 既存HTMLから求人番号を1件ピックアップ
- requests で同一URLを取得してみる
- 取得できた場合、主要フィールドの一致を確認する
"""
import sys
import os
import re
import requests
from pathlib import Path
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.config import hellowork as _cfg

# ---------------------------------------------------------------------------
# 設定
# ---------------------------------------------------------------------------
PAGE_URL = (
    "https://www.hellowork.mhlw.go.jp/kensaku/GECA110010.do"
    "?screenId=GECA110010&action=dispDetailBtn&kJNo={job_number}&kJKbn={kyujintype}"
)

# 検証対象の求人番号（既存HTMLから自動選択）
HTML_BASE = Path(_cfg["html_dir"])

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ja,en-US;q=0.7,en;q=0.3",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}

# ---------------------------------------------------------------------------
# ヘルパー
# ---------------------------------------------------------------------------

def find_latest_html_dir() -> Path | None:
    """最新日付のHTMLディレクトリを返す。"""
    dirs = sorted(HTML_BASE.glob("2*"), reverse=True)
    return dirs[0] if dirs else None


def extract_fields(soup: BeautifulSoup) -> dict:
    """主要フィールドをIDで取得。"""
    ids = [
        "ID_kjNo",         # 求人番号
        "ID_jgshMei",      # 事業所名
        "ID_sksu",         # 職種
        "ID_koyoKeitai",   # 雇用形態
        "ID_chgn",         # 賃金
        "ID_shgJn1",       # 就業時間1
        "ID_kyjs",         # 休日等
        "ID_shkiKigenHi",  # 受付期限日
    ]
    result = {}
    for id_ in ids:
        tag = soup.find(id=id_)
        result[id_] = tag.get_text(separator=" ", strip=True) if tag else None
    return result


def compare(fields_selenium: dict, fields_requests: dict) -> bool:
    """2つのフィールド辞書を比較して結果を出力する。True = 完全一致。"""
    all_match = True
    for key in fields_selenium:
        v_sel = fields_selenium[key]
        v_req = fields_requests.get(key)
        match = v_sel == v_req
        if not match:
            all_match = False
        status = "OK " if match else "NG "
        print(f"  {status} {key}")
        if not match:
            print(f"       Selenium  : {repr(v_sel)}")
            print(f"       requests  : {repr(v_req)}")
    return all_match


# ---------------------------------------------------------------------------
# メイン
# ---------------------------------------------------------------------------

def main():
    html_dir = find_latest_html_dir()
    if not html_dir:
        print("[ERROR] HTMLディレクトリが見つかりません:", HTML_BASE)
        sys.exit(1)

    html_files = list(html_dir.glob("*.html"))
    if not html_files:
        print("[ERROR] HTMLファイルが見つかりません:", html_dir)
        sys.exit(1)

    # 検証件数（デフォルト3件）
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 3
    targets = html_files[:n]

    print(f"検証対象ディレクトリ: {html_dir}")
    print(f"検証件数: {len(targets)}\n")

    session = requests.Session()
    session.headers.update(HEADERS)

    total_ok = 0
    total_ng = 0

    for html_path in targets:
        job_number = html_path.stem  # ファイル名（拡張子なし）= 求人番号
        # kyujintype は求人番号先頭2桁から推定（簡易）
        kyujintype = 1
        url = PAGE_URL.format(job_number=job_number, kyujintype=kyujintype)

        print(f"{'='*60}")
        print(f"求人番号: {job_number}")
        print(f"URL     : {url}")

        # --- Selenium で保存済みの HTML を解析 ---
        with open(html_path, encoding="utf-8") as f:
            soup_sel = BeautifulSoup(f.read(), "html.parser")
        fields_sel = extract_fields(soup_sel)

        # --- requests で取得 ---
        try:
            resp = session.get(url, timeout=15)
            http_status = resp.status_code
            print(f"HTTP    : {http_status}")
        except Exception as e:
            print(f"[ERROR] requests 取得失敗: {e}")
            total_ng += 1
            continue

        if http_status != 200:
            print(f"[SKIP] HTTPステータス {http_status} のためスキップ")
            total_ng += 1
            continue

        soup_req = BeautifulSoup(resp.content, "html.parser")
        fields_req = extract_fields(soup_req)

        # --- ID_kjNo が取得できているか確認 ---
        if fields_req.get("ID_kjNo") is None:
            print("[NG] ID_kjNo が見つかりません — 詳細情報が含まれていない可能性があります")
            # ページタイトルとbodyの一部を表示
            title = soup_req.title.get_text(strip=True) if soup_req.title else "(no title)"
            print(f"    ページタイトル: {title}")
            # エラーメッセージや警告を探す
            for cls in ["error", "alert", "warning", "danger"]:
                msgs = soup_req.find_all(class_=re.compile(cls, re.I))
                for m in msgs[:2]:
                    txt = m.get_text(separator=" ", strip=True)
                    if txt:
                        print(f"    [{cls}] {txt[:120]}")
            total_ng += 1
            continue

        # --- フィールド比較 ---
        ok = compare(fields_sel, fields_req)
        if ok:
            print("  => 全フィールド一致")
            total_ok += 1
        else:
            print("  => 差異あり")
            total_ng += 1

    print(f"\n{'='*60}")
    print(f"結果: {total_ok} 件一致 / {total_ng} 件不一致 (計 {total_ok+total_ng} 件)")


if __name__ == "__main__":
    main()
