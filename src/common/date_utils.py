# -*- coding: utf-8 -*-
"""
日付変換ユーティリティ

[提供する関数]
  normalize_iso_date  - ISO8601タイムゾーン付き文字列 → YYYY-MM-DD（Pandas Series）
  normalize_jp_date   - "YYYY年M月D日" 形式の文字列   → YYYY-MM-DD（str）
"""
import re

import pandas as pd

# ---------------------------------------------------------------------------
# ISO8601 → YYYY-MM-DD（Pandas Series）
# ---------------------------------------------------------------------------

def normalize_iso_date(series: pd.Series) -> pd.Series:
    """ISO8601タイムゾーン付き日付文字列を YYYY-MM-DD に正規化する。

    例: "2024-03-31T00:00:00+09:00" → "2024-03-31"
        "2024-03-31"                → "2024-03-31"（変化なし）
        ""                          → pd.NA

    Args:
        series: 文字列型の Pandas Series

    Returns:
        正規化後の Pandas Series
    """
    return (
        series
        .str.strip()
        .str.replace(r"T\d{2}:\d{2}:\d{2}[+\-]\d{2}:\d{2}$", "", regex=True)
        .replace("", pd.NA)
    )


# ---------------------------------------------------------------------------
# 日本語日付 → YYYY-MM-DD（str）
# ---------------------------------------------------------------------------

_JP_DATE_RE = re.compile(r"(\d{4})年(\d{1,2})月(\d{1,2})日")


def normalize_jp_date(s: "str | None") -> "str | None":
    """日本語日付文字列 "YYYY年M月D日" を YYYY-MM-DD に変換する。

    例: "2024年3月31日" → "2024-03-31"
        "令和6年3月31日" のような非数字年は変換せずそのまま返す
        None / 空文字    → None

    Args:
        s: 変換対象の文字列（None 可）

    Returns:
        変換後の文字列。変換できない場合は入力値をそのまま返す
    """
    if not s:
        return None
    m = _JP_DATE_RE.search(s)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    return s.strip() or None
