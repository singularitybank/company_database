# -*- coding: utf-8 -*-
"""
Created on Sun Apr 12 11:35:59 2026

@author: singu
"""

import pandas as pd

df_nta = pd.read_parquet("data/staging/nta_20260331.parquet")
df_gbiz_Kihonjoho = pd.read_csv("data/raw/gbizinfo/Kihonjoho_UTF-8.csv",encoding="utf-8")
df_gbiz_Kihonjoho["法人番号"] = df_gbiz_Kihonjoho["法人番号"].astype(str)
# 差分を取るために、共通のキーを指定してマージ
df_diff = pd.merge(
    df_nta[["corporateNumber"]], 
    df_gbiz_Kihonjoho[["法人番号"]], 
    left_on="corporateNumber",
    right_on="法人番号",
    how="outer", indicator=True)