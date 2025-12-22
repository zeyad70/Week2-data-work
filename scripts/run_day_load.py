from pathlib import Path
import sys
import json
from datetime import datetime, timezone
import logging
from turtle import pd

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from bootcamp_data.config import make_paths
from bootcamp_data.io import read_orders_csv, read_users_csv, write_parquet
from bootcamp_data.transforms import enforce_schema


log = logging.getLogger(__name__)

def main() -> None:
    
    pass  

if __name__ == "__main__":
    main()

#run_day_2_week2
def assert_uique_keys(df, key_columns):
    duplicates = df.duplicated(subset=key_columns, keep=False)
    if duplicates.any():
        duplicate_rows = df[duplicates]
        raise ValueError(f"keys found:\n{duplicate_rows}")
    
    
def missingness_report(df):
    n = len(df)
    return(
        df.isna().sum()
        .rename("n_missing")
        .to_frame()
        .assign(p_missing=lambda t: t["n_missing"]/n)
        .sort_values("p_missing",ascending=False)
    )



def add_missing_flag(df, cols):
    out = df.copy()
    for col in cols:
        flag_col = f"{col}_missing"
        out[flag_col] = out[col].isna()
    return out



def square(x):
    return x * x

squared_value = square(5)
print(f"Squared value: {squared_value}")

vals = np.array([1, 2, 3, 5])
print(np.square(vals))      

import pandas as pd
s = pd.Series([1, 2, 3, 5])
print(s ** 2)      