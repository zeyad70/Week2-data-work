import pandas as pd

# -----------------------------
# Task 1 — DateTime Transforms
# -----------------------------
def parse_datetime(
    df: pd.DataFrame,
    col: str,
    *,
    utc: bool = True,
) -> pd.DataFrame:
    """
    Parse a timestamp column safely.
    - Invalid strings become NA (errors="coerce")
    - utc=True gives timezone-aware timestamps
    """
    dt = pd.to_datetime(df[col], errors="coerce", utc=utc)
    return df.assign(**{col: dt})


def add_time_parts(df: pd.DataFrame, ts_col: str) -> pd.DataFrame:
    """
    Add common time grouping columns from a timestamp column.
    """
    ts = df[ts_col]
    return df.assign(
        date=ts.dt.date,
        year=ts.dt.year,
        month=ts.dt.to_period("M").astype("string"),
        dow=ts.dt.day_name(),
        hour=ts.dt.hour,
    )


# -----------------------------
# Task 2 — Outlier Helpers
# -----------------------------
def iqr_bounds(s: pd.Series, k: float = 1.5) -> tuple[float, float]:
    """
    Return (lo, hi) IQR bounds for outlier flagging.
    """
    x = s.dropna()
    q1 = x.quantile(0.25)
    q3 = x.quantile(0.75)
    iqr = q3 - q1
    return float(q1 - k * iqr), float(q3 + k * iqr)


def winsorize(s: pd.Series, lo: float = 0.01, hi: float = 0.99) -> pd.Series:
    """
    Cap values to [p_lo, p_hi] (helpful for visualization, not deletion).
    """
    x = s.dropna()
    a, b = x.quantile(lo), x.quantile(hi)
    return s.clip(lower=a, upper=b)


# -----------------------------
# Task 3 — Optional Outlier Flag
# -----------------------------
def add_outlier_flag(df: pd.DataFrame, col: str, *, k: float = 1.5) -> pd.DataFrame:
    """
    Add a boolean column indicating which values are outliers
    based on IQR method with multiplier k.
    """
    lo, hi = iqr_bounds(df[col], k=k)
    return df.assign(**{f"{col}_is_outlier": (df[col] < lo) | (df[col] > hi)})


# -----------------------------
# Minimal cleaning helpers used by scripts
# -----------------------------
def enforce_schema(df: pd.DataFrame, schema: dict[str, type]) -> pd.DataFrame:
    """Coerce columns to expected dtypes where possible (best-effort)."""
    out = df.copy()
    for col, typ in schema.items():
        if col not in out.columns:
            continue
        try:
            if typ is int:
                out[col] = pd.to_numeric(out[col], errors="coerce").astype("Int64")
            elif typ is float:
                out[col] = pd.to_numeric(out[col], errors="coerce").astype("Float64")
            elif typ is str:
                out[col] = out[col].astype("string")
            elif typ is pd.Timestamp or typ == "datetime":
                out[col] = pd.to_datetime(out[col], errors="coerce", utc=True)
        except Exception:
            # best-effort: leave column as-is on failure
            pass
    return out


def clean_orders(df: pd.DataFrame) -> pd.DataFrame:
    """Basic cleaning for orders: coerce numeric fields and parse timestamps."""
    out = df.copy()
    if "amount" in out.columns:
        out["amount"] = pd.to_numeric(out["amount"], errors="coerce")
    if "quantity" in out.columns:
        out["quantity"] = pd.to_numeric(out["quantity"], errors="coerce").astype("Int64")
    if "created_at" in out.columns:
        out["created_at"] = pd.to_datetime(out["created_at"], errors="coerce", utc=True)
    return out


def clean_users(df: pd.DataFrame) -> pd.DataFrame:
    """Basic cleaning for users: parse signup dates and coerce ids to string."""
    out = df.copy()
    if "signup_date" in out.columns:
        out["signup_date"] = pd.to_datetime(out["signup_date"], dayfirst=True, errors="coerce")
    if "user_id" in out.columns:
        out["user_id"] = out["user_id"].astype("string")
    return out
