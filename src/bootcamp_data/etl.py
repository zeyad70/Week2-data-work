from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Tuple

import pandas as pd

from .io import read_orders_csv, read_users_csv, write_parquet
from .joins import safe_left_join
from .quality import (
    require_columns,
    assert_non_empty,
    assert_unique_key,
)
from .transforms import (
    enforce_schema,
    clean_orders,
    clean_users,
    parse_datetime,
    add_time_parts,
    winsorize,
    add_outlier_flag,
)

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class ETLConfig:
    root: Path
    raw_orders: Path
    raw_users: Path
    out_orders_clean: Path
    out_users: Path
    out_analytics: Path
    run_meta: Path


def load_inputs(cfg: ETLConfig) -> Tuple[pd.DataFrame, pd.DataFrame]:
    orders = read_orders_csv(cfg.raw_orders)
    users = read_users_csv(cfg.raw_users)
    return orders, users


def _normalize_status(s: pd.Series) -> pd.Series:
    return s.astype("string").str.lower().str.strip()


def transform(orders_raw: pd.DataFrame, users: pd.DataFrame) -> pd.DataFrame:
    # Basic checks
    require_columns(
        orders_raw,
        ["order_id", "user_id", "amount", "quantity", "created_at", "status"],
    )
    require_columns(users, ["user_id", "country", "signup_date"]) 
    assert_non_empty(orders_raw, "orders_raw")
    assert_non_empty(users, "users")

    # users uniqueness
    assert_unique_key(users, "user_id")

    # Clean and coerce
    orders = (
        orders_raw.pipe(clean_orders)
        .assign(status_clean=lambda d: _normalize_status(d["status"]))
    )

    users_clean = users.pipe(clean_users)

    # Add missing flags for important numeric fields
    for col in ["amount", "quantity"]:
        flag = f"{col}_missing"
        if col in orders.columns and flag not in orders.columns:
            orders[flag] = orders[col].isna()

    # Time parsing + time parts
    orders = orders.pipe(parse_datetime, col="created_at", utc=True)
    orders = orders.pipe(add_time_parts, ts_col="created_at")

    # Join orders -> users (many_to_one on user_id)
    joined = safe_left_join(
        orders,
        users_clean,
        on="user_id",
        validate="many_to_one",
        suffixes=("", "_user"),
    )
    # ensure join didn't explode rows
    if len(joined) != len(orders):
        raise RuntimeError("Join changed row count; check keys and data")

    # Outlier handling: winsorize amount and flag
    if "amount" in joined.columns:
        joined = joined.assign(amount_winsor=winsorize(joined["amount"]))
        joined = add_outlier_flag(joined, "amount", k=1.5)

    # final analytics table
    return joined


def load_outputs(*, analytics: pd.DataFrame, users: pd.DataFrame, cfg: ETLConfig) -> None:
    # write users and analytics
    cfg.out_users.parent.mkdir(parents=True, exist_ok=True)
    write_parquet(users, cfg.out_users)
    write_parquet(analytics, cfg.out_analytics)

    # write orders_clean (drop user-side columns)
    user_side_cols = [c for c in users.columns if c != "user_id"]
    cols_to_drop = [c for c in user_side_cols if c in analytics.columns]
    orders_clean = analytics.drop(columns=cols_to_drop, errors="ignore")
    write_parquet(orders_clean, cfg.out_orders_clean)


def write_run_meta(cfg: ETLConfig, *, orders_raw: pd.DataFrame, users: pd.DataFrame, analytics: pd.DataFrame) -> None:
    missing_created_at = int(analytics["created_at"].isna().sum()) if "created_at" in analytics.columns else None
    country_match_rate = (
        1.0 - float(analytics["country"].isna().mean()) if "country" in analytics.columns else None
    )

    meta = {
        "rows_in_orders_raw": int(len(orders_raw)),
        "rows_in_users": int(len(users)),
        "rows_out_analytics": int(len(analytics)),
        "missing_created_at": missing_created_at,
        "country_match_rate": country_match_rate,
        "config": {k: str(v) for k, v in asdict(cfg).items()},
    }

    cfg.run_meta.parent.mkdir(parents=True, exist_ok=True)
    cfg.run_meta.write_text(json.dumps(meta, indent=2), encoding="utf-8")


def run_etl(cfg: ETLConfig) -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    log.info("Loading inputs")
    orders_raw, users = load_inputs(cfg)

    log.info("Transforming (orders=%s, users=%s)", len(orders_raw), len(users))
    analytics = transform(orders_raw, users)

    log.info("Writing outputs to %s", cfg.out_analytics.parent)
    load_outputs(analytics=analytics, users=users, cfg=cfg)

    log.info("Writing run metadata: %s", cfg.run_meta)
    write_run_meta(cfg, orders_raw=orders_raw, users=users, analytics=analytics)
