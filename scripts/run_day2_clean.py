import logging
import sys
from pathlib import Path

# Ensure src is on sys.path for package imports
ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from bootcamp_data.config import make_paths
from bootcamp_data.io import (
    read_orders_csv,
    read_users_csv,
    write_parquet,
)
from bootcamp_data.quality import (
    assert_non_empty,
)

log = logging.getLogger(__name__)


def main() -> None:
    paths = make_paths(root=Path(__file__).resolve().parents[1])
    ROOT = Path(__file__).resolve().parents[1]
    SRC = ROOT / "src"

    if str(SRC) not in sys.path:
        sys.path.insert(0, str(SRC))

    raw_dir = paths.raw
    processed_dir = paths.processed
    processed_dir.mkdir(parents=True, exist_ok=True)

    orders = read_orders_csv(raw_dir / "orders.csv")
    users = read_users_csv(raw_dir / "users.csv")

    assert_non_empty(orders, "orders")
    assert_non_empty(users, "users")

    # Write cleaned/processed files
    write_parquet(orders, processed_dir / "orders_clean.parquet")
    write_parquet(users, processed_dir / "users.parquet")


if __name__ == "__main__":
    main()
