from pathlib import Path
import sys

# Ensure src is on sys.path so `bootcamp_data` imports work when running scripts
ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from bootcamp_data.config import make_paths
from bootcamp_data.io import read_orders_csv, read_users_csv, write_parquet


def main() -> None:
    ROOT = Path(__file__).resolve().parents[1]
    SRC = ROOT / "src"
    if str(SRC) not in sys.path:
        sys.path.insert(0, str(SRC))

    paths = make_paths(ROOT)
    processed_dir = paths.processed
    processed_dir.mkdir(parents=True, exist_ok=True)

    orders = read_orders_csv(paths.raw / "orders.csv")
    users = read_users_csv(paths.raw / "users.csv")

    # Write processed files (orders_clean.parquet expected by analytics)
    write_parquet(orders, processed_dir / "orders_clean.parquet")
    write_parquet(users, processed_dir / "users.parquet")

    print("Wrote processed files to:", processed_dir)


if __name__ == "__main__":
    main()