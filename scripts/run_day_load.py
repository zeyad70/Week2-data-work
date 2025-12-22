from pathlib import Path
import sys
import runpy

from data.transforms import enforce_schema


def main() -> None:
    # project root (one level above `scripts`)
    root = Path(__file__).resolve().parent.parent

    # make `src` importable so we can import bootcamp_data
    src_path = root / "src"
    if str(src_path) not in sys.path:
        sys.path.append(str(root / "src"))

    from bootcamp_data.config import make_paths
    from bootcamp_data.io import read_orders_csv, read_users_csv, write_parquet


    paths = make_paths(root=root)

    orders_df = read_orders_csv(paths.raw / "orders.csv")
    users_df = read_users_csv(paths.raw / "users.csv")

    # apply schema enforcement to orders
    orders_df = enforce_schema(orders_df)

    write_parquet(orders_df, paths.processed / "orders.parquet")
    write_parquet(users_df, paths.processed / "users.parquet")

    print(f"Wrote: {paths.processed / 'orders.parquet'}")


if __name__ == "__main__":
    main()