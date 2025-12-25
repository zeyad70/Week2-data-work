import sys
import pandas as pd
from pathlib import Path

# project root (one level above `scripts/`)
ROOT = Path(__file__).resolve().parents[1]
orders_path = ROOT / "data" / "processed" / "orders_clean.parquet"
users_path = ROOT / "data" / "processed" / "users.parquet"


def main() -> None:
	try:
		orders = pd.read_parquet(orders_path)
		users = pd.read_parquet(users_path)
	except Exception as e:
		msg = str(e)
		print(f"Error reading parquet files: {msg}", file=sys.stderr)
		if "Parquet magic bytes not found" in msg or "not a parquet file" in msg.lower():
			print(
				"The processed files in `data/processed` are not valid Parquet files.",
				file=sys.stderr,
			)
			print(
				"Run the earlier pipeline steps first, e.g. `scripts/run_day2_clean.py` or `scripts/run_day_load.py`",
				file=sys.stderr,
			)
			sys.exit(2)
		raise

	print("Orders shape:", orders.shape)
	print("Users shape:", users.shape)


if __name__ == "__main__":
	main()
