import logging
import sys
from pathlib import Path

from bootcamp_data.config import make_paths
from bootcamp_data.io import (
    read_orders_csv,
    read_users_csv,
    write_parquet,
)
from bootcamp_data.transforms import (
    enforce_schema,
    missingness_report,
    add_missing_flags,
    normalize_text,
    apply_mapping,
)
from bootcamp_data.quality import (
    require_columns,
    assert_non_empty,
)

log = logging.getLogger(__name__)


def main() -> None:
    # تحديد مسار المشروع
    ROOT = Path(make_paths()).resolve().parents[1]
    SRC = ROOT / "src"

    if str(SRC) not in sys.path:
        sys.path.insert(0, str(SRC))

    # مسارات البيانات
    raw_dir = ROOT / "data" / "raw"
    processed_dir = ROOT / "data" / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)

    # قراءة البيانات
    orders = read_orders_csv(raw_dir / "orders.csv")
    users = read_users_csv(raw_dir / "users.csv")

    # فحوصات جودة أساسية
    assert_non_empty(orders, "orders")
    assert_non_empty(users, "users")

    # حفظ البيانات
    write_parquet(orders, processed_dir / "orders.parquet")
    write_parquet(users, processed_dir / "users.parquet")


if __name__ == "__main__":
    main()
