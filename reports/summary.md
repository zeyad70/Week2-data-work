# Week 2 Summary — ETL + EDA

## Key findings

- Rows in raw orders: 5
- Rows in raw users: 4
- Rows in analytics output: 5
- Missing `created_at` in analytics: 2
- Country join match rate: 80% (0.8)

## Definitions

- Revenue: sum of `amount` in the `analytics_table` (use `amount_winsor` for visualization to limit outlier impact).
- Refund rate: fraction of orders where `status_clean` == "refund".
- Time window: derived from `created_at` in `orders_clean` / `analytics_table`.

## Data quality caveats

- Several raw rows had malformed fields; CSV readers skip bad lines where necessary.
- Two orders have missing/invalid `created_at` timestamps; time-based metrics will exclude or flag these.
- `amount` contains outliers — winsorized values were produced and an outlier flag added.
- Join coverage is incomplete: about 20% of orders lack a country after the join.

## Next questions

- Can we obtain better timestamps for the rows with missing `created_at`?
- Should we impute country for unmatched users based on behavior or other sources?
- Add automated tests for `io.read_*` and `transforms` to prevent regressions.
