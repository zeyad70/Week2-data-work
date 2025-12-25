# Week2 Data Work — ETL

## Setup

1. Create a virtual environment and activate it:

```bash
python -m venv .venv
# Windows PowerShell
.venv\Scripts\Activate.ps1
# or Windows cmd
.venv\Scripts\activate.bat
```

2. Install dependencies (if you have a `requirements.txt`) and make the package importable:

```bash
pip install -r requirements.txt  # optional
pip install -e .
```

If you don't want to install, run scripts with `PYTHONPATH=src` (Windows PowerShell shown):

```powershell
$env:PYTHONPATH = "src"
python scripts\run_etl.py
```

## Run ETL

Run the ETL pipeline end-to-end:

```bash
python scripts/run_etl.py
```

Outputs will be written to `data/processed`.

## Outputs

- `data/processed/orders_clean.parquet`
- `data/processed/users.parquet`
- `data/processed/analytics_table.parquet`
- `data/processed/_run_meta.json`

## EDA

Open `notebooks/eda.ipynb` and run all cells to reproduce basic exploratory analysis.

## Notes

See `reports/summary.md` for a short summary of key findings and caveats.
