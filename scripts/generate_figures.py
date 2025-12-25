#!/usr/bin/env python
"""Generate EDA figures for reports/figures/"""

import sys
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / 'src') not in sys.path:
    sys.path.insert(0, str(ROOT / 'src'))

sns.set_style('whitegrid')
plt.rcParams['figure.figsize'] = (10, 6)

# Load processed data
analytics = pd.read_parquet(ROOT / 'data' / 'processed' / 'analytics_table.parquet')
users = pd.read_parquet(ROOT / 'data' / 'processed' / 'users.parquet')

figures_dir = ROOT / 'reports' / 'figures'
figures_dir.mkdir(parents=True, exist_ok=True)

# Figure 1: Amount distribution
fig, ax = plt.subplots()
analytics['amount'].dropna().hist(bins=10, ax=ax, edgecolor='black')
ax.set_xlabel('Amount')
ax.set_ylabel('Frequency')
ax.set_title('Distribution of Order Amounts')
fig_path = figures_dir / 'amount_distribution.png'
fig.savefig(fig_path, dpi=100, bbox_inches='tight')
plt.close()
print(f'Saved: {fig_path}')

# Figure 2: Status breakdown
fig, ax = plt.subplots(figsize=(8, 5))
status_counts = analytics['status_clean'].value_counts(dropna=False)
status_counts.plot(kind='bar', ax=ax, color='steelblue')
ax.set_xlabel('Status')
ax.set_ylabel('Count')
ax.set_title('Orders by Status')
ax.tick_params(axis='x', rotation=45)
fig.tight_layout()
fig_path = figures_dir / 'status_breakdown.png'
fig.savefig(fig_path, dpi=100, bbox_inches='tight')
plt.close()
print(f'Saved: {fig_path}')

# Figure 3: Country coverage
fig, ax = plt.subplots(figsize=(8, 5))
country_counts = analytics['country'].value_counts(dropna=False)
country_counts.plot(kind='barh', ax=ax, color='coral')
ax.set_xlabel('Count')
ax.set_title('Orders by Country')
fig.tight_layout()
fig_path = figures_dir / 'country_breakdown.png'
fig.savefig(fig_path, dpi=100, bbox_inches='tight')
plt.close()
print(f'Saved: {fig_path}')

# Figure 4: Amount by status (box plot)
fig, ax = plt.subplots(figsize=(9, 5))
analytics.boxplot(column='amount', by='status_clean', ax=ax)
ax.set_xlabel('Status')
ax.set_ylabel('Amount')
ax.set_title('Amount Distribution by Status')
plt.suptitle('')
fig.tight_layout()
fig_path = figures_dir / 'amount_by_status.png'
fig.savefig(fig_path, dpi=100, bbox_inches='tight')
plt.close()
print(f'Saved: {fig_path}')

print('\n✓ All figures generated to', figures_dir)
