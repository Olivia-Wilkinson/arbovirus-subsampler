import pandas as pd
import os

# Set working directory
os.chdir("/Users/oliviawilko/Documents/Epidemiology/Summer Project")

# Load cleaned metadata
meta = pd.read_csv(
    'data/dengue_metadata_clean.tsv',
    sep='\t',
    parse_dates=['collection_date', 'submission_date']
)

# Compute turnaround time in days and filter out any invalid negatives
meta['turnaround_days'] = (
    meta['submission_date'] - meta['collection_date']
).dt.days
meta = meta.loc[meta['turnaround_days'] >= 0]

# Load case matrix and reshape
cases = pd.read_csv('data/matrix_cases_monthly.tsv', sep='\t')

# Melt to long form
cases_long = cases.melt(
    id_vars='iso3',
    var_name='period_date',
    value_name='cases'
)
cases_long['period_date'] = pd.to_datetime(
    cases_long['period_date'],
    format='%Y-%m'
)

# Define pandemic periods
pandemic_start = pd.Timestamp('2020-03-01')
meta['period'] = meta['collection_date'].lt(pandemic_start).map({True: 'pre', False: 'post'})
cases_long['period'] = cases_long['period_date'].lt(pandemic_start).map({True: 'pre', False: 'post'})

# Aggregate genomes by country & period
genomes = (
    meta
    .groupby(['country_code', 'period'])
    .size()
    .rename('genome_count')
    .reset_index()
)

# Aggregate cases by country & period
cases_period = (
    cases_long
    .groupby(['iso3', 'period'])['cases']
    .sum()
    .reset_index()
    .rename(columns={'iso3': 'country_code'})
)

# Merge and compute genomes per 1,000 cases
metrics = (
    genomes
    .merge(cases_period, on=['country_code', 'period'], how='left')
    .assign(
        genomes_per_1000=lambda df: df['genome_count'] / (df['cases'] / 1_000)
    )
)

# Compute median turnaround per country & period
turnaround = (
    meta
    .groupby(['country_code', 'period'])['turnaround_days']
    .median()
    .reset_index()
    .rename(columns={'turnaround_days': 'median_turnaround_days'})
)

# Final join for long format
metrics = metrics.merge(
    turnaround,
    on=['country_code', 'period'],
    how='left'
)

# Inspect or write out (long format)
print(metrics.head())
metrics.to_csv(
    'results/country_period_metrics.tsv',
    sep='\t',
    index=False
)

# Pivot all metrics into wide format
metrics_wide = metrics.pivot_table(
    index='country_code',
    columns='period',
    values=[
        'genome_count',
        'cases',
        'genomes_per_1000',
        'median_turnaround_days'
    ]
)

# Bring 'country_code' back out of the index
metrics_wide = metrics_wide.reset_index()

# Flatten the MultiIndex, but preserve first-level-only cols
new_cols = []
for col in metrics_wide.columns:
    if isinstance(col, tuple):
        # col == (first_level_name, second_level_name)
        if col[1] == '' or col[1] is None:
            new_cols.append(col[0])
        else:
            new_cols.append(f"{col[1]}_{col[0]}")
    else:
        new_cols.append(col)
metrics_wide.columns = new_cols

# Quick sanity check
print(metrics_wide.columns.tolist())

# Compute overall median turnaround per country
overall_median = (
    meta
    .groupby('country_code')['turnaround_days']
    .median()
    .reset_index()
    .rename(columns={'turnaround_days':'overall_median_turnaround_days'})
)

# Merge in the overall median
metrics_wide = metrics_wide.merge(
    overall_median,
    on='country_code',
    how='left'
)

print(metrics_wide.head())
metrics_wide.to_csv(
    'results/country_turnaround_wide_metrics.tsv',
    sep='\t',
    index=False
)