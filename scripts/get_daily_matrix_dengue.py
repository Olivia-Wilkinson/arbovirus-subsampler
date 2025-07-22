#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd


# In[4]:


get_ipython().run_line_magic('cd', '"/Users/oliviawilko/Documents/Epidemiology/Summer Project"')

# Load the WHO export, sheet 0 
cases_raw = pd.read_excel(
    "data/cases_monthly.xlsx",
    sheet_name=0,         
    parse_dates=["date"], 
    usecols=["iso3", "date", "cases"]  
)

# Quick sanity check
print(cases_raw.head())


# Pivot to wide form: one row per country, one column per month
cases = (
    cases_raw
    .pivot(index="iso3", columns="date", values="cases")
    .fillna(0)              # missing months → 0 cases
    .sort_index(axis=1)     # chronological order of columns
)

# Re-format the column names back to YYYY-MM (strings)
cases.columns = cases.columns.to_series().dt.strftime("%Y-%m")

# Write out the matrix for the pipeline
cases.to_csv("data/matrix_cases_monthly.tsv", sep="\t")

# 6. Inspect new matrix
print(cases.iloc[:5, -5:])

