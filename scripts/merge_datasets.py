#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
from pathlib import Path
import pandas as pd

PROJECT_DIR = Path("/Users/oliviawilko/Documents/Epidemiology/Summer Project")
DATA_DIR    = PROJECT_DIR / "data"


# In[2]:


def prefix_cols(df, source, key="country_code"):
    df = df.copy()
    new_cols = {col: f"{source}_{col}" for col in df.columns if col != key}
    return df.rename(columns=new_cols)


# In[3]:


# 1. Read base metric metadata
dengue = pd.read_csv(DATA_DIR / "country_turnaround_wide_metrics.tsv", sep="\t")


# In[4]:


# 2. Read “auxiliary” datasets


# In[4]:


# Income dataset
income = (
    pd.read_csv(DATA_DIR / "income_classification_worldbank.tsv", sep="\t")
      .rename(columns={"code": "country_code", "country": "country_name"})
)


# In[5]:


# Risk dataset
risk = (
    pd.read_csv(DATA_DIR / "risk.csv")
      .rename(columns={"Sovereignt": "country_name"})
      .merge(income[["country_code", "country_name"]],
             on="country_name", how="left")
)
risk_prefixed = prefix_cols(risk, source="risk")


# In[6]:


# R&D Expenditure (2010--2015 average)
rd = (
    pd.read_csv(DATA_DIR / "R_D_expenditure.csv")
      .rename(columns={"geoUnit": "country_code"})
)
rd_avg = (
    rd[rd["year"].between(2010, 2025)]
      .groupby("country_code", as_index=False)["value"]
      .mean()
)
rd_prefixed = prefix_cols(rd_avg, source="rd")


# In[7]:


# Government Effectivness (2010--2025 average)
gov = pd.read_csv(DATA_DIR / "government_effect.csv")

# Rename for consistency
gov = gov.rename(columns={
    "code": "country_code",
    "indicator": "indicator_name"
})

# Coerce year & estimate into numeric types
gov["year"] = pd.to_numeric(gov["year"], errors="coerce")
gov["estimate"] = (
    gov["estimate"]
      .astype(str)                     # ensure it's a string
      .str.replace(",", "")            # strip thousands separators
      .pipe(pd.to_numeric, errors="coerce")
)

# Filter to 2010–2025
gov_filt = gov[gov["year"].between(2010, 2025)]

# Pivot so each indicator becomes its own column, taking mean of numeric estimates
gov_pivot = (
    gov_filt
      .pivot_table(
          index="country_code",
          columns="indicator_name",
          values="estimate",
          aggfunc="mean",
          dropna=True
      )
      .reset_index()
)

# Prefix columns
gov_prefixed = prefix_cols(gov_pivot, source="gov")


# In[8]:


# Physicians Density (2010--2025 average)
phys = pd.read_csv(DATA_DIR / "physicians.csv")

# Drop any Unnamed junk and the two indicator-metadata cols
phys = phys.loc[:, ~phys.columns.str.startswith("Unnamed")]
phys = phys.drop(columns=["Indicator Name", "Indicator Code"])

# Pick out only the four-digit year columns
year_cols = [c for c in phys.columns if c.isdigit()]

# Melt those into a year/value long form
phys_long = phys.melt(
    id_vars=["Country Name", "Country Code"],
    value_vars=year_cols,
    var_name="year",
    value_name="physicians_per_1000"
)

# Now safe to cast to int
phys_long["year"] = phys_long["year"].astype(int)

# Filter to 2010–2025 and average
phys_avg = (
    phys_long[phys_long["year"].between(2010, 2025)]
      .groupby("Country Code", as_index=False)["physicians_per_1000"]
      .mean()
      .rename(columns={"Country Code": "country_code"})
)

phys_prefixed = prefix_cols(phys_avg, source="physicians")


# In[9]:


# Treatment Seeking dataset
treat = pd.read_csv(DATA_DIR / "treatment_seeking.csv")\
          .rename(columns={"ISO3": "country_code"})

# Coerce year to numeric & filter
treat["year"] = pd.to_numeric(treat["year"], errors="coerce")
treat_filt = treat[treat["year"].between(2010, 2025)]

# Average the three metrics over those years
treat_agg = (
    treat_filt
      .groupby("country_code", as_index=False)[
          ["Publicfrac_pred", "Publicfrac_pred_low", "Publicfrac_pred_high"]
      ]
      .mean()
)

# Prefix and merge as before
treat_prefixed = prefix_cols(treat_agg, source="treatment")


# In[10]:


# GDP dataset (2010--2025 average)
gdp = (
    pd.read_csv(DATA_DIR / "GDP.CSV")
      .rename(columns={"iso3": "country_code"})
)
gdp_avg = (
    gdp[gdp["year"].between(2010, 2025)]
      .groupby("country_code", as_index=False)[["gdp_usd_mean","gdp_ppp_mean"]]
      .mean()
)
gdp_prefixed = prefix_cols(gdp_avg, source="gdp")


# In[22]:


# 4. Merge and finalise


# In[12]:


# MEGA MERGE
merged = (
    dengue
      .merge(income.rename(columns={"country_name":"income_country_name"}),
             on="country_code", how="left")
      .merge(risk_prefixed,   on="country_code", how="left")
      .merge(rd_prefixed,     on="country_code", how="left")
      .merge(gov_prefixed,    on="country_code", how="left")
      .merge(phys_prefixed,   on="country_code", how="left")
      .merge(treat_prefixed,  on="country_code", how="left")
      .merge(gdp_prefixed,    on="country_code", how="left")
)


# In[13]:


# Hope that it's worked
print("Merged shape:", merged.shape)
merged.head()


# In[14]:


from pathlib import Path

RESULTS_DIR = PROJECT_DIR / "results"
RESULTS_DIR.mkdir(exist_ok=True)

# write as CSV
merged.to_csv(RESULTS_DIR / "merged_dengue_dataset.csv", index=False)

print(f"Saved merged dataset to {RESULTS_DIR}")


# In[15]:


# Cell X: Diagnose duplicate country_codes
for df, name in [
    (income,    "income"),
    (risk_prefixed,   "risk"),
    (rd_prefixed,     "rd"),
    (gov_prefixed,    "gov"),
    (phys_prefixed,   "physicians"),
    (treat_prefixed,  "treatment"),
    (gdp_prefixed,    "gdp"),
]:
    total = len(df)
    unique = df["country_code"].nunique()
    print(f"{name:12s}: {total:5d} rows, {unique:5d} unique country_codes")

