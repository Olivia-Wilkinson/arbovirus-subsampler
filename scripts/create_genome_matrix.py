#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
df = pd.read_csv(
    "data/dengue_genbank.csv",
    parse_dates=["Collection_Date", "Release_Date"],
    low_memory=False
)


# In[ ]:


## Drop stinky columns


# In[2]:


df = df[[
        "Accession",
        "Collection_Date",
        "Release_Date",
        "Country"
    ]].rename(
    columns={
        "Accession":       "strain",
        "Collection_Date": "collection_date",
        "Release_Date":    "submission_date",
        "Country":         "country"
    }
)


# In[3]:


# Convert to datetime, coercing any bad or blank entries to NaT
df['collection_date']   = pd.to_datetime(df['collection_date'],   errors='coerce')
df['submission_date']   = pd.to_datetime(df['submission_date'],   errors='coerce')

# Drop only rows where both dates are missing
drop_mask = df[['collection_date','submission_date']].isna().all(axis=1)
if drop_mask.any():
    print(f"Dropping {drop_mask.sum()} rows with no valid dates")
    df = df.loc[~drop_mask].copy()


# In[6]:


n_dropped = len(df) - len(drop_mask)

return n_dropped


# In[7]:


raw = pd.read_csv('data/dengue_genbank.csv', usecols=['Accession','Collection_Date','Release_Date'])
raw = raw.rename(columns={'Accession':'strain'})
failed = raw.loc[bad, ['strain','Collection_Date','Release_Date']]
print(failed.sample(10))


# In[12]:


## Map countries to ISO codes


# In[4]:


import pycountry
def to_iso3(name):
    try:
        return pycountry.countries.lookup(name).alpha_3
    except Exception:
        return pd.NA

df["country_code"] = df["country"].map(to_iso3)


# In[ ]:


## Drop missing rows


# In[5]:


df = df.dropna(subset=["collection_date", "country_code"])


# In[ ]:


## Clean, minimal metadata .tsv


# In[6]:


df[[
    "strain",
    "collection_date",
    "submission_date",
    "country_code"
]].to_csv(
    "data/dengue_metadata_clean.tsv",
    sep="\t",
    index=False
)


# In[ ]:


## Run subsampler scripts


# In[7]:


get_ipython().system('python scripts/get_genome_matrix.py    --metadata      data/dengue_metadata_clean.tsv    --index-column  country_code    --date-column   collection_date    --output        data/matrix_genomes_dengue.tsv')

