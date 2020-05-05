---
jupyter:
  jupytext:
    formats: ipynb,md
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.2'
      jupytext_version: 1.4.0
  kernelspec:
    display_name: Python 3
    language: python
    name: python3
---

# Aggregate log

## Query

```
SELECT 
COUNT(*) as total,
COUNT(DISTINCT(siren)) as siren,
COUNT(DISTINCT(id_etablissement)) as id_ets,
COUNT(DISTINCT(nom_greffe)) as greffe,
COUNT(DISTINCT(ville)) as ville,
COUNT(DISTINCT(enseigne)) as enseigne,
MIN(date_1) as date_min,
MAX(date_1) as date_max
FROM (SELECT *, Coalesce(
         try(date_parse(file_timestamp, '%Y-%m-%d')),
         try(date_parse(file_timestamp, '%Y-%m-%d %hh:%mm:%ss.SSS')),
         try(date_parse(file_timestamp, '%Y-%m-%d %hh:%mm:%ss')),
         try(cast(file_timestamp as timestamp))
       )  as date_1
      FROM ets_partiel_2019  
) 
```

```python
import os, glob
import pandas as pd
```

```python
path = os.getcwd()
index = pd.Series(['Initial', 'New 2019',
'New 2017', 'New 2018', 'Partiel 2018', 'Partiel 2019'], name = 'origin')
df = (pd.concat([
pd.concat(
map(pd.read_csv, glob.glob(os.path.join('', "*.csv")))
).reset_index(drop = True),
index], axis = 1)
.set_index('origin')
.sort_values(by ='total', ascending  = False)
)
print(df.to_markdown())
```
