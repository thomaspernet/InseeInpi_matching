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

<!-- #region -->
# Création variables  count index, count siren et maximum pct intersection

La réalisation des tests de dedoublage demande la création de trois variables supplémentaires, qui sont créé a partir de groupe, a savoir l'index ou le siren.

Les trois variables a créée sont les suivantes:

- count_inpi_index_id_siret: nombre de siret possible par index
- count_inpi_siren_siret: nombre de siret unique par siren
- index_id_max_intersection: Pourcentage maximum de pct_intersection par index
    - Creation de `pct_intersection` lors de l'US [3275](https://tree.taiga.io/project/olivierlubet-air/us/3275) -> `intersection / union_ as pct_intersection`

## Objective(s)



## Metadata 

* Metadata parameters are available here: 
* US Title: Création variables  count index, count siren et maximum pct intersection
* Epic: Epic 5
* US: US 7
* Date Begin: 9/1/2020
* Duration Task: 6
* Status: Active
* Source URL: [US 07 Preparation tables et variables tests](https://coda.io/d/_dCtnoqIftTn/US-07-Preparation-tables-et-variables-tests_suFb9)
* Task type:
  * Jupyter Notebook
* Users: :
  * Thomas Pernet
* Watchers:
  * Thomas Pernet
* Estimated Log points:
  * One being a simple task, 15 a very difficult one
  *  5
* Task tag
  *  #sql-query,#regle-de-gestion,#preparation-nb-index,#preparation-nb-siren,"preparation-pct-max
* Toggl Tag
  * #data-preparation
  
## Input Cloud Storage [AWS]

If link from the internet, save it to the cloud first

### Tables [AWS]

1. Batch 1:
  * Select Provider: 
  * Select table(s): 
    * Select only tables created from the same notebook, else copy/paste selection to add new input tables
    * If table(s) does not exist, add them: Add New Table
    * Information:
      * Region: 
        * Name: 
        * Code: 
      * Database: 
      * Notebook construction file: 
    
## Destination Output/Delivery

* AWS
  1. Athena: 
      * Region: 
      * Database: 
      * Tables (Add name new table): 

## Things to know (Steps, Attention points or new flow of information)

### Sources of information  (meeting notes, Documentation, Query, URL)

Sources of information  (meeting notes, Documentation, Query, URL)
1. Jupyter Notebook (Github Link)
  1. md : https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/08_US_DATUM/05_creation_table_cases.md#create-table
<!-- #endregion -->

## Connexion serveur

```python
from awsPy.aws_authorization import aws_connector
from awsPy.aws_athena import service_athena
from awsPy.aws_s3 import service_s3
from pathlib import Path
import pandas as pd
import numpy as np
import seaborn as sns
import os, shutil

path = os.getcwd()
parent_path = str(Path(path).parent)
path_cred = r"{}/credential_AWS.json".format(parent_path)
con = aws_connector.aws_instantiate(credential = path_cred,
                                       region = 'eu-west-3')

region = 'eu-west-3'
bucket = 'calfdata'
```

```python
con = aws_connector.aws_instantiate(credential = path_cred,
                                       region = region)
client= con.client_boto()
s3 = service_s3.connect_S3(client = client,
                      bucket = bucket, verbose = False) 
```

```python
pandas_setting = True
if pandas_setting:
    cm = sns.light_palette("green", as_cmap=True)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', None)
```

# Input/output

```python
s3_output = 'inpi/sql_output'
database = 'siretisation'
```

```python
query = """
CREATE TABLE siretisation.temp_US_max
WITH (
  format='PARQUET'
) AS
WITH create_var AS (
SELECT 
siren,
siret,
index_id,
CAST(
      cardinality(
        array_distinct(
          array_intersect(
            split(adresse_distance_inpi, ' '), 
            split(adresse_distance_insee, ' ')
          )
        )
      ) AS DECIMAL(10, 2)
    ) as intersection, 
    CAST(
      cardinality(
        array_distinct(
          array_union(
            split(adresse_distance_inpi, ' '), 
            split(adresse_distance_insee, ' ')
          )
        )
      ) AS DECIMAL(10, 2)
    ) as union_, 
    CAST(
      cardinality(
        array_distinct(
          array_intersect(
            split(adresse_distance_inpi, ' '), 
            split(adresse_distance_insee, ' ')
          )
        )
      ) AS DECIMAL(10, 2)
    )/ CAST(
      cardinality(
        array_distinct(
          array_union(
            split(adresse_distance_inpi, ' '), 
            split(adresse_distance_insee, ' ')
          )
        )
      ) AS DECIMAL(10, 2)
    ) as pct_intersection
    FROM siretisation.ets_insee_inpi
)
SELECT 
create_var.siren,
create_var.siret,
create_var.index_id,
count_inpi_index_id_siret,
count_inpi_siren_siret,
create_var.pct_intersection,
index_id_max_intersection
FROM 
    create_var
LEFT JOIN (
          SELECT 
            index_id, 
            COUNT(
              DISTINCT(siret)
            ) AS count_inpi_index_id_siret 
          FROM 
            create_var 
          GROUP BY 
            index_id
        ) AS count_rows_index_id_siret ON create_var.index_id = count_rows_index_id_siret.index_id 
        LEFT JOIN (
          SELECT 
            siren, 
            COUNT(
              DISTINCT(siret)
            ) AS count_inpi_siren_siret 
          FROM 
            create_var 
          GROUP BY 
            siren
        ) AS count_rows_sequence ON create_var.siren = count_rows_sequence.siren 
        LEFT JOIN (
          SELECT 
            index_id, 
            MAX(pct_intersection) AS index_id_max_intersection 
          FROM 
            create_var 
          GROUP BY 
            index_id
        ) AS is_index_id_index_id_max_intersection ON create_var.index_id = is_index_id_index_id_max_intersection.index_id
"""

output = s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = None, ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

```python
query = """
SELECT *
FROM siretisation.temp_US_max
LIMIT 10
"""
tb = s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = 'exemple_US_max', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
tb
```

```python
print(tb.to_markdown())
```

# Test acceptance

1. Vérifier que le nombre de lignes est indentique avant et après la création des variables
2. Compter le nombre de lignes par possibilité pour `count_inpi_index_id_siret` et `count_inpi_siren_siret` -> TOP 10
3. Donner la distribution de  `pct_intersection` et `index_id_max_intersection`
    - distribution -> 0.1,0.25,0.5,0.75,0.8,0.95
4. Donner la distribution de `pct_intersection` par possibilité pour `count_inpi_index_id_siret` et `count_inpi_siren_siret`
    - distribution -> 0.1,0.25,0.5,0.75,0.8,0.95
    - TOP 10
    - BOTTOM 10
5. Donner la distribution de `index_id_max_intersection` par possibilité pour `count_inpi_index_id_siret` et `count_inpi_siren_siret`
    - distribution -> 0.1,0.25,0.5,0.75,0.8,0.95
    - TOP 10
    - BOTTOM 10


## 1. Vérifier que le nombre de lignes est indentique avant et après la création des variables

```python
query = """
SELECT COUNT(*)
FROM siretisation.temp_US_max
"""
s3.run_query(
            query=query,
            database='siretisation',
            s3_output=s3_output,
  filename = 'count_ets_insee_inpi', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

```python
query = """
SELECT COUNT(*)
FROM siretisation.temp_US_max
"""
s3.run_query(
            query=query,
            database='siretisation',
            s3_output=s3_output,
  filename = 'count_ets_insee_inpi', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

## 2. Compter le nombre de lignes par possibilité pour `count_inpi_index_id_siret` et `count_inpi_siren_siret`


`count_inpi_index_id_siret`

```python
query = """
SELECT count_inpi_index_id_siret, COUNT(*) as cnt_count_inpi_index_id_siret
FROM siretisation.temp_us_max  
GROUP BY count_inpi_index_id_siret
ORDER BY cnt_count_inpi_index_id_siret DESC
LIMIT 10
"""
tb = s3.run_query(
            query=query,
            database='siretisation',
            s3_output=s3_output,
  filename = 'cnt_count_inpi_index_id_siret', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
tb
```

```python
print(tb.to_markdown())
```

`count_inpi_siren_siret`

```python
query = """
SELECT count_inpi_siren_siret, COUNT(*) as cnt_count_inpi_siren_siret
FROM siretisation.temp_us_max  
GROUP BY count_inpi_siren_siret
ORDER BY cnt_count_inpi_siren_siret DESC
LIMIT 10
"""
tb = s3.run_query(
            query=query,
            database='siretisation',
            s3_output=s3_output,
  filename = 'cnt_count_inpi_siren_siret', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
tb
```

```python
print(tb.to_markdown())
```

## 3. Donner la distribution de  `pct_intersection` et `index_id_max_intersection`
    
- distribution -> 0.1,0.25,0.5,0.75,0.8,0.95


 `pct_intersection`

```python
query = """
SELECT approx_percentile(
  pct_intersection,
  ARRAY[0.1,0.25,0.5,0.75,0.8,0.95]) AS nest
FROM temp_us_max  
    """
s3.run_query(
            query=query,
            database='siretisation',
            s3_output=s3_output,
  filename = 'dist_pct_intersection', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

`index_id_max_intersection`

```python
query = """
SELECT approx_percentile(
  index_id_max_intersection,
  ARRAY[0.1,0.25,0.5,0.75,0.8,0.95]) AS nest
FROM temp_us_max  
    """
s3.run_query(
            query=query,
            database='siretisation',
            s3_output=s3_output,
  filename = 'dist_max_pct_intersection', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

<!-- #region -->
## 4. Donner la distribution de `pct_intersection` par possibilité pour `count_inpi_index_id_siret` et `count_inpi_siren_siret`


- distribution -> 0.1,0.25,0.5,0.75,0.8,0.95
- TOP 10
- BOTTOM 10
<!-- #endregion -->

`count_inpi_index_id_siret`

```python
query = """
WITH dataset AS (
  
  SELECT 
  count_inpi_index_id_siret,
  MAP(
    ARRAY[0.1,0.25,0.5,0.75,0.8,0.95],
    approx_percentile(
      pct_intersection,
    ARRAY[0.1,0.25,0.5,0.75,0.8,0.95])
    ) AS nest
    FROM temp_us_max 
    GROUP BY count_inpi_index_id_siret
    ) 
    
    SELECT 
    count_inpi_index_id_siret,
    pct, 
    value AS  pct_intersection
    FROM dataset
    CROSS JOIN UNNEST(nest) as t(pct, value)
"""
output = s3.run_query(
            query=query,
            database='siretisation',
            s3_output=s3_output,
  filename = 'pct_count_inpi_index_id_siret', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

```python
tb= output.set_index(['count_inpi_index_id_siret', 'pct']).unstack(-1).head(10)
```

```python
print(tb.to_markdown())
```

```python
tb = output.set_index(['count_inpi_index_id_siret', 'pct']).unstack(-1).tail(10)
tb
```

```python
print(tb.to_markdown())
```

`count_inpi_siren_siret`

```python
query = """
WITH dataset AS (
  
  SELECT 
  count_inpi_siren_siret,
  MAP(
    ARRAY[0.1,0.25,0.5,0.75,0.8,0.95],
    approx_percentile(
      pct_intersection,
    ARRAY[0.1,0.25,0.5,0.75,0.8,0.95])
    ) AS nest
    FROM temp_us_max 
    GROUP BY count_inpi_siren_siret
    ) 
    
    SELECT 
    count_inpi_siren_siret,
    pct, 
    value AS  pct_intersection
    FROM dataset
    CROSS JOIN UNNEST(nest) as t(pct, value)
"""
output = s3.run_query(
            query=query,
            database='siretisation',
            s3_output=s3_output,
  filename = 'pct_count_inpi_siren_siret', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

```python
tb= output.set_index(['count_inpi_siren_siret', 'pct']).unstack(-1).head(10)
tb
```

```python
print(tb.to_markdown())
```

```python
tb = output.set_index(['count_inpi_siren_siret', 'pct']).unstack(-1).tail(10)
tb
```

```python
print(tb.to_markdown())
```

## 5. Donner la distribution de `index_id_max_intersection` par possibilité pour `count_inpi_index_id_siret` et `count_inpi_siren_siret`

- distribution -> 0.1,0.25,0.5,0.75,0.8,0.95
- TOP 10
- BOTTOM 10


`count_inpi_index_id_siret`

```python
query = """
WITH dataset AS (
  
  SELECT 
  count_inpi_index_id_siret,
  MAP(
    ARRAY[0.1,0.25,0.5,0.75,0.8,0.95],
    approx_percentile(
      index_id_max_intersection,
    ARRAY[0.1,0.25,0.5,0.75,0.8,0.95])
    ) AS nest
    FROM temp_us_max 
    GROUP BY count_inpi_index_id_siret
    ) 
    
    SELECT 
    count_inpi_index_id_siret,
    pct, 
    value AS  pct_intersection
    FROM dataset
    CROSS JOIN UNNEST(nest) as t(pct, value)
"""
output = s3.run_query(
            query=query,
            database='siretisation',
            s3_output=s3_output,
  filename = 'pct_count_inpi_index_id_siret', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

```python
tb = output.set_index(['count_inpi_index_id_siret', 'pct']).unstack(-1).head(10)
tb
```

```python
print(tb.to_markdown())
```

```python
tb = output.set_index(['count_inpi_index_id_siret', 'pct']).unstack(-1).tail(10)
tb
```

```python
print(tb.to_markdown())
```

`count_inpi_siren_siret`

```python
query = """
WITH dataset AS (
  
  SELECT 
  count_inpi_siren_siret,
  MAP(
    ARRAY[0.1,0.25,0.5,0.75,0.8,0.95],
    approx_percentile(
      index_id_max_intersection,
    ARRAY[0.1,0.25,0.5,0.75,0.8,0.95])
    ) AS nest
    FROM temp_us_max 
    GROUP BY count_inpi_siren_siret
    ) 
    
    SELECT 
    count_inpi_siren_siret,
    pct, 
    value AS pct_intersection
    FROM dataset
    CROSS JOIN UNNEST(nest) as t(pct, value)
"""
output = s3.run_query(
            query=query,
            database='siretisation',
            s3_output=s3_output,
  filename = 'pct_count_inpi_siren_siret', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

```python
tboutput.set_index(['count_inpi_siren_siret', 'pct']).unstack(-1).head(10)
```

```python
output.set_index(['count_inpi_siren_siret', 'pct']).unstack(-1).tail(10)
```

# Generation report

```python
import os, time, shutil, urllib, ipykernel, json
from pathlib import Path
from notebook import notebookapp
```

```python
def create_report(extension = "html", keep_code = False):
    """
    Create a report from the current notebook and save it in the 
    Report folder (Parent-> child directory)
    
    1. Exctract the current notbook name
    2. Convert the Notebook 
    3. Move the newly created report
    
    Args:
    extension: string. Can be "html", "pdf", "md"
    
    
    """
    
    ### Get notebook name
    connection_file = os.path.basename(ipykernel.get_connection_file())
    kernel_id = connection_file.split('-', 1)[0].split('.')[0]

    for srv in notebookapp.list_running_servers():
        try:
            if srv['token']=='' and not srv['password']:  
                req = urllib.request.urlopen(srv['url']+'api/sessions')
            else:
                req = urllib.request.urlopen(srv['url']+ \
                                             'api/sessions?token=' + \
                                             srv['token'])
            sessions = json.load(req)
            notebookname = sessions[0]['name']
        except:
            pass  
    
    sep = '.'
    path = os.getcwd()
    #parent_path = str(Path(path).parent)
    
    ### Path report
    #path_report = "{}/Reports".format(parent_path)
    #path_report = "{}/Reports".format(path)
    
    ### Path destination
    name_no_extension = notebookname.split(sep, 1)[0]
    source_to_move = name_no_extension +'.{}'.format(extension)
    dest = os.path.join(path,'Reports', source_to_move)
    
    ### Generate notebook
    if keep_code:
        os.system('jupyter nbconvert --to {} {}'.format(
    extension,notebookname))
    else:
        os.system('jupyter nbconvert --no-input --to {} {}'.format(
    extension,notebookname))
    
    ### Move notebook to report folder
    #time.sleep(5)
    shutil.move(source_to_move, dest)
    print("Report Available at this adress:\n {}".format(dest))
```

```python
create_report(extension = "html")
```
