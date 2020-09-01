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

# Creation variable test_enseigne a effectuer pour dedoublonner les lignes

Copy paste from Coda to fill the information

## Objective(s)

* La variable `test_enseigne` indique si tous l'enseigne' de l'INPI est contenu dans l'enseigne de l'INSEE
   * La variable peut prendre 3 valeurs, selon les conditions:
     * TRUE: l'enseigne de l'INPI est contenu dans l'une des possibilités de l'INSEE
     * NULL: L'une des deux listes est nulles
     * FALSE: L'enseigne de l'INPI n'est pas contenu dans l'INSEE

## Metadata 

* Metadata parameters are available here: 
* US Title: Creation variable test_enseigne a effectuer pour dedoublonner les lignes
* Epic: Epic 5
* US: US 7
* Date Begin: 9/1/2020
* Duration Task: 0
* Status: active  
* Source URL: [US 07 Preparation tables et variables tests](https://coda.io/d/_dCtnoqIftTn/US-07-Preparation-tables-et-variables-tests_suFb9)
* Task type:
  * Jupyter Notebook
* Users: :
  * Thomas Pernet
* Watchers:
  * Thomas Pernet
* Estimated Log points:
  * One being a simple task, 15 a very difficult one
  *  1
* Task tag
  *  #sql-query,#regle-de-gestion,#preparation-test-enseigne
* Toggl Tag
  * #data-preparation
  
## Input Cloud Storage [AWS]

If link from the internet, save it to the cloud first

### Tables [AWS]

1. Batch 1:
  * Select Provider: Athena
  * Select table(s): ets_insee_inpi_siretisation
    * Select only tables created from the same notebook, else copy/paste selection to add new input tables
    * If table(s) does not exist, add them: Add New Table
    * Information:
      * Region: 
        * NameEurope (Paris)
        * Code: eu-west-3
      * Database: siretisation
      * Notebook construction file: 
        * [01_merge_ets_insee_inpi](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/08_US_DATUM/01_merge_ets_insee_inpi.md)
    
## Destination Output/Delivery

## Things to know (Steps, Attention points or new flow of information)

### Sources of information  (meeting notes, Documentation, Query, URL)

1. Jupyter Notebook (Github Link)
  1. md : https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/02_siretisation/07_pourcentage_siretisation_v3.md#creation-table-analyse



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
database = 'inpi'
```

```python
query = """
WITH tb_enseigne AS (
SELECT 
siret, 
  index_id,
enseigne,
list_enseigne,
contains(
  list_enseigne,
  enseigne
  ) AS temp_test_enseigne 
FROM siretisation.ets_insee_inpi 
)
SELECT  *
FROM  (
  WITH test AS (
  SELECT
    siret,
    index_id,
enseigne,
list_enseigne,
CASE
WHEN cardinality(list_enseigne) = 0 OR list_enseigne IS NULL OR enseigne = '' THEN 'NULL' 
WHEN temp_test_enseigne = TRUE THEN 'TRUE'
ELSE 'FALSE' END AS test_enseigne
        
FROM tb_enseigne
       )
       SELECT *
       FROM (SELECT * 
             FROm test
       WHERE test_enseigne = 'TRUE'
       LIMIT 1
             )
       UNION (SELECT *
       FROM test
       WHERE cardinality(list_enseigne) = 0
              LIMIT 1
              )
      UNION (SELECT *
       FROM test
       WHERE list_enseigne IS NULL AND enseigne != ''
              LIMIT 1
              )
       UNION (SELECT *
       FROM test
       WHERE cardinality(list_enseigne) > 0 AND enseigne = ''
              LIMIT 1
              )
       UNION (SELECT *
       FROM test
       WHERE enseigne = ''
              LIMIT 1
              )
       UNION (SELECT *
       FROM test
       WHERE test_enseigne = 'FALSE'
              LIMIT 1
              )
       ORDER BY test_enseigne DESC
       )

"""

tb = s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = 'test_enseigne', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )

pd.concat([

pd.concat([
tb[['siret', 'enseigne', 'list_enseigne']]
],keys=["Input"], axis = 1),
pd.concat([
tb[['test_enseigne']]
],keys=["Output"], axis = 1)
], axis = 1
)
```

# Test acceptance

1. Vérifier que le nombre de lignes est indentique avant et après la création des variables
2. Compter le nombre de lignes par test
3. Compter le nombre d'index par test
4. Créer un tableau avec une ligne par test


## 1. Vérifier que le nombre de lignes est indentique avant et après la création des variables

```python
query = """
SELECT COUNT(*)
FROM siretisation.ets_insee_inpi
"""
s3.run_query(
            query=query,
            database='siretisation',
            s3_output=s3_output,
  filename = 'count_ets_insee_inpi', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

## 2. Compter le nombre de lignes par test

```python
query = """
WITH tb_enseigne AS (
SELECT 
siret, 
  index_id,
enseigne,
list_enseigne,
contains(
  list_enseigne,
  enseigne
  ) AS temp_test_enseigne 
FROM siretisation.ets_insee_inpi 
)
SELECT  *
FROM  (
  WITH test AS (
  SELECT
    siret,
    index_id,
enseigne,
list_enseigne,
CASE
WHEN cardinality(list_enseigne) = 0 OR list_enseigne IS NULL OR enseigne = '' THEN 'NULL' 
WHEN temp_test_enseigne = TRUE THEN 'TRUE'
ELSE 'FALSE' END AS test_enseigne
        
FROM tb_enseigne
       )
       SELECT count(*)
       FROM test
       )

"""

s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = 'test_count_enseigne', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

## 3. Compter le nombre d'index par test

```python
query = """
WITH tb_enseigne AS (
SELECT 
siret, 
  index_id,
enseigne,
list_enseigne,
contains(
  list_enseigne,
  enseigne
  ) AS temp_test_enseigne 
FROM siretisation.ets_insee_inpi 
)
SELECT  *
FROM  (
  WITH test AS (
  SELECT
    siret,
    index_id,
enseigne,
list_enseigne,
CASE
WHEN cardinality(list_enseigne) = 0 OR list_enseigne IS NULL OR enseigne = '' THEN 'NULL' 
WHEN temp_test_enseigne = TRUE THEN 'TRUE'
ELSE 'FALSE' END AS test_enseigne
        
FROM tb_enseigne
       )
       SELECT test_enseigne, count(*)
       FROM test
       GROUP BY test_enseigne
       )

"""

s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = 'test_count_ligne_enseigne', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

## 4. Créer un tableau avec une ligne par test

```python
query = """
WITH tb_enseigne AS (
SELECT 
siret, 
  index_id,
enseigne,
list_enseigne,
contains(
  list_enseigne,
  enseigne
  ) AS temp_test_enseigne 
FROM siretisation.ets_insee_inpi 
)
SELECT  *
FROM  (
  WITH test AS (
  SELECT
    siret,
    index_id,
enseigne,
list_enseigne,
CASE
WHEN cardinality(list_enseigne) = 0 OR list_enseigne IS NULL OR enseigne = '' THEN 'NULL' 
WHEN temp_test_enseigne = TRUE THEN 'TRUE'
ELSE 'FALSE' END AS test_enseigne
        
FROM tb_enseigne
       )
       SELECT test_enseigne, count(distinct(index_id))
       FROM test
       GROUP BY test_enseigne
       )

"""

s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = 'test_count_ligne_enseigne', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
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
