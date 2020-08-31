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

# Concatenation champs enseigne INSEE

Copy paste from Coda to fill the information

## Objective(s)

* Dans le but de comparer l’enseigne de l’INPI avec les possibilités coté INSEE, nous devons préparer une liste qui contient l’ensemble des enseignes possibles cotés INSEE.
  * Il faut que cette liste ne contienne que des champs unique  
* La nouvelle table va être sauvegardé dans la database siretisation avec une nouvelle variable appelée list_enseigne  

## Metadata 

* Metadata parameters are available here: 
* US Title: Concatenation champs enseigne INSEE
* Epic: Epic 5
* US: US 7
* Date Begin: 8/31/2020
* Duration Task: 1
* Status:  
* Source URL: [US 07 Preparation siretisation](https://coda.io/d/_dCtnoqIftTn/US-07-Preparation-siretisation_suFb9)
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
  *  #sql-query,#preparation-insee,#enseigne
* Toggl Tag
  * #data-preparation
  
## Input Cloud Storage [AWS]

If link from the internet, save it to the cloud first

### Tables [AWS]

1. Batch 1:
  * Select Provider: Athena
  * Select table(s): insee_final_sql
    * Select only tables created from the same notebook, else copy/paste selection to add new input tables
    * If table(s) does not exist, add them: Add New Table
    * Information:
      * Region: 
        * NameEurope (Paris)
        * Code: eu-west-3
      * Database: inpi
      * Notebook construction file: 
        *  https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/04_ETS_add_variables_insee.md
    
## Destination Output/Delivery

  1. AWS
    1. Athena: 
      * Region: Europe (Paris)
      * Database: siretisation
      * Tables (Add name new table): ets_insee_sql
      * List new tables
      * * ets_insee_sql

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

# Creation tables

## Steps

- Concatener les variables
    - `enseigne1etablissement`
    - `enseigne2etablissement`
    - `enseigne3etablissement`
- Creation list
- Exclusion des doublons (uniquement les mots distincts
- Enlever les espaces de trop

```python
s3_output = 'inpi/sql_output'
database = 'inpi'
```

## Exemple input/ouptup

```python
query = """
SELECT siret, enseigne1etablissement,enseigne2etablissement,enseigne3etablissement, array_remove(
      array_distinct(
        SPLIT(
          concat(
            enseigne1etablissement, ',', enseigne2etablissement, 
            ',', enseigne3etablissement
          ), 
          ','
        )
      ), 
      ''
    ) as list_enseigne
FROM inpi.insee_final_sql 
WHERE 
cardinality(array_remove(
array_distinct(
        SPLIT(
          concat(
            enseigne1etablissement, ',', enseigne2etablissement, 
            ',', enseigne3etablissement
          ), 
          ','
        )
      ), 
      ''
    )) > 1
LIMIT 10

"""

s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = 'Exemple_list_enseigne_insee', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

## Creation table

```python
query = """
CREATE TABLE siretisation.ets_insee_sql WITH (format = 'PARQUET') AS WITH tb AS (
  SELECT 
    *, 
    array_remove(
      array_distinct(
        SPLIT(
          concat(
            enseigne1etablissement, ',', enseigne2etablissement, 
            ',', enseigne3etablissement
          ), 
          ','
        )
      ), 
      ''
    ) as list_enseigne 
  FROM 
    inpi.insee_final_sql
) 
SELECT 
  count_initial_insee, 
  siren, 
  siret, 
  datecreationetablissement, 
  etablissementsiege, 
  etatadministratifetablissement, 
  codepostaletablissement, 
  codecommuneetablissement, 
  libellecommuneetablissement, 
  ville_matching, 
  libellevoieetablissement, 
  complementadresseetablissement, 
  numerovoieetablissement, 
  list_numero_voie_matching_insee, 
  indicerepetitionetablissement_full, 
  typevoieetablissement, 
  voie_clean, 
  adresse_reconstituee_insee, 
  adresse_distance_insee, 
  enseigne1etablissement, 
  enseigne2etablissement, 
  enseigne3etablissement, 
  CASE WHEN cardinality(list_enseigne) = 0 THEN NULL ELSE list_enseigne END AS list_enseigne 
FROM 
  tb

"""
s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = None, ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

## Test Acceptance

1. Compter le nombre le lignes par possibilité
2. Afficher le top 10 des enseignes en terme de compte
3. Imprimer lorsque la liste est égale a 1
4. Imprimer lorsque la liste est égale a 2
5. Imprimer lorsque la liste est égale a 4
6. Imprimer lorsque la liste est égale a 10


### 1. Compter le nombre le lignes par possibilité

```python
query = """
SELECT CARDINALiTY(list_enseigne), COUNT(*) AS nb_obs
FROM siretisation.ets_insee_sql 
GROUP BY CARDINALiTY(list_enseigne)
ORDER BY nb_obs DESC
"""
s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = 'count_enseigne_insee', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

### 2. Afficher le top 10 des enseignes en terme de compte

```python
query = """
SELECT list_enseigne, COUNT(*) AS nb_obs
FROM siretisation.ets_insee_sql 
GROUP BY list_enseigne
ORDER BY nb_obs DESC
LIMIT 10
"""
s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = 'count_top_enseigne_insee', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

### 3. Imprimer lorsque la liste est égale a 1

```python
query = """
SELECT enseigne1etablissement, 
  enseigne2etablissement, 
  enseigne3etablissement,list_enseigne
FROM siretisation.ets_insee_sql 
WHERE CARDINALITY(list_enseigne) = 1
LIMIT 10
"""
s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = 'count_cardinalirty_1_enseigne_insee', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

### 4. Imprimer lorsque la liste est égale a 2

```python
query = """
SELECT enseigne1etablissement, 
  enseigne2etablissement, 
  enseigne3etablissement,list_enseigne
FROM siretisation.ets_insee_sql 
WHERE CARDINALITY(list_enseigne) = 2
LIMIT 10
"""
s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = 'count_cardinalirty_2_enseigne_insee', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

### 5. Imprimer lorsque la liste est égale a 4

```python
query = """
SELECT enseigne1etablissement, 
  enseigne2etablissement, 
  enseigne3etablissement,list_enseigne
FROM siretisation.ets_insee_sql 
WHERE CARDINALITY(list_enseigne) = 3
LIMIT 10
"""
s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = 'count_cardinalirty_3_enseigne_insee', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

### 6. Imprimer lorsque la liste est égale a 10

```python
query = """
SELECT enseigne1etablissement, 
  enseigne2etablissement, 
  enseigne3etablissement,list_enseigne
FROM siretisation.ets_insee_sql 
WHERE CARDINALITY(list_enseigne) = 10
LIMIT 10
"""
s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = 'count_cardinalirty_10_enseigne_insee', ## Add filename to print dataframe
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
