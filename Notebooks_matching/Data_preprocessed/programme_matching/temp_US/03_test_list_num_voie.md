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
# Creation  variable test_list_num_voie a effecuer pour dedoublonner les lignes

Copy paste from Coda to fill the information

## Objective(s)

La siretisation repose sur une matrice de règles de gestion classée de manière ordonnée. Pour créer la matrice, il faut au préalable créer les variables nécéssaires à la création des tests. 

Le tableau ci dessous indique l'ensemble des tests a réaliser ainsi que leur dépendence.

| Rang | Nom_variable                              | Dependence                                                    | Notebook         | Difficulte | Table_input                            | Variables_crees_US                                                       | Possibilites                  |
|------|-------------------------------------------|---------------------------------------------------------------|------------------|------------|----------------------------------------|--------------------------------------------------------------------------|-------------------------------|
| 1    | status_cas                                | intersection,union_,lenght_list_inpi,insee_except,inpi_except | 02_cas_de_figure | Moyen      | ets_insee_inpi_status_cas              | status_cas,intersection,pct_intersection,union_,inpi_except,insee_except | CAS_1,CAS_2,CAS_3,CAS_4,CAS_5 |
| 2    | test_list_num_voie                        | intersection_numero_voie,union_numero_voie                    |                  | Moyen      | ets_insee_inpi_list_num_voie           |                                                                          | FALSE,NULL,TRUE,PARTIAL       |
| 3    | test_enseigne                             | enseigne,list_enseigne,enseigne_contain_insee_inpi            |                  | Moyen      | ets_insee_inpi_list_enseigne           |                                                                          | FALSE,NULL,TRUE               |
| 4    | test_pct_intersection                     | index_id_max_intersection                                     |                  | Facile     | ets_insee_inpi_var_group_max           |                                                                          | FALSE,TRUE                    |
| 4    | test_index_id_duplicate                   | count_inpi_index_id_siret                                     |                  | Facile     | ets_insee_inpi_var_group_max           |                                                                          | FALSE,TRUE                    |
| 4    | test_siren_insee_siren_inpi               | count_initial_insee,count_inpi_siren_siret                    |                  | Facile     | ets_insee_inpi_var_group_max           |                                                                          | FALSE,TRUE                    |
| 5    | test_similarite_exception_words           |                                                               |                  | Difficile  | ets_insee_inpi_similarite_max_word2vec |                                                                          |                               |
| 5    | test_distance_levhenstein_exception_words |                                                               |                  | Difficile  | ets_insee_inpi_similarite_max_word2vec |                                                                          |                               |
| 6    | test_date                                 | datecreationetablissement,date_debut_activite                 |                  | Facile     |                                        |                                                                          | FALSE,TRUE                    |
| 6    | test_siege                                | status_ets,etablissementsiege                                 |                  | Facile     |                                        |                                                                          | FALSE,TRUE,NULL               |
| 6    | test_status_admin                         | etatadministratifetablissement,status_admin                   |                  | Facile     |                                        |                                                                          | FALSE,NULL,TRUE               |


Lors de cette US, nous allons créer 2 variables qui vont permettre a la réalisation du test `test_list_num_voie`. Les deux variables sont les suivantes:

- `intersection_numero_voie`: Numéro de voie en commun entre `list_numero_voie_matching_inpi` et `list_numero_voie_matching_insee`
- `union_numero_voie`: Ensemble des numéros de voie en commun entre `list_numero_voie_matching_inpi` et `list_numero_voie_matching_insee`

## Metadata 

* Metadata parameters are available here: 
* US Title: Creation  variable test_list_num_voie a effecuer pour dedoublonner les lignes
* Epic: Epic 5
* US: US 7
* Date Begin: 9/1/2020
* Duration Task: 0
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
  *  #sql-query,#siretisation,#regle-de-gestion,#preparation-test-list
* Toggl Tag
  * #data-preparation
  
## Input Cloud Storage [AWS]

If link from the internet, save it to the cloud first

### Tables [AWS]

1. Batch 1:
  * Select Provider: Athena
  * Select table(s): ets_insee_inpi
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

 1. AWS
    1. Athena: 
      * Region: Europe (Paris)
      * Database: siretisation
      * Tables (Add name new table): ets_insee_inpi_list_num_voie
      * List new tables
      * ets_insee_inpi_list_num_voie
 
## Things to know (Steps, Attention points or new flow of information)

### Sources of information  (meeting notes, Documentation, Query, URL)

1. Jupyter Notebook (Github Link)
  1. md : https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/02_siretisation/07_pourcentage_siretisation_v3.md#creation-table-analyse

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
database = 'inpi'
```

```python
create_table = """
SELECT 
siret, 
list_numero_voie_matching_inpi, 
list_numero_voie_matching_insee,
CAST(
      cardinality(
        array_distinct(
          array_intersect(
            list_numero_voie_matching_inpi, 
            list_numero_voie_matching_insee
          )
        )
      ) AS DECIMAL(10, 2)
    ) as intersection_numero_voie, 
    CAST(
      cardinality(
        array_distinct(
          array_union(
            list_numero_voie_matching_inpi, 
            list_numero_voie_matching_insee
          )
        )
      ) AS DECIMAL(10, 2)
    ) as union_numero_voie
FROM siretisation.ets_insee_inpi 
"""
```

```python
query = """
WITH tb_list AS (
SELECT 
siret, 
list_numero_voie_matching_inpi, 
list_numero_voie_matching_insee,
CAST(
      cardinality(
        array_distinct(
          array_intersect(
            list_numero_voie_matching_inpi, 
            list_numero_voie_matching_insee
          )
        )
      ) AS DECIMAL(10, 2)
    ) as intersection_numero_voie, 
    CAST(
      cardinality(
        array_distinct(
          array_union(
            list_numero_voie_matching_inpi, 
            list_numero_voie_matching_insee
          )
        )
      ) AS DECIMAL(10, 2)
    ) as union_numero_voie
FROM siretisation.ets_insee_inpi 
)
SELECT  *
FROM  (WITH test AS (
  SELECT
  siret,
list_numero_voie_matching_inpi, 
list_numero_voie_matching_insee,
CASE 
WHEN intersection_numero_voie = union_numero_voie AND (intersection_numero_voie IS NOT NULL  OR union_numero_voie IS NOT NULL ) THEN 'TRUE' 
WHEN (intersection_numero_voie IS NULL OR union_numero_voie IS NULL ) THEN 'NULL' 
WHEN intersection_numero_voie >0 AND intersection_numero_voie != union_numero_voie THEN 'PARTIAL'
ELSE 'FALSE' END AS test_list_num_voie
        
FROM tb_list
       )
       SELECT *
       FROM (SELECT * 
             FROm test
       WHERE test_list_num_voie = 'TRUE'
       LIMIT 1
             )
       UNION (SELECT *
       FROM test
       WHERE test_list_num_voie = 'PARTIAL'
              LIMIT 1
              )
       UNION (SELECT *
       FROM test
       WHERE test_list_num_voie = 'NULL'
              LIMIT 1
              )
       UNION (SELECT *
       FROM test
       WHERE test_list_num_voie = 'FALSE'
              LIMIT 1
              )
       ORDER BY test_list_num_voie DESC
       )

"""

tb = s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = 'test_list_num_voie', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )

tb = s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = 'tb_exemple', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
print(pd.concat([

pd.concat([
tb[['siret', 'list_numero_voie_matching_inpi', 'list_numero_voie_matching_insee']]
],keys=["Input"], axis = 1),
pd.concat([
tb[['test_list_num_voie']]
],keys=["Output"], axis = 1)
], axis = 1
).to_markdown()
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

```python
query = """
WITH tb_list AS (
SELECT 
siret, 
list_numero_voie_matching_inpi, 
list_numero_voie_matching_insee,
CAST(
      cardinality(
        array_distinct(
          array_intersect(
            list_numero_voie_matching_inpi, 
            list_numero_voie_matching_insee
          )
        )
      ) AS DECIMAL(10, 2)
    ) as intersection_numero_voie, 
    CAST(
      cardinality(
        array_distinct(
          array_union(
            list_numero_voie_matching_inpi, 
            list_numero_voie_matching_insee
          )
        )
      ) AS DECIMAL(10, 2)
    ) as union_numero_voie
FROM siretisation.ets_insee_inpi 
)
SELECT  *
FROM  (WITH test AS (
  SELECT
  siret,
list_numero_voie_matching_inpi, 
list_numero_voie_matching_insee,
CASE 
WHEN intersection_numero_voie = union_numero_voie AND (intersection_numero_voie IS NOT NULL  OR union_numero_voie IS NOT NULL ) THEN 'TRUE' 
WHEN (intersection_numero_voie IS NULL OR union_numero_voie IS NULL ) THEN 'NULL' 
WHEN intersection_numero_voie >0 AND intersection_numero_voie != union_numero_voie THEN 'PARTIAL'
ELSE 'FALSE' END AS test_list_num_voie
        
FROM tb_list
       )
       SELECT COUNT(*)
       FROM test
       )

"""
s3.run_query(
            query=query,
            database='siretisation',
            s3_output=s3_output,
  filename = 'count_ets_insee_inpi_list_num', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

## 2. Compter le nombre de lignes par test

```python
query = """
WITH tb_list AS (
SELECT 
siret, 
list_numero_voie_matching_inpi, 
list_numero_voie_matching_insee,
CAST(
      cardinality(
        array_distinct(
          array_intersect(
            list_numero_voie_matching_inpi, 
            list_numero_voie_matching_insee
          )
        )
      ) AS DECIMAL(10, 2)
    ) as intersection_numero_voie, 
    CAST(
      cardinality(
        array_distinct(
          array_union(
            list_numero_voie_matching_inpi, 
            list_numero_voie_matching_insee
          )
        )
      ) AS DECIMAL(10, 2)
    ) as union_numero_voie
FROM siretisation.ets_insee_inpi 
)
SELECT  *
FROM  (WITH test AS (
  SELECT
  siret,
list_numero_voie_matching_inpi, 
list_numero_voie_matching_insee,
CASE 
WHEN intersection_numero_voie = union_numero_voie AND (intersection_numero_voie IS NOT NULL  OR union_numero_voie IS NOT NULL ) THEN 'TRUE' 
WHEN (intersection_numero_voie IS NULL OR union_numero_voie IS NULL ) THEN 'NULL' 
WHEN intersection_numero_voie >0 AND intersection_numero_voie != union_numero_voie THEN 'PARTIAL'
ELSE 'FALSE' END AS test_list_num_voie
        
FROM tb_list
       )
       SELECT test_list_num_voie, COUNT(*)
       FROM test
       GROUP BY test_list_num_voie
       )

"""
print(s3.run_query(
            query=query,
            database='siretisation',
            s3_output=s3_output,
  filename = 'count_ligne_ets_insee_inpi_list_num', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        ).to_markdown()
     )
```

## 3. Compter le nombre d'index par test

```python
query = """
WITH tb_list AS (
SELECT 
siret, index_id,
list_numero_voie_matching_inpi, 
list_numero_voie_matching_insee,
CAST(
      cardinality(
        array_distinct(
          array_intersect(
            list_numero_voie_matching_inpi, 
            list_numero_voie_matching_insee
          )
        )
      ) AS DECIMAL(10, 2)
    ) as intersection_numero_voie, 
    CAST(
      cardinality(
        array_distinct(
          array_union(
            list_numero_voie_matching_inpi, 
            list_numero_voie_matching_insee
          )
        )
      ) AS DECIMAL(10, 2)
    ) as union_numero_voie
FROM siretisation.ets_insee_inpi 
)
SELECT  *
FROM  (WITH test AS (
  SELECT
  siret,index_id,
list_numero_voie_matching_inpi, 
list_numero_voie_matching_insee,
CASE 
WHEN intersection_numero_voie = union_numero_voie AND (intersection_numero_voie IS NOT NULL  OR union_numero_voie IS NOT NULL ) THEN 'TRUE' 
WHEN (intersection_numero_voie IS NULL OR union_numero_voie IS NULL ) THEN 'NULL' 
WHEN intersection_numero_voie >0 AND intersection_numero_voie != union_numero_voie THEN 'PARTIAL'
ELSE 'FALSE' END AS test_list_num_voie
        
FROM tb_list
       )
       SELECT test_list_num_voie, COUNT(DISTINCT(index_id))
       FROM test
       GROUP BY test_list_num_voie
       )

"""
print(s3.run_query(
            query=query,
            database='siretisation',
            s3_output=s3_output,
  filename = 'count_index_ets_insee_inpi_list_num', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        ).to_markdown()
     )
```

## 4. Créer un tableau avec une ligne par test

```python
query = """
WITH tb_list AS (
SELECT 
siret, 
list_numero_voie_matching_inpi, 
list_numero_voie_matching_insee,
CAST(
      cardinality(
        array_distinct(
          array_intersect(
            list_numero_voie_matching_inpi, 
            list_numero_voie_matching_insee
          )
        )
      ) AS DECIMAL(10, 2)
    ) as intersection_numero_voie, 
    CAST(
      cardinality(
        array_distinct(
          array_union(
            list_numero_voie_matching_inpi, 
            list_numero_voie_matching_insee
          )
        )
      ) AS DECIMAL(10, 2)
    ) as union_numero_voie
FROM siretisation.ets_insee_inpi 
)
SELECT  *
FROM  (WITH test AS (
  SELECT
  siret,
list_numero_voie_matching_inpi, 
list_numero_voie_matching_insee,
CASE 
WHEN intersection_numero_voie = union_numero_voie AND (intersection_numero_voie IS NOT NULL  OR union_numero_voie IS NOT NULL ) THEN 'TRUE' 
WHEN (intersection_numero_voie IS NULL OR union_numero_voie IS NULL ) THEN 'NULL' 
WHEN intersection_numero_voie >0 AND intersection_numero_voie != union_numero_voie THEN 'PARTIAL'
ELSE 'FALSE' END AS test_list_num_voie
        
FROM tb_list
       )
       SELECT *
       FROM (SELECT * 
             FROm test
       WHERE test_list_num_voie = 'TRUE'
       LIMIT 1
             )
       UNION (SELECT *
       FROM test
       WHERE test_list_num_voie = 'PARTIAL'
              LIMIT 1
              )
       UNION (SELECT *
       FROM test
       WHERE test_list_num_voie = 'NULL'
              LIMIT 1
              )
       UNION (SELECT *
       FROM test
       WHERE test_list_num_voie = 'FALSE'
              LIMIT 1
              )
       ORDER BY test_list_num_voie DESC
       )

"""

tb = s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = 'test_list_num_voie', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )

tb = s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = 'tb_exemple', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
print(pd.concat([

pd.concat([
tb[['siret', 'list_numero_voie_matching_inpi', 'list_numero_voie_matching_insee']]
],keys=["Input"], axis = 1),
pd.concat([
tb[['test_list_num_voie']]
],keys=["Output"], axis = 1)
], axis = 1
).to_markdown()
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
