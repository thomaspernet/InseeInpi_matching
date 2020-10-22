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

# Creation table contenant le test de la liste enseigne

# Objective(s) 

*  Comparer la liste INSEE contenant les possibilités de l’enseigne et la variable enseigne de l’INPI:
  * `list_enseigne_contain`: Boolean → Si l’enseigne de l’INPI est contenu dans l’enseigne de l’INSEE alors True, sinon False

# Metadata

* Epic: Epic 6
* US: US 5
* Date Begin: 9/29/2020
* Duration Task: 0
* Description: Creation  des variables qui vont servir de test sur l’enseigne
* Step type: Transform table
* Status: Active
  * Change Status task: Active
  * Update table: Modify rows
* Source URL:  
* Task type: Jupyter Notebook
* Users: Thomas Pernet
* Watchers: Thomas Pernet
* User Account: https://937882855452.signin.aws.amazon.com/console
* Estimated Log points: 5
* Task tag: #sql,#athena,#data-preparation,#insee,#inpi,#documentation
* Toggl Tag: #documentation

# Input Cloud Storage [AWS/GCP]

## Table/file

* Origin: 
* Athena
* Name: 
* ets_insee_inpi
* Github: 
  * https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/11_sumup_siretisation/00_merge_ets_insee_inpi.md

# Destination Output/Delivery

## Table/file

* Origin: 
* Athena
* Name:
* ets_insee_inpi_list_enseigne
* GitHub:
* https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/11_sumup_siretisation/03_test_enseigne.md


## Connexion serveur

```python
from awsPy.aws_authorization import aws_connector
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

# Introduction

Pour préparer le test sur l'enseigne, il faut avoir un variables indiquant la ressemblance de la liste de l'enseigne de l'INSEE et celle de l'INPI. Ainsi, dans ce notebook, nous allons créer:

* `list_enseigne_contain`: Boolean → Si l’enseigne de l’INPI est contenu dans l’enseigne de l’INSEE alors True, sinon False

Le tableau ci dessous récapitule les sous-ensembles 1 à 6, qui vont de la création des types de ressemble entre l'adresse de l'INSEE et de l'INPI jusqu'aux tests sur la date, l'état administratif et de l'ensemble

 | Rang | Nom_variable                              | Dependence                                    | Notebook                           | Difficulte | Table_generee                                                                                                                                                            | Variables_crees_US                                                                 | Possibilites                  |
|------|-------------------------------------------|-----------------------------------------------|------------------------------------|------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------|-------------------------------|
| 1    | status_cas                                |                                               | 01_cas_de_figure                   | Moyen      | ets_insee_inpi_status_cas                                                                                                                                              | status_cas,intersection,pct_intersection,union_,inpi_except,insee_except           | CAS_1,CAS_2,CAS_3,CAS_4,CAS_5 |
| 2    | test_list_num_voie                        | intersection_numero_voie,union_numero_voie    | 03_test_list_num_voie              | Moyen      | ets_insee_inpi_list_num_voie                                                                                                                                           | intersection_numero_voie,union_numero_voie                                         | FALSE,NULL,TRUE,PARTIAL       |
| 3    | test_enseigne                             | list_enseigne,enseigne                        | 04_test_enseigne                   | Moyen      | ets_insee_inpi_list_enseigne                                                                                                                                           | list_enseigne_contain                                                              | FALSE,NULL,TRUE               |
| 4    | test_pct_intersection                     | pct_intersection,index_id_max_intersection    | 06_creation_nb_siret_siren_max_pct | Facile     | ets_insee_inpi_var_group_max                                                                                                                                           | count_inpi_index_id_siret,count_inpi_siren_siret,index_id_max_intersection         | FALSE,TRUE                    |
| 4    | test_index_id_duplicate                   | count_inpi_index_id_siret                     | 06_creation_nb_siret_siren_max_pct | Facile     | ets_insee_inpi_var_group_max                                                                                                                                           | count_inpi_index_id_siret,count_inpi_siren_siret,index_id_max_intersection         | FALSE,TRUE                    |
| 4    | test_siren_insee_siren_inpi               | count_initial_insee,count_inpi_siren_siret    | 06_creation_nb_siret_siren_max_pct | Facile     | ets_insee_inpi_var_group_max                                                                                                                                           | count_inpi_index_id_siret,count_inpi_siren_siret,index_id_max_intersection         | FALSE,TRUE                    |
| 5    | test_similarite_exception_words           | max_cosine_distance                           | 08_calcul_cosine_levhenstein       | Difficile  | ets_insee_inpi_similarite_max_word2vec                                                                                                                                 | unzip_inpi,unzip_insee,max_cosine_distance,levenshtein_distance,key_except_to_test | FALSE,NULL,TRUE               |
| 5    | test_distance_levhenstein_exception_words | levenshtein_distance                          | 08_calcul_cosine_levhenstein       | Difficile  | ets_insee_inpi_similarite_max_word2vec                                                                                                                                 | unzip_inpi,unzip_insee,max_cosine_distance,levenshtein_distance,key_except_to_test | FALSE,NULL,TRUE               |
| 6    | test_date                                 | datecreationetablissement,date_debut_activite | 10_match_et_creation_regles.md     | Facile     | ets_insee_inpi_list_num_voie,ets_insee_inpi_list_enseigne,ets_insee_inpi_similarite_max_word2vec,ets_insee_inpi_status_cas,ets_insee_inpi_var_group_max,ets_insee_inpi |                                                                                    | FALSE,TRUE                    |
| 6    | test_siege                                | status_ets,etablissementsiege                 | 10_match_et_creation_regles.md     | Facile     | ets_insee_inpi_list_num_voie,ets_insee_inpi_list_enseigne,ets_insee_inpi_similarite_max_word2vec,ets_insee_inpi_status_cas,ets_insee_inpi_var_group_max,ets_insee_inpi |                                                                                    | FALSE,TRUE,NULL               |
| 6    | test_status_admin                         | etatadministratifetablissement,status_admin   | 10_match_et_creation_regles.md     | Facile     | ets_insee_inpi_list_num_voie,ets_insee_inpi_list_enseigne,ets_insee_inpi_similarite_max_word2vec,ets_insee_inpi_status_cas,ets_insee_inpi_var_group_max,ets_insee_inpi |                                                                                    | FALSE,NULL,TRUE               |


```python
s3_output = 'SQL_OUTPUT_ATHENA'
database = 'ets_siretisation'
```

```python
query = """
DROP TABLE ets_siretisation.ets_insee_inpi_list_enseigne;
"""
s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = None, ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

```python
query = """
CREATE TABLE ets_siretisation.ets_insee_inpi_list_enseigne
WITH (
  format='PARQUET'
) AS
SELECT 
row_id,
siret, 
enseigne,
list_enseigne,
contains(
  list_enseigne,
  enseigne
  ) AS list_enseigne_contain 
FROM ets_siretisation.ets_insee_inpi
"""

s3.run_query(
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
FROM ets_siretisation.ets_insee_inpi_list_enseigne
WHERE list_enseigne_contain IS NOT NULL
LIMIT 10
"""
s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = 'exemple', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

# Analyse

1. Vérifier que le nombre de lignes est indentique avant et après la création des variables
2. Compter le nombre de lignes pour `list_enseigne_contain`
3. Compter le nombre d'occurences selon la cardinalité de `list_enseigne`


## 1. Vérifier que le nombre de lignes est indentique avant et après la création des variables

```python
query = """
SELECT COUNT(*) as CNT
FROM ets_siretisation.ets_insee_inpi_list_enseigne
"""
s3.run_query(
            query=query,
            database='ets_siretisation',
            s3_output=s3_output,
  filename = 'count_ets_insee_inpi', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

```python
query = """
SELECT COUNT(*) as CNT
FROM ets_siretisation.ets_insee_inpi_list_enseigne
"""

s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = 'test_count_enseigne', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

## 2. Compter le nombre de lignes pour `list_enseigne_contain`


```python
query = """
SELECT list_enseigne_contain, count(*) as cnt
       FROM ets_siretisation.ets_insee_inpi_list_enseigne
       GROUP BY list_enseigne_contain
"""

s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = 'test_count_ligne_enseigne', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

## 3. Compter le nombre d'occurences selon la cardinalité de `list_enseigne`


```python
query = """
SELECT CARDINALITY(list_enseigne) AS cardinality, COUNT(*) as cnt
FROM "ets_siretisation"."ets_insee_inpi_list_enseigne"
GROUP BY CARDINALITY(list_enseigne)
ORDER BY cnt
"""

s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = 'cardinality', ## Add filename to print dataframe
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
create_report(extension = "html", keep_code = True)
```
