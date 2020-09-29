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

# Creation table ensemble regles de gestion

# Objective(s)

La siretisation repose sur une matrice de règles de gestion classée de manière ordonnée. Pour créer la matrice, il faut au préalable créer les variables nécéssaires à la création des tests. 

- status_cas,
- test_list_num_voie,
- test_enseigne
- test_pct_intersection
- test_index_id_duplicate
- test_siren_insee_siren_inpi
- test_similarite_exception_words
- test_distance_levhenstein_exception_words
- test_date
- test_siege
- test_status_admin

# Metadata

* Epic: Epic 6
* US: US 5
* Date Begin: 9/29/2020
* Duration Task: 0
* Description: Création de la table des regles de gestion
* Step type: Raw table
* Status: Active
  * Change Status task: Active
  * Update table: Modify rows
* Source URL:  
* Task type: Jupyter Notebook
* Users: Thomas Pernet
* Watchers: Thomas Pernet
* User Account: https://937882855452.signin.aws.amazon.com/console
* Estimated Log points: 5
* Task tag: #computation
* Toggl Tag: #documentation

# Input Cloud Storage [AWS/GCP]

## Table/file

* Origin: 
 
* Name: 
* 
* Github: 
* 

# Destination Output/Delivery

## Table/file

* Origin: 
* Athena
* Name:
* rank_matrice_regles_gestion
* GitHub:
* https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/11_sumup_siretisation/07_creation_table_regles_gestion.md


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

La siretisation repose sur une matrice de règles de gestion classée de manière ordonnée. Pour créer la matrice, il faut au préalable créer les variables nécéssaires à la création des tests. 

Les règles de gestion peuvent avoir les possibilités suivantes:

- `test_pct_intersection` = ['TRUE', 'FALSE']
- `status_cas` = ['CAS_1','CAS_3','CAS_4', 'CAS_5']
- `test_index_id_duplicate` = ['TRUE', 'FALSE']
- `test_list_num_voie` = ['TRUE','PARTIAL', 'NULL', 'FALSE']
- `test_siege` = ['TRUE','NULL','FALSE']
- `test_enseigne` =  ['TRUE','NULL', 'FALSE']
- `test_siren_insee_siren_inpi` = ['TRUE', 'FALSE']
- `test_similarite_exception_words` = ['TRUE', 'FALSE', 'NULL']
- `test_distance_levhenstein_exception_words` = ['TRUE', 'FALSE', 'NULL']
- `test_date` = ['TRUE','NULL','FALSE']
- `test_status_admin` = ['TRUE', 'FALSE']

```python
s3_output = 'SQL_OUTPUT_ATHENA'
database = 'ets_siretisation'
```

```python
query = """
DROP TABLE ets_siretisation.rank_matrice_regles_gestion;
"""

s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = None, ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

Nous utlisons Pandas pour créer la matrice. Il faut prendre le soin d'intégrer les variables dans l'ordre de préférence. A savoir:

1. `test_pct_intersection `
2. `status_cas `
3. `test_index_id_duplicate `
4. `test_list_num_voie `
5. `test_siege `
6. `test_enseigne `
7. `test_siren_insee_siren_inpi `
8. `test_similarite_exception_words `
9. `test_distance_levhenstein_exception_words `
10. `test_date `
11. `test_status_admin `

Il en va de même pour le contenu des variables. L'ordre est très important.

```python
test_pct_intersection = ['TRUE', 'FALSE']
status_cas = ['CAS_1','CAS_3','CAS_4', 'CAS_5']
test_index_id_duplicate = ['TRUE', 'FALSE']
test_list_num_voie = ['TRUE', 'PARTIAL','NULL', 'FALSE']
test_siege = ['TRUE','NULL','FALSE']
test_enseigne =  ['TRUE','NULL', 'FALSE']
test_siren_insee_siren_inpi = ['TRUE', 'FALSE']
test_similarite_exception_words = ['TRUE', 'FALSE', 'NULL']
test_distance_levhenstein_exception_words = ['TRUE', 'FALSE', 'NULL']
test_date = ['TRUE','NULL','FALSE']
test_status_admin = ['TRUE', 'FALSE']

index = pd.MultiIndex.from_product([
    test_pct_intersection,
    status_cas,
    test_index_id_duplicate,
    test_list_num_voie,
    test_siren_insee_siren_inpi,
    test_siege,
    test_enseigne,
    test_similarite_exception_words,
    test_distance_levhenstein_exception_words,
    test_date,
    test_status_admin
],
                                   names = [
                                       'test_pct_intersection',
                                       "status_cas",
                                            'test_index_id_duplicate',
                                            "test_list_num_voie",
                                            "test_siren_insee_siren_inpi",
                                           'test_siege', 
                                           'test_enseigne',
                                           'test_similarite_exception_words',
                                           'test_distance_levhenstein_exception_words',
                                           'test_date',
                                           'test_status_admin'])

df_ = (pd.DataFrame(index = index)
       .reset_index()
       .assign(rank = lambda x: x.index + 1)
       #.to_csv('Regle_tests.csv', index = False)
      )
df_.head()
```

```python
print("Il y a environ {} règles de gestion".format(df_.shape[0]))
```

Le csv se trouve dans le dossier [TEMP_ANALYSE_SIRETISATION/REGLES_TESTS](https://s3.console.aws.amazon.com/s3/buckets/calfdata/TEMP_ANALYSE_SIRETISATION/REGLES_TESTS)

```python
df_.to_csv('Regle_tests.csv', index = False)
s3.upload_file(file_to_upload = 'Regle_tests.csv',
            destination_in_s3 = 'TEMP_ANALYSE_SIRETISATION/REGLES_TESTS')

create_table = """
CREATE EXTERNAL TABLE IF NOT EXISTS ets_siretisation.rank_matrice_regles_gestion (
`test_pct_intersection`                     string,
`status_cas`                     string,
`test_index_id_duplicate`                     string,
`test_list_num_voie`                     string,
`test_siren_insee_siren_inpi`                     string,
`test_siege`                     string,
`test_enseigne`                     string,
`test_similarite_exception_words`                     string,
`test_distance_levhenstein_exception_words`                     string,
`test_date`                     string,
`test_status_admin`                     string,
`rank`                     integer

    )
     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
   'separatorChar' = ',',
   'quoteChar' = '"'
   )
     LOCATION 's3://calfdata/TEMP_ANALYSE_SIRETISATION/REGLES_TESTS'
     TBLPROPERTIES ('has_encrypted_data'='false',
              'skip.header.line.count'='1');"""
s3.run_query(
        query=create_table,
        database='ets_siretisation',
        s3_output=s3_output
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
