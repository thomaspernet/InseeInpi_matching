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

# Création table poids obtenus via le Word2Vec

Copy paste from Coda to fill the information

## Objective(s)

La siretisation repose sur une matrice de règles de gestion classée de manière ordonnée. Lors des US précédents, nous avons calculé 10 variables tests sur les 12. Voici les 10 tests calculés au préalable, avec leur dépendence:

- `test_pct_intersection`
    - Dependence: `index_id_max_intersection`
- `status_cas`
    - Dependence: `intersection`, `union_`, `insee_except` et `inpi_except`
- `index_id_duplicate`
    - Dependence: `count_inpi_index_id_siret`
- `test_siren_insee_siren_inpi`
    - Dependence: `count_inpi_siren_siret`
- `test_list_num_voie`
    - Dependence: `intersection_numero_voie`, `union_numero_voie`, ``
- `test_siren_insee_siren_inpi`
    - Dependence: `count_initial_insee`, `count_inpi_siren_siret`
- `test_siege`
    - Dependence: `status_ets`, `etablissementsiege`
- `test_enseigne`
    - Dependence: `enseigne`, `list_enseigne`
- `test_date `
    - Dependence: `datecreationetablissement`, `date_début_activité`
- `test_status_admin `
    - Dependence: `status_admin`, `etatadministratifetablissement`

Il reste encore deux tests a calculer afin de pouvoir matcher avec la matrice des règles. Les deux règles manquantes sont `test_distance_cosine` et `test_distance_levhenstein`. Néanmoins, ses deux tests requièrent le calcul de deux variables, dont l'une d'elle `max_cosine_distance` dépend des résultats de l'algorithme de Word2Vec. C'est l'object de cette US.

Dans cette US, nous allons calculer les poids permettants de mettre en avant la similarité entre deux mots. La similarité est calculé via le Word2Vec embedding.

## Metadata 

* Metadata parameters are available here: 
* US Title: Création table poids obtenus via le Word2Vec
* Epic: Epic 5
* US: US 7
* Date Begin: 9/7/2020
* Duration Task: 1
* Status:  
* Source URL:US 07 Preparation tables et variables tests
* Task type:
  * Jupyter Notebook
* Users: :
  * Thomas Pernet
* Watchers:
  * Thomas Pernet
* Estimated Log points:
  * One being a simple task, 15 a very difficult one
  *  7
* Task tag
  *  #machine-learning,#word2vec,#siretisation,#sql-query,#similarite,#preparation-tableword3vec
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
      * Database: inpi
      * Notebook construction file: 
        * https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/03_ETS_add_variables.md
    
## Destination Output/Delivery

* AWS
    1. Athena: 
      * Region: Europe (Paris)
      * Database: siretisation
      * Tables (Add name new table): list_pairs_mots_insee_inpi,weights_mots_insee_inpi_word2vec
      * List new tables
          * list_pairs_mots_insee_inpi
* weights_mots_insee_inpi_word2vec

## Things to know (Steps, Attention points or new flow of information)

### Sources of information  (meeting notes, Documentation, Query, URL)



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

"""

output = s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = None, ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

# Test acceptance

1.
2.
3.
4.

```python
query = """

"""

output = s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = None, ## Add filename to print dataframe
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
