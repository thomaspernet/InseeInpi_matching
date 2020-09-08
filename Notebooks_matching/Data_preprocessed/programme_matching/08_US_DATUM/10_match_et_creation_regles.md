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
# Creation table ets_insee_inpi_regle avec les regles de gestion

Copy paste from Coda to fill the information

## Objective(s)

Lors des US précédentes, nous avons créé l'ensemble des variables nécéssaires pour calculer les règles de gestion. 

Le tableau ci dessous indique l'ensemble des tests a réaliser ainsi que leur dépendence.

| Rang | Nom_variable                              | Dependence                                    | Notebook                           | Difficulte | Table_input                                                                                                                                                            | Variables_crees_US                                                                 | Possibilites                  |
|------|-------------------------------------------|-----------------------------------------------|------------------------------------|------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------|-------------------------------|
| 1    | status_cas                                |                                               | 02_cas_de_figure                   | Moyen      | ets_insee_inpi_status_cas                                                                                                                                              | status_cas,intersection,pct_intersection,union_,inpi_except,insee_except           | CAS_1,CAS_2,CAS_3,CAS_4,CAS_5 |
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

- Dans cette US, nous allons créer une table appelée `ets_insee_inpi_regle` qui fait un rapprochement des tables:

- ets_insee_inpi_status_cas
- ets_insee_inpi_list_num_voie
- ets_insee_inpi_list_enseigne
- ets_insee_inpi_var_group_max
- ets_insee_inpi_similarite_max_word2vec
- ets_insee_inpi

puis créer l'ensemble de variables contenant les règles


## Metadata 

* Metadata parameters are available here: 
* US Title: Creation table ets_insee_inpi_regle avec les regles de gestion
* Epic: Epic 8
* US: US 10
* Date Begin: 9/8/2020
* Duration Task: 0
* Status:  
* Source URL: 
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
  *  #computation,#sql-query, #regles
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

1. AWS
    1. Athena: 
      * Region: Europe (Paris)
      * Database: siretisation
      * Tables (Add name new table): ets_insee_inpi_regle
      * List new tables
      * ets_insee_inpi_regle

## Things to know (Steps, Attention points or new flow of information)

### Sources of information  (meeting notes, Documentation, Query, URL)

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
