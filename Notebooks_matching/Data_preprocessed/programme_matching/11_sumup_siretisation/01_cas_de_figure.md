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
# Creation de la table contenant les statut entre les adresses INSEE et INPI

# Objective(s)

*  Creation des cas de figures possible entre la comparaison de l’adresse INSEE et l’adresse de l’INPI
* Please, update the Source URL by clicking on the button after the information have been pasted
  * US 05 Creation variables de siretisation Modify rows
  * Delete tables and Github related to the US: Delete rows

# Metadata

* Epic: Epic 6
* US: US 5
* Date Begin: 9/29/2020
* Duration Task: 0
* Description: Creation de la documentation avec les types de statut et creation de la table statut
* Step type: Transform table
* Status: Active
  * Change Status task: Active
  * Update table: Modify rows
* Source URL: US 05 Creation variables de siretisation
* Task type: Jupyter Notebook
* Users: Thomas Pernet
* Watchers: Thomas Pernet
* User Account: https://937882855452.signin.aws.amazon.com/console
* Estimated Log points: 5
* Task tag: #athena,#sql,#data-preparation,#inpi,#insee
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
* ets_insee_inpi_statut_cas
* GitHub:
* https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/11_sumup_siretisation/01_cas_de_figure.md

<!-- #endregion -->

# Introduction

  *   La version 2 de la siretisation ne se résume plus a une prise en compte de quelques règles écrits pour dédoubler les lignes. Dans cette version, les tests représentent un ensemble cohérent et ordonné de règles gestion. 
  * Avant tout chose, nous devons lister puis créer ses tests à partir de la table ets_insee_inpi créer lors de l’US, [Creation table merge INSEE INPI filtree](https://coda.io/d/CreditAgricole_dCtnoqIftTn/US-07-Preparation-tables-et-variables-tests_suFb9). 
  *  La variable `status_cas`  indique le cas de figure détecté entre l'adresse de l'INSEE et l'INPI. Il y a 5 possibilités au total:
    *   CAS_1: Les mots dans l’adresse de l’INPI sont égales aux mots dans l’adresse de l’INSEE
    *   CAS_2: Aucun des mots de l’adresse de l’INPI sont égales aux mots dans l’adresse de l’INSEE
    *   CAS_3: Cardinalite exception parfaite mots INPI ou INSEE
    *   CAS_4: Cardinalite Exception mots INPI diffférente Cardinalite Exception mots INSEE
    *   CAS_5: Cadrinalite exception insee est égal de 0 ou cardinalite exception inpi est égal de 0
    *   CAS_6: CAS_NO_ADRESSE
  * Creation variables supplémentaires
      * `insee_except`: Liste de mots provenant de l'INSEE non contenue dans l'INPI
      * `inpi_except`: Liste de mots provenant de l'INPI non contenue dans l'INSEE
      * `intersection`: Nombre de mots en commun
      * `union_`: Nombre de mots total entre les deux adresses
      * `pct_intersection`: `intersection` / `union_`
 * Il faut penser a garder la variable `row_id` 
 
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
DROP TABLE siretisation.ets_insee_inpi_status_cas;
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
create_table = """
CREATE TABLE siretisation.ets_insee_inpi_status_cas
WITH (
  format='PARQUET'
) AS
WITH create_var AS (
  SELECT 
    row_id,
    siret, 
    adresse_distance_insee, 
    adresse_distance_inpi, 
    array_distinct(
      array_except(
        split(adresse_distance_insee, ' '), 
        split(adresse_distance_inpi, ' ')
      )
    ) as insee_except, 
    array_distinct(
      array_except(
        split(adresse_distance_inpi, ' '), 
        split(adresse_distance_insee, ' ')
      )
    ) as inpi_except, 
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
  FROM 
    siretisation.ets_insee_inpi
) 

SELECT 
  * 
FROM 
  (
    WITH test AS (
      SELECT 
      row_id,
        siret, 
        adresse_distance_insee, 
        adresse_distance_inpi, 
        CASE WHEN cardinality(insee_except) = 0 THEN NULL ELSE insee_except END as insee_except,
        CASE WHEN cardinality(inpi_except) = 0 THEN NULL ELSE inpi_except END as inpi_except,
        intersection, 
        union_, 
        intersection / union_ as pct_intersection, 
        CASE WHEN intersection = union_ THEN 'CAS_1' WHEN intersection = 0 THEN 'CAS_2' WHEN CARDINALITY(insee_except) = CARDINALITY(inpi_except) 
        AND intersection != union_ 
        AND intersection != union_ 
        AND intersection != 0 THEN 'CAS_3' WHEN (
          CARDINALITY(insee_except) = 0 
          OR CARDINALITY(inpi_except) = 0
        ) 
        AND intersection != union_ 
        AND intersection != 0 THEN 'CAS_5' WHEN CARDINALITY(insee_except) != CARDINALITY(inpi_except) 
        AND intersection != union_ 
        AND intersection != union_ 
        AND intersection != 0 THEN 'CAS_4' ELSE 'CAS_NO_ADRESSE' END AS status_cas 
      FROM 
        create_var 
    )
    SELECT *
    FROM test
    )
"""
s3.run_query(
            query=create_table,
            database=database,
            s3_output=s3_output,
  filename = None, ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

```python
query = """
SELECT *
       FROM (SELECT * 
             FROm siretisation.ets_insee_inpi_status_cas
       WHERE status_cas = 'CAS_1'
       LIMIT 1
             )
       UNION (SELECT *
       FROM siretisation.ets_insee_inpi_status_cas
       WHERE status_cas = 'CAS_2'
              LIMIT 1
              )
       UNION (SELECT *
       FROM siretisation.ets_insee_inpi_status_cas
       WHERE status_cas = 'CAS_3'
              LIMIT 1
              )
       UNION (SELECT *
       FROM siretisation.ets_insee_inpi_status_cas
       WHERE status_cas = 'CAS_4'
              LIMIT 1
              )
       UNION (SELECT *
       FROM siretisation.ets_insee_inpi_status_cas
       WHERE status_cas = 'CAS_5'
              LIMIT 1
              )
       ORDER BY status_cas
       

"""

tb = s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = 'tb_exemple', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )

tb = pd.concat([

pd.concat([
tb[['siret', 'adresse_distance_insee', 'adresse_distance_inpi']]
],keys=["Input"], axis = 1),
pd.concat([
tb[['insee_except', 'inpi_except', 'intersection', 'union_', 'pct_intersection','status_cas']]
],keys=["Output"], axis = 1)
], axis = 1
)

tb
```

# Test acceptance

1. Vérifier que le nombre de lignes est indentique avant et après la création des variables
2. Compter le nombre de lignes par cas
3. Compter le nombre d'index par cas
4. Créer un tableau avec une ligne par cas


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
SELECT count(*)
       FROM ets_insee_inpi_status_cas
 """
s3.run_query(
            query=query,
            database='siretisation',
            s3_output=s3_output,
  filename = 'count_ets_insee_inpi_status_cas', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

## 2. Compter le nombre de lignes par cas

```python
query = """
SELECT status_cas, count(*)
       FROM ets_insee_inpi_status_cas
       GROUP BY status_cas
       ORDER BY status_cas
       """
s3.run_query(
            query=query,
            database='siretisation',
            s3_output=s3_output,
  filename = 'count_group_ets_insee_inpi_status_cas', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

## 4. Créer un tableau avec une ligne par cas

```python
query = """
       SELECT *
       FROM (SELECT * 
             FROM ets_insee_inpi_status_cas
       WHERE status_cas = 'CAS_1'
       LIMIT 1
             )
       UNION (SELECT *
       FROM ets_insee_inpi_status_cas
       WHERE status_cas = 'CAS_2'
              LIMIT 1
              )
       UNION (SELECT *
       FROM ets_insee_inpi_status_cas
       WHERE status_cas = 'CAS_3'
              LIMIT 1
              )
       UNION (SELECT *
       FROM ets_insee_inpi_status_cas
       WHERE status_cas = 'CAS_4'
              LIMIT 1
              )
       UNION (SELECT *
       FROM ets_insee_inpi_status_cas
       WHERE status_cas = 'CAS_5'
              LIMIT 1
              )
       ORDER BY status_cas

"""

tb = s3.run_query(
            query=query,
            database='siretisation',
            s3_output=s3_output,
  filename = 'tb_exemple', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
pd.concat([

pd.concat([
tb[['siret', 'adresse_distance_insee', 'adresse_distance_inpi']]
],keys=["Input"], axis = 1),
pd.concat([
tb[['insee_except', 'inpi_except', 'intersection', 'union_', 'pct_intersection','status_cas']]
],keys=["Output"], axis = 1)
], axis = 1
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
