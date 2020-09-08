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
# Creation table INSEE INPI sans doublon

Copy paste from Coda to fill the information

## Objective(s)

Le rapprochement entre les deux tables, à savoir l’INSEE et l’INPI, va amener à la création de deux vecteurs d’adresse. Un vecteur avec des mots contenus spécifiquement à l’INSEE, et un second vecteur avec les mots de l’adresse de l’INPI. Notre objectif est de comparé ses deux vecteurs pour définir si ils sont identiques ou non. Nous avons distingué 7 cas de figures possibles entre les deux vecteurs (figure 1).

![](https://drive.google.com/uc?export=view&id=1Qj_HooHrhFYSuTsoqFbl4Vxy9tN3V5Bu)

A partir de la, nous avons créé une matrice de règles de gestion, puis créer lesdites règles selon les informations de l'INSEE et de l'INPI.

Dans cette matrice, chacune des lignes vient par ordre croissant, c'est a dire que la ligne 1 est préférée à la ligne 2

Le tableau ci dessous récapitule les règles:


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

Dans cette dernière étape, il suffit de récupérer le rang minimum de la table `ets_insee_inpi_regle` par `index_id`. En récupérant le minimum, la technique retourne la ligne la plus probable par rapport aux autres informations fournies par l'INSEE. AUtrement dit, nous avons récupéré la ligne qui satisfaient le plus de condition. Il est possible d'avoir encore des doublons, qui résultent d'une mauvaise préparation de la donnée ou d'une impossibilité de dédoubler le siret.

## Metadata 

* Metadata parameters are available here: 
* US Title: Creation table 11_creation_table_ets_insee_inpi_no_duplicate
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
  * Select table(s): `ets_insee_inpi_regle`
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
      * Tables (Add name new table): ets_insee_inpi_no_duplicate

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
database = 'siretisation'
```

```python
query = """
DROP TABLE siretisation.ets_insee_inpi_no_duplicate;
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
CREATE TABLE siretisation.ets_insee_inpi_no_duplicate
WITH (
  format='PARQUET'
) AS
WITH tb_min_rank AS (
SELECT 
  rank, 
  min_rank, 
  row_id, 
  ets_insee_inpi_regle.index_id, 
  siren, 
  siret, 
  sequence_id, 
  count_inpi_index_id_siret, 
  count_inpi_siren_siret, 
  count_initial_insee, 
  test_index_id_duplicate, 
  test_siren_insee_siren_inpi, 
  adresse_distance_insee, 
  adresse_distance_inpi, 
  insee_except, 
  inpi_except, 
  intersection, 
  union_, 
  pct_intersection, 
  index_id_max_intersection, 
  status_cas, 
  test_pct_intersection, 
  unzip_inpi, 
  unzip_insee, 
  max_cosine_distance, 
  key_except_to_test, 
  levenshtein_distance, 
  test_similarite_exception_words, 
  test_distance_levhenstein_exception_words, 
  list_numero_voie_matching_inpi, 
  list_numero_voie_matching_insee, 
  intersection_numero_voie, 
  union_numero_voie, 
  test_list_num_voie, 
  enseigne, 
  list_enseigne, 
  list_enseigne_contain, 
  test_enseigne, 
  "date_début_activité", 
  test_date, 
  etablissementsiege, 
  status_ets, 
  test_siege, 
  etatadministratifetablissement, 
  status_admin, 
  test_status_admin 
FROM 
  siretisation.ets_insee_inpi_regle 
  INNER JOIN (
    SELECT 
      index_id, 
      MIN(rank) AS min_rank 
    FROM 
      siretisation.ets_insee_inpi_regle 
    GROUP BY 
      index_id
  ) as tb_min_rank ON ets_insee_inpi_regle.index_id = tb_min_rank.index_id 
  AND ets_insee_inpi_regle.rank = tb_min_rank.min_rank
  )
  SELECT 
  rank, 
  min_rank, 
  row_id, 
  tb_min_rank.index_id, 
  count_index,
  siren, 
  siret, 
  sequence_id, 
  count_inpi_index_id_siret, 
  count_inpi_siren_siret, 
  count_initial_insee, 
  test_index_id_duplicate, 
  test_siren_insee_siren_inpi, 
  adresse_distance_insee, 
  adresse_distance_inpi, 
  insee_except, 
  inpi_except, 
  intersection, 
  union_, 
  pct_intersection, 
  index_id_max_intersection, 
  status_cas, 
  test_pct_intersection, 
  unzip_inpi, 
  unzip_insee, 
  max_cosine_distance, 
  key_except_to_test, 
  levenshtein_distance, 
  test_similarite_exception_words, 
  test_distance_levhenstein_exception_words, 
  list_numero_voie_matching_inpi, 
  list_numero_voie_matching_insee, 
  intersection_numero_voie, 
  union_numero_voie, 
  test_list_num_voie, 
  enseigne, 
  list_enseigne, 
  list_enseigne_contain, 
  test_enseigne, 
  "date_début_activité", 
  test_date, 
  etablissementsiege, 
  status_ets, 
  test_siege, 
  etatadministratifetablissement, 
  status_admin, 
  test_status_admin 
  FROM tb_min_rank
  LEFT JOIN (
    SELECT index_id, COUNT(*) AS count_index
    FROM tb_min_rank
    GROUP BY index_id
    ) as tb_nb_index
    ON tb_min_rank.index_id = tb_nb_index.index_id

"""

s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = None, ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

# Test acceptance

1. Count nombre lignes & index
2. Evaluation des doublons


## 1. Count nombre lignes & index


Nombre de lignes

```python
query = """
SELECT COUNT(*)
FROM ets_insee_inpi_no_duplicate 
"""

s3.run_query(
            query=query,
            database=database,
            s3_output='INPI/sql_output',
      filename = 'cnt_nb_lignes_rank', ## Add filename to print dataframe
      destination_key = None ### Add destination key if need to copy output
        )  
```

Nombre d'index

```python
query = """
SELECT COUNT(distinct(index_id))
FROM ets_insee_inpi_no_duplicate 
"""

s3.run_query(
            query=query,
            database=database,
            s3_output='INPI/sql_output',
      filename = 'cnt_nb_index_rank', ## Add filename to print dataframe
      destination_key = None ### Add destination key if need to copy output
        )
```

Nombre d'index par cas

```python
query = """
SELECT status_cas,  COUNT(distinct(index_id)) as cnt
FROM ets_insee_inpi_no_duplicate 
GROUP BY status_cas
ORDER BY cnt
"""

s3.run_query(
            query=query,
            database=database,
            s3_output='INPI/sql_output',
      filename = 'cnt_nb_index_rank', ## Add filename to print dataframe
      destination_key = None ### Add destination key if need to copy output
        )
```

## 2. Evaluation des doublons

Le tableau ci dessous récapitule les index uniques et les doublons

```python
query = """
SELECT count_index, COUNT(*) as ligne_dup
FROM ets_insee_inpi_no_duplicate 
GROUP BY count_index 
ORDER BY count_index
"""

nb_ligne = s3.run_query(
            query=query,
            database=database,
            s3_output='INPI/sql_output',
      filename = 'cnt_nb_dup_lignes_rank', ## Add filename to print dataframe
      destination_key = None ### Add destination key if need to copy output
)
```

```python
query = """
SELECT count_index, COUNT(DISTINCT(index_id)) as index_dup
FROM ets_insee_inpi_no_duplicate 
GROUP BY count_index 
ORDER BY count_index
"""

nb_index = s3.run_query(
            query=query,
            database=database,
            s3_output='INPI/sql_output',
      filename = 'cnt_nb_dup_index_rank', ## Add filename to print dataframe
      destination_key = None ### Add destination key if need to copy output
        )
```

```python
(
pd.concat([    
 pd.concat([
    pd.concat(
    [
        nb_ligne.sum().to_frame().T.rename(index = {0:'total'}), 
        nb_ligne
    ], axis = 0),
    ],axis = 1,keys=["Lignes"]),
    (
 pd.concat([
    pd.concat(
    [
        nb_index.sum().to_frame().T.rename(index = {0:'total'}), 
        nb_index
    ], axis = 0),
    ],axis = 1,keys=["Index"])
)],axis= 1
    )
    .style
    .format("{:,.0f}")
                  .bar(subset= [
                      ('Lignes','ligne_dup'),
                      ('Index','index_dup'),
                      
                  ],
                       color='#d65f5f')
)
```

Nombre d'index récuperé

```python
nb_index.iloc[0,1]
```

Nombre d'index a trouver

```python
nb_index.sum().to_frame().T.rename(index = {0:'total'}).iloc[0,1]
```

Pourcentage de probable trouvé

```python
round(nb_index.iloc[0,1] / nb_index.sum().to_frame().T.rename(index = {0:'total'}).iloc[0,1], 4)
```

Analyse des ranks

```python
query = """
WITH dataset AS (
  
  SELECT 
  MAP(
    ARRAY[0.1,0.25,0.5,0.75,0.8,0.95],
    approx_percentile(
      min_rank,
    ARRAY[0.1,0.25,0.5,0.75,0.8,0.95])
    ) AS nest
    FROM "siretisation"."ets_insee_inpi_no_duplicate"  
    ) 
    
    SELECT 
    pct, 
    value AS  min_rank
    FROM dataset
    CROSS JOIN UNNEST(nest) as t(pct, value)
"""
s3.run_query(
            query=query,
            database=database,
            s3_output='INPI/sql_output',
      filename = 'distribution_rank', ## Add filename to print dataframe
      destination_key = None ### Add destination key if need to copy output
        )
```

### Regle 10% 

```python
query ="""
SELECT *
FROM rank_matrice_regles_gestion 
WHERE rank = 3937
"""
s3.run_query(
            query=query,
            database=database,
            s3_output='INPI/sql_output',
      filename = 'rules', ## Add filename to print dataframe
      destination_key = None ### Add destination key if need to copy output
        )
```

```python
query ="""
SELECT *
FROM ets_insee_inpi_no_duplicate 
WHERE rank = 3937
LIMIT 3
"""
s3.run_query(
            query=query,
            database=database,
            s3_output='INPI/sql_output',
      filename = 'rules', ## Add filename to print dataframe
      destination_key = None ### Add destination key if need to copy output
        )
```

### Regle 25% 

```python
query ="""
SELECT *
FROM rank_matrice_regles_gestion 
WHERE rank = 3993
"""
s3.run_query(
            query=query,
            database=database,
            s3_output='INPI/sql_output',
      filename = 'rules', ## Add filename to print dataframe
      destination_key = None ### Add destination key if need to copy output
        )
```

```python
query ="""
SELECT *
FROM ets_insee_inpi_no_duplicate 
WHERE rank = 3993
LIMIT 3
"""
s3.run_query(
            query=query,
            database=database,
            s3_output='INPI/sql_output',
      filename = 'rules', ## Add filename to print dataframe
      destination_key = None ### Add destination key if need to copy output
        )
```

### Regle 50% 

```python
query ="""
SELECT *
FROM rank_matrice_regles_gestion 
WHERE rank = 4316
"""
s3.run_query(
            query=query,
            database=database,
            s3_output='INPI/sql_output',
      filename = 'rules', ## Add filename to print dataframe
      destination_key = None ### Add destination key if need to copy output
        )
```

```python
query ="""
SELECT * 
FROM ets_insee_inpi_no_duplicate 
WHERE min_rank = 4316
LIMIT 5
"""
s3.run_query(
            query=query,
            database=database,
            s3_output='INPI/sql_output',
      filename = 'rules_32141', ## Add filename to print dataframe
      destination_key = None ### Add destination key if need to copy output
        )
```

### Regle 75% 

```python
query ="""
SELECT *
FROM rank_matrice_regles_gestion 
WHERE rank = 6259
"""
s3.run_query(
            query=query,
            database=database,
            s3_output='INPI/sql_output',
      filename = 'rules', ## Add filename to print dataframe
      destination_key = None ### Add destination key if need to copy output
        )
```

```python
query ="""
SELECT * 
FROM ets_insee_inpi_no_duplicate 
WHERE min_rank = 6259
LIMIT 5
"""
s3.run_query(
            query=query,
            database=database,
            s3_output='INPI/sql_output',
      filename = 'rules_32141', ## Add filename to print dataframe
      destination_key = None ### Add destination key if need to copy output
        )
```

### Regle 80% 

```python
query ="""
SELECT *
FROM rank_matrice_regles_gestion 
WHERE rank = 11725
"""
s3.run_query(
            query=query,
            database=database,
            s3_output='INPI/sql_output',
      filename = 'rules', ## Add filename to print dataframe
      destination_key = None ### Add destination key if need to copy output
        )
```

```python
query ="""
SELECT * 
FROM ets_insee_inpi_no_duplicate 
WHERE min_rank = 11725
LIMIT 5
"""
s3.run_query(
            query=query,
            database=database,
            s3_output='INPI/sql_output',
      filename = 'rules_32141', ## Add filename to print dataframe
      destination_key = None ### Add destination key if need to copy output
        )
```

### Regle 95% 

```python
query ="""
SELECT *
FROM rank_matrice_regles_gestion 
WHERE rank = 27620
"""
s3.run_query(
            query=query,
            database=database,
            s3_output='INPI/sql_output',
      filename = 'rules', ## Add filename to print dataframe
      destination_key = None ### Add destination key if need to copy output
        )
```

```python
query ="""
SELECT * 
FROM ets_insee_inpi_no_duplicate 
WHERE min_rank = 27620
LIMIT 5
"""
s3.run_query(
            query=query,
            database=database,
            s3_output='INPI/sql_output',
      filename = 'rules_32141', ## Add filename to print dataframe
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
create_report(extension = "html",keep_code = True)
```
