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

# Creation table INSEE INPI sans doublon

# Objective(s)

* Dans cette dernière étape, il suffit de récupérer le rang minimum de la table ets_insee_inpi_regle par index_id. En récupérant le minimum, la technique retourne la ligne la plus probable par rapport aux autres informations fournies par l'INSEE. AUtrement dit, nous avons récupéré la ligne qui satisfaient le plus de condition. Il est possible d'avoir encore des doublons, qui résultent d'une mauvaise préparation de la donnée ou d'une impossibilité de dédoubler le siret.

# Metadata

* Epic: Epic 6
* US: US 7
* Date Begin: 9/29/2020
* Duration Task: 0
* Description: récupérer le rang minimum de la table ets_insee_inpi_regle par index_id.
* Step type: Final table
* Status: Active
  * Change Status task: Active
  * Update table: Modify rows
* Source URL: US 07 Dedoublonnement
* Task type: Jupyter Notebook
* Users: Thomas Pernet
* Watchers: Thomas Pernet
* User Account: https://937882855452.signin.aws.amazon.com/console
* Estimated Log points: 5
* Task tag: #athena,#lookup-table,#sql,#remove-duplicate,#siretisation,#inpi,#siren,#siret,#insee,#documentation
* Toggl Tag: #documentation

# Input Cloud Storage [AWS/GCP]

## Table/file

* Origin: 
* Athena
* Name: 
* ets_insee_inpi_regle
* Github: 
  * https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/11_sumup_siretisation/08_creation_table_match_regles_gestion_insee_inpi.md

# Destination Output/Delivery

## Table/file

* Origin: 
* Athena
* Name:
* ets_insee_inpi_no_duplicate
* GitHub:
* https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/11_sumup_siretisation/09_creation_table_ets_insee_inpi_no_duplicate.md


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

<!-- #region -->
# Introduction

Le rapprochement entre les deux tables, à savoir l’INSEE et l’INPI, va amener à la création de deux vecteurs d’adresse. Un vecteur avec des mots contenus spécifiquement à l’INSEE, et un second vecteur avec les mots de l’adresse de l’INPI. Notre objectif est de comparé ses deux vecteurs pour définir si ils sont identiques ou non. Nous avons distingué 7 cas de figures possibles entre les deux vecteurs (figure 1).

![](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/-/raw/master/IMAGES/11_cas_de_figure.jpeg)

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
<!-- #endregion -->

```python
s3_output = 'SQL_OUTPUT_ATHENA'
database = 'ets_siretisation'
```

```python
query = """
DROP TABLE ets_siretisation.ets_insee_inpi_no_duplicate;
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
CREATE TABLE ets_siretisation.ets_insee_inpi_no_duplicate
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
  date_debut_activite, 
  test_date, 
  etablissementsiege, 
  status_ets, 
  test_siege, 
  etatadministratifetablissement, 
  status_admin, 
  test_status_admin 
FROM 
  ets_siretisation.ets_insee_inpi_regle 
  INNER JOIN (
    SELECT 
      index_id, 
      MIN(rank) AS min_rank 
    FROM 
      ets_siretisation.ets_insee_inpi_regle 
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
  date_debut_activite, 
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

# Analyse

1. Count nombre lignes & index
2. Evaluation des doublons


## 1. Count nombre lignes & index


Nombre de lignes

```python
query = """
SELECT COUNT(*) as CNT
FROM ets_insee_inpi_no_duplicate 
"""

s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
      filename = 'cnt_nb_lignes_rank', ## Add filename to print dataframe
      destination_key = None ### Add destination key if need to copy output
        )  
```

Nombre d'index

```python
query = """
SELECT COUNT(distinct(index_id)) as CNT
FROM ets_insee_inpi_no_duplicate 
"""

s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
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
            s3_output=s3_output,
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
            s3_output=s3_output,
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
            s3_output=s3_output,
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
    FROM "ets_siretisation"."ets_insee_inpi_no_duplicate"  
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
            s3_output=s3_output,
      filename = 'distribution_rank', ## Add filename to print dataframe
      destination_key = None ### Add destination key if need to copy output
        )
```

### Regle 10% 

```python
query ="""
SELECT *
FROM rank_matrice_regles_gestion 
WHERE rank = 2857
"""
s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
      filename = 'rules', ## Add filename to print dataframe
      destination_key = None ### Add destination key if need to copy output
        )
```

```python
query ="""
SELECT *
FROM ets_insee_inpi_no_duplicate 
WHERE rank = 2857
LIMIT 3
"""
s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
      filename = 'rules', ## Add filename to print dataframe
      destination_key = None ### Add destination key if need to copy output
        )
```

### Regle 25% 

```python
query ="""
SELECT *
FROM rank_matrice_regles_gestion 
WHERE rank = 3995
"""
s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
      filename = 'rules', ## Add filename to print dataframe
      destination_key = None ### Add destination key if need to copy output
        )
```

```python
query ="""
SELECT *
FROM ets_insee_inpi_no_duplicate 
WHERE rank = 3995
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
WHERE rank = 4481
"""
s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
      filename = 'rules', ## Add filename to print dataframe
      destination_key = None ### Add destination key if need to copy output
        )
```

```python
query ="""
SELECT * 
FROM ets_insee_inpi_no_duplicate 
WHERE min_rank = 4481
LIMIT 5
"""
s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
      filename = 'rules_32141', ## Add filename to print dataframe
      destination_key = None ### Add destination key if need to copy output
        )
```

### Regle 75% 

```python
query ="""
SELECT *
FROM rank_matrice_regles_gestion 
WHERE rank = 14335
"""
s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
      filename = 'rules', ## Add filename to print dataframe
      destination_key = None ### Add destination key if need to copy output
        )
```

```python
query ="""
SELECT * 
FROM ets_insee_inpi_no_duplicate 
WHERE min_rank = 14336
LIMIT 5
"""
s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
      filename = 'rules_32141', ## Add filename to print dataframe
      destination_key = None ### Add destination key if need to copy output
        )
```

### Regle 80% 

```python
query ="""
SELECT *
FROM rank_matrice_regles_gestion 
WHERE rank = 27295
"""
s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
      filename = 'rules', ## Add filename to print dataframe
      destination_key = None ### Add destination key if need to copy output
        )
```

```python
query ="""
SELECT * 
FROM ets_insee_inpi_no_duplicate 
WHERE min_rank = 27295
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
WHERE rank = 32021
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
WHERE min_rank = 32021
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

# Analysis 



```python
from scipy.stats import chi2_contingency
from scipy.stats import chi2
from statsmodels.stats.multicomp import MultiComparison
import scipy.stats as stats
```

```python
primary_key = "nom_greffe"
proba = .9
dic_tables = {}

to_include_cat = []
to_include_cont = []
```

```python
query = """
SELECT nom_greffe, origin, COUNT(*) AS COUNT
FROM ets_filtre_enrichie_historique 
GROUP BY nom_greffe, origin
"""
df_ = s3.run_query(
            query=query,
            database="ets_inpi",
            s3_output='INPI/sql_output',
      filename = 'test_chi', ## Add filename to print dataframe
      destination_key = None ### Add destination key if need to copy output
        )
```

```python
col = 'origin'
table = (
    df_
    .set_index(['nom_greffe', 'origin'])
    .unstack('origin')
    .fillna(0)
)
```

```python
stat, p, dof, expected = chi2_contingency(table)
critical = chi2.ppf(proba, dof)

if abs(stat) >= critical:
    to_include_cat.append('PO Sub Type')
    result = 'Dependent (reject H0)'
    to_include_cat.append(col)
else:
    result = 'Independent (fail to reject H0)'

dic_results = {
            'test': 'Chi Square',
            'primary_key': primary_key,
            'secondary_key': col,
            'statistic': stat,
            'p_value': p,
            'dof': dof,
            'critical': critical,
            'result': result
        }
```

```python
dic_results
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
