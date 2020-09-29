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

# Creation table ets_insee_inpi_regle avec les regles de gestion

# Objective(s)

* Lors des US précédentes, nous avons créé l'ensemble des variables nécéssaires pour calculer les règles de gestion.
* Dans cette US, nous allons créer une table appelée ets_insee_inpi_regle qui fait un rapprochement des tables:
  * ets_insee_inpi_status_cas
  * ets_insee_inpi_list_num_voie
  * ets_insee_inpi_list_enseigne
  * ets_insee_inpi_var_group_max
  * ets_insee_inpi_similarite_max_word2vec
  * ets_insee_inpi
* puis créer l'ensemble de variables contenant les règles.
* A la fin du match, nous pouvons d'ores et déjà enlever lorsque status_cas est égale a CAS_2 et lorsque que test_list_num_voie est égale à FALSE

# Metadata

* Epic: Epic 6
* US: US 6
* Date Begin: 9/29/2020
* Duration Task: 0
* Description: Creation de l'ensemble de variables contenant les règles
* Step type: Transform table
* Status: Active
  * Change Status task: Active
  * Update table: Modify rows
* Source URL: US 06 Creation Tests
* Task type: Jupyter Notebook
* Users: Thomas Pernet
* Watchers: Thomas Pernet
* User Account: https://937882855452.signin.aws.amazon.com/console
* Estimated Log points: 5
* Task tag: #sql,#data-preparation,#insee,#siret,#siren,#siretisation
* Toggl Tag: #documentation

# Input Cloud Storage [AWS/GCP]

## Table/file

* Origin: 
* Athena
* Name: 
* ets_insee_inpi
* ets_insee_inpi_statut_cas
* ets_insee_inpi_list_num_voie
* ets_insee_inpi_list_enseigne
* ets_insee_inpi_var_group_max
* ets_inpi_similarite_max_word2vec
* Github: 
  * https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/11_sumup_siretisation/00_merge_ets_insee_inpi.md
  * https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/11_sumup_siretisation/01_cas_de_figure.md
  * https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/11_sumup_siretisation/02_test_list_num_voie.md
  * https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/11_sumup_siretisation/03_test_enseigne.md
  * https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/11_sumup_siretisation/04_creation_nb_siret_siren_max_pct.md
  * https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/11_sumup_siretisation/06_calcul_cosine_levhenstein.md

# Destination Output/Delivery

## Table/file

* Origin: 
* Athena
* Name:
* ets_insee_inpi_regle
* GitHub:
* https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/11_sumup_siretisation/08_creation_table_match_regles_gestion_insee_inpi.md




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

# Introduction

nous allons créer une table appelée `ets_insee_inpi_regle` qui fait un rapprochement des tables:

- `ets_insee_inpi_status_cas`
- `ets_insee_inpi_list_num_voie`
- `ets_insee_inpi_list_enseigne`
- `ets_insee_inpi_var_group_max`
- `ets_insee_inpi_similarite_max_word2vec`
- `ets_insee_inpi`

puis créer l'ensemble de variables contenant les règles.

A la fin du match, nous pouvons d'ores et déjà enlever lorsque `status_cas` est égale a `CAS_2` et lorsque que `test_list_num_voie ` est égale à `FALSE`

## Regles de gestion

Ci dessous, il y a un résumé des règles des gestion pour créer les variables

### `test_list_num_voie`

Query:

```
CASE 
WHEN intersection_numero_voie = union_numero_voie AND (intersection_numero_voie IS NOT NULL  OR union_numero_voie IS NOT NULL ) THEN 'TRUE' 
WHEN (intersection_numero_voie IS NULL OR union_numero_voie IS NULL ) THEN 'NULL' 
WHEN intersection_numero_voie >0 AND intersection_numero_voie != union_numero_voie THEN 'PARTIAL'
ELSE 'FALSE' END AS test_list_num_voie
```

### `test_enseigne`

Query:

```
CASE WHEN cardinality(ets_insee_inpi.list_enseigne) = 0 
OR ets_insee_inpi.list_enseigne IS NULL 
OR ets_insee_inpi.enseigne = '' THEN 'NULL' WHEN list_enseigne_contain = TRUE THEN 'TRUE' ELSE 'FALSE' END AS test_enseigne
```

### `test_pct_intersection`

Query:

```
CASE WHEN ets_insee_inpi_var_group_max.pct_intersection = index_id_max_intersection THEN 'TRUE' ELSE 'FALSE' END AS test_pct_intersection
```

### `test_index_id_duplicate`

Query:

```
CASE WHEN count_inpi_index_id_siret > 1 THEN 'TRUE' ELSE 'FALSE' END AS test_index_id_duplicate
```

### `test_siren_insee_siren_inpi`

Query:

```
CASE WHEN count_initial_insee = count_inpi_siren_siret THEN 'TRUE' ELSE 'FALSE' END AS test_siren_insee_siren_inpi,
```

### `test_similarite_exception_words`

Query:

```
CASE WHEN max_cosine_distance >= .6 THEN 'TRUE' 
WHEN CARDINALITY(ets_insee_inpi_status_cas.inpi_except)  IS NULL AND CARDINALITY(ets_insee_inpi_status_cas.insee_except) IS NULL THEN 'NULL'
ELSE 'FALSE' END AS test_similarite_exception_words
```

### `test_distance_levhenstein_exception_words`

Query:

```
CASE WHEN levenshtein_distance(unzip_inpi, unzip_insee) <=1  THEN 'TRUE' 
WHEN CARDINALITY(ets_insee_inpi_status_cas.inpi_except)  IS NULL AND CARDINALITY(ets_insee_inpi_status_cas.insee_except) IS NULL THEN 'NULL'
ELSE 'FALSE' END AS test_distance_levhenstein_exception_words
```

### `test_date`

Query:

```
CASE WHEN datecreationetablissement = "date_début_activité" THEN 'TRUE' WHEN datecreationetablissement = '' 
OR "date_début_activité" ='' THEN 'NULL' ELSE 'FALSE' END AS test_date
```

### `test_siege`

Query:

```

```

### `test_status_admin`

Query:

```
CASE WHEN etatadministratifetablissement = status_admin THEN 'TRUE' WHEN etatadministratifetablissement IS NULL 
        OR status_admin IS NULL THEN 'NULL' WHEN etatadministratifetablissement = '' 
        OR status_admin = '' THEN 'NULL' ELSE 'FALSE' END AS test_status_admin
```

```python
s3_output = 'inpi/sql_output'
database = 'siretisation'
```

```python
query = """
DROP TABLE siretisation.ets_insee_inpi_regle;
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
CREATE TABLE siretisation.ets_insee_inpi_regle
WITH (
  format='PARQUET'
) AS
WITH creation_test AS (

SELECT 

ets_insee_inpi.row_id,
ets_insee_inpi.index_id,
ets_insee_inpi.siren,
ets_insee_inpi.siret, 
sequence_id, 

-- ets_insee_inpi_var_group_max
count_inpi_index_id_siret, 
count_inpi_siren_siret,
count_initial_insee,
CASE WHEN count_inpi_index_id_siret > 1 THEN 'TRUE' ELSE 'FALSE' END AS test_index_id_duplicate,
CASE WHEN count_initial_insee = count_inpi_siren_siret THEN 'TRUE' ELSE 'FALSE' END AS test_siren_insee_siren_inpi,

-- status cas
ets_insee_inpi.adresse_distance_insee, ets_insee_inpi.adresse_distance_inpi, ets_insee_inpi_status_cas.insee_except, ets_insee_inpi_status_cas.inpi_except, intersection, union_, ets_insee_inpi_var_group_max.pct_intersection, index_id_max_intersection,
status_cas,

CASE WHEN ets_insee_inpi_var_group_max.pct_intersection = index_id_max_intersection THEN 'TRUE' ELSE 'FALSE' END AS test_pct_intersection,

-- ets_insee_inpi_similarite_max_word2vec
unzip_inpi, unzip_insee, max_cosine_distance, key_except_to_test, levenshtein_distance, 
CASE WHEN max_cosine_distance >= .6 THEN 'TRUE' 
WHEN CARDINALITY(ets_insee_inpi_status_cas.inpi_except)  IS NULL AND CARDINALITY(ets_insee_inpi_status_cas.insee_except) IS NULL THEN 'NULL'
ELSE 'FALSE' END AS test_similarite_exception_words,

CASE WHEN levenshtein_distance(unzip_inpi, unzip_insee) <=1  THEN 'TRUE' 
WHEN CARDINALITY(ets_insee_inpi_status_cas.inpi_except)  IS NULL AND CARDINALITY(ets_insee_inpi_status_cas.insee_except) IS NULL THEN 'NULL'
ELSE 'FALSE' END AS test_distance_levhenstein_exception_words,

-- liste num voie
ets_insee_inpi.list_numero_voie_matching_inpi,
ets_insee_inpi.list_numero_voie_matching_insee,
intersection_numero_voie,
union_numero_voie,
CASE 
WHEN intersection_numero_voie = union_numero_voie AND (intersection_numero_voie IS NOT NULL  OR union_numero_voie IS NOT NULL ) THEN 'TRUE' 
WHEN (intersection_numero_voie IS NULL OR union_numero_voie IS NULL ) THEN 'NULL' 
WHEN intersection_numero_voie >0 AND intersection_numero_voie != union_numero_voie THEN 'PARTIAL'
ELSE 'FALSE' END AS test_list_num_voie,

-- ets_insee_inpi_list_enseigne
ets_insee_inpi.enseigne,
ets_insee_inpi.list_enseigne,
list_enseigne_contain,
CASE WHEN cardinality(ets_insee_inpi.list_enseigne) = 0 
OR ets_insee_inpi.list_enseigne IS NULL 
OR ets_insee_inpi.enseigne = '' THEN 'NULL' WHEN list_enseigne_contain = TRUE THEN 'TRUE' ELSE 'FALSE' END AS test_enseigne,


-- Test date
datecreationetablissement, "date_début_activité",
CASE WHEN datecreationetablissement = "date_début_activité" THEN 'TRUE' WHEN datecreationetablissement = '' 
OR "date_début_activité" ='' THEN 'NULL' ELSE 'FALSE' END AS test_date,

-- Test siege
etablissementsiege,status_ets,
CASE WHEN etablissementsiege = status_ets THEN 'TRUE' WHEN etablissementsiege IS NULL 
OR status_ets IS NULL THEN 'NULL' WHEN etablissementsiege = '' 
OR status_ets = '' THEN 'NULL' ELSE 'FALSE' END AS test_siege,

-- Test status admin
etatadministratifetablissement,
status_admin,
CASE WHEN etatadministratifetablissement = status_admin THEN 'TRUE' WHEN etatadministratifetablissement IS NULL 
        OR status_admin IS NULL THEN 'NULL' WHEN etatadministratifetablissement = '' 
        OR status_admin = '' THEN 'NULL' ELSE 'FALSE' END AS test_status_admin

FROM siretisation.ets_insee_inpi 

-- status cas
LEFT JOIN siretisation.ets_insee_inpi_status_cas 
ON siretisation.ets_insee_inpi.row_id = siretisation.ets_insee_inpi_status_cas.row_id

-- liste num voie
LEFT JOIN siretisation.ets_insee_inpi_list_num_voie  
ON siretisation.ets_insee_inpi.row_id = siretisation.ets_insee_inpi_list_num_voie.row_id

-- ets_insee_inpi_list_enseigne
LEFT JOIN siretisation.ets_insee_inpi_list_enseigne  
ON siretisation.ets_insee_inpi.row_id = siretisation.ets_insee_inpi_list_enseigne.row_id

-- ets_insee_inpi_var_group_max
LEFT JOIN siretisation.ets_insee_inpi_var_group_max  
ON siretisation.ets_insee_inpi.row_id = siretisation.ets_insee_inpi_var_group_max.row_id

-- ets_insee_inpi_similarite_max_word2vec
LEFT JOIN siretisation.ets_inpi_similarite_max_word2vec  
ON siretisation.ets_insee_inpi.row_id = siretisation.ets_inpi_similarite_max_word2vec.row_id

  )
  SELECT rank, 
  row_id,
index_id,
siren,
siret, 
sequence_id, 

-- ets_insee_inpi_var_group_max
count_inpi_index_id_siret, 
count_inpi_siren_siret,
count_initial_insee,
creation_test.test_index_id_duplicate,
creation_test.test_siren_insee_siren_inpi,

-- status cas
adresse_distance_insee, 
adresse_distance_inpi,
insee_except, 
inpi_except, 
intersection, 
union_,
pct_intersection, 
index_id_max_intersection,
creation_test.status_cas,
creation_test.test_pct_intersection,

-- ets_insee_inpi_similarite_max_word2vec
unzip_inpi, 
unzip_insee, 
max_cosine_distance, 
key_except_to_test,
levenshtein_distance, 
creation_test.test_similarite_exception_words,
creation_test.test_distance_levhenstein_exception_words,

-- liste num voie
list_numero_voie_matching_inpi,
list_numero_voie_matching_insee,
intersection_numero_voie,
union_numero_voie,
creation_test.test_list_num_voie,

-- ets_insee_inpi_list_enseigne
enseigne,
list_enseigne,
list_enseigne_contain,
creation_test.test_enseigne,


-- Test date
datecreationetablissement, 
"date_début_activité",
creation_test.test_date,

-- Test siege
etablissementsiege,
status_ets,
creation_test.test_siege,

-- Test status admin
etatadministratifetablissement,
status_admin,
creation_test.test_status_admin

  FROM creation_test
  LEFT JOIN siretisation.rank_matrice_regles_gestion  

  ON  creation_test.test_pct_intersection = rank_matrice_regles_gestion.test_pct_intersection

  AND  creation_test.status_cas = rank_matrice_regles_gestion.status_cas 
  
  AND creation_test.test_index_id_duplicate = rank_matrice_regles_gestion.test_index_id_duplicate 
  
  AND creation_test.test_list_num_voie = rank_matrice_regles_gestion.test_list_num_voie 
  AND creation_test.test_siren_insee_siren_inpi = rank_matrice_regles_gestion.test_siren_insee_siren_inpi
  AND creation_test.test_siege = rank_matrice_regles_gestion.test_siege 
  AND creation_test.test_enseigne = rank_matrice_regles_gestion.test_enseigne
  
  AND creation_test.test_similarite_exception_words = rank_matrice_regles_gestion.test_similarite_exception_words 
  AND creation_test.test_distance_levhenstein_exception_words = rank_matrice_regles_gestion.test_distance_levhenstein_exception_words
  
  AND creation_test.test_date = rank_matrice_regles_gestion.test_date 
  AND creation_test.test_status_admin = rank_matrice_regles_gestion.test_status_admin
  WHERE creation_test.status_cas != 'CAS_2' AND creation_test.test_list_num_voie != 'FALSE'
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

## test sur `test_list_num_voie`

1. Compter le nombre de lignes par test
2. Compter le nombre d'index par test
3. Créer un tableau avec une ligne par test


### 1. Compter le nombre de lignes par test

```python
query = """
SELECT test_list_num_voie, COUNT(*) as cnt
       FROM ets_insee_inpi_regle
       GROUP BY test_list_num_voie
"""

s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = 'count_ligne_ets_insee_inpi_list_num', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

### 2. Compter le nombre d'index par test

```python
query = """
SELECT test_list_num_voie, COUNT(DISTINCT(index_id)) as cnt
       FROM ets_insee_inpi_regle
       GROUP BY test_list_num_voie
"""

s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = 'count_ligne_ets_insee_inpi_list_num', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

### 3. Créer un tableau avec une ligne par test

```python
query = """
SELECT *
       FROM (SELECT * 
             FROm ets_insee_inpi_regle
       WHERE test_list_num_voie = 'TRUE'
       LIMIT 1
             )
       UNION (SELECT *
       FROM ets_insee_inpi_regle
       WHERE test_list_num_voie = 'PARTIAL'
              LIMIT 1
              )
       UNION (SELECT *
       FROM ets_insee_inpi_regle
       WHERE test_list_num_voie = 'NULL'
              LIMIT 1
              )
       ORDER BY test_list_num_voie DESC
"""
tb = s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = 'test_list_num_voie', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

```python
pd.concat([

pd.concat([
tb[['siret', 'list_numero_voie_matching_inpi', 'list_numero_voie_matching_insee']]
],keys=["Input"], axis = 1),
pd.concat([
tb[['test_list_num_voie']]
],keys=["Output"], axis = 1)
], axis = 1
)
```

## test sur `test_enseigne`

1. Compter le nombre de lignes par test
2. Compter le nombre d'index par test
3. Créer un tableau avec une ligne par test


### 1. Compter le nombre de lignes par test

```python
query = """
SELECT test_enseigne, count(*) as cnt
       FROM ets_insee_inpi_regle
       GROUP BY test_enseigne
"""
s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = 'count_ligne_ets_insee_inpi_list_num', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

### 2. Compter le nombre d'index par test

```python
query = """
SELECT test_enseigne, COUNT(DISTINCT(index_id)) as cnt
       FROM ets_insee_inpi_regle
       GROUP BY test_enseigne
"""
s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = 'count_ligne_ets_insee_inpi_list_num', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

### 3. Créer un tableau avec une ligne par test

```python
query = """
SELECT *
       FROM (SELECT * 
             FROm ets_insee_inpi_regle
       WHERE test_enseigne = 'TRUE'
       LIMIT 1
             )
       UNION (SELECT *
       FROM ets_insee_inpi_regle
       WHERE cardinality(list_enseigne) = 0
              LIMIT 1
              )
      UNION (SELECT *
       FROM ets_insee_inpi_regle
       WHERE list_enseigne IS NULL AND enseigne != ''
              LIMIT 1
              )
       UNION (SELECT *
       FROM ets_insee_inpi_regle
       WHERE cardinality(list_enseigne) > 0 AND enseigne = ''
              LIMIT 1
              )
       UNION (SELECT *
       FROM ets_insee_inpi_regle
       WHERE enseigne = ''
              LIMIT 1
              )
       UNION (SELECT *
       FROM ets_insee_inpi_regle
       WHERE test_enseigne = 'FALSE'
              LIMIT 1
              )
       ORDER BY test_enseigne DESC
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

## test sur variable similarite `test_similarite_exception_words`

1. Compter le nombre de lignes par test
2. Compter le nombre d'index par test
3. Créer un tableau avec une ligne par test


### 1. Compter le nombre de lignes par test

```python
query = """
SELECT test_similarite_exception_words, count(*) as cnt
       FROM ets_insee_inpi_regle
       GROUP BY test_similarite_exception_words
"""
s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = 'count_test_similarite_exception_words', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

### 2. Compter le nombre d'index par test

```python
query = """
SELECT test_similarite_exception_words, count(DISTINCT(index_id)) as cnt
       FROM ets_insee_inpi_regle
       GROUP BY test_similarite_exception_words
"""
s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = 'count_test_similarite_exception_words', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

### 3. Créer un tableau avec une ligne par test

```python
query = """
SELECT *
       FROM (SELECT * 
             FROm ets_insee_inpi_regle
       WHERE test_similarite_exception_words = 'TRUE'
       LIMIT 1
             )
       UNION (SELECT *
       FROM ets_insee_inpi_regle
       WHERE test_similarite_exception_words = 'FALSE'
              LIMIT 1
              )
       UNION (SELECT *
       FROM ets_insee_inpi_regle
       WHERE test_similarite_exception_words = 'NULL'
              LIMIT 1
              )
       ORDER BY test_similarite_exception_words DESC
"""
tb = s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = 'test_similarite_exception_wordse', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

```python
pd.concat([

pd.concat([
tb[['siret', 'unzip_insee', 'unzip_inpi']]
],keys=["Input"], axis = 1),
pd.concat([
tb[['test_similarite_exception_words']]
],keys=["Output"], axis = 1)
], axis = 1
)
```

## test sur variable similarite `test_distance_levhenstein_exception_words`

1. Compter le nombre de lignes par test
2. Compter le nombre d'index par test
3. Créer un tableau avec une ligne par test


### 1. Compter le nombre de lignes par test

```python
query = """
SELECT test_distance_levhenstein_exception_words, count(*) as cnt
       FROM ets_insee_inpi_regle
       GROUP BY test_distance_levhenstein_exception_words
"""
s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = 'test_distance_levhenstein_exception_words', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

### 2. Compter le nombre d'index par test

```python
query = """
SELECT test_distance_levhenstein_exception_words, count(DISTINCT(index_id)) as cnt
       FROM ets_insee_inpi_regle
       GROUP BY test_distance_levhenstein_exception_words
"""
s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = 'test_distance_levhenstein_exception_words', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

### 3. Créer un tableau avec une ligne par test

```python
query = """
SELECT *
       FROM (SELECT * 
             FROm ets_insee_inpi_regle
       WHERE test_distance_levhenstein_exception_words = 'TRUE'
       LIMIT 1
             )
       UNION (SELECT *
       FROM ets_insee_inpi_regle
       WHERE test_distance_levhenstein_exception_words = 'FALSE'
              LIMIT 1
              )
       UNION (SELECT *
       FROM ets_insee_inpi_regle
       WHERE test_distance_levhenstein_exception_words = 'NULL'
              LIMIT 1
              )
       ORDER BY test_distance_levhenstein_exception_words DESC
"""
tb = s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = 'test_distance_levhenstein_exception_words', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
pd.concat([

pd.concat([
tb[['siret', 'unzip_insee', 'unzip_inpi']]
],keys=["Input"], axis = 1),
pd.concat([
tb[['test_distance_levhenstein_exception_words']]
],keys=["Output"], axis = 1)
], axis = 1
)
```

## test sur variables groupée

- 'test_index_id_duplicate', 'test_siege', 'test_siren_insee_siren_inpi', 'test_status_admin', 'test_pct_intersection'

1. Compter le nombre de lignes par test
2. Compter le nombre d'index par test
3. Créer un tableau avec une ligne par test


### 1. Compter le nombre de lignes par test

```python
groups = ['test_index_id_duplicate', 'test_siege',
    'test_siren_insee_siren_inpi', 'test_status_admin', 'test_pct_intersection']

top = """
WITH tb_test_date AS (
SELECT test_date as groups, COUNT(*) as cnt_test_date
FROM siretisation.ets_insee_inpi_regle 
GROUP BY test_date
  )
  SELECT groups,  cnt_test_date, 
  
"""
top_2 = ""

top_3 = " FROM tb_test_date "

middle_1 ="""
LEFT JOIN (
  
  SELECT {0}, COUNT(*) as cnt_{0}
FROM siretisation.ets_insee_inpi_regle 
GROUP BY {0}
  
  ) AS tb_{0}
  ON tb_test_date.groups = tb_{0}.{0}
"""
middle_2 = ""
for i, value in enumerate(groups):
    if i == len(groups) - 1:
        top_2 += ' cnt_{}'.format(value)
        middle_2 += middle_1.format(value)
   #    middle_2 += '{} )'.format(middle_1.format(value))
    else:
        middle_2 += middle_1.format(value)
        top_2 += ' cnt_{}, '.format(value)

query = top +top_2 +top_3 + middle_2
s3.run_query(
            query=query,
            database='siretisation',
            s3_output=s3_output,
  filename = 'count_ets_insee_inpicase_groups', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

### 3. Compter le nombre d'index par test

```python
groups = ['test_index_id_duplicate', 'test_siege',
    'test_siren_insee_siren_inpi', 'test_status_admin', 'test_pct_intersection']

top = """
WITH tb_test_date AS (
SELECT test_date as groups, COUNT(distinct(index_id)) as cnt_test_date
FROM siretisation.ets_insee_inpi_regle 
GROUP BY test_date
  )
  SELECT groups,  cnt_test_date, 
  
"""
top_2 = ""

top_3 = " FROM tb_test_date "

middle_1 ="""
LEFT JOIN (
  
  SELECT {0}, COUNT(distinct(index_id)) as cnt_{0}
FROM siretisation.ets_insee_inpi_regle 
GROUP BY {0}
  
  ) AS tb_{0}
  ON tb_test_date.groups = tb_{0}.{0}
"""
middle_2 = ""
for i, value in enumerate(groups):
    if i == len(groups) - 1:
        top_2 += ' cnt_{}'.format(value)
        middle_2 += middle_1.format(value)
   #    middle_2 += '{} )'.format(middle_1.format(value))
    else:
        middle_2 += middle_1.format(value)
        top_2 += ' cnt_{}, '.format(value)

query = top +top_2 +top_3 + middle_2
s3.run_query(
            query=query,
            database='siretisation',
            s3_output=s3_output,
  filename = 'count_distinct_ets_insee_inpicase_groups', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

### 4. Créer un tableau avec une ligne par test

```python
query = """
WITH tb_test AS (
SELECT 
index_id,
siren,
siret, 

-- test_date
datecreationetablissement, 
"date_début_activité",
test_date,

-- test_index_id_duplicate
count_inpi_index_id_siret,
test_index_id_duplicate,
  
-- test_siege
etablissementsiege, 
status_ets, 
test_siege,

-- test_siren_insee_siren_inpi
count_initial_insee,
count_inpi_siren_siret,
test_siren_insee_siren_inpi,

-- test_status_admin
etatadministratifetablissement, 
        status_admin,
test_status_admin,
  
-- test_pct_intersection,
pct_intersection,
index_id_max_intersection,
test_pct_intersection
FROM siretisation.ets_insee_inpi_regle 
)
 SELECT *
 -- test_date
       FROM (SELECT * 
             FROm tb_test
       WHERE test_date = 'TRUE'
       LIMIT 1
             )
       UNION (SELECT *
       FROM tb_test
       WHERE test_date = 'NULL'
              LIMIT 1
              )
       UNION (SELECT *
       FROM tb_test
       WHERE test_date = 'FALSE'
              LIMIT 1
              )
              
-- test_index_id_duplicate,
       UNION (SELECT *
       FROM tb_test
       WHERE test_index_id_duplicate = 'TRUE'
              LIMIT 1
              )
       UNION (SELECT *
       FROM tb_test
       WHERE test_index_id_duplicate = 'FALSE'
              LIMIT 1
              )

-- test_siege
       UNION (SELECT *
             FROm tb_test
       WHERE test_siege = 'TRUE'
       LIMIT 1
             )
       UNION (SELECT *
       FROM tb_test
       WHERE test_siege = 'NULL'
              LIMIT 1
              )
       UNION (SELECT *
       FROM tb_test
       WHERE test_siege = 'FALSE'
              LIMIT 1
              )

-- test_siren_insee_siren_inpi
       UNION (SELECT *
       FROM tb_test
       WHERE test_siren_insee_siren_inpi = 'TRUE'
              LIMIT 1
              )
       UNION (SELECT *
       FROM tb_test
       WHERE test_siren_insee_siren_inpi = 'FALSE'
              LIMIT 1
              )

-- test_status_admin
       UNION (SELECT *
             FROm tb_test
       WHERE test_status_admin = 'TRUE'
       LIMIT 1
             )
       UNION (SELECT *
       FROM tb_test
       WHERE test_status_admin = 'NULL'
              LIMIT 1
              )
       UNION (SELECT *
       FROM tb_test
       WHERE test_status_admin = 'FALSE'
              LIMIT 1
              )

-- test_pct_intersection
       UNION (SELECT *
       FROM tb_test
       WHERE test_pct_intersection = 'TRUE'
              LIMIT 1
              )
       UNION (SELECT *
       FROM tb_test
       WHERE test_pct_intersection = 'FALSE'
              LIMIT 1
              )
              ORDER BY test_date
"""

s3.run_query(
            query=query,
            database='siretisation',
            s3_output=s3_output,
  filename = 'exemple_other_test', ## Add filename to print dataframe
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
