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

# Calcul de la distance de Cosine et Levhenstein

# Objective(s)

*  Lors de cette US, nous allons créer 6 variables qui vont permettre a la réalisation des tests test_similarite_exception_words et test_distance_levhenstein_exception_words. Les six variables sont les suivantes:
  * unzip_inpi: Mot comparé coté inpi
  * unzip_insee: Mot comparé coté insee
  * max_cosine_distance: Score de similarité entre le mot compaté coté inpi et coté insee
  * levenshtein_distance: Nombre d'édition qu'il faut réaliser pour arriver à reproduire les deux mots
  * key_except_to_test: Champs clé-valeur pour toutes les possibiltés des mots qui ne sont pas en communs entre l'insee et l'inpi

# Metadata

* Epic: Epic 6
* US: US 5
* Date Begin: 9/29/2020
* Duration Task: 0
* Description: Creation des variables permettant de calculer la distance de levhenstein et le cosine
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
* Task tag: #computation,#athena
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
* ets_inpi_similarite_max_word2vec
* GitHub:
* https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/11_sumup_siretisation/06_calcul_cosine_levhenstein.md


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

Lors de cette US, nous allons créer 6 variables qui vont permettre a la réalisation des tests `test_similarite_exception_words` et `test_distance_levhenstein_exception_words`. Les six variables sont les suivantes:

- `unzip_inpi`: Mot comparé coté inpi
- `unzip_insee`: Mot comparé coté insee
- `max_cosine_distance`: Score de similarité entre le mot compaté coté inpi et coté insee
- `levenshtein_distance`: Nombre d'édition qu'il faut réaliser pour arriver à reproduire les deux mots
- `key_except_to_test`: Champs clé-valeur pour toutes les possibiltés des mots qui ne sont pas en communs entre l'insee et l'inpi
* Il faut penser a garder la variable `row_id` 
- La similarité doit etre calculée sur l'ensemble des éléments non communs, puis il faut récupérer la distance la plus élevée.

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
DROP TABLE ets_siretisation.ets_inpi_similarite_max_word2vec;
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
CREATE TABLE ets_siretisation.ets_inpi_similarite_max_word2vec
WITH (
  format='PARQUET'
) AS
WITH dataset AS (
  SELECT 
    ets_siretisation.ets_insee_inpi.row_id, 
    index_id, 
    status_cas, 
    inpi_except, 
    insee_except, 
    transform(
      sequence(
        1, 
        CARDINALITY(insee_except)
      ), 
      x -> insee_except
    ), 
    ZIP(
      inpi_except, 
      transform(
        sequence(
          1, 
          CARDINALITY(inpi_except)
        ), 
        x -> insee_except
      )
    ) as test 
  FROM 
    ets_siretisation.ets_insee_inpi  
    
  LEFT JOIN ets_siretisation.ets_insee_inpi_statut_cas 
  ON ets_siretisation.ets_insee_inpi.row_id = ets_siretisation.ets_insee_inpi_statut_cas.row_id
  where 
    (status_cas != 'CAS_2' AND CARDINALITY(inpi_except)  > 0 AND CARDINALITY(insee_except) > 0)
  )
 SELECT 
  * 
FROM 
  (
    WITH distance AS (
      SELECT 
        * 
      FROM 
        (
          WITH list_weights_insee_inpi AS (
            SELECT 
              row_id, 
              index_id, 
              status_cas, 
              inpi_except, 
              insee_except, 
              unzip_inpi, 
              unzip_insee, 
              list_weights_inpi, 
              list_weights_insee 
            FROM 
              (
                SELECT 
                  row_id, 
                  index_id, 
                  status_cas, 
                  inpi_except, 
                  insee_except, 
                  unzip.field0 as unzip_inpi, 
                  unzip.field1 as insee, 
                  test 
                FROM 
                  dataset CROSS 
                  JOIN UNNEST(test) AS new (unzip)
              ) CROSS 
              JOIN UNNEST(insee) as test (unzip_insee) 
              LEFT JOIN (
                SELECT 
                  words, 
                  list_weights as list_weights_inpi 
                FROM 
                  ets_siretisation.list_weight_mots_insee_inpi_word2vec 
              ) tb_weight_inpi ON unzip_inpi = tb_weight_inpi.words 
              LEFT JOIN (
                SELECT 
                  words, 
                  list_weights as list_weights_insee 
                FROM 
                  ets_siretisation.list_weight_mots_insee_inpi_word2vec 
              ) tb_weight_insee ON unzip_insee = tb_weight_insee.words 
          ) 
          SELECT 
            row_id, 
            index_id, 
            status_cas, 
            inpi_except, 
            insee_except, 
            unzip_inpi, 
            unzip_insee, 
            REDUCE(
              zip_with(
                list_weights_inpi, 
                list_weights_insee, 
                (x, y) -> x * y
              ), 
              CAST(
                ROW(0.0) AS ROW(sum DOUBLE)
              ), 
              (s, x) -> CAST(
                ROW(x + s.sum) AS ROW(sum DOUBLE)
              ), 
              s -> s.sum
            ) / (
              SQRT(
                REDUCE(
                  transform(
                    list_weights_inpi, 
                    (x) -> POW(x, 2)
                  ), 
                  CAST(
                    ROW(0.0) AS ROW(sum DOUBLE)
                  ), 
                  (s, x) -> CAST(
                    ROW(x + s.sum) AS ROW(sum DOUBLE)
                  ), 
                  s -> s.sum
                )
              ) * SQRT(
                REDUCE(
                  transform(
                    list_weights_insee, 
                    (x) -> POW(x, 2)
                  ), 
                  CAST(
                    ROW(0.0) AS ROW(sum DOUBLE)
                  ), 
                  (s, x) -> CAST(
                    ROW(x + s.sum) AS ROW(sum DOUBLE)
                  ), 
                  s -> s.sum
                )
              )
            ) AS cosine_distance 
          FROM 
            list_weights_insee_inpi
        )
    ) 
    SELECT 
      row_id, 
      dataset.index_id, 
      inpi_except, 
      insee_except, 
      unzip_inpi, 
      unzip_insee, 
      max_cosine_distance,
      -- CASE WHEN max_cosine_distance >= .6 THEN 'TRUE' ELSE 'FALSE' END AS test_distance_cosine,
      test as key_except_to_test,
      levenshtein_distance(unzip_inpi, unzip_insee) AS levenshtein_distance
      -- CASE WHEN levenshtein_distance(unzip_inpi, unzip_insee) <=1  THEN 'TRUE' ELSE 'FALSE' END AS test_distance_levhenstein
    
    FROM 
      dataset 
      LEFT JOIN (
        SELECT 
          distance.index_id, 
          unzip_inpi, 
          unzip_insee, 
          max_cosine_distance 
        FROM 
          distance 
          RIGHT JOIN (
            SELECT 
              index_id, 
              MAX(cosine_distance) as max_cosine_distance 
            FROM 
              distance 
            GROUP BY 
              index_id
          ) as tb_max_distance ON distance.index_id = tb_max_distance.index_id 
          AND distance.cosine_distance = tb_max_distance.max_cosine_distance
      ) as tb_max_distance_lookup ON dataset.index_id = tb_max_distance_lookup.index_id
  )
"""

s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = None, ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

# Pas a pas

Pour récupérer la similarité la plus élevée entre les mots qui ne sont pas communs entre l'adresse de l'INSEE et de l'INPI, il faut suivre plusieurs étapes. Les étapes sont les suivantes:




### 1. filtre et creation ensemble des similarités a calculer

- Filtrer les lignes qui ne correspondent pas au cas 2 et qui ont une cardinalité des mots `except` supérieure à 0. Effectivement, il n'est pas nécéssaire de calculer une similarité si l'une des listes, insee ou inpi, est vide.
- Création d'un champ clé valeur qui indique l'ensemble des similarités a calculer

On utilise les fonctions:

- `transform`
- `ZIP`
- `sequence`

La difficulté dans cette étape était de trouver un moyen de dupliquer la liste de l'INSEE pour chacune des clés de la liste de l'INPI. Le trick est d'utiliser `sequence` afin de répeter autant de fois la liste de l'INSEE qu'il y a de clé à l'INPI. Plus précisément, si la liste de l'INPI a deux valeurs, ie deux clés, et que la liste de l'INSEE a trois éléments. La taille de l'INSEE n'a pas d'impact, ce qui est important c'est de connaitre la taille de l'INPI. Dans notre exemple, le code va répéter la liste de l'INSEE 2 fois, car il y a deux clés à l'INPI.

Exemple concret:

- INPI -> [FRERES, AMADEO]
- INSEE -> [MARTYRS, RESISTANCE]
- Il faut comparer: 
    - FRERES -> [MARTYRS, RESISTANCE] 
    - AMADEO -> [MARTYRS, RESISTANCE]
- Clé valeur finale -> [
{field0=FRERES, field1=[MARTYRS, RESISTANCE]},
{field0=AMADEO, field1=[MARTYRS, RESISTANCE]}
]

```python
query = """
WITH dataset AS (
  SELECT 
    ets_siretisation.ets_insee_inpi.row_id, 
    index_id, 
    status_cas, 
    inpi_except, 
    insee_except, 
    transform(
      sequence(
        1, 
        CARDINALITY(insee_except)
      ), 
      x -> insee_except
    ), 
    ZIP(
      inpi_except, 
      transform(
        sequence(
          1, 
          CARDINALITY(inpi_except)
        ), 
        x -> insee_except
      )
    ) as test 
  FROM 
    ets_siretisation.ets_insee_inpi  
    
  LEFT JOIN ets_siretisation.ets_insee_inpi_statut_cas 
  ON ets_siretisation.ets_insee_inpi.row_id = ets_siretisation.ets_insee_inpi_statut_cas.row_id
  where 
    (status_cas != 'CAS_2' AND CARDINALITY(inpi_except)  > 0 AND CARDINALITY(insee_except) > 0)
  LIMIT 10
  )
  
 SELECT 
  * 
  FROM dataset

"""
s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = 'repeat', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

### 2. Produit cartesien possibilité et liste poids

Dans la seconde étape, nous allons "exploser" la clé-valeur afin de pouvoir attribuler la liste des poids aux mots de l'INPI et de l'INSEE.

Nous allons poursuivre le reste du pas à pas avec l'index `345408`, qui fait référence à l'exemple ci dessus.

L'explosion du champs `test` se fait avec la fonction `CROSS JOIN`. La variable `unzip_inpi` correspond aux clés du champs `test` alors que la variable `unzip_insee` correspond aux valeurs. Le `CROSS JOIN` implique 4 lignes au total. 

Nous avons donc deux colonnes avec les pairs de mots qu'il faut calculer la similarité, et deux colonnes avec les poids.

```python
query = """
WITH dataset AS (
  SELECT 
    ets_siretisation.ets_insee_inpi.row_id, 
    index_id, 
    status_cas, 
    inpi_except, 
    insee_except, 
    transform(
      sequence(
        1, 
        CARDINALITY(insee_except)
      ), 
      x -> insee_except
    ), 
    ZIP(
      inpi_except, 
      transform(
        sequence(
          1, 
          CARDINALITY(inpi_except)
        ), 
        x -> insee_except
      )
    ) as test 
  FROM 
    ets_siretisation.ets_insee_inpi  
    
  LEFT JOIN ets_siretisation.ets_insee_inpi_statut_cas 
  ON ets_siretisation.ets_insee_inpi.row_id = ets_siretisation.ets_insee_inpi_statut_cas.row_id
  where 
    (status_cas != 'CAS_2' AND CARDINALITY(inpi_except)  > 0 AND CARDINALITY(insee_except) > 0 AND index_id = 728021)
  
  )
SELECT 
              row_id, 
              index_id, 
              status_cas, 
              inpi_except, 
              insee_except, 
              unzip_inpi, 
              unzip_insee, 
              list_weights_inpi, 
              list_weights_insee 
            FROM 
              (
                SELECT 
                  row_id, 
                  index_id, 
                  status_cas, 
                  inpi_except, 
                  insee_except, 
                  unzip.field0 as unzip_inpi, 
                  unzip.field1 as insee, 
                  test 
                FROM 
                  dataset CROSS 
                  JOIN UNNEST(test) AS new (unzip)
              ) CROSS 
              JOIN UNNEST(insee) as test (unzip_insee) 
              LEFT JOIN (
                SELECT 
                  words, 
                  list_weights as list_weights_inpi 
                FROM 
                  ets_siretisation.list_weight_mots_insee_inpi_word2vec 
              ) tb_weight_inpi ON unzip_inpi = tb_weight_inpi.words 
              LEFT JOIN (
                SELECT 
                  words, 
                  list_weights as list_weights_insee 
                FROM 
                  ets_siretisation.list_weight_mots_insee_inpi_word2vec 
              ) tb_weight_insee ON unzip_insee = tb_weight_insee.words 
"""
s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = 'explosion', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

### 3. Calcul de la similarité

Le calcul de la similarité s'effectue avec la distance de cosine. Pour connaitre le pas a pas, veuillez vous référer au notebook [07_creation_table_poids_Word2Vec.md#test-acceptanceanalyse-du-modele](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/11_sumup_siretisation/05_creation_table_poids_Word2Vec.md) pour comprendre les étapes

```python
query =  """
WITH dataset AS (
  SELECT 
    ets_siretisation.ets_insee_inpi.row_id, 
    index_id, 
    status_cas, 
    inpi_except, 
    insee_except, 
    transform(
      sequence(
        1, 
        CARDINALITY(insee_except)
      ), 
      x -> insee_except
    ), 
    ZIP(
      inpi_except, 
      transform(
        sequence(
          1, 
          CARDINALITY(inpi_except)
        ), 
        x -> insee_except
      )
    ) as test 
  FROM 
    ets_siretisation.ets_insee_inpi  
    
  LEFT JOIN ets_siretisation.ets_insee_inpi_statut_cas 
  ON ets_siretisation.ets_insee_inpi.row_id = ets_siretisation.ets_insee_inpi_statut_cas.row_id
  where 
    (status_cas != 'CAS_2' AND CARDINALITY(inpi_except)  > 0 AND CARDINALITY(insee_except) > 0 AND index_id = 728021)
  )
  SELECT 
  * 
FROM 
  (
    WITH distance AS (
SELECT 
              row_id, 
              index_id, 
              status_cas, 
              inpi_except, 
              insee_except, 
              unzip_inpi, 
              unzip_insee, 
              list_weights_inpi, 
              list_weights_insee 
            FROM 
              (
                SELECT 
                  row_id, 
                  index_id, 
                  status_cas, 
                  inpi_except, 
                  insee_except, 
                  unzip.field0 as unzip_inpi, 
                  unzip.field1 as insee, 
                  test 
                FROM 
                  dataset CROSS 
                  JOIN UNNEST(test) AS new (unzip)
              ) CROSS 
              JOIN UNNEST(insee) as test (unzip_insee) 
              LEFT JOIN (
                SELECT 
                  words, 
                  list_weights as list_weights_inpi 
                FROM 
                  ets_siretisation.list_weight_mots_insee_inpi_word2vec 
              ) tb_weight_inpi ON unzip_inpi = tb_weight_inpi.words 
              LEFT JOIN (
                SELECT 
                  words, 
                  list_weights as list_weights_insee 
                FROM 
                  ets_siretisation.list_weight_mots_insee_inpi_word2vec 
              ) tb_weight_insee ON unzip_insee = tb_weight_insee.words 
      )
    SELECT row_id, 
            index_id, 
            status_cas, 
            inpi_except, 
            insee_except, 
            unzip_inpi, 
            unzip_insee, 
            REDUCE(
              zip_with(
                list_weights_inpi, 
                list_weights_insee, 
                (x, y) -> x * y
              ), 
              CAST(
                ROW(0.0) AS ROW(sum DOUBLE)
              ), 
              (s, x) -> CAST(
                ROW(x + s.sum) AS ROW(sum DOUBLE)
              ), 
              s -> s.sum
            ) / (
              SQRT(
                REDUCE(
                  transform(
                    list_weights_inpi, 
                    (x) -> POW(x, 2)
                  ), 
                  CAST(
                    ROW(0.0) AS ROW(sum DOUBLE)
                  ), 
                  (s, x) -> CAST(
                    ROW(x + s.sum) AS ROW(sum DOUBLE)
                  ), 
                  s -> s.sum
                )
              ) * SQRT(
                REDUCE(
                  transform(
                    list_weights_insee, 
                    (x) -> POW(x, 2)
                  ), 
                  CAST(
                    ROW(0.0) AS ROW(sum DOUBLE)
                  ), 
                  (s, x) -> CAST(
                    ROW(x + s.sum) AS ROW(sum DOUBLE)
                  ), 
                  s -> s.sum
                )
              )
            ) AS cosine_distance  
    FROM distance
    )
"""
s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = 'cosine', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        ).sort_values(by = 'cosine_distance', ascending = False)
```

### 4. Recupération de la similarité maximum par `row_id` et calcul Levensthein

La dernière étape consiste a récupérer la similarité maximum sur les doublons provenant du `row_id` de sorte à n'avoir qu'une ligne par pair 

```python
query = """
WITH dataset AS (
  SELECT 
    ets_siretisation.ets_insee_inpi.row_id, 
    index_id, 
    status_cas, 
    inpi_except, 
    insee_except, 
    transform(
      sequence(
        1, 
        CARDINALITY(insee_except)
      ), 
      x -> insee_except
    ), 
    ZIP(
      inpi_except, 
      transform(
        sequence(
          1, 
          CARDINALITY(inpi_except)
        ), 
        x -> insee_except
      )
    ) as test 
  FROM 
    ets_siretisation.ets_insee_inpi  
    
  LEFT JOIN ets_siretisation.ets_insee_inpi_statut_cas 
  ON ets_siretisation.ets_insee_inpi.row_id = ets_siretisation.ets_insee_inpi_statut_cas.row_id
  where 
    (status_cas != 'CAS_2' AND CARDINALITY(inpi_except)  > 0 AND CARDINALITY(insee_except) > 0 AND index_id = 728021)
  )
 SELECT 
  * 
FROM 
  (
    WITH distance AS (
      SELECT 
        * 
      FROM 
        (
          WITH list_weights_insee_inpi AS (
            SELECT 
              row_id, 
              index_id, 
              status_cas, 
              inpi_except, 
              insee_except, 
              unzip_inpi, 
              unzip_insee, 
              list_weights_inpi, 
              list_weights_insee 
            FROM 
              (
                SELECT 
                  row_id, 
                  index_id, 
                  status_cas, 
                  inpi_except, 
                  insee_except, 
                  unzip.field0 as unzip_inpi, 
                  unzip.field1 as insee, 
                  test 
                FROM 
                  dataset CROSS 
                  JOIN UNNEST(test) AS new (unzip)
              ) CROSS 
              JOIN UNNEST(insee) as test (unzip_insee) 
              LEFT JOIN (
                SELECT 
                  words, 
                  list_weights as list_weights_inpi 
                FROM 
                  ets_siretisation.list_weight_mots_insee_inpi_word2vec 
              ) tb_weight_inpi ON unzip_inpi = tb_weight_inpi.words 
              LEFT JOIN (
                SELECT 
                  words, 
                  list_weights as list_weights_insee 
                FROM 
                  ets_siretisation.list_weight_mots_insee_inpi_word2vec 
              ) tb_weight_insee ON unzip_insee = tb_weight_insee.words 
          ) 
          SELECT 
            row_id, 
            index_id, 
            status_cas, 
            inpi_except, 
            insee_except, 
            unzip_inpi, 
            unzip_insee, 
            REDUCE(
              zip_with(
                list_weights_inpi, 
                list_weights_insee, 
                (x, y) -> x * y
              ), 
              CAST(
                ROW(0.0) AS ROW(sum DOUBLE)
              ), 
              (s, x) -> CAST(
                ROW(x + s.sum) AS ROW(sum DOUBLE)
              ), 
              s -> s.sum
            ) / (
              SQRT(
                REDUCE(
                  transform(
                    list_weights_inpi, 
                    (x) -> POW(x, 2)
                  ), 
                  CAST(
                    ROW(0.0) AS ROW(sum DOUBLE)
                  ), 
                  (s, x) -> CAST(
                    ROW(x + s.sum) AS ROW(sum DOUBLE)
                  ), 
                  s -> s.sum
                )
              ) * SQRT(
                REDUCE(
                  transform(
                    list_weights_insee, 
                    (x) -> POW(x, 2)
                  ), 
                  CAST(
                    ROW(0.0) AS ROW(sum DOUBLE)
                  ), 
                  (s, x) -> CAST(
                    ROW(x + s.sum) AS ROW(sum DOUBLE)
                  ), 
                  s -> s.sum
                )
              )
            ) AS cosine_distance 
          FROM 
            list_weights_insee_inpi
        )
    ) 
    SELECT 
      row_id, 
      dataset.index_id, 
      inpi_except, 
      insee_except, 
      unzip_inpi, 
      unzip_insee, 
      max_cosine_distance,
      test as key_except_to_test,
      levenshtein_distance(unzip_inpi, unzip_insee) AS levenshtein_distance
    
    FROM 
      dataset 
      LEFT JOIN (
        SELECT 
          distance.index_id, 
          unzip_inpi, 
          unzip_insee, 
          max_cosine_distance 
        FROM 
          distance 
          RIGHT JOIN (
            SELECT 
              index_id, 
              MAX(cosine_distance) as max_cosine_distance 
            FROM 
              distance 
            GROUP BY 
              index_id
          ) as tb_max_distance ON distance.index_id = tb_max_distance.index_id 
          AND distance.cosine_distance = tb_max_distance.max_cosine_distance
      ) as tb_max_distance_lookup ON dataset.index_id = tb_max_distance_lookup.index_id
  )
"""
s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = 'max_cosine', ## Add filename to print dataframe
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
