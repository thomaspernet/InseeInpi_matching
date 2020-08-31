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
# Creation notebook presentation processus siretisation 

Copy paste from Coda to fill the information

## Objective(s)

- Créer un notebook qui récapitule le workflow de la siretisation via l’ensemble des notebooks créé dans les US

## Metadata 

* Metadata parameters are available here: 
* US Title: Creation notebook presentation processus siretisation
* Epic: Epic 5
* US: US 7
* Date Begin: 8/31/2020
* Duration Task: 0
* Status: Active
* Source URL:US 07 Preparation siretisation
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
  *  #sql-query,#regle-de-gestion,#presentation
* Toggl Tag
  * #documentation
  
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
        *  https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/03_ETS_add_variables.md 
    
## Destination Output/Delivery

* AWS
  1. Athena: 
      * Region: 
      * Database: 
      * Tables (Add name new table): 

## Things to know (Steps, Attention points or new flow of information)

### Sources of information  (meeting notes, Documentation, Query, URL)


1. Jupyter Notebook (Github Link)
  1. md : Notebooks_matching/Data_preprocessed/programme_matching/02_siretisation
<!-- #endregion -->

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

```python
s3_output = 'INPI/sql_output'
database = 'inpi'
```

# Creation tables

## Steps 


### Merger INPI - INSEE

Nous allons merger la table de l'INSEE avec la table de l'INPI. 

Le nombre de lignes et d'index est indiqué ci dessous


Nombre de lignes

```python
query =  """
SELECT COUNT(*) nb_lignes
FROM ets_insee_inpi
"""

s3.run_query(
        query=query,
        database='inpi',
        s3_output=s3_output, 
        filename = 'count_ets_inpi_insee'
    
    )
```

Nombre de index

```python
query =  """
SELECT COUNT(distinct(index_id)) as nb_index
FROM ets_insee_inpi
"""

s3.run_query(
        query=query,
        database='inpi',
        s3_output=s3_output, 
        filename = 'count_ets_inpi_insee'
    
    )
```

```python
query =  """
SELECT *
FROM ets_insee_inpi
limit 5
"""

s3.run_query(
        query=query,
        database='inpi',
        s3_output=s3_output, 
        filename = 'count_ets_inpi_insee'
    
    )
```

## Similarité entre deux adresses

Le rapprochement entre les deux tables, à savoir l’INSEE et l’INPI, va amener à la création de deux vecteurs d’adresse. Un vecteur avec des mots contenus spécifiquement à l’INSEE, et un second vecteur avec les mots de l’adresse de l’INPI. Notre objectif est de comparé ses deux vecteurs pour définir si ils sont identiques ou non. Nous avons distingué 7 cas de figures possibles entre les deux vecteurs (figure 1).

![](https://drive.google.com/uc?export=view&id=1Qj_HooHrhFYSuTsoqFbl4Vxy9tN3V5Bu)

### Creation table cas règle

Il faut créer les tests suivants:

- test_pct_intersection = ['TRUE', 'FALSE']:
    - Si le pourcentage intersection/union de la ligne est égal au maximum du pourcentage intersection / union par index alors TRUE
- status_cas = ['CAS_1','CAS_3','CAS_4', 'CAS_5','CAS_7', 'CAS_6']:
    - Informe du statut de l'adresse de la ligne 
- index_id_duplicate = ['TRUE', 'FALSE']:
    - informe si l'index a plusieurs siret possibles
- test_list_num_voie = ['TRUE', 'NULL', 'FALSE']:
    - informe si les numéros distincts de la voie sont identiques a l'INPI et a l'INSEE
- test_siege = ['TRUE','NULL','FALSE']:
    - informe si la ligne est un siege ou non
- test_enseigne =  ['TRUE','NULL', 'FALSE']:
    - informe si l'enseigne est identique
- test_siren_insee_siren_inpi = ['TRUE', 'FALSE']:
    - informe si le nombre de siret par siren est identique à l'insee et inpi
- test_distance_cosine = ['TRUE', 'FALSE', 'NULL']:
    - informe si la distance maximale (word2vec) entre les mots "exceptions" insee/inpi est supérieure à 0.6
- test_distance_levhenstein = ['TRUE', 'FALSE', 'NULL']:
    - informe si l'edit entre les deux mots les plus proches trouvée via la distance word2vec  est inférieure ou égale à 1
- test_date = ['TRUE','NULL','FALSE']:
    - informe si la date de création de l'établissement est égale à la date de début d'activité
- test_status_admin = ['TRUE', 'FALSE']:
    - informe si le status administratif est identique à l'INSEE et à l'INPI

```python
drop_table = False
if drop_table:
    output = s3.run_query(
        query="DROP TABLE `ets_inpi_insee_cases`;",
        database='inpi',
        s3_output=s3_output
    )
```

```python
create_table = """

CREATE TABLE inpi.ets_inpi_insee_cases WITH (format = 'PARQUET') AS 
WITH test_proba AS (
  SELECT 
    count_initial_insee, 
    index_id, 
    sequence_id, 
    siren, 
    siret, 
    Coalesce(
      try(
        date_parse(
          datecreationetablissement, '%Y-%m-%d'
        )
      ), 
      try(
        date_parse(
          datecreationetablissement, '%Y-%m-%d %hh:%mm:%ss.SSS'
        )
      ), 
      try(
        date_parse(
          datecreationetablissement, '%Y-%m-%d %hh:%mm:%ss'
        )
      ), 
      try(
        cast(
          datecreationetablissement as timestamp
        )
      )
    ) as datecreationetablissement, 
    Coalesce(
      try(
        date_parse(
          "date_début_activité", '%Y-%m-%d'
        )
      ), 
      try(
        date_parse(
          "date_début_activité", '%Y-%m-%d %hh:%mm:%ss.SSS'
        )
      ), 
      try(
        date_parse(
          "date_début_activité", '%Y-%m-%d %hh:%mm:%ss'
        )
      ), 
      try(
        cast(
          "date_début_activité" as timestamp
        )
      )
    ) as date_debut_activite, 
    etatadministratifetablissement, 
    status_admin, 
    etablissementsiege, 
    status_ets, 
    codecommuneetablissement, 
    code_commune, 
    codepostaletablissement, 
    code_postal_matching, 
    numerovoieetablissement, 
    numero_voie_matching, 
    typevoieetablissement, 
    type_voie_matching, 
    adresse_distance_inpi, 
    adresse_distance_insee, 
    list_numero_voie_matching_inpi, 
    list_numero_voie_matching_insee, 
    array_distinct(
      split(adresse_distance_inpi, ' ')
    ) as list_inpi, 
    cardinality(
      array_distinct(
        split(adresse_distance_inpi, ' ')
      )
    ) as lenght_list_inpi, 
    array_distinct(
      split(adresse_distance_insee, ' ')
    ) as list_insee, 
    cardinality(
      array_distinct(
        split(adresse_distance_insee, ' ')
      )
    ) as lenght_list_insee, 
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
    ) as union_numero_voie, 
    REGEXP_REPLACE(
      NORMALIZE(enseigne, NFD), 
      '\pM', 
      ''
    ) AS enseigne, 
    enseigne1etablissement, 
    enseigne2etablissement, 
    enseigne3etablissement, 
    array_remove(
      array_distinct(
        SPLIT(
          concat(
            enseigne1etablissement, ',', enseigne2etablissement, 
            ',', enseigne3etablissement
          ), 
          ','
        )
      ), 
      ''
    ) as test, 
    contains(
      array_remove(
        array_distinct(
          SPLIT(
            concat(
              enseigne1etablissement, ',', enseigne2etablissement, 
              ',', enseigne3etablissement
            ), 
            ','
          )
        ), 
        ''
      ), 
      REGEXP_REPLACE(
        NORMALIZE(enseigne, NFD), 
        '\pM', 
        ''
      )
    ) AS temp_test_enseigne 
  FROM 
    "inpi"."ets_insee_inpi" -- limit 10
    ) 
SELECT 
  * 
FROM 
  (
    WITH test_rules AS (
      SELECT 
        ROW_NUMBER() OVER () AS row_id, 
        count_initial_insee, 
        index_id, 
        sequence_id, 
        siren, 
        siret, 
        CASE WHEN cardinality(list_numero_voie_matching_inpi) = 0 THEN NULL ELSE list_numero_voie_matching_inpi END as list_numero_voie_matching_inpi, 
        CASE WHEN cardinality(
          list_numero_voie_matching_insee
        ) = 0 THEN NULL ELSE list_numero_voie_matching_insee END as list_numero_voie_matching_insee, 
        intersection_numero_voie, 
        union_numero_voie, 
        CASE WHEN intersection_numero_voie = union_numero_voie 
        AND (
          intersection_numero_voie IS NOT NULL 
          OR union_numero_voie IS NOT NULL
        ) THEN 'TRUE' WHEN (
          intersection_numero_voie IS NULL 
          OR union_numero_voie IS NULL
        ) THEN 'NULL' ELSE 'FALSE' END AS test_list_num_voie, 
        datecreationetablissement, 
        date_debut_activite, 
        CASE WHEN datecreationetablissement = date_debut_activite THEN 'TRUE' WHEN datecreationetablissement IS NULL 
        OR date_debut_activite IS NULL THEN 'NULL' --WHEN datecreationetablissement = '' 
        ELSE 'FALSE' END AS test_date, 
        etatadministratifetablissement, 
        status_admin, 
        CASE WHEN etatadministratifetablissement = status_admin THEN 'TRUE' WHEN etatadministratifetablissement IS NULL 
        OR status_admin IS NULL THEN 'NULL' WHEN etatadministratifetablissement = '' 
        OR status_admin = '' THEN 'NULL' ELSE 'FALSE' END AS test_status_admin, 
        etablissementsiege, 
        status_ets, 
        CASE WHEN etablissementsiege = status_ets THEN 'TRUE' WHEN etablissementsiege IS NULL 
        OR status_ets IS NULL THEN 'NULL' WHEN etablissementsiege = '' 
        OR status_ets = '' THEN 'NULL' ELSE 'FALSE' END AS test_siege, 
        codecommuneetablissement, 
        code_commune, 
        CASE WHEN codecommuneetablissement = code_commune THEN 'TRUE' WHEN codecommuneetablissement IS NULL 
        OR code_commune IS NULL THEN 'NULL' WHEN codecommuneetablissement = '' 
        OR code_commune = '' THEN 'NULL' ELSE 'FALSE' END AS test_code_commune, 
        codepostaletablissement, 
        code_postal_matching, 
        numerovoieetablissement, 
        numero_voie_matching, 
        CASE WHEN numerovoieetablissement = numero_voie_matching THEN 'TRUE' WHEN numerovoieetablissement IS NULL 
        OR numero_voie_matching IS NULL THEN 'NULL' WHEN numerovoieetablissement = '' 
        OR numero_voie_matching = '' THEN 'NULL' ELSE 'FALSE' END AS test_numero_voie, 
        typevoieetablissement, 
        type_voie_matching, 
        CASE WHEN typevoieetablissement = type_voie_matching THEN 'TRUE' WHEN typevoieetablissement IS NULL 
        OR type_voie_matching IS NULL THEN 'NULL' WHEN typevoieetablissement = '' 
        OR type_voie_matching = '' THEN 'NULL' ELSE 'FALSE' END AS test_type_voie, 
        CASE WHEN cardinality(list_inpi) = 0 THEN NULL ELSE list_inpi END as list_inpi, 
        lenght_list_inpi, 
        CASE WHEN cardinality(list_insee) = 0 THEN NULL ELSE list_insee END as list_insee, 
        lenght_list_insee, 
        CASE WHEN cardinality(inpi_except) = 0 THEN NULL ELSE inpi_except END as inpi_except, 
        CASE WHEN cardinality(insee_except) = 0 THEN NULL ELSE insee_except END as insee_except, 
        intersection, 
        union_, 
        intersection / union_ as pct_intersection, 
        cardinality(inpi_except) AS len_inpi_except, 
        cardinality(insee_except) AS len_insee_except, 
        CASE WHEN intersection = union_ THEN 'CAS_1' WHEN intersection = 0 THEN 'CAS_2' WHEN lenght_list_inpi = intersection 
        AND intersection != union_ THEN 'CAS_3' WHEN lenght_list_insee = intersection 
        AND intersection != union_ THEN 'CAS_4' WHEN cardinality(insee_except) = cardinality(inpi_except) 
        AND intersection != 0 
        AND cardinality(insee_except) > 0 THEN 'CAS_5' WHEN cardinality(insee_except) > cardinality(inpi_except) 
        AND intersection != 0 
        AND cardinality(insee_except) > 0 
        AND cardinality(inpi_except) > 0 THEN 'CAS_6' WHEN cardinality(insee_except) < cardinality(inpi_except) 
        AND intersection != 0 
        AND cardinality(insee_except) > 0 
        AND cardinality(inpi_except) > 0 THEN 'CAS_7' ELSE 'CAS_NO_ADRESSE' END AS status_cas, 
        enseigne, 
        enseigne1etablissement, 
        enseigne2etablissement, 
        enseigne3etablissement, 
        CASE WHEN cardinality(test) = 0 THEN 'NULL' WHEN enseigne = '' THEN 'NULL' WHEN temp_test_enseigne = TRUE THEN 'TRUE' ELSE 'FALSE' END AS test_enseigne 
      FROM 
        test_proba
      
    ) 
    
    SELECT *
    FROM (
      WITH test AS(
        SELECT *,
        CASE WHEN status_cas = 'CAS_1' OR
        status_cas = 'CAS_3' OR 
        status_cas = 'CAS_4' THEN 'TRUE' ELSE 'FALSE' END AS test_adresse_cas_1_3_4 
        FROM test_rules
        WHERE test_list_num_voie != 'FALSE' and status_cas != 'CAS_2'
        )
    
    SELECT 
      --rank,
      row_id, 
      test.index_id, 
      test.sequence_id, 
      test.siren, 
      test.siret,
      
      count_initial_insee, 
      count_inpi_siren_siret, 
      count_inpi_siren_sequence, 
      count_inpi_sequence_siret, 
      count_inpi_sequence_stat_cas_siret,
      count_inpi_index_id_siret,
      count_inpi_index_id_stat_cas_siret,
      count_inpi_index_id_stat_cas,
      CASE WHEN count_inpi_index_id_siret > 1 THEN 'TRUE' ELSE 'FALSE' END AS index_id_duplicate,
      CASE WHEN count_inpi_sequence_siret = 1 THEN 'TRUE' ELSE 'FALSE' END AS test_sequence_siret,
      CASE WHEN count_inpi_index_id_stat_cas_siret = 1 THEN 'TRUE' ELSE 'FALSE' END AS test_index_siret,
      CASE WHEN count_initial_insee = count_inpi_siren_siret THEN 'TRUE' ELSE 'FALSE' END AS test_siren_insee_siren_inpi, 
    
      CASE WHEN count_inpi_sequence_siret = count_inpi_sequence_stat_cas_siret THEN 'TRUE' ELSE 'FALSE' END AS test_sequence_siret_many_cas,
    
      list_numero_voie_matching_inpi, 
      list_numero_voie_matching_insee, 
      intersection_numero_voie, 
      union_numero_voie, 
      test.test_list_num_voie, 
      datecreationetablissement, 
      date_debut_activite, 
      test_date, 
      etatadministratifetablissement, 
      status_admin, 
      test_status_admin, 
      etablissementsiege, 
      status_ets, 
      test.test_siege, 
      codecommuneetablissement, 
      code_commune, 
      test_code_commune, 
      codepostaletablissement, 
      code_postal_matching, 
      numerovoieetablissement, 
      numero_voie_matching, 
      test_numero_voie, 
      typevoieetablissement, 
      type_voie_matching, 
      test_type_voie, 
      list_inpi, 
      lenght_list_inpi, 
      list_insee, 
      lenght_list_insee, 
      inpi_except, 
      insee_except, 
      intersection, 
      union_, 
      pct_intersection, 
      index_id_max_intersection,
      CASE WHEN pct_intersection = index_id_max_intersection THEN 'TRUE' ELSE 'FALSE' END AS test_pct_intersection,
      len_inpi_except, 
      len_insee_except, 
      test.status_cas, 
      test_adresse_cas_1_3_4,
      index_id_dup_has_cas_1_3_4,
      CASE
      WHEN test_adresse_cas_1_3_4 = 'TRUE' AND index_id_dup_has_cas_1_3_4 = 'TRUE' AND count_inpi_index_id_siret > 1 THEN 'TO_KEEP' 
      WHEN test_adresse_cas_1_3_4 = 'FALSE' AND index_id_dup_has_cas_1_3_4 = 'TRUE' AND count_inpi_index_id_siret > 1 THEN 'TO_REMOVE'
      WHEN count_inpi_index_id_siret = 1 THEN 'NULL'
      ELSE 'TO_FIND' END AS test_duplicates_is_in_cas_1_3_4,
      enseigne, 
      enseigne1etablissement, 
      enseigne2etablissement, 
      enseigne3etablissement, 
      test.test_enseigne 
    FROM 
      test 
      LEFT JOIN (
        SELECT 
          siren, 
          COUNT(
            DISTINCT(siret)
          ) AS count_inpi_siren_siret 
        FROM 
          test 
        GROUP BY 
          siren
      ) AS count_rows_sequence ON test.siren = count_rows_sequence.siren 
      LEFT JOIN (
        SELECT 
          siren, 
          COUNT(
            DISTINCT(sequence_id)
          ) AS count_inpi_siren_sequence 
        FROM 
          test 
        GROUP BY 
          siren
      ) AS count_rows_siren_sequence ON test.siren = count_rows_siren_sequence.siren 
      LEFT JOIN (
        SELECT 
          sequence_id, 
          COUNT(
            DISTINCT(siret)
          ) AS count_inpi_sequence_siret 
        FROM 
          test 
        GROUP BY 
          sequence_id
      ) AS count_rows_siret ON test.sequence_id = count_rows_siret.sequence_id
    -- 
    LEFT JOIN (
        SELECT 
          sequence_id, 
          status_cas,
          COUNT(
            DISTINCT(siret)
          ) AS count_inpi_sequence_stat_cas_siret 
        FROM 
          test 
        GROUP BY 
          sequence_id,
      status_cas
      ) AS count_rows_status_cas_siret ON test.sequence_id = count_rows_status_cas_siret.sequence_id AND
    test.status_cas = count_rows_status_cas_siret.status_cas
    -- duplicate index
    LEFT JOIN (
        SELECT 
          index_id, 
          COUNT(
            DISTINCT(siret)
          ) AS count_inpi_index_id_siret 
        FROM 
          test 
        GROUP BY 
          index_id
      ) AS count_rows_index_id_siret ON test.index_id = count_rows_index_id_siret.index_id
    -- duplicate index cas
    LEFT JOIN (
        SELECT 
          index_id, 
          status_cas,
          COUNT(
            DISTINCT(siret)
          ) AS count_inpi_index_id_stat_cas_siret 
        FROM 
          test_rules 
        GROUP BY 
          index_id,
          status_cas
      ) AS count_rows_index_status_cas_siret ON test.index_id = count_rows_index_status_cas_siret.index_id AND
    test.status_cas = count_rows_index_status_cas_siret.status_cas
    -- nb de cas par index
    LEFT JOIN (
        SELECT 
          index_id, 
          COUNT(
            DISTINCT(status_cas)
          ) AS count_inpi_index_id_stat_cas
        FROM 
           test 
        GROUP BY 
          index_id
      ) AS count_rows_index_status_cas ON test.index_id = count_rows_index_status_cas.index_id
   LEFT JOIN (
     SELECT 
     index_id,
     MAX(test_adresse_cas_1_3_4) AS index_id_dup_has_cas_1_3_4
     FROM test
     GROUP BY index_id
     ) AS  is_index_id_dup_has_cas_1_3_4 ON test.index_id = is_index_id_dup_has_cas_1_3_4.index_id
   
   
  
    LEFT JOIN (
     SELECT 
     index_id,
     MAX(pct_intersection) AS index_id_max_intersection
     FROM test
     GROUP BY index_id
     ) AS  is_index_id_index_id_max_intersection ON test.index_id = is_index_id_index_id_max_intersection.index_id
   
  )
 )
"""
output = s3.run_query(
        query=create_table,
        database='inpi',
        s3_output='INPI/sql_output'
    )
output
```

```python
query = """
SELECT *
FROM ets_inpi_insee_cases
LIMIT 10
"""
s3.run_query(
        query=query,
        database='inpi',
        s3_output='INPI/sql_output',
    filename  = 'ex_ets_inpi_insee_cases'
    )
```

### Creation table Tests index a dedoublonné

Création d'une table avec l'ensemble des tests possibles. Chacune des lignes vient par ordre croissant, c'est a dire que la ligne 1 est préférée à la ligne 2

```python
query = """
DROP TABLE `regles_tests`;
"""
s3.run_query(
        query=query,
        database='inpi',
        s3_output='INPI/sql_output'
    )
```

```python
test_pct_intersection = ['TRUE', 'FALSE']
status_cas = ['CAS_1','CAS_3','CAS_4', 'CAS_5','CAS_7', 'CAS_6']
index_id_duplicate = ['TRUE', 'FALSE']
test_list_num_voie = ['TRUE', 'NULL', 'FALSE']
test_siege = ['TRUE','NULL','FALSE']
test_enseigne =  ['TRUE','NULL', 'FALSE']
test_siren_insee_siren_inpi = ['TRUE', 'FALSE']
test_distance_cosine = ['TRUE', 'FALSE', 'NULL']
test_distance_levhenstein = ['TRUE', 'FALSE', 'NULL']
test_date = ['TRUE','NULL','FALSE']
test_status_admin = ['TRUE', 'FALSE']

index = pd.MultiIndex.from_product([
    test_pct_intersection,
    status_cas,
    index_id_duplicate,
    test_list_num_voie,
    test_siren_insee_siren_inpi,
    test_siege,
    test_enseigne,
    test_distance_cosine,
    test_distance_levhenstein,
    test_date,
    test_status_admin
],
                                   names = [
                                       'test_pct_intersection',
                                       "status_cas",
                                            'index_id_duplicate',
                                            "test_list_num_voie",
                                            "test_siren_insee_siren_inpi",
                                           'test_siege', 
                                           'test_enseigne',
                                           'test_distance_cosine',
                                           'test_distance_levhenstein',
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
df_.tail()
```

Au total, il y a 69984 règles possibles

```python
df_.shape
```

```python
df_.to_csv('Regle_tests.csv', index = False)
s3.upload_file(file_to_upload = 'Regle_tests.csv',
            destination_in_s3 = 'TEMP_ANALYSE_SIRETISATION/REGLES_TESTS')

create_table = """
CREATE EXTERNAL TABLE IF NOT EXISTS inpi.REGLES_TESTS (
`test_pct_intersection`                     string,
`status_cas`                     string,
`index_id_duplicate`                     string,
`test_list_num_voie`                     string,
`test_siren_insee_siren_inpi`                     string,
`test_siege`                     string,
`test_enseigne`                     string,
`test_distance_cosine`                     string,
`test_distance_levhenstein`                     string,
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
        database='inpi',
        s3_output='INPI/sql_output'
    )
```

### Creation test Distance 

Nous souhaitons connaite la distance maximum entre les mots "exceptions" INSEE et INPI. On ne prend que les cas 5/6/7. voici un exemple

```python
query = """
SELECT inpi_except, insee_except
FROM ets_inpi_insee_cases
WHERE status_cas = 'CAS_6'
LIMIT 10
"""
s3.run_query(
        query=query,
        database='inpi',
        s3_output='INPI/sql_output',
    filename  = 'ex_ets_inpi_insee_cases'
    )
```

Pour cela, nous avons entrainé un modèle via l'algorithme de Word2Vec entre l'ensemble des mots de chacune des lignes INPI/INSEE. On a gardé que les mots avec une récurence d'au moins 5, et nous avons calculé 100 poids par mot.

```python
query = """
SELECT *
FROM machine_learning.list_mots_insee_inpi
LIMIT 10
"""
s3.run_query(
        query=query,
        database='inpi',
        s3_output='INPI/sql_output',
    filename  = 'ex_ets_inpi_insee_cases'
    )
```

Un [rapport](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/07_analytics_ETS/Reports/03_POC_word2Vec_weights_computation.html) a été crée pour montrer les relations possibles.

Les poids ont été mis dans une table, appelée `list_mots_insee_inpi_word2vec_weights`

```python
query = """
SELECT *
FROM machine_learning.list_mots_insee_inpi_word2vec_weights
LIMIT 5
"""
s3.run_query(
        query=query,
        database='inpi',
        s3_output='INPI/sql_output',
    filename  = 'ex_ets_inpi_insee_cases'
    )
```

NOus avons ensuite matché la liste avec la table `ets_inpi_insee_cases`, calculé la distance entre les deux mots exceptions, exempel "APPARTMENT" et "APP". Comme plusieurs possibilitées par lignes, nous avons récupéré la valeur la plus élevée par index. 

```python
query = """
DROP TABLE `inpi.ets_inpi_distance_max_word2vec`;
"""
s3.run_query(
        query=query,
        database='inpi',
        s3_output='INPI/sql_output'
    )
```

```python
query = """
CREATE TABLE inpi.ets_inpi_distance_max_word2vec
WITH (
  format='PARQUET'
) AS
WITH dataset AS (
  SELECT 
    row_id, 
    index_id, 
    status_cas, 
    inpi_except, 
    insee_except, 
    pct_intersection, 
    len_inpi_except, 
    len_insee_except, 
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
    inpi.ets_inpi_insee_cases 
  where 
    (
      status_cas = 'CAS_5' 
      OR status_cas = 'CAS_6' 
      OR status_cas = 'CAS_7'
    ) 
  -- AND index_id = 8759351
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
              len_inpi_except, 
              len_insee_except, 
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
                  len_inpi_except, 
                  len_insee_except, 
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
                  machine_learning.list_mots_insee_inpi_word2vec_weights
              ) tb_weight_inpi ON unzip_inpi = tb_weight_inpi.words 
              LEFT JOIN (
                SELECT 
                  words, 
                  list_weights as list_weights_insee 
                FROM 
                  machine_learning.list_mots_insee_inpi_word2vec_weights
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
            len_inpi_except, 
            len_insee_except, 
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
      CASE WHEN max_cosine_distance >= .6 THEN 'TRUE' ELSE 'FALSE' END AS test_distance_cosine,
      test as key_except_to_test,
      levenshtein_distance(unzip_inpi, unzip_insee) AS levenshtein_distance,
      CASE WHEN levenshtein_distance(unzip_inpi, unzip_insee) <=1  THEN 'TRUE' ELSE 'FALSE' END AS test_distance_levhenstein
    
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
            database='inpi',
            s3_output=s3_output,
  filename = None, ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

```python
query = """
SELECT * 
FROM ets_inpi_distance_max_word2vec  
LIMIT 10
"""
s3.run_query(
        query=query,
        database='inpi',
        s3_output='INPI/sql_output',
    filename  = 'ex_ets_inpi_insee_cases'
    )
```

### Create table ajout distance

Ensuite, nous avons mergé la table contenant les distances avec la table initiale `ets_inpi_insee_cases` et bien sur matché le rank des règles avec la table `regles_tests`. 


```python
query = """
DROP TABLE `inpi.ets_inpi_insee_cases_distance`;
"""
s3.run_query(
        query=query,
        database='inpi',
        s3_output='INPI/sql_output'
    )
```

```python
query = """
CREATE TABLE inpi.ets_inpi_insee_cases_distance
WITH (
  format='PARQUET'
) AS
WITH tb_distance AS (
SELECT 
  ets_inpi_insee_cases.row_id, 
  ets_inpi_insee_cases.index_id, 
  sequence_id, 
  siren, 
  siret,
  list_inpi, 
  lenght_list_inpi, 
  list_insee, 
  lenght_list_insee, 
  ets_inpi_insee_cases.inpi_except, 
  ets_inpi_insee_cases.insee_except, 
  intersection, 
  union_, 
  pct_intersection,
  index_id_max_intersection,
  test_pct_intersection,
  len_inpi_except, 
  len_insee_except, 
  status_cas,
  unzip_inpi,
  unzip_insee,
  max_cosine_distance,
  CASE WHEN test_distance_cosine IS NULL THEN 'NULL' ELSE test_distance_cosine END AS test_distance_cosine,
  -- test_distance_costine,
  levenshtein_distance,
  CASE WHEN test_distance_levhenstein IS NULL THEN 'NULL' ELSE test_distance_levhenstein END AS test_distance_levhenstein,
  -- test_levhenstein, 
  count_initial_insee, 
  count_inpi_siren_siret, 
  count_inpi_siren_sequence, 
  count_inpi_sequence_siret, 
  count_inpi_sequence_stat_cas_siret, 
  count_inpi_index_id_siret, 
  count_inpi_index_id_stat_cas_siret, 
  count_inpi_index_id_stat_cas, 
  index_id_duplicate, 
  test_sequence_siret, 
  test_index_siret, 
  test_siren_insee_siren_inpi, 
  test_sequence_siret_many_cas, 
  list_numero_voie_matching_inpi, 
  list_numero_voie_matching_insee, 
  intersection_numero_voie, 
  union_numero_voie, 
  test_list_num_voie, 
  datecreationetablissement, 
  date_debut_activite, 
  test_date, 
  etatadministratifetablissement, 
  status_admin, 
  test_status_admin, 
  etablissementsiege, 
  status_ets, 
  test_siege, 
  codecommuneetablissement, 
  code_commune, 
  test_code_commune, 
  codepostaletablissement, 
  code_postal_matching, 
  numerovoieetablissement, 
  numero_voie_matching, 
  test_numero_voie, 
  typevoieetablissement, 
  type_voie_matching, 
  test_type_voie, 
  test_adresse_cas_1_3_4, 
  index_id_dup_has_cas_1_3_4, 
  test_duplicates_is_in_cas_1_3_4, 
  enseigne, 
  enseigne1etablissement, 
  enseigne2etablissement, 
  enseigne3etablissement, 
  test_enseigne,
  key_except_to_test
FROM 
  ets_inpi_insee_cases
LEFT JOIN
ets_inpi_distance_max_word2vec 
ON ets_inpi_insee_cases.row_id = ets_inpi_distance_max_word2vec.row_id
)
SELECT 
  rank, 
  row_id, 
  index_id, 
  sequence_id, 
  siren, 
  siret,
  list_inpi, 
  lenght_list_inpi, 
  list_insee, 
  lenght_list_insee, 
  inpi_except, 
  insee_except, 
  intersection, 
  union_, 
  pct_intersection,
  index_id_max_intersection,
  tb_distance.test_pct_intersection,
  len_inpi_except, 
  len_insee_except, 
  tb_distance.status_cas,
  unzip_inpi,
  unzip_insee,
  max_cosine_distance,
  tb_distance.test_distance_cosine,
  -- test_distance_costine,
  levenshtein_distance,
  tb_distance.test_distance_levhenstein,
  -- test_levhenstein, 
  count_initial_insee, 
  count_inpi_siren_siret, 
  count_inpi_siren_sequence, 
  count_inpi_sequence_siret, 
  count_inpi_sequence_stat_cas_siret, 
  count_inpi_index_id_siret, 
  count_inpi_index_id_stat_cas_siret, 
  count_inpi_index_id_stat_cas, 
  tb_distance.index_id_duplicate, 
  test_sequence_siret, 
  test_index_siret, 
  tb_distance.test_siren_insee_siren_inpi, 
  test_sequence_siret_many_cas, 
  list_numero_voie_matching_inpi, 
  list_numero_voie_matching_insee, 
  intersection_numero_voie, 
  union_numero_voie, 
  tb_distance.test_list_num_voie, 
  datecreationetablissement, 
  date_debut_activite, 
  tb_distance.test_date, 
  etatadministratifetablissement, 
  status_admin, 
  tb_distance.test_status_admin, 
  etablissementsiege, 
  status_ets, 
  tb_distance.test_siege, 
  codecommuneetablissement, 
  code_commune, 
  test_code_commune, 
  codepostaletablissement, 
  code_postal_matching, 
  numerovoieetablissement, 
  numero_voie_matching, 
  test_numero_voie, 
  typevoieetablissement, 
  type_voie_matching, 
  test_type_voie, 
  test_adresse_cas_1_3_4, 
  index_id_dup_has_cas_1_3_4, 
  test_duplicates_is_in_cas_1_3_4, 
  enseigne, 
  enseigne1etablissement, 
  enseigne2etablissement, 
  enseigne3etablissement, 
  tb_distance.test_enseigne,
  key_except_to_test
FROM tb_distance
LEFT JOIN regles_tests 

  ON  tb_distance.test_pct_intersection = regles_tests.test_pct_intersection

  AND  tb_distance.status_cas = regles_tests.status_cas 
  
  AND tb_distance.index_id_duplicate = regles_tests.index_id_duplicate 
  
  AND tb_distance.test_list_num_voie = regles_tests.test_list_num_voie 
  AND tb_distance.test_siren_insee_siren_inpi = regles_tests.test_siren_insee_siren_inpi
  AND tb_distance.test_siege = regles_tests.test_siege 
  AND tb_distance.test_enseigne = regles_tests.test_enseigne
  
  AND tb_distance.test_distance_cosine = regles_tests.test_distance_cosine 
  AND tb_distance.test_distance_levhenstein = regles_tests.test_distance_levhenstein
  
  AND tb_distance.test_date = regles_tests.test_date 
  AND tb_distance.test_status_admin = regles_tests.test_status_admin
"""
s3.run_query(
            query=query,
            database='inpi',
            s3_output=s3_output,
  filename = None, ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

```python
query = """
SELECT * 
FROM ets_inpi_insee_cases_distance  
LIMIT 10
"""
s3.run_query(
        query=query,
        database='inpi',
        s3_output='INPI/sql_output',
    filename  = 'ex_ets_inpi_insee_cases'
    )
```

```python
query = """
SELECT * 
FROM ets_inpi_insee_cases_distance 
WHERE index_id_duplicate = 'FALSE' AND status_cas = 'CAS_6'
ORDER BY index_id
LIMIT 10
"""
s3.run_query(
        query=query,
        database='inpi',
        s3_output='INPI/sql_output',
    filename  = 'ex_ets_inpi_insee_cases'
    )
```

### Creation table finale

- Afin de séparer les doublons, il suffit de récupérer le rank minimum par index. Celui ci va nous donner le meilleur des probables.
- Il est bien sur possible d’avoir encore des doublons, dans ces cas la, il faut aller plus loin dans la rédaction des tests
L’objectif, ici, est de récupérer le rank minimum de la table ets_inpi_insee_cases puis de faire une analyse brève des index récupérés.

```python
query = """
DROP TABLE `inpi.ets_inpi_insee_cases_rank`;
"""
s3.run_query(
        query=query,
        database='inpi',
        s3_output='INPI/sql_output'
    )
```

```python
query = """
CREATE TABLE inpi.ets_inpi_insee_cases_rank
WITH (
  format='PARQUET'
) AS
WITH tb_min_rank AS (
SELECT 
min_rank,
  row_id, 
  ets_inpi_insee_cases_distance.index_id, 
  sequence_id, 
  siren, 
  siret,
  list_inpi, 
  lenght_list_inpi, 
  list_insee, 
  lenght_list_insee, 
  inpi_except, 
  insee_except, 
  intersection, 
  union_, 
  pct_intersection, 
  index_id_max_intersection,
  test_pct_intersection,
  len_inpi_except, 
  len_insee_except, 
  status_cas,
  unzip_inpi,
  unzip_insee,
  max_cosine_distance,
  test_distance_cosine,
  levenshtein_distance,
  test_distance_levhenstein, 
  count_initial_insee, 
  count_inpi_siren_siret, 
  count_inpi_siren_sequence, 
  count_inpi_sequence_siret, 
  count_inpi_sequence_stat_cas_siret, 
  count_inpi_index_id_siret, 
  count_inpi_index_id_stat_cas_siret, 
  count_inpi_index_id_stat_cas, 
  index_id_duplicate, 
  test_sequence_siret, 
  test_index_siret, 
  test_siren_insee_siren_inpi, 
  test_sequence_siret_many_cas, 
  list_numero_voie_matching_inpi, 
  list_numero_voie_matching_insee, 
  intersection_numero_voie, 
  union_numero_voie, 
  test_list_num_voie, 
  datecreationetablissement, 
  date_debut_activite, 
  test_date, 
  etatadministratifetablissement, 
  status_admin, 
  test_status_admin, 
  etablissementsiege, 
  status_ets, 
  test_siege, 
  codecommuneetablissement, 
  code_commune, 
  test_code_commune, 
  codepostaletablissement, 
  code_postal_matching, 
  numerovoieetablissement, 
  numero_voie_matching, 
  test_numero_voie, 
  typevoieetablissement, 
  type_voie_matching, 
  test_type_voie, 
  test_adresse_cas_1_3_4, 
  index_id_dup_has_cas_1_3_4, 
  test_duplicates_is_in_cas_1_3_4, 
  enseigne, 
  enseigne1etablissement, 
  enseigne2etablissement, 
  enseigne3etablissement, 
  test_enseigne,
  key_except_to_test
FROM ets_inpi_insee_cases_distance 
INNER JOIN (
  SELECT index_id, MIN(rank) AS min_rank
FROM ets_inpi_insee_cases_distance
GROUP BY index_id
  ) as tb_min_rank
ON ets_inpi_insee_cases_distance.index_id = tb_min_rank.index_id AND
ets_inpi_insee_cases_distance.rank = tb_min_rank.min_rank
  ) 
  SELECT 
  min_rank,
  row_id, 
  tb_min_rank.index_id, 
  count_index,
  sequence_id, 
  siren, 
  siret,
  list_inpi, 
  lenght_list_inpi, 
  list_insee, 
  lenght_list_insee, 
  inpi_except, 
  insee_except, 
  intersection, 
  union_, 
  pct_intersection,
  index_id_max_intersection,
  test_pct_intersection,
  len_inpi_except, 
  len_insee_except, 
  status_cas,
  unzip_inpi,
  unzip_insee,
  max_cosine_distance,
  test_distance_cosine,
  levenshtein_distance,
  test_distance_levhenstein, 
  count_initial_insee, 
  count_inpi_siren_siret, 
  count_inpi_siren_sequence, 
  count_inpi_sequence_siret, 
  count_inpi_sequence_stat_cas_siret, 
  count_inpi_index_id_siret, 
  count_inpi_index_id_stat_cas_siret, 
  count_inpi_index_id_stat_cas, 
  index_id_duplicate, 
  test_sequence_siret, 
  test_index_siret, 
  test_siren_insee_siren_inpi, 
  test_sequence_siret_many_cas, 
  list_numero_voie_matching_inpi, 
  list_numero_voie_matching_insee, 
  intersection_numero_voie, 
  union_numero_voie, 
  test_list_num_voie, 
  datecreationetablissement, 
  date_debut_activite, 
  test_date, 
  etatadministratifetablissement, 
  status_admin, 
  test_status_admin, 
  etablissementsiege, 
  status_ets, 
  test_siege, 
  codecommuneetablissement, 
  code_commune, 
  test_code_commune, 
  codepostaletablissement, 
  code_postal_matching, 
  numerovoieetablissement, 
  numero_voie_matching, 
  test_numero_voie, 
  typevoieetablissement, 
  type_voie_matching, 
  test_type_voie, 
  test_adresse_cas_1_3_4, 
  index_id_dup_has_cas_1_3_4, 
  test_duplicates_is_in_cas_1_3_4, 
  enseigne, 
  enseigne1etablissement, 
  enseigne2etablissement, 
  enseigne3etablissement, 
  test_enseigne,
  key_except_to_test 
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
            database='inpi',
            s3_output=s3_output,
  filename = None, ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

```python
query = """
SELECT * 
FROM ets_inpi_insee_cases_rank  
LIMIT 10
"""
s3.run_query(
        query=query,
        database='inpi',
        s3_output='INPI/sql_output',
    filename  = 'ex_ets_inpi_insee_cases'
    )
```

# Analyse


## Count nombre lignes & index


Nombre de lignes

```python
query = """
SELECT COUNT(*)
FROM ets_inpi_insee_cases_rank 
"""

s3.run_query(
            query=query,
            database='inpi',
            s3_output='INPI/sql_output',
      filename = 'cnt_nb_lignes_rank', ## Add filename to print dataframe
      destination_key = None ### Add destination key if need to copy output
        )  
```

Nombre d'index

```python
query = """
SELECT COUNT(distinct(index_id))
FROM ets_inpi_insee_cases_rank 
"""

s3.run_query(
            query=query,
            database='inpi',
            s3_output='INPI/sql_output',
      filename = 'cnt_nb_index_rank', ## Add filename to print dataframe
      destination_key = None ### Add destination key if need to copy output
        )
```

### Evaluation des doublons

Le tableau ci dessous récapitule les index uniques et les doublons

```python
query = """
SELECT count_index, COUNT(*) as ligne_dup
FROM ets_inpi_insee_cases_rank 
GROUP BY count_index 
ORDER BY count_index
"""

nb_ligne = s3.run_query(
            query=query,
            database='inpi',
            s3_output='INPI/sql_output',
      filename = 'cnt_nb_dup_lignes_rank', ## Add filename to print dataframe
      destination_key = None ### Add destination key if need to copy output
)
```

```python
query = """
SELECT count_index, COUNT(DISTINCT(index_id)) as index_dup
FROM ets_inpi_insee_cases_rank 
GROUP BY count_index 
ORDER BY count_index
"""

nb_index = s3.run_query(
            query=query,
            database='inpi',
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

### Analyse des ranks

```python
query = """
SELECT 
count_index, 
  approx_percentile(min_rank, ARRAY[.1, .15, .20, 0.25,0.50,0.75,.80,.85,.86,.87, .88, .89,.90,0.95, 0.99]) as pct_min_rank
FROM 
  ets_inpi_insee_cases_rank
GROUP BY count_index  
ORDER BY count_index
"""
s3.run_query(
            query=query,
            database='inpi',
            s3_output='INPI/sql_output',
      filename = 'distribution_rank', ## Add filename to print dataframe
      destination_key = None ### Add destination key if need to copy output
        )
```

Prenons par exemple, le rank 32141, qui correspond a la règle suivante:

```python
query ="""
SELECT *
FROM regles_tests 
WHERE rank = 32141
"""
s3.run_query(
            query=query,
            database='inpi',
            s3_output='INPI/sql_output',
      filename = 'rules', ## Add filename to print dataframe
      destination_key = None ### Add destination key if need to copy output
        )
```

```python
query ="""
SELECT *
FROM regles_tests 
WHERE rank = 1563
"""
s3.run_query(
            query=query,
            database='inpi',
            s3_output='INPI/sql_output',
      filename = 'rules', ## Add filename to print dataframe
      destination_key = None ### Add destination key if need to copy output
        )
```

```python
query ="""
SELECT *
FROM regles_tests 
WHERE rank = 1888
"""
s3.run_query(
            query=query,
            database='inpi',
            s3_output='INPI/sql_output',
      filename = 'rules', ## Add filename to print dataframe
      destination_key = None ### Add destination key if need to copy output
        )
```

```python
query ="""
SELECT *
FROM regles_tests 
WHERE rank =  21463
"""
s3.run_query(
            query=query,
            database='inpi',
            s3_output='INPI/sql_output',
      filename = 'rules', ## Add filename to print dataframe
      destination_key = None ### Add destination key if need to copy output
        )
```

Ci dessous, un ensemble de lignes correspondant a la règle 32141

```python
query ="""
SELECT * 
FROM ets_inpi_insee_cases_rank 
WHERE min_rank = 32141
LIMIT 5
"""
s3.run_query(
            query=query,
            database='inpi',
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
create_report(extension = "html")
```
