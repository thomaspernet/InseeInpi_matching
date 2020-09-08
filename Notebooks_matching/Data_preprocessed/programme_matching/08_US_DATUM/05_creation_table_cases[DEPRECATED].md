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

# Creation variables tests pct_intersection, date, index, siege, siren et statut admin a effectuer pour dedoublonner les lignes

Copy paste from Coda to fill the information

## Objective(s)

* Créer un table avec l'ensemble des tests réalisés lors des 3 précédents US et ajouter les tests suivants, avec les valeurs possibles:
  * date:
    - TRUE: La date d'activité correspond à la date de début d'activité
    - FALSE: La date d'activité ne correspond pas à la date de début d'activité
  * test_index_id_duplicate:
    - TRUE: L'index possède qu'un seul siret
    - FALSE: L'index possède plus d'un seul siret
  * test_siege:
    - TRUE: Le statut siège est identique à l'INSEE et a l'INPI
    - FALSE: Le statut siège n'est pas identique à l'INSEE et a l'INPI
  * test_siren_insee_siren_inpi:
    - TRUE: Le nombre de siret par siren est identique à l'INSEE et a l'INPI
    - FALSE: Le nombre de siret par siren n'est pas identique à l'INSEE et a l'INPI
  * test_status_admin:
    - TRUE: Le statut administratif est identique à l'INSEE et a l'INPI
    - FALSE: Le statut administratifn n'est pas identique à l'INSEE et a l'INPI  
  * test_pct_intersection
    - TRUE: Le pct_intersection est identique au maximum du pct_intersection par index 
    - FALSE: Le pct_intersection n'est identique au maximum du pct_intersection par index 
## Metadata 

* Metadata parameters are available here: 
* US Title: Creation variables tests pct_intersection, date, index, siege, siren et statut admin a effectuer pour dedoublonner les lignes
* Epic: Epic 5
* US: US 7
* Date Begin: 9/1/2020
* Duration Task: 0
* Status:  
* Source URL: [US 07 Preparation tables et variables tests](https://coda.io/d/_dCtnoqIftTn/US-07-Preparation-tables-et-variables-tests_suFb9)
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
  *  #sql-query,#regle-de-gestion,#preparation-tests-supplementaire
* Toggl Tag
  * #data-preparation
  
## Input Cloud Storage [AWS]

If link from the internet, save it to the cloud first

### Tables [AWS]

1. Batch 1:
  * Select Provider: Athena
  * Select table(s): ets_insee_inpi_siretisation
    * Select only tables created from the same notebook, else copy/paste selection to add new input tables
    * If table(s) does not exist, add them: Add New Table
    * Information:
      * Region: 
        * NameEurope (Paris)
        * Code: eu-west-3
      * Database: siretisation
      * Notebook construction file: 
        * [01_merge_ets_insee_inpi](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/08_US_DATUM/01_merge_ets_insee_inpi.md)
    
## Destination Output/Delivery

  1. AWS
    1. Athena: 
      * Region: Europe (Paris)
      * Database: siretisation
      * Tables (Add name new table): ets_insee_inpi_cases
      * List new tables
          * ets_insee_inpi_cases

## Things to know (Steps, Attention points or new flow of information)

### Sources of information  (meeting notes, Documentation, Query, URL)

1. Jupyter Notebook (Github Link)
  1. md : https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/02_siretisation/07_pourcentage_siretisation_v3.md#creation-table-analyse
  2. [notebook tests cases](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/08_US_DATUM/02_cas_de_figure.md)
  3. [notebook test list](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/08_US_DATUM/03_test_list_num_voie.md)
  4. [notebook test enseigne](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/08_US_DATUM/04_test_enseigne.md)



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

```python
s3_output = 'inpi/sql_output'
database = 'siretisation'
```

# Create table

```python
query = """
DROP TABLE `ets_insee_inpi_case`;
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
CREATE TABLE siretisation.ets_insee_inpi_case
WITH (
  format='PARQUET'
) AS
WITH create_var AS (
  SELECT 
    index_id, 
    sequence_id, 
    count_initial_insee, 
    siren, 
    siret, 
    codecommuneetablissement, 
    code_commune, 
    code_postal_matching, 
    ville_matching, 
    typevoieetablissement, 
    type_voie_matching, 
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
    ) as pct_intersection, 
    list_numero_voie_matching_inpi, 
    list_numero_voie_matching_insee, 
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
    enseigne, 
    list_enseigne, 
    contains(list_enseigne, enseigne) AS temp_test_enseigne, 
    datecreationetablissement, 
    "date_début_activité", 
    etablissementsiege, 
    status_ets, 
    etatadministratifetablissement, 
    status_admin 
  FROM 
    siretisation.ets_insee_inpi
) 
SELECT 
  * 
FROM 
  (
    WITH test AS (
      SELECT 
        create_var.index_id, 
        count_inpi_index_id_siret, 
        CASE WHEN count_inpi_index_id_siret > 1 THEN 'TRUE' ELSE 'FALSE' END AS test_index_id_duplicate, 
        sequence_id, 
        count_initial_insee, 
        create_var.siren, 
        count_inpi_siren_siret, 
        CASE WHEN count_initial_insee = count_inpi_siren_siret THEN 'TRUE' ELSE 'FALSE' END AS test_siren_insee_siren_inpi, 
        siret, 
        codecommuneetablissement, 
        code_commune, 
        code_postal_matching, 
        ville_matching, 
        typevoieetablissement, 
        type_voie_matching, 
        adresse_distance_insee, 
        adresse_distance_inpi, 
        CASE WHEN cardinality(insee_except) = 0 THEN NULL ELSE insee_except END as insee_except,
        CASE WHEN cardinality(inpi_except) = 0 THEN NULL ELSE inpi_except END as inpi_except,
        intersection, 
        union_, 
        intersection / union_ as pct_intersection, 
        index_id_max_intersection, 
        CASE WHEN intersection / union_ = index_id_max_intersection THEN 'TRUE' ELSE 'FALSE' END AS test_pct_intersection, 
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
        AND intersection != 0 THEN 'CAS_4' ELSE 'CAS_NO_ADRESSE' END AS status_cas, 
        CASE WHEN cardinality(list_numero_voie_matching_inpi) = 0 THEN NULL ELSE list_numero_voie_matching_inpi END as list_numero_voie_matching_inpi, 
        CASE WHEN cardinality(list_numero_voie_matching_insee) = 0 THEN NULL ELSE list_numero_voie_matching_insee END as list_numero_voie_matching_insee, 
        CASE WHEN intersection_numero_voie = union_numero_voie 
        AND (
          intersection_numero_voie IS NOT NULL 
          OR union_numero_voie IS NOT NULL
        ) THEN 'TRUE' WHEN (
          intersection_numero_voie IS NULL 
          OR union_numero_voie IS NULL
        ) THEN 'NULL' WHEN intersection_numero_voie > 0 
        AND intersection_numero_voie != union_numero_voie THEN 'PARTIAL' ELSE 'FALSE' END AS test_list_num_voie, 
        enseigne, 
        CASE WHEN cardinality(list_enseigne) = 0 THEN NULL ELSE list_enseigne END as list_enseigne,
        CASE WHEN cardinality(list_enseigne) = 0 
        OR list_enseigne IS NULL 
        OR enseigne = '' THEN 'NULL' WHEN temp_test_enseigne = TRUE THEN 'TRUE' ELSE 'FALSE' END AS test_enseigne, 
        datecreationetablissement, 
        "date_début_activité", 
        CASE WHEN datecreationetablissement = "date_début_activité" THEN 'TRUE' WHEN datecreationetablissement = '' 
        OR "date_début_activité" ='' THEN 'NULL' ELSE 'FALSE' END AS test_date, 
        etablissementsiege, 
        status_ets, 
        CASE WHEN etablissementsiege = status_ets THEN 'TRUE' WHEN etablissementsiege IS NULL 
        OR status_ets IS NULL THEN 'NULL' WHEN etablissementsiege = '' 
        OR status_ets = '' THEN 'NULL' ELSE 'FALSE' END AS test_siege, 
        etatadministratifetablissement, 
        status_admin, 
        CASE WHEN etatadministratifetablissement = status_admin THEN 'TRUE' WHEN etatadministratifetablissement IS NULL 
        OR status_admin IS NULL THEN 'NULL' WHEN etatadministratifetablissement = '' 
        OR status_admin = '' THEN 'NULL' ELSE 'FALSE' END AS test_status_admin 
      FROM 
        create_var 
        LEFT JOIN (
          SELECT 
            index_id, 
            COUNT(
              DISTINCT(siret)
            ) AS count_inpi_index_id_siret 
          FROM 
            create_var 
          GROUP BY 
            index_id
        ) AS count_rows_index_id_siret ON create_var.index_id = count_rows_index_id_siret.index_id 
        LEFT JOIN (
          SELECT 
            siren, 
            COUNT(
              DISTINCT(siret)
            ) AS count_inpi_siren_siret 
          FROM 
            create_var 
          GROUP BY 
            siren
        ) AS count_rows_sequence ON create_var.siren = count_rows_sequence.siren 
        LEFT JOIN (
          SELECT 
            index_id, 
            MAX(pct_intersection) AS index_id_max_intersection 
          FROM 
            create_var 
          GROUP BY 
            index_id
        ) AS is_index_id_index_id_max_intersection ON create_var.index_id = is_index_id_index_id_max_intersection.index_id
    ) 
    SELECT 
      * 
    FROM 
      test 
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

# Input/output

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
FROM siretisation.ets_insee_inpi_case 
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

tb = s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = ('exemple_other_test'), ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

```python
print(pd.concat([

pd.concat([
tb[['siret', 'datecreationetablissement', 'date_début_activité',
    'count_inpi_index_id_siret',
    'etablissementsiege', 'status_ets', 
    'count_initial_insee', 'count_inpi_siren_siret', 
    'etatadministratifetablissement', 'status_admin',
    'pct_intersection', 'index_id_max_intersection',
   
   ]]
],keys=["Input"], axis = 1),
pd.concat([
tb[['test_date','test_index_id_duplicate', 'test_siege',
    'test_siren_insee_siren_inpi', 'test_status_admin', 'test_pct_intersection']]
],keys=["Output"], axis = 1)
], axis = 1
).to_markdown()
     )
```

# Test acceptance

1. Vérifier que le nombre de lignes est indentique avant et après la création des variables
2. Compter le nombre de lignes par test
3. Compter le nombre d'index par test
4. Créer un tableau avec une ligne par test


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
SELECT COUNT(*)
FROM siretisation.ets_insee_inpi_case
"""
s3.run_query(
            query=query,
            database='siretisation',
            s3_output=s3_output,
  filename = 'count_ets_insee_inpi', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

## 2. Compter le nombre de lignes par test

```python
groups = ['test_index_id_duplicate', 'test_siege',
    'test_siren_insee_siren_inpi', 'test_status_admin', 'test_pct_intersection']

top = """
WITH tb_test_date AS (
SELECT test_date as groups, COUNT(*) as cnt_test_date
FROM siretisation.ets_insee_inpi_case 
GROUP BY test_date
  )
  SELECT groups,  cnt_test_date, 
  
"""
top_2 = ""

top_3 = " FROM tb_test_date "

middle_1 ="""
LEFT JOIN (
  
  SELECT {0}, COUNT(*) as cnt_{0}
FROM siretisation.ets_insee_inpi_case 
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
print(s3.run_query(
            query=query,
            database='siretisation',
            s3_output=s3_output,
  filename = 'count_ets_insee_inpicase_groups', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        ).to_markdown()
     )
```

## 3. Compter le nombre d'index par test

```python
groups = ['test_index_id_duplicate', 'test_siege',
    'test_siren_insee_siren_inpi', 'test_status_admin', 'test_pct_intersection']

top = """
WITH tb_test_date AS (
SELECT test_date as groups, COUNT(distinct(index_id)) as cnt_test_date
FROM siretisation.ets_insee_inpi_case 
GROUP BY test_date
  )
  SELECT groups,  cnt_test_date, 
  
"""
top_2 = ""

top_3 = " FROM tb_test_date "

middle_1 ="""
LEFT JOIN (
  
  SELECT {0}, COUNT(distinct(index_id)) as cnt_{0}
FROM siretisation.ets_insee_inpi_case 
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
print(s3.run_query(
            query=query,
            database='siretisation',
            s3_output=s3_output,
  filename = 'count_distinct_ets_insee_inpicase_groups', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        ).to_markdown()
     )
```

## 4. Créer un tableau avec une ligne par test

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
FROM siretisation.ets_insee_inpi_case 
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

print(s3.run_query(
            query=query,
            database='siretisation',
            s3_output=s3_output,
  filename = 'exemple_other_test', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        ).to_markdown()
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
