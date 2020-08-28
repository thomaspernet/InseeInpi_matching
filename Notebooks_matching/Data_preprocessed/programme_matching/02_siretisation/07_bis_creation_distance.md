---
jupyter:
  jupytext:
    formats: ipynb,md
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.2'
      jupytext_version: 1.4.2
  kernelspec:
    display_name: Python 3
    language: python
    name: python3
---

# Test nombre lignes siretise

Copy paste from Coda to fill the information

## Objective(s)

- Lors de [l’US 7: Test nombre lignes siretise avec nouvelles regles de gestion](https://coda.io/d/CreditAgricole_dCtnoqIftTn/US-07-ETS-version-3_su0VF), nous avons créé une table avec l’ensemble des possibilités de tests, trié par ordre de préférence. 

  - Toutefois, il manque deux variables:

    - max_distance_cosine
    - test_distance_costine:
        - test si la distance max est supérieur a .6
    - levhenstein_distance
    - test_levhenstein
        - test si l'edit distance est inférieure ou égale a 1

  - Dans cette US, nous allons créer ses deux variables et les ajouter à la table ets_inpi_insee_cases. Une nouvelle table sera créé, appelée ets_inpi_insee_cases_distance . Les nouvelles variables a ajouter sont les suivantes:

  - unzip_inpi, 

    - mot ayant servi coté inpi pour trouver la distance

  -  unzip_insee, 

    - mot ayant servi coté inse pour trouver la distance

  - max_cosine_distance, 

    - distance maximum de l’index

  -  test as key_except_to_test

    - liste contenant les clés valeurs des mots non communs

  - Une table intermédiaire contenant le max de la distance sera calculé, avec la Levhenstein aussi. La table s’appelle  ets_inpi_distance_max_word2vec 

## Metadata 

- Metadata parameters are available here: [Ressources_suDYJ#_luZqd](http://Ressources_suDYJ#_luZqd)

  - Task type:

     - Jupyter Notebook

  - Users: :

      - [Thomas Pernet](mailto:t.pernetcoudrier@gmail.com)

  - Watchers:

      - [Thomas Pernet](mailto:t.pernetcoudrier@gmail.com)

  - Estimated Log points:

      - One being a simple task, 15 a very difficult one
        -  10

  - Task tag

      - \#machine-learning,#sql-query,#computation,#word2vec

  - Toggl Tag

      - \#variable-computation
  
## Input Cloud Storage [AWS/GCP]

If link from the internet, save it to the cloud first

### Tables [AWS/BigQuery]

- Batch 1:

  - Select Provider: Athena

    - Select table(s): ets_inpi_insee_cases

    - Select only tables created from the same notebook, else copy/paste selection to add new input tables

      - If table(s) does not exist, add them: 

        Add New Table

      - Information:

      - Region: 

        - NameEurope (Paris)
          - Code: eu-west-3

        - Database: inpi

        - Notebook construction file: [07_pourcentage_siretisation_v3](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/02_siretisation/07_pourcentage_siretisation_v3.md)
    
## Destination Output/Delivery

- AWS

    - Athena: 

      - Region: Europe (Paris)
        - Database: inpi
        - Tables (Add name new table): ets_inpi_distance_max_word2vec,ets_inpi_insee_cases_distance
        - List new tables
        - ets_inpi_distance_max_word2vec, ets_inpi_insee_cases_distance

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
import os, shutil
bucket = 'calfdata'
path = os.getcwd()
parent_path = str(Path(path).parent)
path_cred = r"{}/credential_AWS.json".format(parent_path)

region = 'eu-west-3'
bucket = 'calfdata'
s3_output = 'INPI/sql_data'
```

```python
con = aws_connector.aws_instantiate(credential = path_cred,
                                       region = region)
client= con.client_boto()
s3 = service_s3.connect_S3(client = client,
                      bucket = bucket, verbose = False) 
#athena = service_athena.connect_athena(client = client,
#                      bucket = bucket) 
```

```python
import seaborn as sns

cm = sns.light_palette("green", as_cmap=True)
pd.set_option('display.max_columns', None)
```

# Creation tables

## Steps

- Filtrer les cas 5 à 7. 
- créer deux colonnes avec le pseudo-produit cartésien (table INPI vers table INSEE). Autrement dit, on ne souhaite pas comparer les mots au sein de la même liste, mais entre les listes. 
  - Table `ets_inpi_insee_cases` 
- Merge la liste des poids dans la table `list_mots_insee_inpi_word2vec_weights` 
- Calcul de la Cosine distance (dot product sur la magnitude)
- Calcul de la Cosine distance maximum par group `index_id`
- Recupération de la combinaison maximum par group
- Création de la table `ets_inpi_insee_word2vec` pour analyse

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
      CASE WHEN max_cosine_distance >= .6 THEN 'True' ELSE 'False' END AS test_distance_costine,
      test as key_except_to_test,
      levenshtein_distance(unzip_inpi, unzip_insee) AS levenshtein_distance,
      CASE WHEN levenshtein_distance(unzip_inpi, unzip_insee) <=1  THEN 'True' ELSE 'False' END AS test_levhenstein
    
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

output = s3.run_query(
            query=query,
            database='inpi',
            s3_output=s3_output,
  filename = None, ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

## Breve analyse


### Distribution Distance


```python
pd.set_option('display.max_colwidth', None)
```

```python
query = """
SELECT approx_percentile(
  max_cosine_distance, ARRAY[
    0.25,
    0.50,
    0.60,
    0.70,
    0.75,
    0.80,
    0.85,
    0.95,
    0.99]
  )
  FROM ets_inpi_distance_max_word2vec 
"""
s3.run_query(
            query=query,
            database='inpi',
            s3_output=s3_output,
  filename = 'distance_cosine', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

```python
query = """ 
SELECT test_distance_costine, COUNT(*)
FROM ets_inpi_distance_max_word2vec 
GROUP BY test_distance_costine
"""

s3.run_query(
            query=query,
            database='inpi',
            s3_output=s3_output,
  filename = 'test_distance_cosine', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

```python
query = """
SELECT approx_percentile(
  levenshtein_distance, ARRAY[
    0.25,
    0.50,
    0.60,
    0.70,
    0.75,
    0.80,
    0.85,
    0.95,
    0.99]
  )
  FROM ets_inpi_distance_max_word2vec 
"""
s3.run_query(
            query=query,
            database='inpi',
            s3_output=s3_output,
  filename = 'levenshtein_distance', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

```python
query = """ 
SELECT test_levhenstein, COUNT(*)
FROM ets_inpi_distance_max_word2vec 
GROUP BY test_levhenstein
"""

s3.run_query(
            query=query,
            database='inpi',
            s3_output=s3_output,
  filename = 'test_levhenstein', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

# Create table ajout distance

```python
query = """
CREATE TABLE inpi.ets_inpi_insee_cases_distance
WITH (
  format='PARQUET'
) AS
SELECT 
  rank, 
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
  len_inpi_except, 
  len_insee_except, 
  status_cas,
  unzip_inpi,
  unzip_insee,
  max_cosine_distance,
  test_distance_costine,
  levenshtein_distance,
  test_levhenstein, 
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

"""
```

```python
output = s3.run_query(
            query=query,
            database='inpi',
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
def create_report(extension = "html"):
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
