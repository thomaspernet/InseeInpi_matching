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
- Lors de l’US, [Creation table distance word2vec et merge table ets inpi insee cas](https://coda.io/d/CreditAgricole_dCtnoqIftTn/US-07-ETS-version-3_su0VF), nous avons crée deux variables pour les tests, a savoir le test sur la distance de cosine, et le test de levhenstein. 
  - Afin de séparer les doublons, il suffit de récupérer le rank minimum par index. Celui ci va nous donner le meilleur des probables. 
  - Il est bien sur possible d’avoir encore des doublons, dans ces cas la, il faut aller plus loin dans la rédaction des tests
  - L’objectif de cette US est de récupérer le rank minimum de la table ets_inpi_insee_cases puis de faire une analyse brève des index récupérés. 

## Metadata 

- - Metadata parameters are available here: [Ressources_suDYJ#_luZqd](http://Ressources_suDYJ#_luZqd)

  - Task type:

    - Jupyter Notebook

  - Users: :

    - [Thomas Pernet](mailto:t.pernetcoudrier@gmail.com)

  - Watchers:

    - [Thomas Pernet](mailto:t.pernetcoudrier@gmail.com)

  - Estimated Log points:

    - One being a simple task, 15 a very difficult one
        -  8

  - Task tag

    -  \#sql-query,#matching,#similarite,#regle-de-gestion

  - Toggl Tag

    - \#data-analysis
  
## Input Cloud Storage [AWS/GCP]

If link from the internet, save it to the cloud first

### Tables [AWS/BigQuery]

- Batch 1:

  - Select Provider: Athena

    -  Select table(s): ets_inpi_insee_cases_distance

    - Select only tables created from the same notebook, else copy/paste selection to add new input tables

      - If table(s) does not exist, add them: 

        Add New Table

      - Information:

      - Region: 

        - NameEurope (Paris)
          - Code: eu-west-3

        - Database: inpi

        - Notebook construction file: [07_bis_creation_distance](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/02_siretisation/07_bis_creation_distance.md)
    
## Destination Output/Delivery

- AWS

    -  Athena: 

      -  Region: Europe (Paris)
        - Database: inpi
        - Tables (Add name new table): ets_inpi_insee_cases_rank
        - List new tables
        - ets_inpi_insee_cases_rank 

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
```

```python
con = aws_connector.aws_instantiate(credential = path_cred,
                                       region = region)
client= con.client_boto()
s3 = service_s3.connect_S3(client = client,
                      bucket = bucket, verbose = False) 
#athena = service_athena.connect_athena(client = client,
#                      bucket = bucket) 
s3_output = 'INPI/sql_output'
```

```python
import seaborn as sns

cm = sns.light_palette("green", as_cmap=True)
pd.set_option('display.max_columns', None)
```

# Creation tables

## Steps

- Recupération du rank minimum par index
- Filtre les lignes correspondant au rank minimum par index

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

output = s3.run_query(
            query=query,
            database='inpi',
            s3_output=s3_output,
  filename = None, ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

# Analyse


## Count nombre lignes  & index


Le nombre de lignes est de:

```python
query = """
SELECT COUNT(*)
FROM ets_inpi_insee_cases_rank 
"""

output = s3.run_query(
            query=query,
            database='inpi',
            s3_output='INPI/sql_output',
      filename = 'cnt_nb_lignes_rank', ## Add filename to print dataframe
      destination_key = None ### Add destination key if need to copy output
        )
output
```

Le nombre d'index est de 

```python
query = """
SELECT COUNT(distinct(index_id))
FROM ets_inpi_insee_cases_rank 
"""

output = s3.run_query(
            query=query,
            database='inpi',
            s3_output='INPI/sql_output',
      filename = 'cnt_nb_index_rank', ## Add filename to print dataframe
      destination_key = None ### Add destination key if need to copy output
        )
output
```

## Evaluation des doublons

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
9397225
```

Nombre d'index a trouver

```python
9421163
```

Pourcentage de probable trouvé

```python
round(9397225 / 9421163, 4)
```

## Analyse des ranks

```python
pd.set_option('display.max_colwidth', None)
```

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

Prenons par exemple, le rank 32897, qui correspond a la règle suivante:

```python
query ="""
SELECT *
FROM regles_tests 
WHERE rank = 32897
"""
s3.run_query(
            query=query,
            database='inpi',
            s3_output='INPI/sql_output',
      filename = 'rules', ## Add filename to print dataframe
      destination_key = None ### Add destination key if need to copy output
        )
```

Ci dessous, un ensemble de lignes correspondant a la règle 32897

```python
query ="""
SELECT * 
FROM ets_inpi_insee_cases_rank 
WHERE min_rank = 32897
LIMIT 10
"""
s3.run_query(
            query=query,
            database='inpi',
            s3_output='INPI/sql_output',
      filename = 'rules_32897', ## Add filename to print dataframe
      destination_key = None ### Add destination key if need to copy output
        )
```

## Analyse des cas pour index unique

```python
query = """
SELECT status_cas, count(*) as nb_unique
FROM ets_inpi_insee_cases_rank 
WHERE count_index = 1
GROUP BY status_cas
ORDER BY status_cas
"""

tb = s3.run_query(
            query=query,
            database='inpi',
            s3_output='INPI/sql_output',
      filename = 'nb_cas_index_unique_rank', ## Add filename to print dataframe
      destination_key = None ### Add destination key if need to copy output
        )
(
    tb.
assign(
cumsum = lambda x: x['nb_unique'].cumsum(),
    pct_total = lambda x: x['nb_unique']/x['nb_unique'].sum(),
    pct_total_cum = lambda x: x['pct_total'].cumsum()
)
    .style
    .format("{0:,.2%}", subset=["pct_total", 'pct_total_cum'])
    .bar(subset= ['cumsum'],
                       color='#d65f5f')
    .bar(subset= ['nb_unique'],
                       color='#228B22')
)
```

## Exemple doublons

Il reste encore des doublons. C'est le cas lorsque le test sur la distance retourne le même resultat pour toutes les lignes. 

Une des possibilités serait de récupérer la valeur la plus élevée de `pct_intersection`. 

```python
query ="""
SELECT * 
FROM ets_inpi_insee_cases_rank 
WHERE count_index = 10
ORDER BY min_rank
LIMIT 10
"""
s3.run_query(
            query=query,
            database='inpi',
            s3_output='INPI/sql_output',
      filename = 'count_index_10', ## Add filename to print dataframe
      destination_key = None ### Add destination key if need to copy output
        )
```

D'autres possibilités de doublons sont dues a des erreurs de préparation de la donnée. 

```python
query = """
SELECT * 
FROM ets_inpi_insee_cases_rank 
WHERE count_index = 2
ORDER BY index_id, min_rank
LIMIT 10
"""
s3.run_query(
            query=query,
            database='inpi',
            s3_output='INPI/sql_output',
      filename = 'count_index_2', ## Add filename to print dataframe
      destination_key = None ### Add destination key if need to copy output
        )
```

C'est le cas pour le siren 847918893. Il y a deux lignes pour la même date de transmission. Il aurait fallu prendre uniquement la dernière ligne.

```python
query = """
SELECT *
FROM ets_final_sql 
WHERE index_id = 1265
"""
s3.run_query(
            query=query,
            database='inpi',
            s3_output='INPI/sql_output',
      filename = 'issue_siren_847918893', ## Add filename to print dataframe
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
