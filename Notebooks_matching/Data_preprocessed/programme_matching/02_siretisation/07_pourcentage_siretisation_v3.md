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

# Test nombre lignes siretise

Objective(s)

*   Lors de l’US 6 Union et intersection, nous avons compté séparément le nombre de lignes siretisés avec les tests unitaires, indépendamment des autres tests. Le niveau de matching avec les tests unitaires va de 39% a 70%. Dans cette US, nous allons faire le test avec une prise en compte de l’ensemble des tests, autrement dit, compter le nombre de lignes sirétisé lorsque les tests sont co-dépendant.
  * L’objectif principal étant de fournir un pourcentage avec le nombre de lignes siretisé.
  * Les tests vont suivre l’arborescence suivante:
    * Calcul des cas 1 à 7
      * Calcul des tests 
      * Calcul des duplicates sur index & séquence
        * Filtrer les lignes sans duplicate et tests (a definir les règles sur les tests)

## Metadata

* Metadata parameters are available here: Ressources_suDYJ#_luZqd
* Task type:
  * Jupyter Notebook
* Users: :
  * Thomas Pernet
* Watchers:
  * Thomas Pernet
* Estimated Log points:
  * One being a simple task, 15 a very difficult one
  *  9
* Task tag
  *  #sql-query,#matching,#test-codependance,#siretisation
* Toggl Tag
  * #data-analysis
* Instance [AWS/GCP]
  *   
  
## Input Cloud Storage [AWS/GCP]

If link from the internet, save it to the cloud first

### Tables [AWS/BigQuery]

1. Batch 1:
  * Select Provider: Athena
  * Select table(s): ets_insee_inpi
    * Select only tables created from the same notebook, else copy/paste selection to add new input tables
    * If table(s) does not exist, add them: Add New Table
    * Information:
      * Region: 
        * Name: Europe (Paris)
        * Code: eu-west-3
      * Database: inpi
      * Notebook construction file: https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/03_ETS_add_variables.md
    
## Destination Output/Delivery

* AWS
  1. Athena: 
      * Region: Europe (Paris)
      * Database: inpi
      * Tables (Add name new table): ets_inpi_insee_cases
      * List new tables
      * ets_inpi_insee_cases
  
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
con = aws_connector.aws_instantiate(credential = path_cred,
                                       region = 'eu-west-3')
client= con.client_boto()
s3 = service_s3.connect_S3(client = client,
                      bucket = 'calfdata', verbose = False) 
athena = service_athena.connect_athena(client = client,
                      bucket = 'calfdata') 
```

# Creation table analyse


```python
query = """
/*match insee inpi 7 cas de figs*/
CREATE TABLE inpi.ets_inpi_insee_cases
WITH (
  format='PARQUET'
) AS
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
  enseigne, enseigne1etablissement, enseigne2etablissement, enseigne3etablissement, 
  array_remove(
array_distinct(
SPLIT(
  concat(
  enseigne1etablissement,',', enseigne2etablissement,',', enseigne3etablissement),
  ',')
  ), ''
  ) as test, 
  
contains( 
         array_remove(
array_distinct(
SPLIT(
  concat(
  enseigne1etablissement,',', enseigne2etablissement,',', enseigne3etablissement),
  ',')
  ), ''
  ),enseigne
         ) AS temp_test_enseigne
  FROM 
    "inpi"."ets_insee_inpi" -- limit 10
    ) 
SELECT 
count_initial_insee,
  index_id, 
  sequence_id, 
  siren, 
  siret, 
  CASE WHEN cardinality(list_numero_voie_matching_inpi) = 0 THEN NULL ELSE list_numero_voie_matching_inpi END as list_numero_voie_matching_inpi, 
  CASE WHEN cardinality(list_numero_voie_matching_insee) = 0 THEN NULL ELSE list_numero_voie_matching_insee END as list_numero_voie_matching_insee,
  intersection_numero_voie,
  union_numero_voie,
  CASE WHEN intersection_numero_voie = union_numero_voie AND (intersection_numero_voie IS NOT NULL OR union_numero_voie IS NOT NULL) THEN 'True' 
  WHEN (intersection_numero_voie IS NULL OR union_numero_voie IS NULL) THEN 'Null'
  ELSE 'False' END AS test_list_num_voie,
  
  datecreationetablissement, 
  date_debut_activite, 
  CASE WHEN datecreationetablissement = date_debut_activite THEN 'True' WHEN datecreationetablissement IS NULL 
  OR date_debut_activite IS NULL THEN 'NULL' ELSE 'False' END AS test_date, 
  etatadministratifetablissement, 
  status_admin, 
  CASE WHEN etatadministratifetablissement = status_admin THEN 'True' WHEN etatadministratifetablissement = '' 
  OR status_admin = '' THEN 'NULL' ELSE 'False' END AS test_status_admin, 
  etablissementsiege, 
  status_ets, 
  CASE WHEN etablissementsiege = status_ets THEN 'True' WHEN etablissementsiege = '' 
  OR status_ets = '' THEN 'NULL' ELSE 'False' END AS test_siege, 
  codecommuneetablissement, 
  code_commune, 
  CASE WHEN codecommuneetablissement = code_commune THEN 'True' WHEN codecommuneetablissement = '' 
  OR code_commune = '' THEN 'NULL' ELSE 'False' END AS test_code_commune, 
  codepostaletablissement, 
  code_postal_matching, 
  numerovoieetablissement, 
  numero_voie_matching, 
  CASE WHEN numerovoieetablissement = numero_voie_matching THEN 'True' WHEN numerovoieetablissement = '' 
  OR numero_voie_matching = '' THEN 'NULL' ELSE 'False' END AS test_numero_voie, 
  typevoieetablissement, 
  type_voie_matching, 
  CASE WHEN typevoieetablissement = type_voie_matching THEN 'True' WHEN typevoieetablissement = '' 
  OR type_voie_matching = '' THEN 'NULL' ELSE 'False' END AS test_type_voie, 
  CASE WHEN cardinality(list_inpi) = 0 THEN NULL ELSE list_inpi END as list_inpi,
  
  lenght_list_inpi, 
  CASE WHEN cardinality(list_insee) = 0 THEN NULL ELSE list_insee END as list_insee,
  lenght_list_insee, 
  CASE WHEN cardinality(inpi_except) = 0 THEN NULL ELSE inpi_except END as inpi_except,
  CASE WHEN cardinality(insee_except) = 0 THEN NULL ELSE insee_except END as insee_except,
   
  intersection, 
  union_, 
  CASE WHEN intersection = union_  THEN 'CAS_1' WHEN intersection = 0 THEN 'CAS_2' WHEN lenght_list_inpi = intersection 
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
  enseigne, enseigne1etablissement, enseigne2etablissement, enseigne3etablissement, 
  CASE WHEN cardinality(test) = 0 THEN 'Null'
WHEN enseigne = '' THEN 'Null'
WHEN temp_test_enseigne = TRUE THEN 'True'
ELSE 'False' END AS test_enseigne 
  
FROM 
  test_proba
"""
```

# Analyse


## Nombre observations par cas

Le nombre d'observations doit correspondre au suivant:

|   Cas de figure | Titre                   |   Total |   Total cumulé |   pourcentage |   Pourcentage cumulé | Comment                 |
|----------------:|:------------------------|--------:|---------------:|--------------:|---------------------:|:------------------------|
|               1 | similarité parfaite     | 7775392 |        7775392 |     0.670261  |             0.670261 | Match parfait           |
|               2 | Exclusion parfaite      |  974444 |        8749836 |     0.0839998 |             0.75426  | Exclusion parfaite      |
|               3 | Match partiel parfait   |  407404 |        9157240 |     0.0351194 |             0.78938  | Match partiel parfait   |
|               4 | Match partiel parfait   |  558992 |        9716232 |     0.0481867 |             0.837566 | Match partiel parfait   |
|               5 | Match partiel compliqué | 1056406 |       10772638 |     0.0910652 |             0.928632 | Match partiel compliqué |
|               6 | Match partiel compliqué |  361242 |       11133880 |     0.0311401 |             0.959772 | Match partiel compliqué |
|               7 | Match partiel compliqué |  466671 |       11600551 |     0.0402283 |             1        | Match partiel compliqué |

```python
query = """
SELECT status_cas, COUNT(*) as count
FROM ets_inpi_insee_cases 
GROUP BY status_cas
"""
```

## Nombre ets par cas

```python
query = """
SELECT status_cas, COUNT(DISTINCT(index_id)) as distinct_ets
FROM ets_inpi_insee_cases 
GROUP BY status_cas
ORDER BY status_cas
"""
```

## Nombre etb unique INSEE par cas

```python
query = """
SELECT * 
FROM (
SELECT status_cas, count_initial_insee, COUNT(*) as count
FROM ets_inpi_insee_cases 
GROUP BY status_cas, count_initial_insee
  )
  WHERE count_initial_insee = 1
ORDER BY status_cas, count_initial_insee
"""
```

## Distribution somme enseigne

```python
query = """
SELECT 
  approx_percentile(sum_enseigne, ARRAY[0.25,0.50,0.75,.80,.85,.86,.87, .88, .89,.90,0.95, 0.99]) as sum_enseigne
FROM 
  ets_inpi_insee_cases 
"""
```

# Create table par cas


## Creation functions

La fonction ci dessous va générer le tableau d'analayse via une query, et retourne un dataframe Pandas, tout en stockant le resultat dans le dossier suivant:

- [calfdata/Analyse_cas_similarite_adresse](https://s3.console.aws.amazon.com/s3/buckets/calfdata/Analyse_cas_similarite_adresse/?region=eu-west-3&tab=overview)

```python
df_ = (pd.DataFrame(data = {'index_unique': range(1,21)})
       .to_csv('index_20.csv', index = False)
      )

s3.upload_file(file_to_upload = 'index_20.csv',
            destination_in_s3 = 'TEMP_ANALYSE_SIRETISATION/INDEX_20')
```

```python
create_table = """
CREATE EXTERNAL TABLE IF NOT EXISTS inpi.index_20 (
`index_unique`                     integer
    )
     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
   'separatorChar' = ',',
   'quoteChar' = '"'
   )
     LOCATION 's3://calfdata/TEMP_ANALYSE_SIRETISATION/INDEX_20'
     TBLPROPERTIES ('has_encrypted_data'='false',
              'skip.header.line.count'='1');"""
output = athena.run_query(
        query=create_table,
        database='inpi',
        s3_output='INPI/sql_output'
    )
```

```python
def create_table_test_not_false(cas = "CAS_1"):
    """
    
    """
    top = """
    SELECT count_test_list_num_voie.status_cas,
    index_unique,
    count_cas,
    test_list_num_voie,
    test_siege,
    test_enseigne,
    test_date, 
    status_admin,
    test_code_commune,
    test_type_voie
    FROM index_20 
    
    LEFT JOIN (
    SELECT count_, COUNT(count_) as count_cas
    FROM (
    SELECT COUNT(index_id) as count_
    FROM ets_inpi_insee_cases 
    WHERE status_cas = '{0}'
    GROUP BY index_id
    ORDER BY count_ DESC
  )
  GROUP BY count_
  ORDER BY count_
  ) AS count_unique
  ON index_20.index_unique = count_unique.count_ 
    """.format(cas)
    query = """
    LEFT JOIN (
    SELECT status_cas,count_index,  count(count_index) AS {0}
    FROM (
    SELECT status_cas, index_id, COUNT(test_enseigne) as count_index
    FROM ets_inpi_insee_cases 
    WHERE status_cas = '{1}' AND  {0} != 'False'
    GROUP BY status_cas, index_id
      ) as c
      GROUP BY status_cas, count_index
      ORDER BY count_index
      ) AS count_{0}
      ON index_20.index_unique = count_{0}.count_index 
    """

    for i, table in enumerate(["test_list_num_voie",
              "test_siege",
              "test_enseigne",
              "test_date", "status_admin", "test_code_commune", "test_type_voie"]):

        top += query.format(table, cas)
    top += "ORDER BY index_unique" 
    
    ### run query
    output = athena.run_query(
        query=top,
        database='inpi',
        s3_output='INPI/sql_output'
    )

    results = False
    filename = 'table_{}_test_not_false.csv'.format(cas)
    
    while results != True:
        source_key = "{}/{}.csv".format(
                            'INPI/sql_output',
                            output['QueryExecutionId']
                                   )
        destination_key = "{}/{}".format(
                                'ANALYSE_PRE_SIRETISATION',
                                filename
                            )
        
        results = s3.copy_object_s3(
                                source_key = source_key,
                                destination_key = destination_key,
                                remove = True
                            )
        
    #filename = 'table_{}_test_not_false.csv'.format('CAS_1')
    index_unique_inpi = 10981811
    reindex= ['status_cas', 'index_unique','count_cas',
              'test_list_num_voie',
              'count_num_voie',
              'test_siege',
              'count_siege',
           'test_enseigne',
               'count_enseigne',
              'test_date',
               'count_date',
              'status_admin',
              'count_admin',
              'test_code_commune',
              'count_code_commune',
           'test_type_voie',
              'count_type_voie']
    test_1 = (s3.read_df_from_s3(
            key = 'ANALYSE_PRE_SIRETISATION/{}'.format(filename), sep = ',')
     .assign(

         count_num_voie = lambda x: x['test_list_num_voie'] /  index_unique_inpi,
         count_siege = lambda x: x['test_siege'] /  index_unique_inpi,
         count_enseigne	 = lambda x: x['test_enseigne'] /  index_unique_inpi,
         count_date = lambda x: x['test_date'] /  index_unique_inpi,
         count_admin = lambda x: x['status_admin'] /  index_unique_inpi,
         count_code_commune = lambda x: x['test_code_commune'] /  index_unique_inpi,
         count_type_voie = lambda x: x['test_type_voie'] /  index_unique_inpi,
         status_cas = lambda x: x['status_cas'].fillna(cas)
     )
     .reindex(columns = reindex)
     .fillna(0)
                  .style
                  .format("{:,.0f}", subset =  [
                      "count_cas",
                      'test_list_num_voie',
                                                'test_siege',
                                                'test_enseigne',
                                                'test_date',
                                                'status_admin',
                                                'test_code_commune',
                                                'test_type_voie'])
                  .format("{:.2%}", subset =  ['count_num_voie',
                                               'count_siege',
                                               'count_enseigne',
                                               'count_date',
                                               'count_admin',
                                               'count_code_commune',
                                               'count_type_voie'])
                  .bar(subset= ['count_num_voie',
                                               'count_siege',
                                               'count_enseigne',
                                               'count_date',
                                               'count_admin',
                                               'count_code_commune',
                                               'count_type_voie'],
                       color='#d65f5f')
     )
    
    return test_1
```

```python
create_table_test_not_false(cas = "CAS_1")
```

```python
create_table_test_not_false(cas = "CAS_2")
```

```python
create_table_test_not_false(cas = "CAS_3")
```

```python
create_table_test_not_false(cas = "CAS_4")
```

```python
create_table_test_not_false(cas = "CAS_5")
```

```python
create_table_test_not_false(cas = "CAS_6")
```

```python
create_table_test_not_false(cas = "CAS_7")
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
