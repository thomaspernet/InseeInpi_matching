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
# Test similarite exception list mots INSEE et INPI siretisation

Objective(s)

*  L’objectif de cette tache est de trouver une solution pour retourner la distance donnée par le Word2Vec entre 2 listes contenant des mots qui ne sont pas communs dans l’adresse INSEE et INPI
* Il faut faire le test lorsque la variable status_cas est egal a CAS_5,6 ou 7
* Par exemple:
    * inpi_except: [A, B]
    * insee_except: [A,C]
    * Le test: [[A,A], [A,C], [B,A],[B,C]]
    * Output: [p1, p2, p3, p4]
    * Recupération max list output
    * Variables nécéssaire:
        * inpi_except 
        * insee_except 
        * status_cas

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
  *  14
* Task tag
  *  #sql-query,#matching,#siretisation,#machine-learning,#word2vec
* Toggl Tag
  * #poc
  
## Input Cloud Storage [AWS/GCP]

If link from the internet, save it to the cloud first

### Tables [AWS/BigQuery]

1. Batch 1:
    * Select Provider: Athena
      * Select table(s): ets_inpi_insee_cases
        * Select only tables created from the same notebook, else copy/paste selection to add new input tables
        * If table(s) does not exist, add them: Add New Table
        * Information:
          * Region: 
            * NameEurope (Paris)
            * Code: eu-west-3
          * Database: inpi
          * Notebook construction file: [07_pourcentage_siretisation_v3](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/02_siretisation/07_pourcentage_siretisation_v3.md)
    
## Destination Output/Delivery

* Athena: 
    * Region: Europe (Paris)
    * Database: inpi
    * Tables (Add name new table): ets_inpi_inse_wordvec

  
## Things to know (Steps, Attention points or new flow of information)

### Sources of information  (meeting notes, Documentation, Query, URL)

1. Jupyter Notebook (Github Link)
  1. md : [Test_word2Vec.md](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/02_siretisation/Test_word2Vec.md)
<!-- #endregion -->

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
drop_table = False
if drop_table:
    output = athena.run_query(
        query="DROP TABLE `ets_inpi_insee_cases`;",
        database='inpi',
        s3_output='INPI/sql_output'
    )
```

```python
create_table = """
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
     REGEXP_REPLACE(
  NORMALIZE(
  enseigne, 
            NFD
          ), 
          '\pM', 
          ''
        ) AS enseigne,
  enseigne1etablissement, enseigne2etablissement, enseigne3etablissement, 
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
  ),REGEXP_REPLACE(
  NORMALIZE(
  enseigne, 
            NFD
          ), 
          '\pM', 
          ''
        )
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
  WHEN (intersection_numero_voie IS NULL OR union_numero_voie IS NULL) THEN 'NULL'
  ELSE 'False' END AS test_list_num_voie,
  
  datecreationetablissement, 
  date_debut_activite, 
  
  CASE WHEN datecreationetablissement = date_debut_activite THEN 'True' 
  WHEN datecreationetablissement IS NULL 
  OR date_debut_activite IS NULL  THEN 'NULL'
  --WHEN datecreationetablissement = '' 
  --OR date_debut_activite = ''   THEN 'NULL'
  ELSE 'False' 
  END AS test_date, 
  
  etatadministratifetablissement, 
  status_admin, 
  
  CASE WHEN etatadministratifetablissement = status_admin THEN 'True' 
  WHEN etatadministratifetablissement IS NULL 
  OR status_admin IS NULL  THEN 'NULL'
  WHEN etatadministratifetablissement = '' 
  OR status_admin = '' THEN 'NULL'
  ELSE 'False'  
  END AS test_status_admin, 
  
  etablissementsiege, 
  status_ets, 
  
  CASE WHEN etablissementsiege = status_ets THEN 'True' 
  WHEN etablissementsiege IS NULL 
  OR status_ets IS NULL  THEN 'NULL'
  WHEN etablissementsiege = '' 
  OR status_ets = ''   THEN 'NULL'
  ELSE 'False'  
  END AS test_siege, 
  
  codecommuneetablissement, 
  code_commune, 
  
  CASE WHEN codecommuneetablissement = code_commune THEN 'True' 
  WHEN codecommuneetablissement IS NULL 
  OR code_commune IS NULL  THEN 'NULL'
  WHEN codecommuneetablissement = '' 
  OR code_commune = ''   THEN 'NULL'
  ELSE 'False'  
  END AS test_code_commune, 
  
  codepostaletablissement, 
  code_postal_matching, 
  numerovoieetablissement, 
  numero_voie_matching, 
  
  CASE WHEN numerovoieetablissement = numero_voie_matching THEN 'True' 
  WHEN numerovoieetablissement IS NULL 
  OR numero_voie_matching IS NULL  THEN 'NULL'
  WHEN numerovoieetablissement = '' 
  OR numero_voie_matching = ''   THEN 'NULL'
  ELSE 'False'  
  END AS test_numero_voie, 
  
  typevoieetablissement, 
  type_voie_matching, 
  
  CASE WHEN typevoieetablissement = type_voie_matching THEN 'True' 
  WHEN typevoieetablissement IS NULL 
  OR type_voie_matching IS NULL  THEN 'NULL'
  WHEN typevoieetablissement = '' 
  OR type_voie_matching = ''   THEN 'NULL'
  ELSE 'False'  
  END AS test_type_voie, 
  
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
  CASE WHEN cardinality(test) = 0 THEN 'NULL'
WHEN enseigne = '' THEN 'NULL'
WHEN temp_test_enseigne = TRUE THEN 'True'
ELSE 'False' END AS test_enseigne 
  
FROM 
  test_proba
"""
output = athena.run_query(
        query=create_table,
        database='inpi',
        s3_output='INPI/sql_output'
    )
```

# Create table par cas


## Creation functions

La fonction ci dessous va générer le tableau d'analayse via une query, et retourne un dataframe Pandas, tout en stockant le resultat dans le dossier suivant:

- [calfdata/TEMP_ANALYSE_SIRETISATION/INDEX_20](https://s3.console.aws.amazon.com/s3/buckets/calfdata/TEMP_ANALYSE_SIRETISATION/INDEX_20/?region=eu-west-3&tab=overview)
- [calfdata/TEMP_ANALYSE_SIRETISATION/INDEX_20_TRUE](https://s3.console.aws.amazon.com/s3/buckets/calfdata/TEMP_ANALYSE_SIRETISATION/INDEX_20_TRUE/?region=eu-west-3&tab=overview)

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
a = range(1,10)
b = ["True", "False", "NULL"]



index = pd.MultiIndex.from_product([a, b], names = ["index_unique", "groups"])

df_ = (pd.DataFrame(index = index)
       .reset_index()
       .sort_values(by = ["index_unique", "groups"])
       .to_csv('index_20_true.csv', index = False)
      )

s3.upload_file(file_to_upload = 'index_20_true.csv',
            destination_in_s3 = 'TEMP_ANALYSE_SIRETISATION/INDEX_20_TRUE')
```

```python
create_table = """
CREATE EXTERNAL TABLE IF NOT EXISTS inpi.index_20_true (
`index_unique`                     integer,
`groups`                     string

    )
     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
   'separatorChar' = ',',
   'quoteChar' = '"'
   )
     LOCATION 's3://calfdata/TEMP_ANALYSE_SIRETISATION/INDEX_20_TRUE'
     TBLPROPERTIES ('has_encrypted_data'='false',
              'skip.header.line.count'='1');"""
output = athena.run_query(
        query=create_table,
        database='inpi',
        s3_output='INPI/sql_output'
    )
```

### Fonctions

```python
def create_table_test_not_false(cas = "CAS_1"):
    """
    
    """
    top = """
    SELECT count_test_list_num_voie.status_cas,
    nb_unique_index, 
    index_unique,
    count_cas,
    test_list_num_voie,
    test_siege,
    test_enseigne,
    test_date, 
    test_status_admin,
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
    SELECT status_cas,count_index,  count(count_index) AS {1}
    FROM (
    SELECT status_cas, index_id, COUNT(test_enseigne) as count_index
    FROM ets_inpi_insee_cases 
    WHERE status_cas = '{0}' AND  {1} != 'False'
    GROUP BY status_cas, index_id
      ) as c
      GROUP BY status_cas, count_index
      ORDER BY count_index
      ) AS count_{1}
      ON index_20.index_unique = count_{1}.count_index 
    """
    
    bottom = """
    LEFT JOIN (
    SELECT  DISTINCT(status_cas), COUNT(DISTINCT(index_id)) as nb_unique_index
    FROM ets_inpi_insee_cases 
    WHERE status_cas = '{0}' 
    GROUP BY status_cas
    ) as index_unique
    ON index_unique.status_cas = count_test_list_num_voie.status_cas
    ORDER BY index_unique
    """.format(cas)

    for i, table in enumerate(["test_list_num_voie",
              "test_siege",
              "test_enseigne",
              "test_date", "test_status_admin", "test_code_commune", "test_type_voie"]):

        top += query.format(cas, table)
    top += bottom
    
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
    reindex= ['status_cas','nb_unique_index', 'index_unique','count_cas',
              'test_list_num_voie',
              'count_num_voie',
              'test_siege',
              'count_siege',
           'test_enseigne',
               'count_enseigne',
              'test_date',
               'count_date',
              'test_status_admin',
              'count_admin',
              'test_code_commune',
              'count_code_commune',
           'test_type_voie',
              'count_type_voie']
    test_1 = (s3.read_df_from_s3(
            key = 'ANALYSE_PRE_SIRETISATION/{}'.format(filename), sep = ',')
             )
    
    df_ = (
        test_1
     .assign(

         count_num_voie = lambda x: x['test_list_num_voie'] /  index_unique_inpi,
         count_siege = lambda x: x['test_siege'] /  index_unique_inpi,
         count_enseigne	 = lambda x: x['test_enseigne'] /  index_unique_inpi,
         count_date = lambda x: x['test_date'] /  index_unique_inpi,
         count_admin = lambda x: x['test_status_admin'] /  index_unique_inpi,
         count_code_commune = lambda x: x['test_code_commune'] /  index_unique_inpi,
         count_type_voie = lambda x: x['test_type_voie'] /  index_unique_inpi,
         status_cas = lambda x: x['status_cas'].fillna(method='ffill'),
         nb_unique_index = lambda x: x['nb_unique_index'].fillna(method='ffill')
     )
     .reindex(columns = reindex)
     .fillna(0)
                  .style
                  .format("{:,.0f}", subset =  [
                      "nb_unique_index",
                      "count_cas",
                      'test_list_num_voie',
                                                'test_siege',
                                                'test_enseigne',
                                                'test_date',
                                                'test_status_admin',
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
    
    unique_1 = test_1.loc[lambda x: x['index_unique'].isin([1])]
    dic_ = {
    
    'nb_index_unique_{}'.format(cas): int(unique_1['nb_unique_index'].values[0]),
     'index_unique_inpi':index_unique_inpi,   
    'lignes_matches': {   
        'lignes_matche_list_num': int(unique_1['test_list_num_voie'].values[0]),
    'lignes_matche_list_num_pct': unique_1['test_list_num_voie'].values[0] / index_unique_inpi
    },    
    'lignes_a_trouver': {
        'test_list_num_voie':[
            int((unique_1['nb_unique_index'].values - unique_1['test_list_num_voie'].values)[0]),
            (unique_1['test_list_num_voie'].values / unique_1['nb_unique_index'].values)[0]
        ],
        'test_siege':[
            int((unique_1['nb_unique_index'].values - unique_1['test_siege'].values)[0]),
            (unique_1['test_siege'].values / unique_1['nb_unique_index'].values)[0]
        ],
        'test_enseigne':[
            int((unique_1['nb_unique_index'].values - unique_1['test_enseigne'].values)[0]),
            (unique_1['test_enseigne'].values / unique_1['nb_unique_index'].values)[0]
        ],
        'test_date':[
            int((unique_1['nb_unique_index'].values - unique_1['test_date'].values)[0]),
            (unique_1['test_date'].values / unique_1['nb_unique_index'].values)[0]
        ],
        'status_admin':[
            int((unique_1['nb_unique_index'].values - unique_1['test_status_admin'].values)[0]),
            (unique_1['test_status_admin'].values / unique_1['nb_unique_index'].values)[0]
        ],
        'test_code_commune':[
            int((unique_1['nb_unique_index'].values - unique_1['test_code_commune'].values)[0]),
            (unique_1['test_code_commune'].values / unique_1['nb_unique_index'].values)[0]
        ],
        'test_type_voie':[
            int((unique_1['nb_unique_index'].values - unique_1['test_type_voie'].values)[0]),
            (unique_1['test_type_voie'].values / unique_1['nb_unique_index'].values)[0]
        ],
    }
}
    
    return test_1, dic_
```

```python
def table_list_num_other_tests(cas = 'CAS_1'):
    """
    """
    top = """
    SELECT 
    count_test_siege.status_cas,
    index_unique, 
    groups, 
    cnt_test_list_num_voie,
    cnt_test_siege,
    cnt_test_enseigne,
    cnt_test_date, 
    cnt_test_status_admin,
    cnt_test_code_commune,
    cnt_test_type_voie
    FROM index_20_true 
    """
    
    query = """
    -- {0}
    LEFT JOIN (
    SELECT status_cas, count_index,{0}, COUNT(index_id) as cnt_{0}
    FROM (
    SELECT ets_inpi_insee_cases.status_cas, count_index, ets_inpi_insee_cases.index_id, {0}
    FROM ets_inpi_insee_cases
    RIGHT JOIN (
    SELECT *
    FROM(
    SELECT status_cas, index_id, COUNT(index_id) as count_index
    FROM ets_inpi_insee_cases 
    WHERE status_cas = '{1}' AND  test_list_num_voie != 'False'
    GROUP BY status_cas, index_id
  )
  ) as index_
  ON ets_inpi_insee_cases.status_cas = index_.status_cas AND
  ets_inpi_insee_cases.index_id = index_.index_id
  WHERE ets_inpi_insee_cases.status_cas = '{1}' AND  test_list_num_voie != 'False'
  ) 
  GROUP BY status_cas, count_index, {0}
  ) as count_{0}
  ON index_20_true.index_unique = count_{0}.count_index AND
  index_20_true.groups = count_{0}.{0}
 
    """
    
    bottom =   """ORDER BY index_unique, groups"""
    for i, table in enumerate(["test_list_num_voie",
              "test_siege",
              "test_enseigne",
              "test_date", "test_status_admin", "test_code_commune", "test_type_voie"]):

        top += query.format(table, cas)
    top += bottom
    ### run query
    output = athena.run_query(
        query=top,
        database='inpi',
        s3_output='INPI/sql_output'
    )

    results = False
    filename = 'table_{}_num_voie_test_not_false.csv'.format(cas)
    
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
    reindex= ['status_cas',
          'index_unique',
          'groups',
              "total_rows",
              'cnt_test_list_num_voie',
              'count_list_num_voie',
              'cnt_test_siege',
              'count_siege',
           'cnt_test_enseigne',
               'count_enseigne',
              'cnt_test_date',
               'count_date',
              'cnt_test_status_admin',
              'count_admin',
              'cnt_test_code_commune',
              'count_code_commune',
           'cnt_test_type_voie',
              'count_type_voie']

    test_1 = (s3.read_df_from_s3(
            key = 'ANALYSE_PRE_SIRETISATION/{}'.format(filename), sep = ',')
          .assign(
         total_rows = lambda x: x['cnt_test_siege'].groupby(x['index_unique']).transform('sum'),
         count_list_num_voie = lambda x: x['cnt_test_list_num_voie'] /  x['total_rows'],
         count_siege = lambda x: x['cnt_test_siege'] /  x['total_rows'],
         count_enseigne	 = lambda x: x['cnt_test_enseigne'] /  x['total_rows'],
         count_date = lambda x: x['cnt_test_date'] /  x['total_rows'],
         count_admin = lambda x: x['cnt_test_status_admin'] /  x['total_rows'],
         count_code_commune = lambda x: x['cnt_test_code_commune'] /  x['total_rows'],
         count_type_voie = lambda x: x['cnt_test_type_voie'] /  x['total_rows'],
         status_cas = lambda x: x['status_cas'].fillna(method='ffill'),
         groups = lambda x: x['groups'].fillna('Null')
          )
          .reindex(columns = reindex)
          .fillna(0)
          .style
                  .format("{:,.0f}", subset =  ['total_rows',
                                                'cnt_test_list_num_voie',
                                                'cnt_test_siege',
                                                'cnt_test_enseigne',
                                                'cnt_test_date',
                                                'cnt_test_status_admin',
                                                'cnt_test_code_commune',
                                                'cnt_test_type_voie'])
                  .format("{:.2%}", subset =  ['count_list_num_voie',
                                               'count_siege',
                                               'count_enseigne',
                                               'count_date',
                                               'count_admin',
                                               'count_code_commune',
                                               'count_type_voie'])
                  .bar(subset= ['count_list_num_voie',
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
def filter_list_num_test_false(cas = 'CAS_1',test = 'test_type_voie'):
    """
    """
    
    to_append = """count_initial_insee, index_id, sequence_id, siren, siret,
             list_inpi, list_insee,etablissementsiege, status_ets,
             enseigne, enseigne1etablissement, enseigne2etablissement,
             enseigne3etablissement, datecreationetablissement,
             date_debut_activite, etatadministratifetablissement, status_admin,
             typevoieetablissement, type_voie_matching"""

    for i, value in enumerate(["test_siege", "test_enseigne", "test_date", "test_status_admin", "test_type_voie"]):
        if value not in [test]:
            to_append += ",{}".format(value) 
    
    query = """
    SELECT  

count_initial_insee,filter_a.index_id, sequence_id, siren, siret,list_inpi, list_insee,
etablissementsiege, status_ets, test_siege, 
enseigne, enseigne1etablissement, enseigne2etablissement, enseigne3etablissement, test_enseigne, 
datecreationetablissement, date_debut_activite, test_date, 
etatadministratifetablissement, status_admin, test_status_admin, 
test_type_voie, typevoieetablissement, type_voie_matching 

    FROM (
    SELECT ets_inpi_insee_cases.status_cas, count_index, ets_inpi_insee_cases.index_id, {1}
    FROM ets_inpi_insee_cases
    RIGHT JOIN (
    SELECT *
    FROM(
    SELECT status_cas, index_id, COUNT(index_id) as count_index
    FROM ets_inpi_insee_cases 
    WHERE status_cas = '{0}' AND  test_list_num_voie != 'False'
    GROUP BY status_cas, index_id
  )
      WHERE count_index = 1
  ) as index_
  ON ets_inpi_insee_cases.status_cas = index_.status_cas AND
  ets_inpi_insee_cases.index_id = index_.index_id
  WHERE ets_inpi_insee_cases.status_cas = '{0}' AND  test_list_num_voie != 'False'
  ) as filter_a
  
  LEFT JOIN (
    
    SELECT {2}
    
    FROM ets_inpi_insee_cases
    WHERE ets_inpi_insee_cases.status_cas = '{0}' AND  test_list_num_voie != 'False'
    ) as filter_b
    ON filter_a.index_id = filter_b.index_id
    WHERE {1} = 'False'
    LIMIT 10
    """
    #print(query.format(cas, test,to_append))
    output = athena.run_query(
        query=query.format(cas, test,to_append),
        database='inpi',
        s3_output='INPI/sql_output'
    )

    results = False
    filename = 'table_{0}_{1}_example_filter.csv'.format(cas, test)
    
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
    
    test_1 = (s3.read_df_from_s3(
            key = 'ANALYSE_PRE_SIRETISATION/{}'.format(filename), sep = ',')
             )
    
    return test_1
    
    
    
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


## Nombre ets par cas

```python
query = """
SELECT status_cas, COUNT(*) as count
FROM ets_inpi_insee_cases 
GROUP BY status_cas
"""
```

## Nombre etb unique INSEE par cas

```python
query = """
SELECT status_cas, COUNT(DISTINCT(index_id)) as distinct_ets
FROM ets_inpi_insee_cases 
GROUP BY status_cas
ORDER BY status_cas
"""
```

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

# Anayse cas

Explication:

- Dictionnaire:
    - 

- Table 1:
    - nb_unique_index: Nombre d'index unique pour un cas donnée. Ex. Il y a 7,584,503 index unique pour la cas 1
    - index_unique: . Possibilité de duplicate allant 1 (aucun duplicate) a 20. Si supérieur à 1, cela indique le nombre de lignes ayant 2,3,4 etc doublons
    - count_cas: Compte le nombre de duplicate par cas et index_unique. Par exemple, le cas 1 possède 128,821 lignes avec deux doublons pour un index donnée
    - `test_*`: Nombre de lignes ayant un result de test différent de false, pour chaqun des duplicates. par exemple, il y a 7,471,838 lignes ayant passées le test test_list_num_voie et n'ayant aucun duplicate.
    - `count_*`: test_* / nb_unique_index. Informe du pourcentage de lignes ayant un test concluant sur le nombre d'index unique. Se référé à la ligne 0.
- Table 2:
    - index_unique: Idem que index_unique
    - groups: Possibilité des résultats des tests -> True, False, NULL. NULL si aucune info dans les variables pour faire le test
    - total_rows: Nombre de lignes ayant réussi le test test_list_num_voie. Le chiffre doit correspondre à test_list_num_voie, ligne 0
    - `cnt_test_*`: Nombre de lignes ayant résussi le test test_list_num_voie, puis décomposé par résultat pour chaque test. Par exemple, il y a 3,037,959 lignes parmi les 7,471,838 lignes n'ayant pas de duplicates qui ont un test_siege egal à True.
    - `count_*`: cnt_test_* / total_rows. Pourcentage de lignes par décomposition des tests sur le nombre de lignes ayant réussi le test test_list_num_voie, décomposé par duplicate.
    


## Cas 01: similarité parfaite

* Definition: Les mots dans l’adresse de l’INPI sont égales aux mots dans l’adresse de l’INSEE
- Math definition: $\frac{|INSEE \cap INPI|}{|INSEE|+|INPI|-|INSEE \cap INPI|} =1$
- Règle: $ \text{intersection} = \text{union} \rightarrow \text{cas 1}$
* Query [case 1](https://eu-west-3.console.aws.amazon.com/athena/home?region=eu-west-3#query/history/24e58c22-4a67-4a9e-b98d-4eb9d65e7f27)

| list_inpi              | list_insee             | insee_except | intersection | union_ |
|------------------------|------------------------|--------------|--------------|--------|
| [BOULEVARD, HAUSSMANN] | [BOULEVARD, HAUSSMANN] | []           | 2            | 2      |
| [QUAI, GABUT]          | [QUAI, GABUT]          | []           | 2            | 2      |
| [BOULEVARD, VOLTAIRE]  | [BOULEVARD, VOLTAIRE]  | []           | 2            | 2      |

```python
tb1, dic_tb1 = create_table_test_not_false(cas = "CAS_1")
```

```python
dic_tb1
```

```python
table_list_num_other_tests(cas = 'CAS_1')
```

```python
pd.set_option('display.max_columns', None) 
```

```python
filter_list_num_test_false(cas = 'CAS_1',test = 'test_enseigne')
```

## CAS 03: Intersection parfaite INPI

* Definition:  Tous les mots dans l’adresse de l’INPI  sont contenus dans l’adresse de l’INSEE
* Math définition: $\frac{|INPI|}{|INSEE \cap INPI|}  \text{  = 1 and }|INSEE \cap INPI| <> |INSEE \cup INPI|$
* Query [case 3](https://eu-west-3.console.aws.amazon.com/athena/home?region=eu-west-3#query/history/7fb420a1-5f50-4256-a2ba-b8c7c2b63c9b)
* Règle: $|\text{list_inpi}|= \text{intersection}  \text{  = 1 and }\text{intersection} \neq  \text{union} \rightarrow \text{cas 3}$

```python
tb3, dic_tb3 = create_table_test_not_false(cas = "CAS_3")
```

```python
dic_tb3
```

```python
tb3
```

```python
table_list_num_other_tests(cas = 'CAS_3')
```

```python
filter_list_num_test_false(cas = 'CAS_3',test = 'test_type_voie')
```

## CAS 04: Intersection parfaite INSEE

* Definition:  Tous les mots dans l’adresse de l’INSEE  sont contenus dans l’adresse de l’INPI
* Math definition: $\frac{|INSEE|}{|INSEE \cap INPI|}  \text{  = 1 and }|INSEE \cap INPI| <> |INSEE \cup INPI|$
* Query [case 4](https://eu-west-3.console.aws.amazon.com/athena/home?region=eu-west-3#query/history/65344bf4-8999-4ddb-a65e-11bb825f5f40)
* Règle: $|\text{list_insee}|= \text{intersection}  \text{  = 1 and }\text{intersection} \neq  \text{union} \rightarrow \text{cas 4}$

| list_inpi                                                 | list_insee                                      | insee_except | intersection | union_ |
|-----------------------------------------------------------|-------------------------------------------------|--------------|--------------|--------|
| [ROUTE, D, ENGHIEN]                                       | [ROUTE, ENGHIEN]                                | []           | 2            | 3      |
| [ZAC, PARC, D, ACTIVITE, PARIS, EST, ALLEE, LECH, WALESA] | [ALLEE, LECH, WALESA, ZAC, PARC, ACTIVITE, EST] | []           | 7            | 9      |
| [LIEU, DIT, PADER, QUARTIER, RIBERE]                      | [LIEU, DIT, RIBERE]                             | []           | 3            | 5      |
| [A, BOULEVARD, CONSTANTIN, DESCAT]                        | [BOULEVARD, CONSTANTIN, DESCAT]                 | []           | 3            | 4      |
| [RUE, MENILMONTANT, BP]                                   | [RUE, MENILMONTANT]                             | []           | 2            | 3      |


```python
tb4, dic_tb4 = create_table_test_not_false(cas = "CAS_4")
```

```python
dic_tb4
```

```python
tb4
```

```python
table_list_num_other_tests(cas = 'CAS_4')
```

```python
filter_list_num_test_false(cas = 'CAS_4',test = 'test_type_voie')
```

## CAS 05: Cardinality exception parfaite INSEE INPI, intersection positive

* Definition:  L’adresse de l’INPI contient des mots de l’adresse de l’INSEE et la cardinality des mots non présents dans les deux adresses est équivalente
* Math definition: $|INPI|-|INPI \cap INSEE| = |INSEE|-|INPI \cap INSEE|$
* Query [case 5](https://eu-west-3.console.aws.amazon.com/athena/home?region=eu-west-3#query/history/fec67222-3a7b-4bfb-af20-dd70d82932e3)
* Règle: $|\text{insee_except}| = |\text{inpi_except}| \text{ and } \text{intersection} > 0 \rightarrow \text{cas 5}$

```python
tb5, dic_tb5 = create_table_test_not_false(cas = "CAS_5")
```

```python
dic_tb5
```

```python
tb5
```

```python
table_list_num_other_tests(cas = 'CAS_5')
```

```python
filter_list_num_test_false(cas = 'CAS_5',test = 'test_type_voie')
```

## CAS 06: Cardinality exception INSEE supérieure INPI, intersection positive 

* Definition:  L’adresse de l’INPI contient des mots de l’adresse de l’INSEE et la cardinality des mots non présents dans l’adresse de l’INSEE est supérieure à la cardinality de l’adresse de l’INPI
* Math definition: $|INPI|-|INPI \cap INSEE| < |INSEE|-|INPI \cap INSEE|$
* Query [case 6](https://eu-west-3.console.aws.amazon.com/athena/home?region=eu-west-3#query/history/9bdce567-5871-4a5a-add4-d5cca6a83528)
* Règle: $|\text{insee_except}| > |\text{inpi_except}| \text{ and } \text{intersection} > 0 \rightarrow \text{cas 6}$

```python
tb6, dic_tb6 = create_table_test_not_false(cas = "CAS_6")
```

```python
dic_tb6
```

```python
tb6
```

```python
table_list_num_other_tests(cas = 'CAS_6')
```

```python
filter_list_num_test_false(cas = 'CAS_6',test = 'test_type_voie')
```

## CAS 07: Cardinality exception INPI supérieure INSEE, intersection positive 

* Definition:  L’adresse de l’INSEE contient des mots de l’adresse de l’INPI et la cardinality des mots non présents dans l’adresse de l’INPI est supérieure à la cardinality de l’adresse de l’INSEE
* Math definition: $|INPI|-|INPI \cap INSEE| > |INSEE|-|INPI \cap INSEE|$
* Règle: $|\text{insee_except}| < |\text{inpi_except}| \text{ and } \text{intersection} > 0 \rightarrow \text{cas 7}$

```python
tb7, dic_tb7 = create_table_test_not_false(cas = "CAS_7")
```

```python
dic_tb7
```

```python
tb7
```

```python
table_list_num_other_tests(cas = 'CAS_7')
```

```python
filter_list_num_test_false(cas = 'CAS_7',test = 'test_type_voie')
```

## Resume tests

La différence du nombre d'observation vient du cas numéro 2, ou les siren ont été matché mais aucune des deux adresses ne correspond

```python
nb_to_find = {
    'cas':[],
    'lignes_matche_list_num':[],
    'to_find':[],
    'lignes_matche_list_num_pct': [],
    
}

for d, value in enumerate([dic_tb1,dic_tb3,dic_tb4,dic_tb5,dic_tb6,dic_tb7]):
    cas = d + 1
    if d >= 1:
        cas = d + 2
    nb_to_find['cas'].append(cas)
    nb_to_find['to_find'].append(value['lignes_a_trouver']['test_list_num_voie'][0]),
    nb_to_find['lignes_matche_list_num'].append(value['lignes_matches']['lignes_matche_list_num']),
    nb_to_find['lignes_matche_list_num_pct'].append(value['lignes_matches']['lignes_matche_list_num_pct'])
    
reindex = ["cas",
           "lignes_matche_list_num", "lignes_matche_list_num_pct", "cum_sum_matche","cum_sum_matche_pct",
           "to_find","to_find_pct", "cum_sum_to_find", "cum_sum_to_find_pct"
          ]
    
(pd.DataFrame(nb_to_find).assign(
    cum_sum_to_find = lambda x: x['to_find'].cumsum(),
    cum_sum_matche = lambda x: x['lignes_matche_list_num'].cumsum(),
    cum_sum_matche_pct = lambda x: x['lignes_matche_list_num_pct'].cumsum(),
    to_find_pct = lambda x:  x['to_find']/x['to_find'].sum(),
    cum_sum_to_find_pct = lambda x: x['cum_sum_to_find']/x['to_find'].sum(),
    #cum_sum_to_find_pct = lambda x: x['pct_total'].cumsum(),
    #cum_sum_pct_inverse = lambda x: 1-x['pct_total'].cumsum(),
    #cum_pct_match = lambda x: x['pct_match'].cumsum(),
    
)
 .reindex(columns  = reindex)
 .style
 .format("{:.2%}", subset =  ['lignes_matche_list_num_pct', 'cum_sum_matche_pct', 'to_find_pct',
                              'cum_sum_to_find_pct'])
 .format("{:,.0f}", subset =  ['lignes_matche_list_num','cum_sum_matche', 'to_find', 'cum_sum_to_find'])
 .bar(subset= ['lignes_matche_list_num_pct','to_find_pct'], color='#d65f5f')
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
