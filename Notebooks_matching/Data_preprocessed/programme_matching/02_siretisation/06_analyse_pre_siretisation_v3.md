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

# Analyse divergence adresse INPI INSEE

Objective(s)

* Création d’une table d’analyse rapprochant la table préparée de l’INPI et la table préparée de l’INSEE
* Analyse des similarités/dissimilarités entre l’adresse de l’INPI et de l’INSEE
* L’analyse comporte le compte du nombre d’observations impactant chacun des cas (1 a 7)
* L’analyse doit comporter pour chacun des cas, l’analyse des divergences en prenant en compte les informations complémentaires de la base de données, a savoir:
  * Information sur établissement
    * Date de création
      * datecreationetablissement 
      * date_début_activité 
    * Etablissement ouvert/fermé
      * etatadministratifetablissement 
      * status_admin 
    * Etablissement Siege 
      * etablissementsiege 
      * status_ets 
  * Information sur l’adresse
    * Code commune
      * codecommuneetablissement 
      * code_commune  
    * Code commune
      * codecommuneetablissement 
      * code_commune  
    * Adresse 
      * Numéro de voie
        * numerovoieetablissement 
        * numero_voie_matching 
      * Type de voie
        * typevoieetablissement 
        * type_voie_matching 
* L’analyse doit aussi comporter le nombre de doublon (les lignes index_id  par cas)
  * Pour un cas donnée
  * Pour un cas et test donnée
* L’analyse doit aussi comporter le nombre de doublon (les sequences sequence_id  par cas)
  * Pour un cas donnée
  * Pour un cas et test donnée  

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
  *  10
* Task tag
  *  #sql-query,#probability,#matching
* Toggl Tag
  * #datanalaysis
* Instance [AWS/GCP]
  *  
  
## Input Cloud Storage [AWS/GCP]

If link from the internet, save it to the cloud first

### AWS

1. S3
  * File (csv/json) + name and link: 
    * 
    * Notebook construction file (data lineage, md link from Github) 
      * md :
      * py :
2. Athena 
  * Region: eu-west-3 
  * Database: inpi 
    * Table: ets_final_sql  
  * Notebook construction file (data lineage) 
    * md : https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/03_ETS_add_variables.md
  * Region: Europe (Paris)
  * Database: inpi 
    * Table: insee_final_sql  
  * Notebook construction file (data lineage) 
    * md : 04_ETS_add_variables_insee.md
    
## Destination Output/Delivery

1. Table/Data (AWS/GCP link)
  * Description expected outcome:
    *  La table rassemble l’INSEE et l’INPI et un ensemble de variables connexe pour distinguer les siret
  * AWS
    * Bucket:
      * Link
  * Athena: 
    * Region: Europe (Paris)
    * Database: inpi 
    *  Table:   ets_insee_inpi  
    
## Things to know (Steps, Attention points or new flow of information)

### Sources of information  (meeting notes, Documentation, Query, URL)

1. Other source [Name](link)
  * Source 1: Ensemble fonctions Presto a appliquer sur les arrays. Les fonctions uniques, distinct, intersect sont intéressantes dans notre cas de figure


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

# Functions

La fonction ci dessous va générer le tableau d'analayse via une query, et retourne un dataframe Pandas, tout en stockant le resultat dans le dossier suivant:

- [calfdata/Analyse_cas_similarite_adresse](https://s3.console.aws.amazon.com/s3/buckets/calfdata/Analyse_cas_similarite_adresse/?region=eu-west-3&tab=overview)

```python
a = ["True", "False", "NULL"]
b = range(1,20)


index = pd.MultiIndex.from_product([a, b], names = ["groups", "cnt_test"])

df_ = (pd.DataFrame(index = index)
       .reset_index()
       .sort_values(by = ["cnt_test", "groups"])
       .to_csv('cartesian_table.csv', index = False)
      )

s3.upload_file(file_to_upload = 'cartesian_table.csv',
            destination_in_s3 = 'Temp_table_analyse_similarite')
```

```python
create_table = """
CREATE EXTERNAL TABLE IF NOT EXISTS inpi.cartesian_table (
`groups`                     string,
`cnt_test`                   int
    )
     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
   'separatorChar' = ',',
   'quoteChar' = '"'
   )
     LOCATION 's3://calfdata/Temp_table_analyse_similarite'
     TBLPROPERTIES ('has_encrypted_data'='false',
              'skip.header.line.count'='1');"""
output = athena.run_query(
        query=create_table,
        database='inpi',
        s3_output='INPI/sql_output'
    )
```

## Compte nombre obs par cas

```python
query_count = """
WITH test_proba AS (
  SELECT 
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
    ) as union_ 
  FROM 
    "inpi"."ets_insee_inpi" -- limit 10
    ) 
SELECT 
 count(*) 
FROM 
  test_proba 
WHERE {}
"""

filter_ = ""
```

```python
def compte_obs_cas(case = 1):
    """
    """
    
    if case ==1:
        
        filter_= "intersection = union_"
        
    if case ==2:
        
        filter_= "intersection = 0"
    
    if case ==3:
        
        filter_= "lenght_list_inpi = intersection AND intersection != union_"
    
    if case ==4:
        
        filter_= "lenght_list_insee = intersection AND intersection != union_"
    
    if case ==5:
        
        filter_= "cardinality(insee_except) = cardinality(inpi_except) AND intersection != 0 AND cardinality(insee_except) > 0"
    
    if case ==6:
        
        filter_= "cardinality(insee_except) > cardinality(inpi_except) AND intersection != 0 AND cardinality(insee_except) > 0 AND cardinality(inpi_except) > 0"
    
    if case ==7:
        
        filter_= "cardinality(insee_except) < cardinality(inpi_except) AND intersection != 0 AND cardinality(insee_except) > 0 AND cardinality(inpi_except) > 0"
    
    query_ =query_count.format(filter_)
    
    output = athena.run_query(
        query=query_,
        database='inpi',
        s3_output='INPI/sql_output'
    )

    results = False
    
    filename = 'nb_obs_cas_{}.csv'.format(case)
    
    while results != True:
        source_key = "{}/{}.csv".format(
                            'INPI/sql_output',
                            output['QueryExecutionId']
                                   )
        destination_key = "{}/{}".format(
                                'Analyse_cas_similarite_adresse',
                                filename
                            )

        results = s3.copy_object_s3(
                                source_key = source_key,
                                destination_key = destination_key,
                                remove = True
                            )
        
    df_ = (s3.read_df_from_s3(
        key = 'Analyse_cas_similarite_adresse/{}'.format(filename), sep = ',')
          ).values[0][0]
    
    return df_
```

## Compte nombre duplicate par cas

```python
query_duplicate_cas = """
WITH test_proba AS (
  SELECT 
    {0}, 
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
    ) as inpi_except, 
    array_distinct(
      array_except(
        split(adresse_distance_inpi, ' '), 
        split(adresse_distance_insee, ' ')
      )
    ) as insee_except, 
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
    ) as union_ 
  FROM 
    "inpi"."ets_insee_inpi" -- limit 10
    ) 
SELECT 
  * 
FROM 
  (
    WITH tests AS (
      SELECT 
        {0}, 
        adresse_distance_inpi, 
        adresse_distance_insee, 
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
        CASE WHEN codepostaletablissement = code_postal_matching THEN 'True' WHEN codepostaletablissement = '' 
        OR code_postal_matching = '' THEN 'NULL' ELSE 'False' END AS test_code_postal, 
        numerovoieetablissement, 
        numero_voie_matching, 
        CASE WHEN numerovoieetablissement = numero_voie_matching THEN 'True' WHEN numerovoieetablissement = '' 
        OR numero_voie_matching = '' THEN 'NULL' ELSE 'False' END AS test_numero_voie, 
        typevoieetablissement, 
        type_voie_matching, 
        CASE WHEN typevoieetablissement = type_voie_matching THEN 'True' WHEN typevoieetablissement = '' 
        OR type_voie_matching = '' THEN 'NULL' ELSE 'False' END AS test_type_voie, 
        list_inpi, 
        list_insee, 
        inpi_except, 
        insee_except, 
        intersection, 
        union_ 
      FROM 
        test_proba 
      WHERE 
        {1}
    ) 
    SELECT 
      count_index_id, 
      COUNT(*) count_duplicate_index_id 
    FROM 
      (
        SELECT 
          {0}, 
          COUNT(*) AS count_index_id 
        FROM 
          tests 
        GROUP BY 
          {0}
      ) 
    GROUP BY 
      count_index_id -- WHERE test_type_voie = 'False'
      ) 
ORDER BY 
  count_index_id,
  count_duplicate_index_id DESC
"""
```

```python
def compte_dup_cas(var = 'index_id', case = 1):
    """
    """
    
    if case ==1:
        
        filter_= "intersection = union_"
        
    if case ==2:
        
        filter_= "intersection = 0"
    
    if case ==3:
        
        filter_= "lenght_list_inpi = intersection AND intersection != union_"
    
    if case ==4:
        
        filter_= "lenght_list_insee = intersection AND intersection != union_"
    
    if case ==5:
        
        filter_= "cardinality(insee_except) = cardinality(inpi_except) AND intersection != 0 AND cardinality(insee_except) > 0"
    
    if case ==6:
        
        filter_= "cardinality(insee_except) > cardinality(inpi_except) AND intersection != 0 AND cardinality(insee_except) > 0 AND cardinality(inpi_except) > 0"
    
    if case ==7:
        
        filter_= "cardinality(insee_except) < cardinality(inpi_except) AND intersection != 0 AND cardinality(insee_except) > 0 AND cardinality(inpi_except) > 0"
    
    query_ =query_duplicate_cas.format(var, filter_)
    
    output = athena.run_query(
        query=query_,
        database='inpi',
        s3_output='INPI/sql_output'
    )

    results = False
    
    filename = 'nb_dup_cas_{}.csv'.format(case)
    
    while results != True:
        source_key = "{}/{}.csv".format(
                            'INPI/sql_output',
                            output['QueryExecutionId']
                                   )
        destination_key = "{}/{}".format(
                                'Analyse_cas_similarite_adresse',
                                filename
                            )

        results = s3.copy_object_s3(
                                source_key = source_key,
                                destination_key = destination_key,
                                remove = True
                            )
        
    df_ = (s3.read_df_from_s3(
        key = 'Analyse_cas_similarite_adresse/{}'.format(filename), sep = ',')
           .assign(percentage = lambda x: x['count_duplicate_index_id'] / 
                  x['count_duplicate_index_id'].sum())
           .style
           .bar(subset= ['count_duplicate_index_id'],
                   color='#d65f5f')
           .format("{:.2%}", subset =  ['percentage'])
           .format("{:,.0f}", subset =  ['count_duplicate_index_id'])
           
          )
    
    return df_
```

## Compte nombre obs par cas et test

```python
query_count_case = """
WITH test_proba AS (
  SELECT 
  Coalesce(
         try(date_parse(datecreationetablissement, '%Y-%m-%d')),
         try(date_parse(datecreationetablissement, '%Y-%m-%d %hh:%mm:%ss.SSS')),
         try(date_parse(datecreationetablissement, '%Y-%m-%d %hh:%mm:%ss')),
         try(cast(datecreationetablissement as timestamp))
       )  as datecreationetablissement,

Coalesce(
         try(date_parse("date_début_activité", '%Y-%m-%d')),
         try(date_parse("date_début_activité", '%Y-%m-%d %hh:%mm:%ss.SSS')),
         try(date_parse("date_début_activité", '%Y-%m-%d %hh:%mm:%ss')),
         try(cast("date_début_activité" as timestamp))
  ) as date_debut_activite,
  etatadministratifetablissement, status_admin,
  etablissementsiege,status_ets,
  codecommuneetablissement, code_commune,
  codepostaletablissement, code_postal_matching,
  numerovoieetablissement, numero_voie_matching,
  typevoieetablissement, type_voie_matching,
  
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
            )as inpi_except, 
  array_distinct(
              array_except(
                split(adresse_distance_inpi, ' '), 
                split(adresse_distance_insee, ' ')
              )
            )as insee_except,
  
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
    ) as union_
  FROM "inpi"."ets_insee_inpi"-- limit 10
  )
  SELECT *
  FROM (WITH tests AS (
    SELECT 
  datecreationetablissement,date_debut_activite,
  CASE WHEN datecreationetablissement = date_debut_activite THEN 'True'
  WHEN datecreationetablissement IS NULL OR date_debut_activite IS NULL THEN 'NULL'
  ELSE 'False' END AS test_date,
  
  etatadministratifetablissement,status_admin,
  CASE WHEN etatadministratifetablissement = status_admin THEN 'True' 
  WHEN etatadministratifetablissement = '' OR status_admin = '' THEN 'NULL'
  ELSE 'False' END AS test_status_admin,
  
  etablissementsiege,status_ets,
  CASE WHEN etablissementsiege = status_ets THEN 'True' 
  WHEN etablissementsiege = '' OR status_ets = '' THEN 'NULL'
  ELSE 'False' END AS test_siege,
  
  codecommuneetablissement,code_commune,
  CASE WHEN codecommuneetablissement = code_commune THEN 'True' 
  WHEN codecommuneetablissement = '' OR code_commune = '' THEN 'NULL'
  ELSE 'False' END AS test_code_commune,
  
  codepostaletablissement,code_postal_matching,
  CASE WHEN codepostaletablissement = code_postal_matching THEN 'True' 
  WHEN codepostaletablissement = '' OR code_postal_matching = '' THEN 'NULL'
  ELSE 'False' END AS test_code_postal,
  
  numerovoieetablissement,numero_voie_matching,
  CASE WHEN numerovoieetablissement = numero_voie_matching THEN 'True' 
  WHEN numerovoieetablissement = '' OR numero_voie_matching = '' THEN 'NULL'
  ELSE 'False' END AS test_numero_voie,
  
  typevoieetablissement,type_voie_matching,
  CASE WHEN typevoieetablissement = type_voie_matching THEN 'True'
  WHEN typevoieetablissement = '' OR type_voie_matching = '' THEN 'NULL'
  ELSE 'False' END AS test_type_voie,
  
  list_inpi, list_insee, inpi_except, insee_except, intersection, union_
  
  FROM test_proba
  WHERE {}
    )
        
  SELECT DISTINCT(test_date) as groups,
        count_test_date,
        count_test_status_admin,
        count_test_siege,
        count_test_commune,
        count_test_cp,
        count_test_num_voie,
        count_test_type_voie
        
  FROM tests
  LEFT JOIN (
    SELECT test_date as groups,  count(*) as count_test_date
    FROM tests
    GROUP BY test_date
    ) as date_
  ON date_.groups = tests.test_date
        
  LEFT JOIN (
    SELECT test_status_admin as groups,  count(*) as count_test_status_admin
    FROM tests
    GROUP BY test_status_admin
    ) as admin_
  ON admin_.groups = tests.test_date      
        
LEFT JOIN (
    SELECT test_siege as groups,  count(*) as count_test_siege
    FROM tests
    GROUP BY test_siege
    ) as siege_
  ON siege_.groups = tests.test_date

LEFT JOIN (
      SELECT test_code_commune as groups,  count(*) as count_test_commune
      FROM tests
      GROUP BY test_code_commune
      ) as code_commune_
    ON code_commune_.groups = tests.test_date

LEFT JOIN (
        SELECT test_code_postal as groups,  count(*) as count_test_cp
        FROM tests
        GROUP BY test_code_postal
        ) as cp_
      ON cp_.groups = tests.test_date

LEFT JOIN (
          SELECT test_numero_voie as groups,  count(*) as count_test_num_voie
          FROM tests
          GROUP BY test_numero_voie
          ) as num_voie_
        ON num_voie_.groups = tests.test_date

LEFT JOIN (
            SELECT test_type_voie as groups,  count(*) as count_test_type_voie
            FROM tests
            GROUP BY test_type_voie
            ) as type_voie_
          ON type_voie_.groups = tests.test_date
 )
"""
```

```python
def generate_analytical_table(case = 1):
    """
    """
    if case ==1:
        
        filter_= "intersection = union_"
        
    if case ==2:
        
        filter_= "intersection = 0"
    
    if case ==3:
        
        filter_= "lenght_list_inpi = intersection AND intersection != union_"
    
    if case ==4:
        
        filter_= "lenght_list_insee = intersection AND intersection != union_"
    
    if case ==5:
        
        filter_= "cardinality(insee_except) = cardinality(inpi_except) AND intersection != 0 AND cardinality(insee_except) > 0"
    
    if case ==6:
        
        filter_= "cardinality(insee_except) > cardinality(inpi_except) AND intersection != 0 AND cardinality(insee_except) > 0 AND cardinality(inpi_except) > 0"
    
    if case ==7:
        
        filter_= "cardinality(insee_except) < cardinality(inpi_except) AND intersection != 0 AND cardinality(insee_except) > 0 AND cardinality(inpi_except) > 0"

    output = athena.run_query(
        query=query_count_case.format(filter_),
        database='inpi',
        s3_output='INPI/sql_output'
    )

    results = False
    filename = 'cas_{}.csv'.format(case)
    
    while results != True:
        source_key = "{}/{}.csv".format(
                            'INPI/sql_output',
                            output['QueryExecutionId']
                                   )
        destination_key = "{}/{}".format(
                                'Analyse_cas_similarite_adresse',
                                filename
                            )

        results = s3.copy_object_s3(
                                source_key = source_key,
                                destination_key = destination_key,
                                remove = True
                            )

    test_1 = (s3.read_df_from_s3(
        key = 'Analyse_cas_similarite_adresse/{}'.format(filename), sep = ',')
              .assign(test = 'cas_{}'.format(case),
                     count_test_date_pct = lambda x: x['count_test_date'] / x['count_test_date'].sum(),
                     count_test_status_admin_pct = lambda x: x['count_test_status_admin'] / x['count_test_status_admin'].sum(),
                     count_test_siege_pct = lambda x: x['count_test_siege'] / x['count_test_siege'].sum(),
                     count_test_commune_pct = lambda x: x['count_test_commune'] / x['count_test_commune'].sum(),
                     count_test_cp_pct = lambda x: x['count_test_cp'] / x['count_test_cp'].sum(),
                     count_test_num_voie_pct = lambda x: x['count_test_num_voie'] / x['count_test_num_voie'].sum(),
                     count_test_type_voie_pct = lambda x: x['count_test_type_voie'] / x['count_test_type_voie'].sum(),
                     )

              .replace({'groups' :{np.nan: 'Null'}})
              #.set_index(['test'])
              .reindex(columns = [
                  'test',
                  'groups',
                   'count_test_num_voie','count_test_num_voie_pct',
                  'count_test_type_voie','count_test_type_voie_pct',
                  'count_test_commune','count_test_commune_pct',
                  'count_test_date','count_test_date_pct',
                  'count_test_status_admin','count_test_status_admin_pct',
                  'count_test_siege','count_test_siege_pct',
                  
                  'count_test_cp','count_test_cp_pct',
                 
              ])
              .fillna(0)
              .style
              .format("{:,.0f}", subset =  ['count_test_date',
                                            'count_test_status_admin',
                                            'count_test_siege',
                                            'count_test_commune',
                                            'count_test_cp',
                                            'count_test_num_voie',
                                            'count_test_type_voie'])
              .format("{:.2%}", subset =  ['count_test_date_pct',
                                           'count_test_status_admin_pct',
                                           'count_test_siege_pct',
                                           'count_test_commune_pct',
                                           'count_test_cp_pct',
                                           'count_test_num_voie_pct',
                                           'count_test_type_voie_pct'])
              .bar(subset= ['count_test_date',
                                            'count_test_status_admin',
                                            'count_test_siege',
                                            'count_test_commune',
                                            'count_test_cp',
                                            'count_test_num_voie',
                                            'count_test_type_voie'],
                   color='#d65f5f')
              #.unstack(0)
             )    
    
    return test_1
```

## Compte nombre duplicate par cas et test

```python
query_dup_cas = """
WITH test_proba AS (
  SELECT 
  {0},
  Coalesce(
         try(date_parse(datecreationetablissement, '%Y-%m-%d')),
         try(date_parse(datecreationetablissement, '%Y-%m-%d %hh:%mm:%ss.SSS')),
         try(date_parse(datecreationetablissement, '%Y-%m-%d %hh:%mm:%ss')),
         try(cast(datecreationetablissement as timestamp))
       )  as datecreationetablissement,

Coalesce(
         try(date_parse("date_début_activité", '%Y-%m-%d')),
         try(date_parse("date_début_activité", '%Y-%m-%d %hh:%mm:%ss.SSS')),
         try(date_parse("date_début_activité", '%Y-%m-%d %hh:%mm:%ss')),
         try(cast("date_début_activité" as timestamp))
  ) as date_debut_activite,
  etatadministratifetablissement, status_admin,
  etablissementsiege,status_ets,
  codecommuneetablissement, code_commune,
  codepostaletablissement, code_postal_matching,
  numerovoieetablissement, numero_voie_matching,
  typevoieetablissement, type_voie_matching,
  
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
            )as inpi_except, 
  array_distinct(
              array_except(
                split(adresse_distance_inpi, ' '), 
                split(adresse_distance_insee, ' ')
              )
            )as insee_except,
  
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
    ) as union_
  FROM "inpi"."ets_insee_inpi"-- limit 10
  )
  SELECT *
  FROM (WITH tests AS (
    SELECT 
    {0},
  datecreationetablissement,date_debut_activite,
  CASE WHEN datecreationetablissement = date_debut_activite THEN 'True'
  WHEN datecreationetablissement IS NULL OR date_debut_activite IS NULL THEN 'NULL'
  ELSE 'False' END AS test_date,
  
  etatadministratifetablissement,status_admin,
  CASE WHEN etatadministratifetablissement = status_admin THEN 'True' 
  WHEN etatadministratifetablissement = '' OR status_admin = '' THEN 'NULL'
  ELSE 'False' END AS test_status_admin,
  
  etablissementsiege,status_ets,
  CASE WHEN etablissementsiege = status_ets THEN 'True' 
  WHEN etablissementsiege = '' OR status_ets = '' THEN 'NULL'
  ELSE 'False' END AS test_siege,
  
  codecommuneetablissement,code_commune,
  CASE WHEN codecommuneetablissement = code_commune THEN 'True' 
  WHEN codecommuneetablissement = '' OR code_commune = '' THEN 'NULL'
  ELSE 'False' END AS test_code_commune,
  
  codepostaletablissement,code_postal_matching,
  CASE WHEN codepostaletablissement = code_postal_matching THEN 'True' 
  WHEN codepostaletablissement = '' OR code_postal_matching = '' THEN 'NULL'
  ELSE 'False' END AS test_code_postal,
  
  numerovoieetablissement,numero_voie_matching,
  CASE WHEN numerovoieetablissement = numero_voie_matching THEN 'True' 
  WHEN numerovoieetablissement = '' OR numero_voie_matching = '' THEN 'NULL'
  ELSE 'False' END AS test_numero_voie,
  
  typevoieetablissement,type_voie_matching,
  CASE WHEN typevoieetablissement = type_voie_matching THEN 'True'
  WHEN typevoieetablissement = '' OR type_voie_matching = '' THEN 'NULL'
  ELSE 'False' END AS test_type_voie,
  
  list_inpi, list_insee, inpi_except, insee_except, intersection, union_
  
  FROM test_proba
  WHERE {1}
    )
        SELECT 
        cartesian_table.groups, 
        cnt_test,
        cnt_index_date,
        cnt_index_admin,
        cnt_index_siege,
        cnt_index_commune,
        cnt_index_cp,
        cnt_index_num_voie,
        cnt_index_type_voie

        
  FROM cartesian_table
        
  LEFT JOIN (
    
    SELECT groups, cnt_test_date, COUNT(*) AS cnt_index_date   
    FROM (    
    SELECT test_date as groups,{0},   count(*) as cnt_test_date
    FROM tests
    GROUP BY test_date, {0}
    ) as date_
    GROUP BY groups, cnt_test_date
        ) as count_dup_date
   ON count_dup_date.groups = cartesian_table.groups and
        count_dup_date.cnt_test_date = cartesian_table.cnt_test
        
   LEFT JOIN (
    
    SELECT groups, cnt_test_admin, COUNT(*) AS cnt_index_admin
    FROM (    
    SELECT test_status_admin as groups,{0}, count(*) as cnt_test_admin
    FROM tests
    GROUP BY test_status_admin, {0}
    ) as admin_
    GROUP BY groups, cnt_test_admin
        ) as count_dup_admin_
   ON count_dup_admin_.groups = cartesian_table.groups and
        count_dup_admin_.cnt_test_admin = cartesian_table.cnt_test   
        
   
   LEFT JOIN (
    
    SELECT groups, cnt_test_siege, COUNT(*) AS cnt_index_siege
    FROM (    
    SELECT test_siege as groups,{0}, count(*) as cnt_test_siege
    FROM tests
    GROUP BY test_siege, {0}
    ) as admin_
    GROUP BY groups, cnt_test_siege
        ) as count_dup_siege_
   ON count_dup_siege_.groups = cartesian_table.groups and
        count_dup_siege_.cnt_test_siege = cartesian_table.cnt_test
        
   LEFT JOIN (
    
    SELECT groups, cnt_test_commune, COUNT(*) AS cnt_index_commune
    FROM (    
    SELECT test_code_commune as groups,{0}, count(*) as cnt_test_commune
    FROM tests
    GROUP BY test_code_commune, {0}
    ) as siege_
    GROUP BY groups, cnt_test_commune
        ) as count_dup_commune_
   ON count_dup_commune_.groups = cartesian_table.groups and
        count_dup_commune_.cnt_test_commune = cartesian_table.cnt_test
        
    LEFT JOIN (
    
    SELECT groups, cnt_test_cp, COUNT(*) AS cnt_index_cp
    FROM (    
    SELECT test_code_postal as groups,{0}, count(*) as cnt_test_cp
    FROM tests
    GROUP BY test_code_postal, {0}
    ) as cp_
    GROUP BY groups, cnt_test_cp
        ) as count_dup_cp_
   ON count_dup_cp_.groups = cartesian_table.groups and
        count_dup_cp_.cnt_test_cp = cartesian_table.cnt_test
        
   LEFT JOIN (
    
    SELECT groups, cnt_test_num_voie, COUNT(*) AS cnt_index_num_voie
    FROM (    
    SELECT test_numero_voie as groups,{0}, count(*) as cnt_test_num_voie
    FROM tests
    GROUP BY test_numero_voie, {0}
    ) as num_voie_
    GROUP BY groups, cnt_test_num_voie
        ) as count_dup_num_voie_
   ON count_dup_num_voie_.groups = cartesian_table.groups and
        count_dup_num_voie_.cnt_test_num_voie = cartesian_table.cnt_test
        
   LEFT JOIN (
    
    SELECT groups, cnt_test_type_voie, COUNT(*) AS cnt_index_type_voie
    FROM (    
    SELECT test_type_voie as groups,{0}, count(*) as cnt_test_type_voie
    FROM tests
    GROUP BY test_type_voie, {0}
    ) as type_voie_
    GROUP BY groups, cnt_test_type_voie
        ) as count_dup_type_voie_
   ON count_dup_type_voie_.groups = cartesian_table.groups and
        count_dup_type_voie_.cnt_test_type_voie = cartesian_table.cnt_test
   
        )
   ORDER BY cnt_test ASC, groups
"""
```

```python
def generate_analytical_table_dup(var = 'index_id', case = 1):
    """
    """
    if case ==1:
        
        filter_= "intersection = union_"
        
    if case ==2:
        
        filter_= "intersection = 0"
    
    if case ==3:
        
        filter_= "lenght_list_inpi = intersection AND intersection != union_"
    
    if case ==4:
        
        filter_= "lenght_list_insee = intersection AND intersection != union_"
    
    if case ==5:
        
        filter_= "cardinality(insee_except) = cardinality(inpi_except) AND intersection != 0 AND cardinality(insee_except) > 0"
    
    if case ==6:
        
        filter_= "cardinality(insee_except) > cardinality(inpi_except) AND intersection != 0 AND cardinality(insee_except) > 0 AND cardinality(inpi_except) > 0"
    
    if case ==7:
        
        filter_= "cardinality(insee_except) < cardinality(inpi_except) AND intersection != 0 AND cardinality(insee_except) > 0 AND cardinality(inpi_except) > 0"

    output = athena.run_query(
        query=query_dup_cas.format(var, filter_),
        database='inpi',
        s3_output='INPI/sql_output'
    )

    results = False
    filename = 'cas_dup_{}.csv'.format(case)
    
    while results != True:
        source_key = "{}/{}.csv".format(
                            'INPI/sql_output',
                            output['QueryExecutionId']
                                   )
        destination_key = "{}/{}".format(
                                'Analyse_cas_similarite_adresse',
                                filename
                            )

        results = s3.copy_object_s3(
                                source_key = source_key,
                                destination_key = destination_key,
                                remove = True
                            )

    test_1 = (s3.read_df_from_s3(
        key = 'Analyse_cas_similarite_adresse/{}'.format(filename), sep = ',')
              .assign(test = 'cas_{}'.format(case),
                     count_test_date_pct = lambda x: x['cnt_index_date'] / x['cnt_index_date'].sum(),
                     count_test_status_admin_pct = lambda x: x['cnt_index_admin'] / x['cnt_index_admin'].sum(),
                     count_test_siege_pct = lambda x: x['cnt_index_siege'] / x['cnt_index_siege'].sum(),
                     count_test_commune_pct = lambda x: x['cnt_index_commune'] / x['cnt_index_commune'].sum(),
                     count_test_cp_pct = lambda x: x['cnt_index_cp'] / x['cnt_index_cp'].sum(),
                     count_test_num_voie_pct = lambda x: x['cnt_index_num_voie'] / x['cnt_index_num_voie'].sum(),
                     count_test_type_voie_pct = lambda x: x['cnt_index_type_voie'] / x['cnt_index_type_voie'].sum(),
                     )

              .replace({'groups' :{np.nan: 'Null'}})
              #.set_index(['test'])
              .reindex(columns = [
                  'test',
                  'groups',
                  "cnt_test",
                  'cnt_index_num_voie','count_test_num_voie_pct',
                  'cnt_index_type_voie','count_test_type_voie_pct',
                  'cnt_index_commune','count_test_commune_pct',
                  'cnt_index_date','count_test_date_pct',
                  'cnt_index_admin','count_test_status_admin_pct',
                  'cnt_index_siege','count_test_siege_pct',        
                  'cnt_index_cp','count_test_cp_pct',
                  
                  
              ])
              .fillna(0)
              .style
              .format("{:,.0f}", subset =  ['cnt_index_date',
                                            'cnt_index_admin',
                                            'cnt_index_siege',
                                            'cnt_index_commune',
                                            'cnt_index_cp',
                                            'cnt_index_num_voie',
                                            'cnt_index_type_voie'])
              .format("{:.2%}", subset =  ['count_test_date_pct',
                                           'count_test_status_admin_pct',
                                           'count_test_siege_pct',
                                           'count_test_commune_pct',
                                           'count_test_cp_pct',
                                           'count_test_num_voie_pct',
                                           'count_test_type_voie_pct'])
              .bar(subset= ['cnt_index_date',
                                            'cnt_index_admin',
                                            'cnt_index_siege',
                                            'cnt_index_commune',
                                            'cnt_index_cp',
                                            'cnt_index_num_voie',
                                            'cnt_index_type_voie'],
                   color='#d65f5f')
              #.unstack(0)
             )    
    
    return test_1
```

# Creation table analyse

## Full Pipeline

*   Dans ce notebook, tous les codes SQL pour faire la siretisation seront présent de manière atomique afin de faciliter l’écriture des US. 
   * La première query consiste à rapprocher les deux tables INPI & INSEE
   * La second partie consiste a calculer Levenshtein edit distance sur l’adresse et l’enseigne 
   * La troisième partie consiste a calculer la distance de Jaccard sur l’adresse (au niveau de la lettre) et l’enseigne
   * La quatrième partie consiste a calculer la présence d’un des mots de l’adresse de l’INPI dans l’adresse de l’INSEE  
   * La cinquième partie consiste a calculer la distance de Jaccard sur l’adresse au niveau du mot 

```python
query = """
CREATE TABLE inpi.ets_insee_inpi WITH (format = 'PARQUET') AS WITH insee_inpi AS (
  SELECT 
    index_id, 
    sequence_id, 
    count_initial_insee, 
    ets_final_sql.siren, 
    siret, 
    code_greffe, 
    nom_greffe, 
    numero_gestion, 
    id_etablissement, 
    status, 
    origin, 
    date_greffe, 
    file_timestamp, 
    datecreationetablissement, 
    "date_début_activité", 
    libelle_evt, 
    last_libele_evt, 
    etatadministratifetablissement, 
    status_admin, 
    type, 
    etablissementsiege, 
    status_ets, 
    adresse_reconstituee_inpi, 
    adresse_reconstituee_insee, 
    adresse_regex_inpi, 
    adresse_distance_inpi, 
    adresse_distance_insee, 
    list_numero_voie_matching_inpi, 
    list_numero_voie_matching_insee, 
    numerovoieetablissement, 
    numero_voie_matching, 
    typevoieetablissement, 
    type_voie_matching, 
    ets_final_sql.code_postal_matching, 
    ets_final_sql.ville_matching, 
    codecommuneetablissement, 
    code_commune, 
    enseigne, 
    enseigne1etablissement, 
    enseigne2etablissement, 
    enseigne3etablissement 
  FROM 
    ets_final_sql 
    INNER JOIN (
      SELECT 
        count_initial_insee, 
        siren, 
        siret, 
        datecreationetablissement, 
        etablissementsiege, 
        etatadministratifetablissement, 
        codepostaletablissement, 
        codecommuneetablissement, 
        ville_matching, 
        list_numero_voie_matching_insee, 
        numerovoieetablissement, 
        typevoieetablissement, 
        adresse_reconstituee_insee, 
        adresse_distance_insee, 
        enseigne1etablissement, 
        enseigne2etablissement, 
        enseigne3etablissement 
      FROM 
        insee_final_sql
    ) as insee ON ets_final_sql.siren = insee.siren 
    AND ets_final_sql.ville_matching = insee.ville_matching 
    AND ets_final_sql.code_postal_matching = insee.codepostaletablissement 
  WHERE 
    status != 'IGNORE'
) 
SELECT 
  index_id, 
  sequence_id, 
  count_initial_insee, 
  siren, 
  siret, 
  code_greffe, 
  nom_greffe, 
  numero_gestion, 
  id_etablissement, 
  status, 
  origin, 
  date_greffe, 
  file_timestamp, 
  datecreationetablissement, 
  "date_début_activité", 
  libelle_evt, 
  last_libele_evt, 
  etatadministratifetablissement, 
  status_admin, 
  type, 
  etablissementsiege, 
  status_ets, 
  adresse_reconstituee_inpi, 
  adresse_reconstituee_insee, 
  adresse_regex_inpi, 
  adresse_distance_inpi, 
  adresse_distance_insee, 
  (
    CAST(
      cardinality(
        array_distinct(
          split(adresse_distance_inpi, ' ')
        )
      ) AS DECIMAL(10, 2)
    ) / (
      CAST(
        cardinality(
          array_distinct(
            split(adresse_distance_insee, ' ')
          )
        ) AS DECIMAL(10, 2)
      )
    )
  ) / NULLIF(
    CAST(
      cardinality(
        array_distinct(
          array_except(
            split(adresse_distance_insee, ' '), 
            split(adresse_distance_inpi, ' ')
          )
        )
      ) AS DECIMAL(10, 2)
    ), 
    0
  )* (
    cardinality(
      array_distinct(
        split(adresse_distance_inpi, ' ')
      )
    )* cardinality(
      array_distinct(
        split(adresse_distance_insee, ' ')
      )
    )
  )/(
    NULLIF(
      CAST(
        cardinality(
          array_distinct(
            array_union(
              split(adresse_distance_inpi, ' '), 
              split(adresse_distance_insee, ' ')
            )
          )
        ) AS DECIMAL(10, 2)
      ), 
      0
    ) * NULLIF(
      CAST(
        cardinality(
          array_distinct(
            array_intersect(
              split(adresse_distance_inpi, ' '), 
              split(adresse_distance_insee, ' ')
            )
          )
        ) AS DECIMAL(10, 2)
      ), 
      0
    )
  ) as score_pairing, 
  CASE WHEN cardinality(
    array_distinct(
      split(adresse_distance_inpi, ' ')
    )
  ) = 0 THEN NULL ELSE array_distinct(
    split(adresse_distance_inpi, ' ')
  ) END as liste_distinct_inpi, 
  CASE WHEN cardinality(
    array_distinct(
      split(adresse_distance_insee, ' ')
    )
  ) = 0 THEN NULL ELSE array_distinct(
    split(adresse_distance_insee, ' ')
  ) END as liste_distinct_insee, 
  CASE WHEN cardinality(
    array_distinct(
      array_except(
        split(adresse_distance_insee, ' '), 
        split(adresse_distance_inpi, ' ')
      )
    )
  ) = 0 THEN NULL ELSE array_distinct(
    array_except(
      split(adresse_distance_insee, ' '), 
      split(adresse_distance_inpi, ' ')
    )
  ) END as insee_exclusion, 
  CASE WHEN cardinality(
    array_distinct(
      array_except(
        split(adresse_distance_inpi, ' '), 
        split(adresse_distance_insee, ' ')
      )
    )
  ) = 0 THEN NULL ELSE array_distinct(
    array_except(
      split(adresse_distance_inpi, ' '), 
      split(adresse_distance_insee, ' ')
    )
  ) END as inpi_exclusion, 
  regexp_like(
    adresse_reconstituee_insee, adresse_regex_inpi
  ) as regex_adresse, 
  list_numero_voie_matching_inpi, 
  list_numero_voie_matching_insee, 
  numerovoieetablissement, 
  numero_voie_matching, 
  typevoieetablissement, 
  type_voie_matching, 
  code_postal_matching, 
  ville_matching, 
  codecommuneetablissement, 
  code_commune, 
  enseigne, 
  enseigne1etablissement, 
  enseigne2etablissement, 
  enseigne3etablissement, 
  levenshtein_distance(
    enseigne, enseigne1etablissement
  ) as edit_enseigne1, 
  levenshtein_distance(
    enseigne, enseigne2etablissement
  ) as edit_enseigne2, 
  levenshtein_distance(
    enseigne, enseigne3etablissement
  ) as edit_enseigne3, 
  1 - CAST(
    cardinality(
      array_intersect(
        regexp_extract_all(enseigne, '(\d+)|([A-Z])'), 
        regexp_extract_all(
          enseigne1etablissement, '(\d+)|([A-Z])'
        )
      )
    ) AS DECIMAL(10, 2)
  ) / NULLIF(
    CAST(
      cardinality(
        array_union(
          regexp_extract_all(enseigne, '(\d+)|([A-Z])'), 
          regexp_extract_all(
            enseigne1etablissement, '(\d+)|([A-Z])'
          )
        )
      ) AS DECIMAL(10, 2)
    ), 
    0
  ) as jaccard_enseigne1_lettre, 
  1 - CAST(
    cardinality(
      array_intersect(
        regexp_extract_all(enseigne, '(\d+)|([A-Z])'), 
        regexp_extract_all(
          enseigne2etablissement, '(\d+)|([A-Z])'
        )
      )
    ) AS DECIMAL(10, 2)
  ) / NULLIF(
    CAST(
      cardinality(
        array_union(
          regexp_extract_all(enseigne, '(\d+)|([A-Z])'), 
          regexp_extract_all(
            enseigne2etablissement, '(\d+)|([A-Z])'
          )
        )
      ) AS DECIMAL(10, 2)
    ), 
    0
  ) as jaccard_enseigne2_lettre, 
  1 - CAST(
    cardinality(
      array_intersect(
        regexp_extract_all(enseigne, '(\d+)|([A-Z])'), 
        regexp_extract_all(
          enseigne3etablissement, '(\d+)|([A-Z])'
        )
      )
    ) AS DECIMAL(10, 2)
  ) / NULLIF(
    CAST(
      cardinality(
        array_union(
          regexp_extract_all(enseigne, '(\d+)|([A-Z])'), 
          regexp_extract_all(
            enseigne3etablissement, '(\d+)|([A-Z])'
          )
        )
      ) AS DECIMAL(10, 2)
    ), 
    0
  ) as jaccard_enseigne3_lettre 
FROM 
  insee_inpi

"""
```

# Evaluation nombre de cas

## Similarité entre deux adresses

Le rapprochement entre les deux tables, à savoir l’INSEE et l’INPI, va amener à la création de deux vecteurs d’adresse. Un vecteur avec des mots contenus spécifiquement à l’INSEE, et un second vecteur avec les mots de l’adresse de l’INPI. Notre objectif est de comparé ses deux vecteurs pour définir si ils sont identiques ou non. Nous avons distingué 7 cas de figures possibles entre les deux vecteurs (figure 1).

![](https://drive.google.com/uc?export=view&id=1Qj_HooHrhFYSuTsoqFbl4Vxy9tN3V5Bu)

## Définition

![](https://upload.wikimedia.org/wikipedia/commons/thumb/1/1f/Intersection_of_sets_A_and_B.svg/400px-Intersection_of_sets_A_and_B.svg.png)




La table `ets_insee_inpi` contient 11 600 551 observations

```python
initial_obs = 11600551
```

## Tableau recapitulatif

|   Cas de figure | Titre                   |   Total |   Total cumulé |   pourcentage |   Pourcentage cumulé | Comment                 |
|----------------:|:------------------------|--------:|---------------:|--------------:|---------------------:|:------------------------|
|               1 | similarité parfaite     | 7775392 |        7775392 |     0.670261  |             0.670261 | Match parfait           |
|               2 | Exclusion parfaite      |  974444 |        8749836 |     0.0839998 |             0.75426  | Match parfait           |
|               3 | Match partiel parfait   |  407404 |        9157240 |     0.0351194 |             0.78938  | Match partiel parfait   |
|               4 | Match partiel parfait   |  558992 |        9716232 |     0.0481867 |             0.837566 | Match partiel parfait   |
|               5 | Match partiel compliqué | 1056406 |       10772638 |     0.0910652 |             0.928632 | Match partiel parfait   |
|               6 | Match partiel compliqué | 1056406 |       11133880 |     0.0311401 |             0.959772 | Match partiel compliqué |
|               7 | Match partiel compliqué | 1056406 |       11600551 |     0.0402283 |             1        | Match partiel compliqué |

```python
dic_ = {
    'Cas de figure': [], 
    'Titre': [], 
    'Total': [], 
    'Total cumulé': [], 
    'pourcentage': [], 
    'Pourcentage cumulé': [], 
    'Comment': [], 
}
```

## Cas de figure 1: similarité parfaite

* Definition: Les mots dans l’adresse de l’INPI sont égales aux mots dans l’adresse de l’INSEE
- Math definition: $\frac{|INSEE \cap INPI|}{|INSEE|+|INPI|-|INSEE \cap INPI|} =1$
- Règle: $ \text{intersection} = \text{union} \rightarrow \text{cas 1}$
* Query [case 1](https://eu-west-3.console.aws.amazon.com/athena/home?region=eu-west-3#query/history/24e58c22-4a67-4a9e-b98d-4eb9d65e7f27)

| list_inpi              | list_insee             | insee_except | intersection | union_ |
|------------------------|------------------------|--------------|--------------|--------|
| [BOULEVARD, HAUSSMANN] | [BOULEVARD, HAUSSMANN] | []           | 2            | 2      |
| [QUAI, GABUT]          | [QUAI, GABUT]          | []           | 2            | 2      |
| [BOULEVARD, VOLTAIRE]  | [BOULEVARD, VOLTAIRE]  | []           | 2            | 2      |

- Nombre d'observation: 7 774 986
    - Percentage initial: 0.67
    

```python
cas_1 =  compte_obs_cas(case= 1)
```

```python
dic_['Cas de figure'].append(1)
dic_['Titre'].append('similarité parfaite')
dic_['Total'].append(cas_1)
dic_['Total cumulé'].append(cas_1)
dic_['pourcentage'].append(cas_1/initial_obs)
dic_['Pourcentage cumulé'].append(cas_1/initial_obs)
dic_['Comment'].append("Match parfait")
```

```python
generate_analytical_table(case = 1)
```

```python
compte_dup_cas(var = 'index_id', case = 1)
```

```python
generate_analytical_table_dup(var = 'index_id', case = 1)
```

```python
compte_dup_cas(var = 'sequence_id', case = 1)
```

```python
generate_analytical_table_dup(var = 'sequence_id', case = 1)
```

## Cas de figure 2: Dissimilarité parfaite

* Definition: Aucun des mots de l’adresse de l’INPI sont égales aux mots dans l’adresse de l’INSEE
* Math definition: $\frac{|INSEE \cap INPI|}{|INSEE|+|INPI|-|INSEE \cap INPI|}$
* Query [case 2](https://eu-west-3.console.aws.amazon.com/athena/home?region=eu-west-3#query/history/4363e8b4-b3c7-4964-804f-4e66b0780a17)
* Règle: $\text{intersection} = 0 \rightarrow \text{cas 2}$

| list_inpi                               | list_insee                              | insee_except                            | intersection | union_ |
|-----------------------------------------|-----------------------------------------|-----------------------------------------|--------------|--------|
| [CHEMIN, MOUCHE]                        | [AVENUE, CHARLES, GAULLE, SAINT, GENIS] | [AVENUE, CHARLES, GAULLE, SAINT, GENIS] | 0            | 7      |
| [AVENUE, CHARLES, GAULLE, SAINT, GENIS] | [CHEMIN, MOUCHE]                        | [CHEMIN, MOUCHE]                        | 0            | 7      |

- Nombre d'observation: 974 727
    - Percentage initial: 0.08

```python
cas_2 =compte_obs_cas(case= 2)
```

```python
dic_['Cas de figure'].append(2)
dic_['Titre'].append('Exclusion parfaite')
dic_['Total'].append(cas_2)
dic_['Total cumulé'].append(cas_1 + cas_2)
dic_['pourcentage'].append(cas_2/initial_obs)
dic_['Pourcentage cumulé'].append((cas_1 +cas_2)/initial_obs)
dic_['Comment'].append("Exclusion parfaite")
```

```python
generate_analytical_table(case = 2)
```

```python
compte_dup_cas(var = 'index_id', case = 2)
```

```python
generate_analytical_table_dup(var = 'index_id', case = 2)
```

```python
compte_dup_cas(var = 'sequence_id', case = 2)
```

```python
generate_analytical_table_dup(var = 'sequence_id', case = 2)
```

## Cas de figure 3: Intersection parfaite INPI

* Definition:  Tous les mots dans l’adresse de l’INPI  sont contenus dans l’adresse de l’INSEE
* Math définition: $\frac{|INPI|}{|INSEE \cap INPI|}  \text{  = 1 and }|INSEE \cap INPI| <> |INSEE \cup INPI|$
* Query [case 3](https://eu-west-3.console.aws.amazon.com/athena/home?region=eu-west-3#query/history/7fb420a1-5f50-4256-a2ba-b8c7c2b63c9b)
* Règle: $|\text{list_inpi}|= \text{intersection}  \text{  = 1 and }\text{intersection} \neq  \text{union} \rightarrow \text{cas 3}$

| list_inpi                    | list_insee                                               | insee_except                            | intersection | union_ |
|------------------------------|----------------------------------------------------------|-----------------------------------------|--------------|--------|
| [ALLEE, BERLIOZ]             | [ALLEE, BERLIOZ, CHEZ, MME, IDALI]                       | [CHEZ, MME, IDALI]                      | 2            | 5      |
| [RUE, MAI, OLONNE, SUR, MER] | [RUE, HUIT, MAI, OLONNE, SUR, MER]                       | [HUIT]                                  | 5            | 6      |
| [RUE, CAMILLE, CLAUDEL]      | [RUE, CAMILLE, CLAUDEL, VITRE]                           | [VITRE]                                 | 3            | 4      |
| [ROUTE, D, ESLETTES]         | [ROUTE, D, ESLETTES, A]                                  | [A]                                     | 3            | 4      |
| [AVENUE, MAI]                | [AVENUE, HUIT, MAI]                                      | [HUIT]                                  | 2            | 3      |
| [RUE, SOUS, DINE]            | [RUE, SOUS, DINE, RES, SOCIALE, HENRIETTE, D, ANGEVILLE] | [RES, SOCIALE, HENRIETTE, D, ANGEVILLE] | 3            | 8      |

- Nombre d'observation: 407 320
    - Percentage initial: 0.03

```python
cas_3 = compte_obs_cas(case= 3)
```

```python
dic_['Cas de figure'].append(3)
dic_['Titre'].append('Match partiel parfait')
dic_['Total'].append(cas_3)
dic_['Total cumulé'].append(cas_1 + cas_2 +cas_3)
dic_['pourcentage'].append(cas_3/initial_obs)
dic_['Pourcentage cumulé'].append((cas_1 + cas_2 +cas_3)/initial_obs)
dic_['Comment'].append("Match partiel parfait")
```

```python
generate_analytical_table(case = 3)
```

```python
compte_dup_cas(var = 'index_id', case = 3)
```

```python
generate_analytical_table_dup(var = 'index_id', case = 3)
```

```python
compte_dup_cas(var = 'sequence_id', case = 3)
```

```python
generate_analytical_table_dup(var = 'sequence_id', case = 3)
```

## Cas de figure 4: Intersection parfaite INSEE

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

- Nombre d'observation: 558 956
    - Percentage initial: 0.04

```python
cas_4 = compte_obs_cas(case= 4)
```

```python
dic_['Cas de figure'].append(4)
dic_['Titre'].append('Match partiel parfait')
dic_['Total'].append(cas_4)
dic_['Total cumulé'].append(cas_1 + cas_2 + cas_3 + cas_4)
dic_['pourcentage'].append(cas_4/initial_obs)
dic_['Pourcentage cumulé'].append((cas_1 + cas_2 + cas_3 + cas_4) / initial_obs)
dic_['Comment'].append("Match partiel parfait")
```

```python
generate_analytical_table(case = 4)
```

```python
compte_dup_cas(var = 'index_id', case = 4)
```

```python
generate_analytical_table_dup(var = 'index_id', case = 4)
```

```python
compte_dup_cas(var = 'sequence_id', case = 4)
```

```python
generate_analytical_table_dup(var = 'sequence_id', case = 4)
```

## Cas de figure 5: Cardinality exception parfaite INSEE INPI, intersection positive

* Definition:  L’adresse de l’INPI contient des mots de l’adresse de l’INPI et la cardinality des mots non présents dans les deux adresses est équivalente
* Math definition: $|INPI|-|INPI \cap INSEE| = |INSEE|-|INPI \cap INSEE|$
* Query [case 5](https://eu-west-3.console.aws.amazon.com/athena/home?region=eu-west-3#query/history/fec67222-3a7b-4bfb-af20-dd70d82932e3)
* Règle: $|\text{insee_except}| = |\text{inpi_except}| \text{ and } \text{intersection} > 0 \rightarrow \text{cas 5}$

| list_inpi                                                                                  | list_insee                                                                              | insee_except | inpi_except  | intersection | union_ |
|--------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------|--------------|--------------|--------------|--------|
| [AVENUE, GEORGES, VACHER, C, A, SAINTE, VICTOIRE, IMMEUBLE, CCE, CD, ZI, ROUSSET, PEYNIER] | [AVENUE, GEORGES, VACHER, C, A, STE, VICTOIRE, IMMEUBLE, CCE, CD, ZI, ROUSSET, PEYNIER] | [STE]        | [SAINTE]     | 12           | 14     |
| [BIS, AVENUE, PAUL, DOUMER, RES, SAINT, MARTIN, BAT, D, C, O, M, ROSSI]                    | [BIS, AVENUE, PAUL, DOUMER, RES, ST, MARTIN, BAT, D, C, O, M, ROSSI]                    | [ST]         | [SAINT]      | 12           | 14     |
| [ROUTE, DEPARTEMENTALE, CHEZ, SOREME, CENTRE, COMMERCIAL, L, OCCITAN, PLAN, OCCIDENTAL]    | [ROUTE, DEPARTEMENTALE, CHEZ, SOREME, CENTRE, COMMERCIAL, L, OCCITAN, PLAN, OC]         | [OC]         | [OCCIDENTAL] | 9            | 11     |
| [LIEU, DIT, FOND, CHAMP, MALTON, PARC, EOLIEN, SUD, MARNE, PDL]                            | [LIEU, DIT, FONDD, CHAMP, MALTON, PARC, EOLIEN, SUD, MARNE, PDL]                        | [FONDD]      | [FOND]       | 9            | 11     |
| [AVENUE, ROBERT, BRUN, ZI, CAMP, LAURENT, LOT, NUMERO, ST, BERNARD]                        | [AVENUE, ROBERT, BRUN, ZI, CAMP, LAURENT, LOT, ST, BERNARD, N]                          | [N]          | [NUMERO]     | 9            | 11     |
| [PLACE, MARCEL, DASSAULT, PARC, D, ACTIVITES, TY, NEHUE, BATIMENT, H]                      | [PLACE, MARCEL, DASSAULT, PARC, D, ACTIVITES, TY, NEHUE, BAT, H]                        | [BAT]        | [BATIMENT]   | 9            | 11     |

- Nombre d'observation: 1 056 522
    - Percentage initial: 0.09

```python
cas_5 = compte_obs_cas(case= 5)
```

```python
dic_['Cas de figure'].append(5)
dic_['Titre'].append('Match partiel compliqué')
dic_['Total'].append(cas_5)
dic_['Total cumulé'].append(cas_1 + cas_2 + cas_3 + cas_4 + cas_5)
dic_['pourcentage'].append(cas_5/initial_obs)
dic_['Pourcentage cumulé'].append((cas_1 + cas_2 + cas_3 + cas_4 + cas_5)/initial_obs)
dic_['Comment'].append("Match partiel compliqué")
```

```python
generate_analytical_table(case = 5)
```

```python
compte_dup_cas(var = 'index_id', case = 5)
```

```python
generate_analytical_table_dup(var = 'index_id', case = 5)
```

```python
compte_dup_cas(var = 'sequence_id', case = 5)
```

```python
generate_analytical_table_dup(var = 'sequence_id', case = 5)
```

## Cas de figure 6: Cardinality exception INSEE supérieure INPI, intersection positive 

* Definition:  L’adresse de l’INPI contient des mots de l’adresse de l’INPI et la cardinality des mots non présents dans l’adresse de l’INSEE est supérieure à la cardinality de l’adresse de l’INPI
* Math definition: $|INPI|-|INPI \cap INSEE| < |INSEE|-|INPI \cap INSEE|$
* Query [case 6](https://eu-west-3.console.aws.amazon.com/athena/home?region=eu-west-3#query/history/9bdce567-5871-4a5a-add4-d5cca6a83528)
* Règle: $|\text{insee_except}| > |\text{inpi_except}| \text{ and } \text{intersection} > 0 \rightarrow \text{cas 6}$

| list_inpi                                                                         | list_insee                                                                               | insee_except          | inpi_except   | intersection | union_ |
|-----------------------------------------------------------------------------------|------------------------------------------------------------------------------------------|-----------------------|---------------|--------------|--------|
| [AVENUE, AUGUSTE, PICARD, POP, UP, TOURVILLE, CC, EMPLACEMENT, DIT, PRECAIRE, N]  | [AVENUE, AUGUSTE, PICARD, POP, UP, TOURVILL, CC, TOURVILLE, EMPLACEMT, DIT, PRECAIRE, N] | [TOURVILL, EMPLACEMT] | [EMPLACEMENT] | 10           | 13     |
| [ROUTE, COTE, D, AZUR, C, O, TENERGIE, ARTEPARC, MEYREUIL, BAT, A]                | [ROUTE, C, O, TENERGIE, ARTEPARC, MEYREUI, BAT, A, RTE, COTE, D, AZUR]                   | [MEYREUI, RTE]        | [MEYREUIL]    | 10           | 13     |
| [C, O, TENERGIE, ARTEPARC, MEYREUIL, BATIMENT, A, ROUTE, COTE, D, AZUR]           | [ROUTE, C, O, TENERGIE, ARTEPARC, MEYREUI, BATIMENT, A, RTE, COTE, D, AZUR]              | [MEYREUI, RTE]        | [MEYREUIL]    | 10           | 13     |
| [LOTISSEMENT, VANGA, DI, L, ORU, VILLA, FRANCK, TINA, CHEZ, COLOMBANI, CHRISTIAN] | [LIEU, DIT, VANGA, DI, L, ORU, VILLA, FRANCK, TINA, CHEZ, COLOMBANI, CHRISTIAN]          | [LIEU, DIT]           | [LOTISSEMENT] | 10           | 13     |
| [AVENUE, DECLARATION, DROITS, HOMME, RES, CLOS, ST, MAMET, BAT, C, APPT]          | [AVENUE, DECL, DROITS, L, HOMME, RES, CLOS, ST, MAMET, BAT, C, APPT]                     | [DECL, L]             | [DECLARATION] | 10           | 13     |

- Nombre d'observation: 361 353
    - Percentage initial: 0.03

```python
cas_6 = compte_obs_cas(case= 6)
```

```python
dic_['Cas de figure'].append(6)
dic_['Titre'].append('Match partiel compliqué')
dic_['Total'].append(cas_6)
dic_['Total cumulé'].append(cas_1 + cas_2 +cas_3 +cas_4 + cas_5 +cas_6)
dic_['pourcentage'].append(cas_6/initial_obs)
dic_['Pourcentage cumulé'].append((cas_1 + cas_2 +cas_3 +cas_4 + cas_5 +cas_6)/initial_obs)
dic_['Comment'].append("Match partiel compliqué")
```

```python
generate_analytical_table(case = 6)
```

```python
compte_dup_cas(var = 'index_id', case = 6)
```

```python
generate_analytical_table_dup(var = 'index_id', case = 6)
```

```python
compte_dup_cas(var = 'sequence_id', case = 6)
```

```python
generate_analytical_table_dup(var = 'sequence_id', case = 6)
```

## Cas de figure 7: Cardinality exception INPI supérieure INSEE, intersection positive 

* Definition:  L’adresse de l’INSEE contient des mots de l’adresse de l’INPI et la cardinality des mots non présents dans l’adresse de l’INPI est supérieure à la cardinality de l’adresse de l’INSEE
* Math definition: $|INPI|-|INPI \cap INSEE| > |INSEE|-|INPI \cap INSEE|$
* Règle: $|\text{insee_except}| < |\text{inpi_except}| \text{ and } \text{intersection} > 0 \rightarrow \text{cas 7}$

| list_inpi                                                                                    | list_insee                                                                   | insee_except | inpi_except                 | intersection | union_ |
|----------------------------------------------------------------------------------------------|------------------------------------------------------------------------------|--------------|-----------------------------|--------------|--------|
| [RTE, CABRIERES, D, AIGUES, CHEZ, MR, DOL, JEAN, CLAUDE, LIEUDIT, PLAN, PLUS, LOIN]          | [ROUTE, CABRIERES, D, AIGUES, CHEZ, MR, DOL, JEAN, CLAUDE, PLAN, PLUS, LOIN] | [ROUTE]      | [RTE, LIEUDIT]              | 11           | 14     |
| [ROUTE, N, ZAC, PONT, RAYONS, CC, GRAND, VAL, ILOT, B, BAT, A, LOCAL]                        | [ZONE, ZAC, PONT, RAYONS, CC, GRAND, VAL, ILOT, B, BAT, A, LOCAL]            | [ZONE]       | [ROUTE, N]                  | 11           | 14     |
| [BOULEVARD, PAUL, VALERY, BAT, B, ESC, H, APPT, C, O, MADAME, BLANDINE, BOVE]                | [BOULEVARD, PAUL, VALERY, BAT, B, ESC, H, APT, C, O, BOVE, BLANDINE]         | [APT]        | [APPT, MADAME]              | 11           | 14     |
| [RUE, JEANNE, D, ARC, A, L, ANGLE, N, ROLLON, EME, ETAGE, POLE, PRO, AGRI]                   | [RUE, JEANNE, D, ARC, A, L, ANGLE, N, ROLLON, E, ETAGE]                      | [E]          | [EME, POLE, PRO, AGRI]      | 10           | 15     |
| [CHEZ, MR, MME, DANIEL, DEZEMPTE, AVENUE, BALCONS, FRONT, MER, L, OISEAU, BLEU, BATIMENT, B] | [AVENUE, BALCONS, FRONT, MER, CHEZ, MR, MME, DANIEL, DEZEMPTE, L, OISEA]     | [OISEA]      | [OISEAU, BLEU, BATIMENT, B] | 10           | 15     |

- Nombre d'observation: 466687
    - Percentage initial: 0.04

```python
cas_7 = compte_obs_cas(case= 7)
```

```python
dic_['Cas de figure'].append(7)
dic_['Titre'].append('Match partiel compliqué')
dic_['Total'].append(cas_7)
dic_['Total cumulé'].append(cas_1 + cas_2 + cas_3 + cas_4 + cas_5+ cas_6 + cas_7)
dic_['pourcentage'].append(cas_7/initial_obs)
dic_['Pourcentage cumulé'].append((cas_1 + cas_2 + cas_3 + cas_4 + cas_5+ cas_6 + cas_7)/initial_obs)
dic_['Comment'].append("Match partiel compliqué")
```

```python
generate_analytical_table(case = 7)
```

```python
compte_dup_cas(var = 'index_id', case = 7)
```

```python
generate_analytical_table_dup(var = 'index_id', case = 7)
```

```python
compte_dup_cas(var = 'sequence_id', case = 7)
```

```python
generate_analytical_table_dup(var = 'sequence_id', case = 7)
```

## Recapitulatif cas

```python
(pd.DataFrame(dic_)
 .style
 .format("{:,.0f}", subset =  ['Total',
                                            'Total cumulé'])
              .format("{:.2%}", subset =  ['pourcentage',
                                           'Pourcentage cumulé'])
              .bar(subset= ['Total',
                                            'Total cumulé'],
                   color='#d65f5f')
)
```

```python
#print(pd.DataFrame(dic_).set_index('Cas de figure').to_markdown())
```

# Generate report

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
