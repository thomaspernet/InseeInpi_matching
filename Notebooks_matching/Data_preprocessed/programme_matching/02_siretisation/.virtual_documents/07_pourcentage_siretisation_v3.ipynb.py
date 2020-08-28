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


import seaborn as sns

cm = sns.light_palette("green", as_cmap=True)
pd.set_option('display.max_columns', None)


status_cas = ['CAS_1','CAS_3','CAS_4', 'CAS_5', 'CAS_6', 'CAS_7']
test_list_num_voie = ['True', 'NULL', 'False']
test_siege = ['True','NULL','False']
test_enseigne =  ['True','NULL', 'False']

index = pd.MultiIndex.from_product([status_cas,test_list_num_voie, test_siege,test_enseigne],
                                   names = ["status_cas",
                                            "test_list_num_voie",
                                           'test_siege', 
                                           'test_enseigne'])

df_ = (pd.DataFrame(index = index)
       .reset_index()
       .assign(rank = lambda x: x.index + 1)
       #.to_csv('Regle_tests.csv', index = False)
      )
df_.head()


df_.to_csv('Regle_tests.csv', index = False)
s3.upload_file(file_to_upload = 'Regle_tests.csv',
            destination_in_s3 = 'TEMP_ANALYSE_SIRETISATION/REGLES_TESTS')

create_table = """
CREATE EXTERNAL TABLE IF NOT EXISTS inpi.REGLES_TESTS (
`status_cas`                     string,
`test_list_num_voie`                     string,
`test_siege`                     string,
`test_enseigne`                     string,
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
output = s3.run_query(
        query=create_table,
        database='inpi',
        s3_output='INPI/sql_output'
    )


drop_table = False
if drop_table:
    output = athena.run_query(
        query="DROP TABLE `ets_inpi_insee_cases`;",
        database='inpi',
        s3_output='INPI/sql_output'
    )


create_table = """

/*match insee inpi 7 cas de figs*/
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
          datecreationetablissement, 'get_ipython().run_line_magic("Y-%m-%d'", "")
        )
      ), 
      try(
        date_parse(
          datecreationetablissement, 'get_ipython().run_line_magic("Y-%m-%d", " %hh:%mm:%ss.SSS'")
        )
      ), 
      try(
        date_parse(
          datecreationetablissement, 'get_ipython().run_line_magic("Y-%m-%d", " %hh:%mm:%ss'")
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
          "date_début_activité", 'get_ipython().run_line_magic("Y-%m-%d'", "")
        )
      ), 
      try(
        date_parse(
          "date_début_activité", 'get_ipython().run_line_magic("Y-%m-%d", " %hh:%mm:%ss.SSS'")
        )
      ), 
      try(
        date_parse(
          "date_début_activité", 'get_ipython().run_line_magic("Y-%m-%d", " %hh:%mm:%ss'")
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
        ) THEN 'True' WHEN (
          intersection_numero_voie IS NULL 
          OR union_numero_voie IS NULL
        ) THEN 'NULL' ELSE 'False' END AS test_list_num_voie, 
        datecreationetablissement, 
        date_debut_activite, 
        CASE WHEN datecreationetablissement = date_debut_activite THEN 'True' WHEN datecreationetablissement IS NULL 
        OR date_debut_activite IS NULL THEN 'NULL' --WHEN datecreationetablissement = '' 
        ELSE 'False' END AS test_date, 
        etatadministratifetablissement, 
        status_admin, 
        CASE WHEN etatadministratifetablissement = status_admin THEN 'True' WHEN etatadministratifetablissement IS NULL 
        OR status_admin IS NULL THEN 'NULL' WHEN etatadministratifetablissement = '' 
        OR status_admin = '' THEN 'NULL' ELSE 'False' END AS test_status_admin, 
        etablissementsiege, 
        status_ets, 
        CASE WHEN etablissementsiege = status_ets THEN 'True' WHEN etablissementsiege IS NULL 
        OR status_ets IS NULL THEN 'NULL' WHEN etablissementsiege = '' 
        OR status_ets = '' THEN 'NULL' ELSE 'False' END AS test_siege, 
        codecommuneetablissement, 
        code_commune, 
        CASE WHEN codecommuneetablissement = code_commune THEN 'True' WHEN codecommuneetablissement IS NULL 
        OR code_commune IS NULL THEN 'NULL' WHEN codecommuneetablissement = '' 
        OR code_commune = '' THEN 'NULL' ELSE 'False' END AS test_code_commune, 
        codepostaletablissement, 
        code_postal_matching, 
        numerovoieetablissement, 
        numero_voie_matching, 
        CASE WHEN numerovoieetablissement = numero_voie_matching THEN 'True' WHEN numerovoieetablissement IS NULL 
        OR numero_voie_matching IS NULL THEN 'NULL' WHEN numerovoieetablissement = '' 
        OR numero_voie_matching = '' THEN 'NULL' ELSE 'False' END AS test_numero_voie, 
        typevoieetablissement, 
        type_voie_matching, 
        CASE WHEN typevoieetablissement = type_voie_matching THEN 'True' WHEN typevoieetablissement IS NULL 
        OR type_voie_matching IS NULL THEN 'NULL' WHEN typevoieetablissement = '' 
        OR type_voie_matching = '' THEN 'NULL' ELSE 'False' END AS test_type_voie, 
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
        AND intersection get_ipython().getoutput("= union_ THEN 'CAS_3' WHEN lenght_list_insee = intersection ")
        AND intersection get_ipython().getoutput("= union_ THEN 'CAS_4' WHEN cardinality(insee_except) = cardinality(inpi_except) ")
        AND intersection get_ipython().getoutput("= 0 ")
        AND cardinality(insee_except) > 0 THEN 'CAS_5' WHEN cardinality(insee_except) > cardinality(inpi_except) 
        AND intersection get_ipython().getoutput("= 0 ")
        AND cardinality(insee_except) > 0 
        AND cardinality(inpi_except) > 0 THEN 'CAS_6' WHEN cardinality(insee_except) < cardinality(inpi_except) 
        AND intersection get_ipython().getoutput("= 0 ")
        AND cardinality(insee_except) > 0 
        AND cardinality(inpi_except) > 0 THEN 'CAS_7' ELSE 'CAS_NO_ADRESSE' END AS status_cas, 
        enseigne, 
        enseigne1etablissement, 
        enseigne2etablissement, 
        enseigne3etablissement, 
        CASE WHEN cardinality(test) = 0 THEN 'NULL' WHEN enseigne = '' THEN 'NULL' WHEN temp_test_enseigne = TRUE THEN 'True' ELSE 'False' END AS test_enseigne 
      FROM 
        test_proba
      
    ) 
    
    SELECT *
    FROM (
      WITH test AS(
        SELECT *,
        CASE WHEN status_cas = 'CAS_1' OR
        status_cas = 'CAS_3' OR 
        status_cas = 'CAS_4' THEN 'True' ELSE 'False' END AS test_adresse_cas_1_3_4 
        FROM test_rules
        WHERE test_list_num_voie get_ipython().getoutput("= 'False' and status_cas != 'CAS_2'")
        )
    
    SELECT 
      rank,
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
      CASE WHEN count_inpi_index_id_siret > 1 THEN 'True' ELSE 'False' END AS index_id_duplicate,
      CASE WHEN count_inpi_sequence_siret = 1 THEN 'True' ELSE 'False' END AS test_sequence_siret,
      CASE WHEN count_inpi_index_id_stat_cas_siret = 1 THEN 'True' ELSE 'False' END AS test_index_siret,
      CASE WHEN count_initial_insee = count_inpi_siren_siret THEN 'True' ELSE 'False' END AS test_siren_insee_siren_inpi, 
    
      CASE WHEN count_inpi_sequence_siret = count_inpi_sequence_stat_cas_siret THEN 'True' ELSE 'False' END AS test_sequence_siret_many_cas,
    
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
      len_inpi_except, 
      len_insee_except, 
      test.status_cas, 
      test_adresse_cas_1_3_4,
      index_id_dup_has_cas_1_3_4,
      CASE
      WHEN test_adresse_cas_1_3_4 = 'True' AND index_id_dup_has_cas_1_3_4 = 'True' AND count_inpi_index_id_siret > 1 THEN 'TO_KEEP' 
      WHEN test_adresse_cas_1_3_4 = 'False' AND index_id_dup_has_cas_1_3_4 = 'True' AND count_inpi_index_id_siret > 1 THEN 'TO_REMOVE'
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
   
  LEFT JOIN regles_tests 
  ON  test.status_cas = regles_tests.status_cas 
  AND test.test_list_num_voie = regles_tests.test_list_num_voie 
  AND test.test_siege = regles_tests.test_siege 
  AND test.test_enseigne = regles_tests.test_enseigne    
  )
 )
"""
output = athena.run_query(
        query=create_table,
        database='inpi',
        s3_output='INPI/sql_output'
    )


df_ = (pd.DataFrame(data = {'index_unique': range(1,21)})
       .to_csv('index_20.csv', index = False)
      )

s3.upload_file(file_to_upload = 'index_20.csv',
            destination_in_s3 = 'TEMP_ANALYSE_SIRETISATION/INDEX_20')


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


query = """
SELECT status_cas, COUNT(*) as count
FROM ets_inpi_insee_cases 
GROUP BY status_cas
"""


query = """
SELECT status_cas, COUNT(DISTINCT(index_id)) as distinct_ets
FROM ets_inpi_insee_cases 
GROUP BY status_cas
ORDER BY status_cas
"""


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


query = """
SELECT 
  approx_percentile(sum_enseigne, ARRAY[0.25,0.50,0.75,.80,.85,.86,.87, .88, .89,.90,0.95, 0.99]) as sum_enseigne
FROM 
  ets_inpi_insee_cases 
"""



query = """
SELECT COUNT(DISTINCT(index_id))
FROM ets_inpi_insee_cases 
"""

output = athena.run_query(
            query=query,
            database='inpi',
            s3_output='INPI/sql_output'
        )

results = False
filename = 'index_a_trouver.csv'

while results get_ipython().getoutput("= True:")
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
    
nb_index = (s3.read_df_from_s3(
            key = 'ANALYSE_PRE_SIRETISATION/{}'.format(filename), sep = ',')
         )
nb_index.values[0][0]


query = """
SELECT index_id, sequence_id, siren, siret, count_inpi_index_id_siret,
list_inpi,list_insee, inpi_except, insee_except, status_cas, test_adresse_cas_1_3_4,
index_id_dup_has_cas_1_3_4, test_duplicates_is_in_cas_1_3_4  
FROM ets_inpi_insee_cases 
WHERE index_id = 1142
"""
output = athena.run_query(
            query=query,
            database='inpi',
            s3_output='INPI/sql_output'
        )

results = False
filename_ligne = 'exemple_index_id_1142.csv'

while results get_ipython().getoutput("= True:")
    source_key = "{}/{}.csv".format(
                                'INPI/sql_output',
                                output['QueryExecutionId']
                                       )
    destination_key = "{}/{}".format(
                                    'ANALYSE_PRE_SIRETISATION',
                                    filename_ligne
                                )

    results = s3.copy_object_s3(
                                    source_key = source_key,
                                    destination_key = destination_key,
                                    remove = True
                                )
(s3.read_df_from_s3(
            key = 'ANALYSE_PRE_SIRETISATION/{}'.format(filename_ligne), sep = ',')
 .set_index(['index_id', 'sequence_id', 'siren', 'count_inpi_index_id_siret'])
         )    


query = """
SELECT index_id, sequence_id, siren, siret, count_inpi_index_id_siret, 
list_inpi,list_insee, inpi_except, insee_except, test_adresse_cas_1_3_4,
index_id_dup_has_cas_1_3_4, test_duplicates_is_in_cas_1_3_4  
FROM ets_inpi_insee_cases 
WHERE index_id = 4560
"""
output = athena.run_query(
            query=query,
            database='inpi',
            s3_output='INPI/sql_output'
        )

results = False
filename_ligne = 'exemple_index_id_4560.csv'

while results get_ipython().getoutput("= True:")
    source_key = "{}/{}.csv".format(
                                'INPI/sql_output',
                                output['QueryExecutionId']
                                       )
    destination_key = "{}/{}".format(
                                    'ANALYSE_PRE_SIRETISATION',
                                    filename_ligne
                                )

    results = s3.copy_object_s3(
                                    source_key = source_key,
                                    destination_key = destination_key,
                                    remove = True
                                )
(s3.read_df_from_s3(
            key = 'ANALYSE_PRE_SIRETISATION/{}'.format(filename_ligne), sep = ',')
 .set_index(['index_id', 'sequence_id', 'siren', 'count_inpi_index_id_siret'])
         )    


query = """
SELECT COUNT(DISTINCT(index_id))
FROM ets_inpi_insee_cases 
-- WHERE test_duplicates_is_in_cas_1_3_4 get_ipython().getoutput("=  'TO_REMOVE'")

"""

output = athena.run_query(
            query=query,
            database='inpi',
            s3_output='INPI/sql_output'
        )

results = False
filename = 'index_a_trouver.csv'

while results get_ipython().getoutput("= True:")
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
    
nb_index_before = (s3.read_df_from_s3(
            key = 'ANALYSE_PRE_SIRETISATION/{}'.format(filename), sep = ',')
         )
nb_index_before.values[0][0]


query = """
SELECT COUNT(DISTINCT(index_id))
FROM ets_inpi_insee_cases 
WHERE test_duplicates_is_in_cas_1_3_4 get_ipython().getoutput("=  'TO_REMOVE'")

"""

output = athena.run_query(
            query=query,
            database='inpi',
            s3_output='INPI/sql_output'
        )

results = False
filename = 'index_a_trouver_remove_test_duplicates_is_in_cas_1_3_4.csv'

while results get_ipython().getoutput("= True:")
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
    
nb_index_after = (s3.read_df_from_s3(
            key = 'ANALYSE_PRE_SIRETISATION/{}'.format(filename), sep = ',')
         )
nb_index_after.values[0][0]


nb_index_before.values[0][0] - nb_index_after.values[0][0] ==  0


query = """
SELECT count_inpi_index_id_siret,test_duplicates_is_in_cas_1_3_4,  COUNT(index_id) as nb_distinct
FROM ets_inpi_insee_cases
WHERE index_id_duplicate = 'True'
GROUP BY count_inpi_index_id_siret, test_duplicates_is_in_cas_1_3_4
ORDER BY count_inpi_index_id_siret, test_duplicates_is_in_cas_1_3_4
"""

output = athena.run_query(
            query=query,
            database='inpi',
            s3_output='INPI/sql_output'
        )

results = False
filename_multi = 'duplicate_test_filename_multi.csv'

while results get_ipython().getoutput("= True:")
    source_key = "{}/{}.csv".format(
                                'INPI/sql_output',
                                output['QueryExecutionId']
                                       )
    destination_key = "{}/{}".format(
                                    'ANALYSE_PRE_SIRETISATION',
                                    filename_multi
                                )

    results = s3.copy_object_s3(
                                    source_key = source_key,
                                    destination_key = destination_key,
                                    remove = True
                                )  
    
(
pd.concat([
(test_ligne
 .groupby('test_duplicates_is_in_cas_1_3_4')['nb_distinct']
 .sum()
 .to_frame()
 .T
),
    (test_ligne
 .fillna(0)
 .set_index(['count_inpi_index_id_siret', 'test_duplicates_is_in_cas_1_3_4'])
 .unstack(-1)
     .droplevel(level = 0, axis = 1)
)], axis = 0
)
    .assign( 
 total_row = lambda x : x.sum(axis = 1)
 )
.style
 .format("{:,.0f}")
)


#### Lignes
query = """
SELECT count_inpi_index_id_siret, COUNT(*) as nb_distinct
FROM ets_inpi_insee_cases 
GROUP BY count_inpi_index_id_siret
ORDER BY count_inpi_index_id_siret
"""

output = athena.run_query(
            query=query,
            database='inpi',
            s3_output='INPI/sql_output'
        )

results = False
filename_ligne = 'nb_duplicates_ligne.csv'

while results get_ipython().getoutput("= True:")
    source_key = "{}/{}.csv".format(
                                'INPI/sql_output',
                                output['QueryExecutionId']
                                       )
    destination_key = "{}/{}".format(
                                    'ANALYSE_PRE_SIRETISATION',
                                    filename_ligne
                                )

    results = s3.copy_object_s3(
                                    source_key = source_key,
                                    destination_key = destination_key,
                                    remove = True
                                )
    
##### Index
query = """
SELECT count_inpi_index_id_siret, COUNT(DISTINCT(index_id)) as nb_distinct
FROM ets_inpi_insee_cases 
GROUP BY count_inpi_index_id_siret
ORDER BY count_inpi_index_id_siret
"""

output = athena.run_query(
            query=query,
            database='inpi',
            s3_output='INPI/sql_output'
        )

results = False
filename_index = 'nb_duplicates_index.csv'

while results get_ipython().getoutput("= True:")
    source_key = "{}/{}.csv".format(
                                'INPI/sql_output',
                                output['QueryExecutionId']
                                       )
    destination_key = "{}/{}".format(
                                    'ANALYSE_PRE_SIRETISATION',
                                    filename_index
                                )

    results = s3.copy_object_s3(
                                    source_key = source_key,
                                    destination_key = destination_key,
                                    remove = True
                                )
    
test_ligne = (s3.read_df_from_s3(
            key = 'ANALYSE_PRE_SIRETISATION/{}'.format(filename_ligne), sep = ',')
         )

test_index = (s3.read_df_from_s3(
            key = 'ANALYSE_PRE_SIRETISATION/{}'.format(filename_index), sep = ',')
         )
(
pd.concat([    
 pd.concat([
    pd.concat(
    [
        test_ligne.sum().to_frame().T.rename(index = {0:'total'}), 
        test_ligne
    ], axis = 0),
    ],axis = 1,keys=["Lignes"]),
    (
 pd.concat([
    pd.concat(
    [
        test_index.sum().to_frame().T.rename(index = {0:'total'}), 
        test_index
    ], axis = 0),
    ],axis = 1,keys=["Index"])
)],axis= 1
    )
    .style
    .format("{:,.0f}")
                  .bar(subset= [
                      ('Lignes','nb_distinct'),
                      ('Index','nb_distinct'),
                      
                  ],
                       color='#d65f5f')
)


#### Lignes
query = """ 
SELECT count_inpi_index_id_siret,count_inpi_index_id_stat_cas, COUNT(index_id) as nb_distinct
FROM ets_inpi_insee_cases 
GROUP BY count_inpi_index_id_stat_cas, count_inpi_index_id_siret
ORDER BY count_inpi_index_id_siret, count_inpi_index_id_stat_cas
"""
output = athena.run_query(
            query=query,
            database='inpi',
            s3_output='INPI/sql_output'
        )

results = False
filename_ligne = 'nb_cas_per_duplicate_ligne.csv'

while results get_ipython().getoutput("= True:")
    source_key = "{}/{}.csv".format(
                                'INPI/sql_output',
                                output['QueryExecutionId']
                                       )
    destination_key = "{}/{}".format(
                                    'ANALYSE_PRE_SIRETISATION',
                                    filename_ligne
                                )

    results = s3.copy_object_s3(
                                    source_key = source_key,
                                    destination_key = destination_key,
                                    remove = True
                                )

#### Index
query = """ 
SELECT count_inpi_index_id_siret,count_inpi_index_id_stat_cas, COUNT(Distinct(index_id)) as nb_distinct
FROM ets_inpi_insee_cases 
GROUP BY count_inpi_index_id_stat_cas, count_inpi_index_id_siret
ORDER BY count_inpi_index_id_siret, count_inpi_index_id_stat_cas
"""
output = athena.run_query(
            query=query,
            database='inpi',
            s3_output='INPI/sql_output'
        )

results = False
filename_index = 'nb_cas_per_duplicate_index.csv'

while results get_ipython().getoutput("= True:")
    source_key = "{}/{}.csv".format(
                                'INPI/sql_output',
                                output['QueryExecutionId']
                                       )
    destination_key = "{}/{}".format(
                                    'ANALYSE_PRE_SIRETISATION',
                                    filename_index
                                )

    results = s3.copy_object_s3(
                                    source_key = source_key,
                                    destination_key = destination_key,
                                    remove = True
                                )
test_ligne = s3.read_df_from_s3(
            key = 'ANALYSE_PRE_SIRETISATION/{}'.format(filename_ligne), sep = ',')    
test_index = s3.read_df_from_s3(
            key = 'ANALYSE_PRE_SIRETISATION/{}'.format(filename_index), sep = ',')

(
    pd.concat([
pd.concat([    
pd.concat([
(
 test_ligne
 .set_index(['count_inpi_index_id_siret','count_inpi_index_id_stat_cas'])
 .unstack(-1)
 .sum()
 .to_frame()
 .unstack(-1)
 .droplevel(level = 0, axis=1)
),
(
 test_ligne
 .set_index(['count_inpi_index_id_siret','count_inpi_index_id_stat_cas'])
 .unstack(-1)
 .fillna(0)
    .droplevel(level = 0, axis=1)
)], axis = 0)
    .assign( 
 total_row = lambda x : x.sum(axis = 1)
 )],axis = 1, keys=["Lignes"]
    ),

pd.concat([    
pd.concat([
(
 test_index
 .set_index(['count_inpi_index_id_siret','count_inpi_index_id_stat_cas'])
 .unstack(-1)
 .sum()
 .to_frame()
 .unstack(-1)
 .droplevel(level = 0, axis=1)
),
(
 test_index
 .set_index(['count_inpi_index_id_siret','count_inpi_index_id_stat_cas'])
 .unstack(-1)
 .fillna(0)
    .droplevel(level = 0, axis=1)
)], axis = 0)
    .assign( 
 total_row = lambda x : x.sum(axis = 1)
 )],axis = 1, keys=["Index"])],
        axis= 1)
    .style
    .format("{:,.0f}")
                  .bar(subset= [
                      ('Lignes','total_row'),
                      ('Index','total_row'),
                      
                  ],
                       color='#d65f5f')
           
)


### Lignes
query = """ 
SELECT count_inpi_index_id_siret,status_cas, COUNT(index_id) as nb_distinct
FROM ets_inpi_insee_cases 
GROUP BY status_cas, count_inpi_index_id_siret
ORDER BY count_inpi_index_id_siret, status_cas
"""
output = athena.run_query(
            query=query,
            database='inpi',
            s3_output='INPI/sql_output'
        )

results = False
filename_ligne = 'nb_cas_per_duplicate_ligne.csv'

while results get_ipython().getoutput("= True:")
    source_key = "{}/{}.csv".format(
                                'INPI/sql_output',
                                output['QueryExecutionId']
                                       )
    destination_key = "{}/{}".format(
                                    'ANALYSE_PRE_SIRETISATION',
                                    filename_ligne
                                )

    results = s3.copy_object_s3(
                                    source_key = source_key,
                                    destination_key = destination_key,
                                    remove = True
                                )
### index
query = """ 
SELECT count_inpi_index_id_siret,status_cas, COUNT(DISTINCT(index_id)) as nb_distinct
FROM ets_inpi_insee_cases 
GROUP BY status_cas, count_inpi_index_id_siret
ORDER BY count_inpi_index_id_siret, status_cas
"""
output = athena.run_query(
            query=query,
            database='inpi',
            s3_output='INPI/sql_output'
        )

results = False
filename_index = 'nb_cas_per_duplicate_index.csv'

while results get_ipython().getoutput("= True:")
    source_key = "{}/{}.csv".format(
                                'INPI/sql_output',
                                output['QueryExecutionId']
                                       )
    destination_key = "{}/{}".format(
                                    'ANALYSE_PRE_SIRETISATION',
                                    filename_index
                                )

    results = s3.copy_object_s3(
                                    source_key = source_key,
                                    destination_key = destination_key,
                                    remove = True
                                )
    
test_ligne = s3.read_df_from_s3(
            key = 'ANALYSE_PRE_SIRETISATION/{}'.format(filename_ligne), sep = ',')    
test_index = s3.read_df_from_s3(
            key = 'ANALYSE_PRE_SIRETISATION/{}'.format(filename_index), sep = ',')

(
    pd.concat([
pd.concat([    
pd.concat([
(
 test_ligne
 .set_index(['count_inpi_index_id_siret','status_cas'])
 .unstack(-1)
 .sum()
 .to_frame()
 .unstack(-1)
 .droplevel(level = 0, axis=1)
),
(
 test_ligne
 .set_index(['count_inpi_index_id_siret','status_cas'])
 .unstack(-1)
 .fillna(0)
    .droplevel(level = 0, axis=1)
)], axis = 0)
    .assign( 
 total_row = lambda x : x.sum(axis = 1)
 )],axis = 1, keys=["Lignes"]
    ),

pd.concat([    
pd.concat([
(
 test_index
 .set_index(['count_inpi_index_id_siret','status_cas'])
 .unstack(-1)
 .sum()
 .to_frame()
 .unstack(-1)
 .droplevel(level = 0, axis=1)
),
(
 test_index
 .set_index(['count_inpi_index_id_siret','status_cas'])
 .unstack(-1)
 .fillna(0)
    .droplevel(level = 0, axis=1)
)], axis = 0)
    .assign( 
 total_row = lambda x : x.sum(axis = 1)
 )],axis = 1, keys=["Index"])],
        axis= 1)
    .style
    .format("{:,.0f}")
                  .bar(subset= [
                      ('Lignes','total_row'),
                      ('Index','total_row'),
                      
                  ],
                       color='#d65f5f')
           
)  


#### Lignes
query = """
SELECT status_cas,count_inpi_index_id_siret, COUNT(index_id) as nb_distinct
FROM ets_inpi_insee_cases
WHERE test_siren_insee_siren_inpi get_ipython().getoutput("= 'False'  ")
GROUP BY status_cas, count_inpi_index_id_siret
ORDER BY status_cas

"""
output = athena.run_query(
            query=query,
            database='inpi',
            s3_output='INPI/sql_output'
        )

results = False
filename_ligne = 'nb_duplicate_par_cas_test_siren_insee_siren_inpi_ligne.csv'

while results get_ipython().getoutput("= True:")
    source_key = "{}/{}.csv".format(
                                'INPI/sql_output',
                                output['QueryExecutionId']
                                       )
    destination_key = "{}/{}".format(
                                    'ANALYSE_PRE_SIRETISATION',
                                    filename_ligne
                                )

    results = s3.copy_object_s3(
                                    source_key = source_key,
                                    destination_key = destination_key,
                                    remove = True
                                )
#### index
query = """
SELECT status_cas,count_inpi_index_id_siret, COUNT(DISTINCT(index_id)) as nb_distinct
FROM ets_inpi_insee_cases
WHERE test_siren_insee_siren_inpi get_ipython().getoutput("= 'False'  ")
GROUP BY status_cas, count_inpi_index_id_siret
ORDER BY status_cas

"""
output = athena.run_query(
            query=query,
            database='inpi',
            s3_output='INPI/sql_output'
        )

results = False
filename_index = 'nb_duplicate_par_cas_test_siren_insee_siren_inpi_index.csv'

while results get_ipython().getoutput("= True:")
    source_key = "{}/{}.csv".format(
                                'INPI/sql_output',
                                output['QueryExecutionId']
                                       )
    destination_key = "{}/{}".format(
                                    'ANALYSE_PRE_SIRETISATION',
                                    filename_index
                                )

    results = s3.copy_object_s3(
                                    source_key = source_key,
                                    destination_key = destination_key,
                                    remove = True
                                )  
    
test_ligne = s3.read_df_from_s3(
            key = 'ANALYSE_PRE_SIRETISATION/{}'.format(filename_ligne), sep = ',')    
test_index = s3.read_df_from_s3(
            key = 'ANALYSE_PRE_SIRETISATION/{}'.format(filename_index), sep = ',')

(
    pd.concat([
pd.concat([    
pd.concat([
(
 test_ligne
 .set_index(['count_inpi_index_id_siret','status_cas'])
 .unstack(-1)
 .sum()
 .to_frame()
 .unstack(-1)
 .droplevel(level = 0, axis=1)
),
(
 test_ligne
 .set_index(['count_inpi_index_id_siret','status_cas'])
 .unstack(-1)
 .fillna(0)
    .droplevel(level = 0, axis=1)
)], axis = 0)
    .assign( 
 total_row = lambda x : x.sum(axis = 1)
 )],axis = 1, keys=["Lignes"]
    ),

pd.concat([    
pd.concat([
(
 test_index
 .set_index(['count_inpi_index_id_siret','status_cas'])
 .unstack(-1)
 .sum()
 .to_frame()
 .unstack(-1)
 .droplevel(level = 0, axis=1)
),
(
 test_index
 .set_index(['count_inpi_index_id_siret','status_cas'])
 .unstack(-1)
 .fillna(0)
    .droplevel(level = 0, axis=1)
)], axis = 0)
    .assign( 
 total_row = lambda x : x.sum(axis = 1)
 )],axis = 1, keys=["Index"])],
        axis= 1)
    .style
    .format("{:,.0f}")
                  .bar(subset= [
                      ('Lignes','total_row'),
                      ('Index','total_row'),
                      
                  ],
                       color='#d65f5f')
           
)      


top_1 = """
SELECT 
index_unique, groups,
"""
top_2 = " FROM index_20_true "

middle_1 = ""

middle_2 =  """

-- {0}

LEFT JOIN (

SELECT {0}, count_inpi_index_id_siret, COUNT(index_id) as nb_dict_{0}
FROM ets_inpi_insee_cases
WHERE test_list_num_voie get_ipython().getoutput("= 'False'  ")
GROUP BY {0}, count_inpi_index_id_siret
  ) as nb_{0}
ON index_20_true.index_unique = nb_{0}.count_inpi_index_id_siret AND
index_20_true.groups = nb_{0}.{0}

"""

bottom = "ORDER BY index_unique, groups"

tests = [
    "test_sequence_siret",
    "test_index_siret",
    "test_siren_insee_siren_inpi",
    "test_sequence_siret_many_cas",
    "test_list_num_voie",
    "test_date",
    "test_status_admin",
    "test_siege",
    "test_code_commune",
    "test_type_voie",
    "test_enseigne"] 

for i, test in enumerate(tests):
    var = 'nb_dict_{}'.format(test)
    if i == len(tests) -1:
        top_1 += '{}'.format(var)
    else:
        top_1+='{},'.format(var)
        
    middle_1+= middle_2.format(test)

query = top_1 + top_2 + middle_1 + bottom

output = athena.run_query(
            query=query,
            database='inpi',
            s3_output='INPI/sql_output'
        )

results = False
filename_ligne = 'nb_duplicate_par_cas_list_true_lignes_tests.csv'

while results get_ipython().getoutput("= True:")
    source_key = "{}/{}.csv".format(
                                'INPI/sql_output',
                                output['QueryExecutionId']
                                       )
    destination_key = "{}/{}".format(
                                    'ANALYSE_PRE_SIRETISATION',
                                    filename_ligne
                                )

    results = s3.copy_object_s3(
                                    source_key = source_key,
                                    destination_key = destination_key,
                                    remove = True
                                )  
    
test_ligne = s3.read_df_from_s3(
            key = 'ANALYSE_PRE_SIRETISATION/{}'.format(filename_ligne), sep = ',')  
(test_ligne
 .assign(groups = lambda x: x['groups'].fillna('NULL'))
 .fillna(0)
 .set_index(['index_unique', 'groups'])
 .unstack(-1)
 .style
 .format("{:,.0f}")
)


top_1 = """
SELECT 
index_unique, groups,
"""
top_2 = " FROM index_20_true "

middle_1 = ""

middle_2 =  """

-- {0}

LEFT JOIN (

SELECT {0}, count_inpi_index_id_siret, COUNT(DISTINCT(index_id)) as nb_dict_{0}
FROM ets_inpi_insee_cases
WHERE test_list_num_voie get_ipython().getoutput("= 'False'  ")
GROUP BY {0}, count_inpi_index_id_siret
  ) as nb_{0}
ON index_20_true.index_unique = nb_{0}.count_inpi_index_id_siret AND
index_20_true.groups = nb_{0}.{0}

"""

bottom = "ORDER BY index_unique, groups"

tests = [
    "test_sequence_siret",
    "test_index_siret",
    "test_siren_insee_siren_inpi",
    "test_sequence_siret_many_cas",
    "test_list_num_voie",
    "test_date",
    "test_status_admin",
    "test_siege",
    "test_code_commune",
    "test_type_voie",
    "test_enseigne"] 

for i, test in enumerate(tests):
    var = 'nb_dict_{}'.format(test)
    if i == len(tests) -1:
        top_1 += '{}'.format(var)
    else:
        top_1+='{},'.format(var)
        
    middle_1+= middle_2.format(test)

query = top_1 + top_2 + middle_1 + bottom

output = athena.run_query(
            query=query,
            database='inpi',
            s3_output='INPI/sql_output'
        )

results = False
filename_index = 'nb_duplicate_par_cas_list_true_index_tests.csv'

while results get_ipython().getoutput("= True:")
    source_key = "{}/{}.csv".format(
                                'INPI/sql_output',
                                output['QueryExecutionId']
                                       )
    destination_key = "{}/{}".format(
                                    'ANALYSE_PRE_SIRETISATION',
                                    filename_index
                                )

    results = s3.copy_object_s3(
                                    source_key = source_key,
                                    destination_key = destination_key,
                                    remove = True
                                )  
    
test_ligne = s3.read_df_from_s3(
            key = 'ANALYSE_PRE_SIRETISATION/{}'.format(filename_index), sep = ',')  
(test_ligne
 .assign(groups = lambda x: x['groups'].fillna('NULL'))
 .fillna(0)
 .set_index(['index_unique', 'groups'])
 .unstack(-1)
 .style
 .format("{:,.0f}")
)


top_1 = """
SELECT 
index_unique, groups,
"""
top_2 = " FROM index_20_true "

middle_1 = ""

middle_2 =  """

-- {0}

LEFT JOIN (

SELECT {0}, count_inpi_index_id_siret, COUNT(index_id) as nb_dict_{0}
FROM ets_inpi_insee_cases
WHERE test_siren_insee_siren_inpi get_ipython().getoutput("= 'False'  ")
GROUP BY {0}, count_inpi_index_id_siret
  ) as nb_{0}
ON index_20_true.index_unique = nb_{0}.count_inpi_index_id_siret AND
index_20_true.groups = nb_{0}.{0}

"""

bottom = "ORDER BY index_unique, groups"

tests = [
    #"test_sequence_siret",
    "test_index_siret",
    "test_siren_insee_siren_inpi",
    "test_sequence_siret_many_cas",
    "test_list_num_voie",
    "test_date",
    "test_status_admin",
    "test_siege",
    "test_code_commune",
    "test_type_voie",
    "test_enseigne"] 

for i, test in enumerate(tests):
    var = 'nb_dict_{}'.format(test)
    if i == len(tests) -1:
        top_1 += '{}'.format(var)
    else:
        top_1+='{},'.format(var)
        
    middle_1+= middle_2.format(test)

query = top_1 + top_2 + middle_1 + bottom

output = athena.run_query(
            query=query,
            database='inpi',
            s3_output='INPI/sql_output'
        )

results = False
filename_ligne = 'nb_duplicate_test_siren_insee_siren_inpi_tests.csv'

while results get_ipython().getoutput("= True:")
    source_key = "{}/{}.csv".format(
                                'INPI/sql_output',
                                output['QueryExecutionId']
                                       )
    destination_key = "{}/{}".format(
                                    'ANALYSE_PRE_SIRETISATION',
                                    filename_ligne
                                )

    results = s3.copy_object_s3(
                                    source_key = source_key,
                                    destination_key = destination_key,
                                    remove = True
                                )  
    
test_ligne = s3.read_df_from_s3(
            key = 'ANALYSE_PRE_SIRETISATION/{}'.format(filename_ligne), sep = ',')  
(test_ligne
 .assign(groups = lambda x: x['groups'].fillna('NULL'))
 .fillna(0)
 .set_index(['index_unique', 'groups'])
 .unstack(-1)
 .style
 .format("{:,.0f}")
)


top_1 = """
SELECT 
index_unique, groups,
"""
top_2 = " FROM index_20_true "

middle_1 = ""

middle_2 =  """

-- {0}

LEFT JOIN (

SELECT {0}, count_inpi_index_id_siret, COUNT(index_id) as nb_dict_{0}
FROM ets_inpi_insee_cases
WHERE index_id_duplicate = 'True' AND test_adresse_cas_1_3_4 = 'True'  
GROUP BY {0}, count_inpi_index_id_siret
  ) as nb_{0}
ON index_20_true.index_unique = nb_{0}.count_inpi_index_id_siret AND
index_20_true.groups = nb_{0}.{0}

"""

bottom = "ORDER BY index_unique, groups"

tests = [
    #"test_sequence_siret",
    "test_index_siret",
    "test_siren_insee_siren_inpi",
    "test_sequence_siret_many_cas",
    "test_list_num_voie",
    "test_date",
    "test_status_admin",
    "test_siege",
    "test_code_commune",
    "test_type_voie",
    "test_enseigne"] 

for i, test in enumerate(tests):
    var = 'nb_dict_{}'.format(test)
    if i == len(tests) -1:
        top_1 += '{}'.format(var)
    else:
        top_1+='{},'.format(var)
        
    middle_1+= middle_2.format(test)

query = top_1 + top_2 + middle_1 + bottom

output = athena.run_query(
            query=query,
            database='inpi',
            s3_output='INPI/sql_output'
        )

results = False
filename_ligne = 'duplicate_test_complementaire_match_adresse_full.csv'

while results get_ipython().getoutput("= True:")
    source_key = "{}/{}.csv".format(
                                'INPI/sql_output',
                                output['QueryExecutionId']
                                       )
    destination_key = "{}/{}".format(
                                    'ANALYSE_PRE_SIRETISATION',
                                    filename_ligne
                                )

    results = s3.copy_object_s3(
                                    source_key = source_key,
                                    destination_key = destination_key,
                                    remove = True
                                )  
    
test_ligne = s3.read_df_from_s3(
            key = 'ANALYSE_PRE_SIRETISATION/{}'.format(filename_ligne), sep = ',')  
(test_ligne
 .assign(groups = lambda x: x['groups'].fillna('NULL'))
 .fillna(0)
 .set_index(['index_unique', 'groups'])
 .unstack(-1)
 .style
 .format("{:,.0f}")
)


import os, time, shutil, urllib, ipykernel, json
from pathlib import Path
from notebook import notebookapp


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


create_report(extension = "html")
