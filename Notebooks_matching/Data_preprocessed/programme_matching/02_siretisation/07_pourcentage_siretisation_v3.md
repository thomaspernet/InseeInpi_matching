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

<!-- #region -->
# Test nombre lignes siretise

Objective(s)

*   Lors de l’US 6 Union et intersection, nous avons compté séparément le nombre de lignes siretisés avec les tests unitaires, indépendamment des autres tests. Le niveau de matching avec les tests unitaires va de 39% a 70%. Dans cette US, nous allons faire le test avec une prise en compte de l’ensemble des tests, autrement dit, compter le nombre de lignes sirétisé lorsque les tests sont co-dépendant.
  * L’objectif principal étant de fournir un pourcentage avec le nombre de lignes siretisé.
  * Les tests vont suivre l’arborescence suivante:
    * Calcul des cas 1 à 7
      * Calcul des tests 
      * Calcul des duplicates sur index & séquence
        * Filtrer les lignes sans duplicate et tests (a definir les règles sur les tests)
   * ATTENTION, on exclut lorsque le test list_num_voie est faux, et que le status_cas est egal à 2. En effet, il n’est pas nécéssaire d’analyser lorsqu’au des numéros ne se trouve dans l’une des deux listes ou bien qu’aucun des mots de l’adresse n’est commun.
    * row_id
    * count_initial_insee
    * index_id, 
    * sequence_id, 
    * siren, 
    * siret,
    * count_initial_insee, 
    *  count_inpi_siren_siret, 
      * Pour un siren donné, combien de siret possible. Par exemple, si → 7, cela signifie que pour un même siren, il y a 7 siret (etb). 
      * Si cette variable est égale à count_initial_insee, potentiellement, tous les etb on été trouvé
    *  count_inpi_siren_sequence,
      * Pour un siren donné, combien de séquence (etb au sens de l’INPI) possible. Si → 3, cela signifie que pour un même siren, il y a 3 établissements au sens de l’INPI
    *  count_inpi_sequence_siret, 
      * Pour une séquence donnée, combien de siret possible. Cette variable indique le nombre de duplicate par séquence. Si → 3, cela signifie que pour la meme séquence, il y a 3 siret possible
    * count_inpi_sequence_stat_cas_siret:
      * Pour une séquence-status cas donnée, combien de siret possible. Cette variable indique le nombre de siret possible pour chaque cas. En effet, un duplicate peut se retrouver dans plusieurs cas; C’est le cas lorsque les doublons n’ont pas de relation avec l’adresse de l’INPI. Si 3 → cela signifie que pour la même séquence, appartenant au même cas, il y a trois siret possible.
      * Si la variable est différente de count_inpi_sequence_siret, cela signifie que l’adresse de l’INSEE appartient a plusieurs cas.
    * count_inpi_index_id_siret,
      * Pour un index id, combien de siret possible. Cette variable indique le nombre de ligne dupliqué 
      * Si 3 → Il y a 3 siret pour un index, donc 3 lignes dupliquées
    * count_inpi_index_id_stat_cas_siret
      * Pour une pair index id cas, combien de siret possible. Cette variable indique le nombre de ligne dupliqué pour chacun des cas
      * Si 3 → Il y a 3 siret pour un index-cas, donc 3 lignes dupliquées pour le même cas
    * count_inpi_index_id_stat_cas:
      * Nombre de cas par index.
      * Si 3 → Il y a trois cas possible pour le meme index
    * index_id_duplicate: 
    * count_inpi_index_id_siret > 1 THEN 'True' ELSE 'False'
    * Indique si la ligne est doublée
    * test_siren_insee_siren_inpi
      * count_initial_insee = count_inpi_siren_siret THEN 'True' ELSE 'False'
      * Si la variable est ‘True’ alors tous les établissements ont été trouvé
    * test_sequence_siret
      * count_inpi_sequence_siret = 1 THEN ‘True’ ELSE ‘False
      * Si la variable est ‘True’ alors, il n’y a pas de duplicate pour une séquence, candidat très probable
    * test_index_siret
      * count_inpi_index_id_stat_cas_siret = 1 THEN 'True' ELSE 'False'
      * Si la variable est true, alors, il n’y a qu”un seul cas de figure par index 
    * test_sequence_siret_many_cas:
      * count_inpi_sequence_siret = count_inpi_sequence_stat_cas_siret THEN 'True' ELSE 'False
      * test si la séquence appartient a plusieurs cas
    * list_numero_voie_matching_inpi,
    *  list_numero_voie_matching_insee,
    * intersection_numero_voie
    * union_numero_voie
    * test_list_num_voie: Numéros contenus dans list_numero_voie_matching_inpi sont dans list_numero_voie_matching_insee alors True, sinon False. Si liste Null alors Null
    *  datecreationetablissement, 
    * date_debut_activite,
    * test_date
    * etatadministratifetablissement, 
    * status_admin,
    * test_status_admin
    * etablissementsiege, 
    * status_ets,
    * test_siege
    * codecommuneetablissement, 
    * code_commune, 
    * test_code_commune
    * codepostaletablissement, 
    * code_postal_matching, 
    * numerovoieetablissement, 
    * numero_voie_matching,
    * test_numero_voie
    * typevoieetablissement, 
    *  type_voie_matching,
    * test_type_voie
    * list_inpi,  
    *   lenght_list_inpi,
    *   list_insee,
    *   lenght_list_insee,
    *   inpi_except,
    *   insee_except,
    *   intersection,
    *   union_
    - index_id_max_intersection,
    -  CASE WHEN pct_intersection = index_id_max_intersection THEN 'TRUE' ELSE 'FALSE' END AS test_pct_intersection,
    * status_cas:
      * Cas 1, 2,3,4,5,6,7
    * index_id_dup_has_cas_1_3_4
      * MAX(test_adresse_cas_1_3_4) by index_id
      * Informe si l’un des doublons de l’index peut etre trouvé via les cas 1,3 ou 4
    * test_duplicates_is_in_cas_1_3_4
      * test_adresse_cas_1_3_4 = 'True' AND index_id_dup_has_cas_1_3_4 = 'True' AND count_inpi_index_id_siret > 1 THEN 'TO_KEEP' WHEN test_adresse_cas_1_3_4 = 'False' AND index_id_dup_has_cas_1_3_4 = 'True' AND count_inpi_index_id_siret > 1 THEN 'TO_REMOVE' WHEN count_inpi_index_id_siret = 1 THEN 'NULL' ELSE 'TO_FIND' END AS test_duplicates_is_in_cas_1_3_4
      * Indique si la ligne peut etre supprimée car possibilité d’être trouvé via cas 1,3,4
    * test_adresse_cas_1_3_4:
      * test.status_cas = 'CAS_1' OR test.status_cas = 'CAS_3' OR  test.status_cas = 'CAS_4
      * indique si la ligne appartient a un full match ou pas
    * enseigne, → enelver les accents
    *  enseigne1etablissement
    * enseigne2etablissement,
    *  enseigne3etablissement, 
    * test_enseigne:  Si une des enseignes de l’INSEE est contenue dans l’INPI alors True else False, If une des enseignes INPI ou INSEE est null alors null
  * Pour fournir le taux de matching, il faut créer un ensemble de test pour chacun des cas (1 à 7).
  * Pour fournir le taux de matching, il faut créer un ensemble de test pour chacun des cas (1 à 7).

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

## Similarité entre deux adresses

Le rapprochement entre les deux tables, à savoir l’INSEE et l’INPI, va amener à la création de deux vecteurs d’adresse. Un vecteur avec des mots contenus spécifiquement à l’INSEE, et un second vecteur avec les mots de l’adresse de l’INPI. Notre objectif est de comparé ses deux vecteurs pour définir si ils sont identiques ou non. Nous avons distingué 7 cas de figures possibles entre les deux vecteurs (figure 1).

![](https://drive.google.com/uc?export=view&id=1Qj_HooHrhFYSuTsoqFbl4Vxy9tN3V5Bu)


### Sources of information  (meeting notes, Documentation, Query, URL)

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
#athena = service_athena.connect_athena(client = client,
#                      bucket = 'calfdata') 
```

```python
import seaborn as sns

cm = sns.light_palette("green", as_cmap=True)
pd.set_option('display.max_columns', None)
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
output = s3.run_query(
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
output = s3.run_query(
        query=create_table,
        database='inpi',
        s3_output='INPI/sql_output'
    )
```

# Analyse


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

Nombre d'index a trouver

```python
query = """
SELECT COUNT(DISTINCT(index_id))
FROM ets_inpi_insee_cases 
"""

output = s3.run_query(
            query=query,
            database='inpi',
            s3_output='INPI/sql_output'
        )

results = False
filename = 'index_a_trouver.csv'

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
    
nb_index = (s3.read_df_from_s3(
            key = 'ANALYSE_PRE_SIRETISATION/{}'.format(filename), sep = ',')
         )
nb_index.values[0][0]
```

# Verification nombre index post-filtre

Lors de la création de la table `ets_inpi_insee_cases`, nous avons exclu les lignes dont le `status_cas` était différent de `CAS_2`, a savoir aucun mot en commun dans l'adresse. De plus, nous avons filré toutes les lignes n'ayant aucun chiffre en commun. 

un troisième filtre peut être appliqué, lorsque la variable `index_id` a des doublons (plusieurs siret), et qu'au moins une des lignes peut être retrouvée via le cas de figure 1, 3 ou 4. 

Ci dessous un exemple d'`index_id` qui satisfait la troisième condition:

L'index 1142 a deux siret possibles, toutefois l'un des deux peut être retrouvé via le `cas_1`. La variable `test_adresse_cas_1_3_4` indique si la ligne fait partie des cas 1, 3 ou 4, alors que la variable `index_id_dup_has_cas_1_3_4` informe si la séquence à au moins une des lignes fait partie des cas 1, 3 ou 4. Le test `test_duplicates_is_in_cas_1_3_4` résume les possiblités, a savoir `TO_KEEP` si il faut garder la ligne, `TO_REMOVE` si il faut la supprimer, `TO_FIND` au cas ou la séquence ne possède pas de cas 1,3 ou 4 (cf exemple ci dessous) ou `NULL` si la séquence n'a pas de doublon.

```python
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

while results != True:
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
```

Exemple d'index id sans cas de figure 1, 3, ou 4.

```python
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

while results != True:
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
```

Le nombre d'index avant se filtre et de:

```python
query = """
SELECT COUNT(DISTINCT(index_id))
FROM ets_inpi_insee_cases 
-- WHERE test_duplicates_is_in_cas_1_3_4 !=  'TO_REMOVE'

"""

output = athena.run_query(
            query=query,
            database='inpi',
            s3_output='INPI/sql_output'
        )

results = False
filename = 'index_a_trouver.csv'

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
    
nb_index_before = (s3.read_df_from_s3(
            key = 'ANALYSE_PRE_SIRETISATION/{}'.format(filename), sep = ',')
         )
nb_index_before.values[0][0]
```

Le nombre d'index après se filtre et de:

```python
query = """
SELECT COUNT(DISTINCT(index_id))
FROM ets_inpi_insee_cases 
WHERE test_duplicates_is_in_cas_1_3_4 !=  'TO_REMOVE'

"""

output = athena.run_query(
            query=query,
            database='inpi',
            s3_output='INPI/sql_output'
        )

results = False
filename = 'index_a_trouver_remove_test_duplicates_is_in_cas_1_3_4.csv'

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
    
nb_index_after = (s3.read_df_from_s3(
            key = 'ANALYSE_PRE_SIRETISATION/{}'.format(filename), sep = ',')
         )
nb_index_after.values[0][0]
```

Le nombre d'index doit être identique. Si ce n'est pas le cas, il y a un problème

```python
nb_index_before.values[0][0] - nb_index_after.values[0][0] ==  0
```

Le tableau ci dessous récapitule le nombre de lignes selon le status de `test_duplicates_is_in_cas_1_3_4`

```python
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

while results != True:
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
```

# Nombre ligne duplicate-index

Dans le tableau ci dessous, on regarde le nombre de siret possible par index, quelque soit le cas.


Par exemple, il y a 251,612 lignes avec 2 siret possibles, qui constituent 125,694 index a trouver

```python
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

while results != True:
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

while results != True:
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
```

## Nombre de cas par index dupliqué

Le tableau ci dessous est intéréssant car il informe sur le nombre de cas de figure possible pour chacun des index dupliqués (plusieurs siret possible). Par exemple, lorsque le nombre de doublon par index est de deux, il est composé de 80,230 lignes (40,073 index) appartenant au cas de figure 1 et 171,382 lignes (85,621) au cas de figure 2.

```python
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

while results != True:
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

while results != True:
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
```

Dans le tableau précédent, nous avons regardé le nombre de cas possibles pour chacun des duplicates. Pour connaitre les cas de figure concernant les duplicates, il faut regarder le tableau ci dessous. Par exemple, il y a 16,594 lignes (14,491 index) pour lesquelles il y a deux doublons par index concernant le cas de figure 2.

Le tableau nous informe aussi de cas de figure ou l'ensemble des mots de l'adresse sont identiques, avec aussi les numéros de voie, mais il y a encore des doublons. C'est le cas pour 110,798 lignes (86,652 index). Ce cas peut être trouvé dans la colonne `CAS_1` et `status_cas` supérieur à 1. Lorsque ce genre de cas arrive, il faut appliquer d'avantage de règles que nous véront plus tard dans le notebook.  

```python
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

while results != True:
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

while results != True:
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
```

# Analyse `test_siren_insee_siren_inpi`

Dans cette partie, nous nous intéréssons aux resultats des tests lorsque `test_siren_insee_siren_inpi` n'est pas égal à `False`



## Analyse `test_siren_insee_siren_inpi`

Dans ce tableau, le nombre de lignes n'ayant pas de doublon et la méthode sur l'adresse correspond au 3 est égal à  223,608 (223,461). 

```python
#### Lignes
query = """
SELECT status_cas,count_inpi_index_id_siret, COUNT(index_id) as nb_distinct
FROM ets_inpi_insee_cases
WHERE test_siren_insee_siren_inpi != 'False'  
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

while results != True:
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
WHERE test_siren_insee_siren_inpi != 'False'  
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

while results != True:
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
```

## Analyse `test_list_num` par rapport aux autres tests

Lors de la partie précédente, nous avons mis en évidence des cas ou l'adresse peut être identique en tout point mais possède des doublons. Pour cela, il faut appliquer d'autres tests. Nous avons recensé les tests suivants:

- `test_sequence_siret`: Pertinence faible
    - `count_inpi_sequence_siret = 1 THEN ‘True’ ELSE ‘False``
    - Si la variable est ‘True’ alors, il n’y a pas de duplicate pour une séquence
- `test_index_siret`: Pertinence faible
    - `count_inpi_index_id_stat_cas_siret = 1 THEN 'True' ELSE 'False'`
    - Si la variable est true, alors, il n’y a qu”un seul cas de figure par index 
- `test_siren_insee_siren_inpi`: Pertinence elevée
    - `count_initial_insee = count_inpi_siren_siret THEN 'True' ELSE 'False'`
    - Si la variable est ‘True’ alors tous les établissements ont été trouvé
- `test_sequence_siret_many_cas`: Pertinence faible
    - `count_inpi_sequence_siret = count_inpi_sequence_stat_cas_siret THEN 'True' ELSE 'False`
    - test si la séquence appartient a plusieurs cas
- `test_date`: Pertinence moyenne
    - `WHEN datecreationetablissement = date_debut_activite THEN 'True' WHEN datecreationetablissement IS NULL 
        OR date_debut_activite IS NULL THEN 'NULL' --WHEN datecreationetablissement = '' 
        ELSE 'False'``
    - Test si la date de création de l'établissement est égale à la date de création. 
- `test_status_admin`: Pertinence moyenne
    - `WHEN etatadministratifetablissement = status_admin THEN 'True' WHEN etatadministratifetablissement IS NULL 
        OR status_admin IS NULL THEN 'NULL' WHEN etatadministratifetablissement = '' 
        OR status_admin = '' THEN 'NULL' ELSE 'False'``
    - Test si l'établissement est fermé ou non. Pas radié mais fermé. L'INSEE n'indique pas les radiations, et le fichier ETS de l'INPI n'indique pas les radiations et n'indique pas les fermetures resultants de radiation. Pour cela il faut construire la variable via la table PM ou PP.
- `test_siege`: Pertinence elevée
    - `etablissementsiege = status_ets THEN 'True' WHEN etablissementsiege IS NULL 
        OR status_ets IS NULL THEN 'NULL' WHEN etablissementsiege = '' 
        OR status_ets = '' THEN 'NULL' ELSE 'False'``
    - Test si le siret est un siège ou non. 
- `test_code_commune`: Pertinence faible
    - `codecommuneetablissement = code_commune THEN 'True' WHEN codecommuneetablissement IS NULL 
        OR code_commune IS NULL THEN 'NULL' WHEN codecommuneetablissement = '' 
        OR code_commune = '' THEN 'NULL' ELSE 'False'``
    - Test si le code commune est identique entre l'INPI et l'INSEE. Pas suffisament fiable
- `test_type_voie`: Pertinence faible
    - `numerovoieetablissement = numero_voie_matching THEN 'True' WHEN numerovoieetablissement IS NULL 
        OR numero_voie_matching IS NULL THEN 'NULL' WHEN numerovoieetablissement = '' 
        OR numero_voie_matching = '' THEN 'NULL' ELSE 'False'`
    - Test si le type de voie est identique entre les deux variables. Methode d'extraction que nous avons utilisé n'est pas suffisement pertinente
- `test_enseigne`: Pertinence moyenne
    - `WHEN cardinality(test) = 0 THEN 'NULL' WHEN enseigne = '' THEN 'NULL' WHEN temp_test_enseigne = TRUE THEN 'True' ELSE 'False'`
    - Test si l'enseigne est identique entre les variables. Aucun retraitement si ce n'est mise en majuscule et exclusion des accents. Ne regardepas les fautes d'orthographe
    
###  `test_list_num`:  Doublon ligne

Le tableau ci dessous indique que lorsque l'index id n'a pas de doublon, 6,619,308 lignes ont passé le test, 	3,770,568 ne sont pas des sièges, 5,523,137 lignes avec siège.

```python
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
WHERE test_list_num_voie != 'False'  
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

while results != True:
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
```

###  `test_list_num`:  Doublon index

Le tableau ci dessous indique que lorsque l'index id n'a pas de doublon, 1,640,499 lignes ont une divergence entre fermeture/ouverture et 7,639,370 lignes ont un status administratif identique.

```python
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
WHERE test_list_num_voie != 'False'  
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

while results != True:
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
```

## Verification `test_siren_insee_siren_inpi` 

On regarde les tests lorsque `test_siren_insee_siren_inpi` est égal à true (pas de doublon) -> Lignes

```python
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
WHERE test_siren_insee_siren_inpi != 'False'  
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

while results != True:
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
```

## Analyse CAS 1,3,4 et doublons index

Il y a environ 150k lignes qui ont un match parfait de l'adresse, des numéros de voie mais qui ont plusieurs siret. Dans cette partie, nous allons regarder les tests complémentaires pour determiner combien peuvent être récupérer.

```python
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

while results != True:
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
