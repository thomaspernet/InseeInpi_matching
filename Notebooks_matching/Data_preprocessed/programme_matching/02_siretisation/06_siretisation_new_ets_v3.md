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
    * Adresse 
      * Numéro de voie
        * numerovoieetablissement 
        * numero_voie_matching 
      * Type de voie
        * typevoieetablissement 
        * type_voie_matching 
  
Metadata

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
import os 
os.getcwd()
```

```python
from awsPy.aws_authorization import aws_connector
from awsPy.aws_athena import service_athena
from awsPy.aws_s3 import service_s3
from pathlib import Path
import pandas as pd
import os, shutil
bucket = 'calfdata'
path = os.getcwd()
parent_path = str(Path(path).parent)
path_cred = r"{}/credential_AWS.json".format(parent_path)
con = aws_connector.aws_instantiate(credential = path_cred,
                                       region = 'eu-west-3')
client= con.client_boto()
s3 = service_s3.connect_S3(client = client,
                      bucket = 'calfdata') 
athena = service_athena.connect_athena(client = client,
                      bucket = 'calfdata') 
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

| Cas de figure | Titre                                                              | Total   | Total cumulé | pourcentage | Pourcentage cumulé | Comment                 |
|---------------|--------------------------------------------------------------------|---------|--------------|-------------|--------------------|-------------------------|
| 1             | similarité parfaite                                                | 7774986 | 7774986      | 0.67        | 0.67               | Match parfait           |
| 2             | Dissimilarité parfaite                                             | 974727  | 8749713      | 0.08        | 0.75               | Exclusion parfaite      |
| 3             | Intersection parfaite INPI                                         | 407320  | 9157033      | 0.035       | 0.78               | Match partiel parfait   |
| 4             | Intersection parfaite INSEE                                        | 558956  | 9715989      | 0.048       | 0.83               | Match partiel parfait   |
| 5             | Cardinality exception parfaite INSEE INPI, intersection positive   | 1056522 | 10772511     | 0.091       | 0.92               | Match partiel compliqué |
| 6             | Cardinality exception INSEE supérieure INPI, intersection positive | 361353  | 11133864     | 0.03        | 0.95               | Match partiel compliqué |
| 7             | Cardinality exception INPI supérieure INSEE, intersection positive | 466687  | 11600551     | 0.04        | 1                  | Match partiel compliqué |


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
cas_1 =  7774986
cas_1 / initial_obs
```

```python
query = """
WITH test_proba AS (
  SELECT 
    array_distinct(
      split(adresse_distance_inpi, ' ')
    ) as list_inpi,
  
    array_distinct(
      split(adresse_distance_insee, ' ')
    ) as list_insee, 
  
  array_distinct(
              array_except(
                split(adresse_distance_insee, ' '), 
                split(adresse_distance_inpi, ' ')
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
  SELECT count(*) 
  FROM test_proba
  WHERE intersection = union_
  -- 7 774 986
"""
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
cas_2 =974727

cas_2/initial_obs 
```

```python
cas_1 +cas_2
```

```python
(cas_1 +cas_2)/initial_obs
```

```python
query = """
WITH test_proba AS (
  SELECT 
    array_distinct(
      split(adresse_distance_inpi, ' ')
    ) as list_inpi,
  
    array_distinct(
      split(adresse_distance_insee, ' ')
    ) as list_insee, 
  
  array_distinct(
              array_except(
                split(adresse_distance_insee, ' '), 
                split(adresse_distance_inpi, ' ')
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
  SELECT count(*) 
  FROM test_proba
  WHERE intersection = 0
"""
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

- Nombre d'observation:407320
    - Percentage initial: 0.03

```python
cas_3 = 407320
cas_3/ initial_obs
```

```python
cas_1 + cas_2 +cas_3
```

```python
(cas_1 + cas_2 +cas_3)/initial_obs
```

```python
query = """
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
WHERE lenght_list_inpi = intersection AND intersection != union_
-- LIMIT 10 
--  10 -- WHERE intersection = 0

"""
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
cas_4 = 558956
cas_4 / initial_obs
```

```python
cas_1 + cas_2 + cas_3 + cas_4
```

```python
(cas_1 + cas_2 + cas_3 + cas_4) / initial_obs
```

```python
query = """
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
WHERE lenght_list_insee = intersection AND intersection != union_
-- LIMIT 10 
--  10 -- WHERE intersection = 0

"""
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
cas_5 = 1056522
cas_5/ initial_obs
```

```python
cas_1 + cas_2 + cas_3 + cas_4 + cas_5
```

```python
(cas_1 + cas_2 + cas_3 + cas_4 + cas_5)/initial_obs
```

```python
query = """
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
-- *
   count(*) 
FROM 
  test_proba 
WHERE cardinality(insee_except) = cardinality(inpi_except) AND intersection != 0 AND cardinality(insee_except) > 0
--    
--  10 -- WHERE intersection = 0

"""
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
cas_6 = 361353
cas_6/ initial_obs
```

```python
cas_1 + cas_2 +cas_3 +cas_4 + cas_5 +cas_6
```

```python
(cas_1 + cas_2 +cas_3 +cas_4 + cas_5 +cas_6)/ initial_obs
```

```python
query = """
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
-- *
   count(*) 
FROM 
  test_proba 
WHERE cardinality(insee_except) > cardinality(inpi_except) AND intersection != 0 AND cardinality(insee_except) > 0 AND cardinality(inpi_except) > 0
--    
--  10 -- WHERE intersection = 0

"""
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
cas_7 = 466687
cas_7 / initial_obs
```

```python
cas_1 + cas_2 + cas_3 + cas_4 + cas_5+ cas_6 + cas_7
```

```python
(cas_1 + cas_2 + cas_3 + cas_4 + cas_5+ cas_6 + cas_7)/initial_obs
```

```python
query = """
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
-- *
   count(*) 
FROM 
  test_proba 
WHERE cardinality(insee_except) < cardinality(inpi_except) AND intersection != 0 AND cardinality(insee_except) > 0 AND cardinality(inpi_except) > 0
--    
--  10 -- WHERE intersection = 0

"""
```

# Parametres et fonctions

- `split_duplication`: Split un dataframe si l'index (la variable, pas l'index) contient des doublons
- `find_regex`: Performe une recherche regex entre deux colonnes
- `jackard_distance`: Calcul l'indice de dissimilarité entre deux colonnes
- `edit_distance`: Calcul le nombre de modification a faire pour obtenir la même séquence
- `import_dask`: Charge csv en Dask DataFrame pour clusteriser les calculs 


# Detail steps


## Etape 1: rapprochement INSEE-INPI

*  Rapprocher la table de l’INSEE avec celle de l’INPI avec les variables de matching suivantes:
   * `siren`
   * `ville_matching`  → `ville_matching`
   * `code_postal_matching`  → `codepostaletablissement`
* La première query consiste à rapprocher les deux tables INPI & INSEE [NEW]

```python
query = """

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
  ) as insee
ON ets_final_sql.siren = insee.siren
AND ets_final_sql.ville_matching = insee.ville_matching
AND ets_final_sql.code_postal_matching = insee.codepostaletablissement
WHERE 
  status != 'IGNORE'
"""
```

### test Acceptance 

* Description of the rule(s) to validate the US

  1.  Compter le nombre d’observations à l’INPI matché à l’INSEE
  2. Compter le nombre de doublons (via index_id )
  
- Athena: 

  - [Query test 1](https://eu-west-3.console.aws.amazon.com/athena/home?region=eu-west-3#query/history/4a8a96e2-ee37-417f-93f9-4c1065e5e0b6)
  - [Query test 2](https://eu-west-3.console.aws.amazon.com/athena/home?region=eu-west-3#query/history/5c11d8ad-1b36-4ba4-b542-220e3abfa046)

```python
query = """
SELECT COUNT(*) FROM "inpi"."ets_insee_inpi"
"""
```

```python
query = """
SELECT occurrences, count(occurrences) as count_
FROM (
SELECT index_id, COUNT(*) AS occurrences
FROM "inpi"."ets_insee_inpi"
GROUP BY index_id
  )
  GROUP BY occurrences
  ORDER BY occurrences
"""
```

## Etape 2: Calcul Levenshtein edit distance

L'objectif de cette query est de calculer la *Levenshtein edit distance* entre les variables de l’enseigne.

* Enseigne
   * INSEE
     * `enseigne1etablissement` 
     * `enseigne2etablissement` 
     * `enseigne3etablissement` 
* Nom des nouvelles variables
   * Enseigne:
     * `edit_enseigne1` 
     * `edit_enseigne2` 
     * `edit_enseigne3` 
     
Par exemple, si `edit_adresse` est égal à 3, cela signifie qu'il faut 3 éditions (ajout, suppression) pour faire correspondre les deux strings.

```python
query = """
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
  adress_reconstituee_inpi,
  adress_regex_inpi,
  adress_distance_inpi, 
  adress_reconstituee_insee,
  levenshtein_distance(adress_distance_inpi, adress_reconstituee_insee) as edit_adresse,
  numerovoieetablissement, 
  numero_voie_matching,
  typevoieetablissement,
  voie_clean, 
  type_voie_matching,
  code_postal_matching, 
  ville_matching, 
  codecommuneetablissement,
  code_commune, 
  enseigne, 
  enseigne1etablissement, 
  enseigne2etablissement, 
  enseigne3etablissement,
  levenshtein_distance(enseigne, enseigne1etablissement) as edit_enseigne1,
  levenshtein_distance(enseigne, enseigne2etablissement) as edit_enseigne2,
  levenshtein_distance(enseigne, enseigne3etablissement) as edit_enseigne3
FROM "inpi"."ets_insee_inpi"
"""
```

### test Acceptance 

* Description of the rule to validate the US
  *  Calculer le nombre de fois ou la distance est égale a 0 (adresse +enseigne)
  * Donner la distribution des distances (adresse +enseigne)
    * Exclure les NA
    
- Athena: 

  - Query test 2

    - [Enseigne](https://eu-west-3.console.aws.amazon.com/athena/home?region=eu-west-3#query/history/21de44d8-10b5-449a-920a-b54e6625ce8f)

      - Attention beaucoup de 0 car champs vide dans l’enseigne

```python
query = """
SELECT
approx_percentile(edit_enseigne1, ARRAY[0.25,0.50,0.75,0.95, 0.99]) as percentiles_edit_enseigne1,
approx_percentile(edit_enseigne2, ARRAY[0.25,0.50,0.75,0.95, 0.99]) as percentiles_edit_enseigne2,
approx_percentile(edit_enseigne3, ARRAY[0.25,0.50,0.75,0.95, 0.99]) as percentiles_edit_enseigne3

FROM(
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
  adress_reconstituee_inpi,
  adress_regex_inpi,
  adress_distance_inpi, 
  adress_reconstituee_insee,
  numerovoieetablissement, 
  numero_voie_matching,
  typevoieetablissement,
  voie_clean, 
  type_voie_matching,
  code_postal_matching, 
  ville_matching, 
  codecommuneetablissement,
  code_commune, 
  enseigne, 
  enseigne1etablissement, 
  enseigne2etablissement, 
  enseigne3etablissement,
  levenshtein_distance(enseigne, enseigne1etablissement) as edit_enseigne1,
  levenshtein_distance(enseigne, enseigne2etablissement) as edit_enseigne2,
  levenshtein_distance(enseigne, enseigne3etablissement) as edit_enseigne3
FROM "inpi"."ets_insee_inpi"
)
-- WHERE edit_adresse = 0
"""
```

## Etape 2: Calcul Jaccard distance (niveau mots)

*  Calculer la distance de Jaccard entre les variables de l’enseigne
  * Enseigne
    * INPI
      * `enseigne` 
    * INSEE
      * `enseigne1etablissement` 
      * `enseigne2etablissement` 
      * `enseigne3etablissement` 
  * Nom des nouvelles variables
    * Enseigne:
      * `jaccard_enseigne1_lettre` 
      * `jaccard_enseigne2_lettre` 
      * `jaccard_enseigne3_lettre` 
      
Par exemple, si `jaccard_adresse_lettre` est égal à .10, ce la signifie qu'il y a 10% des lettre qui ne correspondent pas dans les deux strings.

```python
query = """"
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
  adress_reconstituee_inpi, 
  adress_regex_inpi, 
  adress_distance_inpi, 
  adress_reconstituee_insee, 
  numerovoieetablissement, 
  numero_voie_matching, 
  typevoieetablissement, 
  voie_clean, 
  type_voie_matching, 
  code_postal_matching, 
  ville_matching, 
  codecommuneetablissement, 
  code_commune, 
  enseigne, 
  enseigne1etablissement, 
  enseigne2etablissement, 
  enseigne3etablissement,
  1- CAST(
    cardinality(
      array_intersect(
        regexp_extract_all(enseigne, '(\d+)|([A-Z])'), 
        regexp_extract_all(enseigne1etablissement, '(\d+)|([A-Z])')
      )
    ) AS DECIMAL(10, 2)
  ) / NULLIF(
    CAST(
      cardinality(
        array_union(
          regexp_extract_all(enseigne, '(\d+)|([A-Z])'), 
          regexp_extract_all(enseigne1etablissement, '(\d+)|([A-Z])')
        )
      ) AS DECIMAL(10, 2)
    ), 
    0
  ) as jaccard_enseigne1_lettre,
  1- CAST(
    cardinality(
      array_intersect(
        regexp_extract_all(enseigne, '(\d+)|([A-Z])'), 
        regexp_extract_all(enseigne2etablissement, '(\d+)|([A-Z])')
      )
    ) AS DECIMAL(10, 2)
  ) / NULLIF(
    CAST(
      cardinality(
        array_union(
          regexp_extract_all(enseigne, '(\d+)|([A-Z])'), 
          regexp_extract_all(enseigne2etablissement, '(\d+)|([A-Z])')
        )
      ) AS DECIMAL(10, 2)
    ), 
    0
  ) as jaccard_enseigne2_lettre,
  1- CAST(
    cardinality(
      array_intersect(
        regexp_extract_all(enseigne, '(\d+)|([A-Z])'), 
        regexp_extract_all(enseigne3etablissement, '(\d+)|([A-Z])')
      )
    ) AS DECIMAL(10, 2)
  ) / NULLIF(
    CAST(
      cardinality(
        array_union(
          regexp_extract_all(enseigne, '(\d+)|([A-Z])'), 
          regexp_extract_all(enseigne3etablissement, '(\d+)|([A-Z])')
        )
      ) AS DECIMAL(10, 2)
    ), 
    0
  ) as jaccard_enseigne3_lettre
FROM 
  ets_insee_inpi 
""""    
```

### test Acceptance 

* Description of the rule to validate the US
  *  Calculer le nombre de fois ou la distance est égale a 0 (adresse +enseigne)
  * Donner la distribution des distances (adresse +enseigne)
    * Exclure les NA
    
- Athena: 

  - Query test 1

    - [Adresse](https://eu-west-3.console.aws.amazon.com/athena/home?region=eu-west-3#query/history/d5891641-fd2e-4840-88c3-4355c12481cc)

  - Query test 2

    - [Enseigne](https://eu-west-3.console.aws.amazon.com/athena/home?region=eu-west-3#query/history/21de44d8-10b5-449a-920a-b54e6625ce8f)

      - Attention beaucoup de 0 car champs vide dans l’enseigne


# Archive


## Etape 2: Calcul Levenshtein edit distance

L'objectif de cette query est de calculer la *Levenshtein edit distance* entre les variables de l’adresse et les variables de l’enseigne.

* Adresse:
   * INPI: 
     * `adress_distance_inpi`: Clean via **article**, **accent** et **espace**
   * INSEE:
     * `adress_reconstituee_insee`: Clean via **article**
* Enseigne
   * INPI
     * `enseigne` 
   * INSEE
     * `enseigne1etablissement` 
     * `enseigne2etablissement` 
     * `enseigne3etablissement` 
* Nom des nouvelles variables
   * Adresse:
     * `edit_adresse` 
   * Enseigne:
     * `edit_enseigne1` 
     * `edit_enseigne2` 
     * `edit_enseigne3` 
     
Par exemple, si `edit_adresse` est égal à 3, cela signifie qu'il faut 3 éditions (ajout, suppression) pour faire correspondre les deux strings.

```python
query = """
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
  adress_reconstituee_inpi,
  adress_regex_inpi,
  adress_distance_inpi, 
  adress_reconstituee_insee,
  levenshtein_distance(adress_distance_inpi, adress_reconstituee_insee) as edit_adresse,
  numerovoieetablissement, 
  numero_voie_matching,
  typevoieetablissement,
  voie_clean, 
  type_voie_matching,
  code_postal_matching, 
  ville_matching, 
  codecommuneetablissement,
  code_commune, 
  enseigne, 
  enseigne1etablissement, 
  enseigne2etablissement, 
  enseigne3etablissement,
  levenshtein_distance(enseigne, enseigne1etablissement) as edit_enseigne1,
  levenshtein_distance(enseigne, enseigne2etablissement) as edit_enseigne2,
  levenshtein_distance(enseigne, enseigne3etablissement) as edit_enseigne3
FROM "inpi"."ets_insee_inpi"
"""
```

### test Acceptance 

* Description of the rule to validate the US
  *  Calculer le nombre de fois ou la distance est égale a 0 (adresse +enseigne)
  * Donner la distribution des distances (adresse +enseigne)
    * Exclure les NA
    
- Athena: 

  - Query test 1

    - [Adresse](https://eu-west-3.console.aws.amazon.com/athena/home?region=eu-west-3#query/history/d5891641-fd2e-4840-88c3-4355c12481cc)

  - Query test 2

    - [Enseigne](https://eu-west-3.console.aws.amazon.com/athena/home?region=eu-west-3#query/history/21de44d8-10b5-449a-920a-b54e6625ce8f)

      - Attention beaucoup de 0 car champs vide dans l’enseigne

```python
query = """
SELECT
approx_percentile(edit_adresse, ARRAY[0.25,0.50,0.75,0.95, 0.99]) as percentiles_edit_adress,
approx_percentile(edit_enseigne1, ARRAY[0.25,0.50,0.75,0.95, 0.99]) as percentiles_edit_enseigne1,
approx_percentile(edit_enseigne2, ARRAY[0.25,0.50,0.75,0.95, 0.99]) as percentiles_edit_enseigne2,
approx_percentile(edit_enseigne3, ARRAY[0.25,0.50,0.75,0.95, 0.99]) as percentiles_edit_enseigne3

FROM(
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
  adress_reconstituee_inpi,
  adress_regex_inpi,
  adress_distance_inpi, 
  adress_reconstituee_insee,
  levenshtein_distance(adress_distance_inpi, adress_reconstituee_insee) as edit_adresse,
  numerovoieetablissement, 
  numero_voie_matching,
  typevoieetablissement,
  voie_clean, 
  type_voie_matching,
  code_postal_matching, 
  ville_matching, 
  codecommuneetablissement,
  code_commune, 
  enseigne, 
  enseigne1etablissement, 
  enseigne2etablissement, 
  enseigne3etablissement,
  levenshtein_distance(enseigne, enseigne1etablissement) as edit_enseigne1,
  levenshtein_distance(enseigne, enseigne2etablissement) as edit_enseigne2,
  levenshtein_distance(enseigne, enseigne3etablissement) as edit_enseigne3
FROM "inpi"."ets_insee_inpi"
)
-- WHERE edit_adresse = 0
"""
```

## Etape 2: Calcul Jaccard distance (niveau lettre)

*  Calculer la distance de Jaccard entre les variables de l’adresse (au niveau de la lettre) et les variables de l’enseigne
  * Adresse:
    * INPI: 
      * `adress_distance_inpi` 
    * INSEE:
      * `adress_reconstituee_insee` 
  * Enseigne
    * INPI
      * `enseigne` 
    * INSEE
      * `enseigne1etablissement` 
      * `enseigne2etablissement` 
      * `enseigne3etablissement` 
  * Nom des nouvelles variables
    * Adresse:
      * `jaccard_adresse_lettre` 
    * Enseigne:
      * `jaccard_enseigne1_lettre` 
      * `jaccard_enseigne2_lettre` 
      * `jaccard_enseigne3_lettre` 
      
Par exemple, si `jaccard_adresse_lettre` est égal à .10, ce la signifie qu'il y a 10% des lettre qui ne correspondent pas dans les deux strings.

```python
query = """
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
  adress_reconstituee_inpi, 
  adress_regex_inpi, 
  adress_distance_inpi, 
  adress_reconstituee_insee, 
  1- CAST(
    cardinality(
      array_intersect(
        regexp_extract_all(adress_distance_inpi, '(\d+)|([A-Z])'), 
        regexp_extract_all(adress_reconstituee_insee, '(\d+)|([A-Z])')
      )
    ) AS DECIMAL(10, 2)
  ) / NULLIF(
    CAST(
      cardinality(
        array_union(
          regexp_extract_all(adress_distance_inpi, '(\d+)|([A-Z])'), 
          regexp_extract_all(adress_reconstituee_insee, '(\d+)|([A-Z])')
        )
      ) AS DECIMAL(10, 2)
    ), 
    0
  ) as jaccard_adresse_lettre, 
  numerovoieetablissement, 
  numero_voie_matching, 
  typevoieetablissement, 
  voie_clean, 
  type_voie_matching, 
  code_postal_matching, 
  ville_matching, 
  codecommuneetablissement, 
  code_commune, 
  enseigne, 
  enseigne1etablissement, 
  enseigne2etablissement, 
  enseigne3etablissement,
  1- CAST(
    cardinality(
      array_intersect(
        regexp_extract_all(enseigne, '(\d+)|([A-Z])'), 
        regexp_extract_all(enseigne1etablissement, '(\d+)|([A-Z])')
      )
    ) AS DECIMAL(10, 2)
  ) / NULLIF(
    CAST(
      cardinality(
        array_union(
          regexp_extract_all(enseigne, '(\d+)|([A-Z])'), 
          regexp_extract_all(enseigne1etablissement, '(\d+)|([A-Z])')
        )
      ) AS DECIMAL(10, 2)
    ), 
    0
  ) as jaccard_enseigne1_lettre,
  1- CAST(
    cardinality(
      array_intersect(
        regexp_extract_all(enseigne, '(\d+)|([A-Z])'), 
        regexp_extract_all(enseigne2etablissement, '(\d+)|([A-Z])')
      )
    ) AS DECIMAL(10, 2)
  ) / NULLIF(
    CAST(
      cardinality(
        array_union(
          regexp_extract_all(enseigne, '(\d+)|([A-Z])'), 
          regexp_extract_all(enseigne2etablissement, '(\d+)|([A-Z])')
        )
      ) AS DECIMAL(10, 2)
    ), 
    0
  ) as jaccard_enseigne2_lettre,
  1- CAST(
    cardinality(
      array_intersect(
        regexp_extract_all(enseigne, '(\d+)|([A-Z])'), 
        regexp_extract_all(enseigne3etablissement, '(\d+)|([A-Z])')
      )
    ) AS DECIMAL(10, 2)
  ) / NULLIF(
    CAST(
      cardinality(
        array_union(
          regexp_extract_all(enseigne, '(\d+)|([A-Z])'), 
          regexp_extract_all(enseigne3etablissement, '(\d+)|([A-Z])')
        )
      ) AS DECIMAL(10, 2)
    ), 
    0
  ) as jaccard_enseigne3_lettre
FROM 
  ets_insee_inpi 
"""
```

### test Acceptance 

* Description of the rule to validate the US
  * Calculer le nombre de fois ou la distance est égale a 0 (adresse +enseigne)
  * Donner la distribution des distances (adresse +enseigne)
    * Exclure les NA 
  * Comparer avec Levenshtein edit distance
  
* Athena: 
  * Query test 1
  * Query test 2
  * Query test 3  

```python

```

## Etape 3: Calcul Jaccard distance (niveau mot)

*  Calculer la distance de Jaccard entre les variables de l’adresse et les variables de l’enseigne
  * Adresse:
    * INPI: 
      * `adress_distance_inpi` 
    * INSEE:
      * `adress_reconstituee_insee` 
  * Nom des nouvelles variables
    * Adresse:
      * `jaccard_adresse_mot` 
      * `difference_adresse_mot`
      
- Par exemple, si `jaccard_adresse_mot` est égal à .90, ce la signifie qu'il y a 90% des mots qui correspondent dans les deux strings.  
- Par exemple, si `difference_adresse_mot` est égal à 3, ce la signifie qu'il manque 3 mots à l'INPI pour arriver à l'adresse de l'INSEE.

```python
query = """
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
  adress_reconstituee_inpi, 
  adress_regex_inpi, 
  adress_distance_inpi, 
  adress_reconstituee_insee, 
  CAST(
    cardinality(
      array_intersect(
        split(adress_distance_inpi, ' '), 
        split(adress_reconstituee_insee, ' ')
      )
    ) AS DECIMAL(10, 2)
  ) / NULLIF(
    CAST(
      cardinality(
        array_union(
          split(adress_distance_inpi, ' '), 
          split(adress_reconstituee_insee, ' ')
        )
      ) AS DECIMAL(10, 2)
    ), 
    0
  ) as jaccard_adresse_mot, 
  CAST(
      cardinality(
        -- array_intersect(
          -- split(adress_distance_inpi, ' '), 
          split(adress_reconstituee_insee, ' ')
        --)
      ) AS DECIMAL(10, 2)
    ) - 
  CAST(
    cardinality(
      array_intersect(
        split(adress_distance_inpi, ' '), 
        split(adress_reconstituee_insee, ' ')
      )
    ) AS DECIMAL(10, 2)
  )  as difference_adresse_mots,
  numerovoieetablissement, 
  numero_voie_matching, 
  typevoieetablissement, 
  voie_clean, 
  type_voie_matching, 
  code_postal_matching, 
  ville_matching, 
  codecommuneetablissement, 
  code_commune, 
  enseigne, 
  enseigne1etablissement, 
  enseigne2etablissement, 
  enseigne3etablissement 
FROM 
  ets_insee_inpi 
"""
```

## Etape 4: Creation test regex

* Calculer la distance de Jaccard entre les variables de l’adresse 
   * Adresse:
     * INPI: 
       * `adress_regex_inpi` 
     * INSEE:
       * `adress_reconstituee_insee` 
 * Nom des nouvelles variables
   * Adresse:
     * `regex_adresse`
     
Par exemple, si `regex_adresse` est égal à true, cela signifie qu'au moins un des mots (excluant les types de voie) de l'adresse de l'INPI est présent dans l'adresse de l'INSEE. 

```python
query = """
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
  adress_reconstituee_inpi,
  adress_regex_inpi,
  adress_distance_inpi, 
  adress_reconstituee_insee,
  regexp_like(adress_reconstituee_insee, adress_regex_inpi) as regex_adresse,
  numerovoieetablissement, 
  numero_voie_matching,
  typevoieetablissement,
  voie_clean, 
  type_voie_matching,
  code_postal_matching, 
  ville_matching, 
  codecommuneetablissement,
  code_commune, 
  enseigne, 
  enseigne1etablissement, 
  enseigne2etablissement, 
  enseigne3etablissement
FROM ets_insee_inpi 
"""
```

```python

```

### Test acceptance

* Description of the rule to validate the US
  *  Compter le nombre de true/false
  * Comparer avec Levenshtein edit distance/Jaccard 
    * Lorsque regex + Edit + Jaccard  egal True + 0 +0 
    * Lorsque regex + Edit + Jaccard  egal False + 0 +0 

```python

```
