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
# INSEE ETS ajout nouvelles variables

This notebook has been generated on 18/07/2020

L'objectif de ce notebook est de préparer la data de l'INSEE en vue de siretiser la séquence siren, code_greffe, nom_greffe, numero_gestion, id_etablissement

## Global steps 

The global steps to construct the dataset are the following:


 *  Préparer les variables suivantes dans la table de l’INSEE:
    * voie_clean 
    * ville_matching 
    * adress_reconstituee_insee  
    * count_initial_insee 

## Input data sources

The data source to construct the dataset are the following:

- Athena 
  - region: eu-west-3 
  - Database: inpi 
  -  Table:  insee_rawdata  
  -  Notebook construction file (data lineage) 
      -  md :

## Output data sources

  * Athena: 
    * region: eu-west-3 
    * Database: inpi 
    * table: insee_final_sql 

     
## Things to know

  * Notebook création variables INSEE en Python:
    * https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/02_siretisation/05_siretisation_new_ets_v2.md#etape-1-merge-1
  * Fonction pour calculer le nombre de siret par siren à l’INSEE
    * https://github.com/thomaspernet/InseeInpi_matching/blob/8629f930cab8c29f3db493c731ecaf3596c1ac42/Notebooks_matching/Data_preprocessed/programme_matching/inpi_insee/preparation_data.py#L94
 
<!-- #endregion -->

# Parametre SQL

```python
from awsPy.aws_authorization import aws_connector
from awsPy.aws_athena import service_athena
from awsPy.aws_s3 import service_s3
from pathlib import Path
import pandas as pd
import os
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

### Préparation json parameters

```python
dic_ = {
    'global':{
        'database':'inpi',
        'output':'INPI/sql_output',
        'output_preparation':'INPI/sql_output_preparation',
        'Parameters':{
            #'stop_word': 's3://calfdata/Parameters/STOP_WORD',
            'type_voie':'s3://calfdata/Parameters/TYPE_VOIE_SQL'
        }
        #'ETS_step4_id':[],
        #'table_final_id':{
        #    'ETS':{
        #    }
        #}
    }
}
```

# Creation table `insee_final_sql`

La query met environ 5 minutes pour s'éxecuter. Il est possible d'améliorer les patterns regex

## Etapes

* `voie_clean` 
    - Ajout de la variable non abbrégée du type de voie. Exemple, l'INSEE indique CH, pour chemin, il faut donc indiquer CHEMIN
* ville_matching 
    - Nettoyage de la ville de l'INSEE (`libelleCommuneEtablissement`) de la même manière que l'INPI
* adress_reconstituee_insee:
    - Reconstitution de l'adresse à l'INSEE en utilisant le numéro de voie `numeroVoieEtablissement`, le type de voie non abbrégé, `voie_clean`, l'adresse `libelleVoieEtablissement`  et le `complementAdresseEtablissement` et suppression des articles
* `count_initial_insee`
    - Compte du nombre de siret (établissement) par siren (entreprise).

```python
query = """
CREATE TABLE inpi.insee_final_sql
WITH (
  format='PARQUET'
) AS
WITH remove_empty_siret AS (
SELECT
siren,
siret, 
regexp_like(siret, '\d+') as test_siret,
dateCreationEtablissement,
         etablissementSiege,
         etatAdministratifEtablissement,
         complementAdresseEtablissement,
         numeroVoieEtablissement,
         indiceRepetitionEtablissement,
         CASE 
WHEN indiceRepetitionEtablissement = 'B' THEN 'BIS'
WHEN indiceRepetitionEtablissement = 'T' THEN 'TER' 
WHEN indiceRepetitionEtablissement = 'Q' THEN 'QUATER' 
WHEN indiceRepetitionEtablissement = 'C' THEN 'QUINQUIES' 
ELSE indiceRepetitionEtablissement END as indiceRepetitionEtablissement_full,
         typeVoieEtablissement,
         -- type_voie.voie_clean,
         libelleVoieEtablissement,
         codePostalEtablissement,
         libelleCommuneEtablissement,
         libelleCommuneEtrangerEtablissement,
         distributionSpecialeEtablissement,
         codeCommuneEtablissement,
         codeCedexEtablissement,
         libelleCedexEtablissement,
         codePaysEtrangerEtablissement,
         libellePaysEtrangerEtablissement,
         enseigne1Etablissement,
         enseigne2Etablissement,
         enseigne3Etablissement
FROM insee_rawdata 
-- WHERE siren = '797406154'  
-- WHERE siren = '797406188'
 )
  
SELECT *
FROM (
  WITH concat_adress AS(
SELECT 
siren,
siret, 
dateCreationEtablissement,
         etablissementSiege,
         etatAdministratifEtablissement,
         codePostalEtablissement,
         codeCommuneEtablissement,
         libelleCommuneEtablissement,
         ville_matching,
         numeroVoieEtablissement,
         typeVoieEtablissement,
         voie_clean,
         libelleVoieEtablissement,
         complementAdresseEtablissement,
         indiceRepetitionEtablissement_full, 
         REGEXP_REPLACE(
            REGEXP_REPLACE(
              REGEXP_REPLACE(
                 REGEXP_REPLACE(
                      CONCAT(
                        COALESCE(numeroVoieEtablissement,''),
                        ' ',
                        COALESCE(indiceRepetitionEtablissement_full,''),
                        ' ',
                        COALESCE(voie_clean,''), ' ',  -- besoin sinon exclu
                        COALESCE(libelleVoieEtablissement,''), ' ',
                        COALESCE(complementAdresseEtablissement,'')
                      ), 
                '[^\w\s]| +', 
                ' '
              ), 
              '(?:^|(?<= ))(AU|AUX|AVEC|CE|CES|DANS|DE|DES|DU|ELLE|EN|ET|EUX|IL|ILS|LA|LE|LES)(?:(?= )|$)',  
              ''
            ), 
            '\s\s+', 
            ' '
          ), 
          '^\s+|\s+$', 
          ''
      ) AS adress_reconstituee_insee,
         enseigne1Etablissement,
         enseigne2Etablissement,
         enseigne3Etablissement
FROM (
  SELECT  
siren,
siret, 
test_siret,
dateCreationEtablissement,
         etablissementSiege,
         etatAdministratifEtablissement,
         codePostalEtablissement,
         codeCommuneEtablissement,
         libelleCommuneEtablissement,
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              REGEXP_REPLACE(
                REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    libelleCommuneEtablissement, 
                    '^\d+\s|\s\d+\s|\s\d+$', 
                    -- digit
                    ''
                  ), 
                  '^LA\s+|^LES\s+|^LE\s+|\\(.*\\)|^L(ES|A|E) | L(ES|A|E) | L(ES|A|E)$|CEDEX | CEDEX | CEDEX|^E[R*] | E[R*] | E[R*]$', 
                  ''
                ), 
                '^STE | STE | STE$|^STES | STES | STES', 
                'SAINTE'
              ), 
              '^ST | ST | ST$', 
              'SAINT'
            ), 
            'S/|^S | S | S$', 
            'SUR'
          ), 
          '/S', 
          'SOUS'
        ), 
        '[^\w\s]|\([^()]*\)|ER ARRONDISSEMENT|E ARRONDISSEMENT|" \
"|^SUR$|CEDEX|[0-9]+|\s+', 
        ''
    ) as ville_matching,
         libelleVoieEtablissement,
         complementAdresseEtablissement,
         numeroVoieEtablissement,
         -- indiceRepetitionEtablissement,
  indiceRepetitionEtablissement_full,
         typeVoieEtablissement,
         enseigne1Etablissement,
         enseigne2Etablissement,
         enseigne3Etablissement
  FROM remove_empty_siret
  )
LEFT JOIN type_voie 
ON typevoieetablissement = type_voie.voie_matching
WHERE test_siret = true
)
SELECT 
  count_initial_insee,
  concat_adress.siren,
siret, 
dateCreationEtablissement,
         etablissementSiege,
         etatAdministratifEtablissement,
         codePostalEtablissement,
         codeCommuneEtablissement,
         libelleCommuneEtablissement,
  ville_matching,
  libelleVoieEtablissement,
         complementAdresseEtablissement,
         numeroVoieEtablissement,
         indiceRepetitionEtablissement_full,
         typeVoieEtablissement,
  voie_clean,
  adress_reconstituee_insee,
         enseigne1Etablissement,
         enseigne2Etablissement,
         enseigne3Etablissement
FROM concat_adress
LEFT JOIN (
  SELECT siren, COUNT(siren) as count_initial_insee
  FROM concat_adress
  GROUP BY siren
  ) as count_siren
ON concat_adress.siren = count_siren.siren
)
"""
```

# Details Etapes


## Etape 1: Préparation `voie_clean`

Pour créer le pattern regex, on utilise une liste de type de voie disponible dans le Gitlab et à l'INSEE, que nous avons ensuite modifié manuellement. 

- Input
    - CSV: [TypeVoie.csv](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/data/input/Parameters/typeVoieEtablissement.csv)
        - CSV dans S3: [Parameters/upper_stop.csv](https://s3.console.aws.amazon.com/s3/buckets/calfdata/Parameters/TYPE_VOIE/)
        - A créer en table
   - Athena: type_voie
       - CSV dans S3: [Parameters/type_voie.csv](https://s3.console.aws.amazon.com/s3/buckets/calfdata/Parameters/TYPE_VOIE_SQL/)
- Code Python: [Exemple Input 1](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/05_redaction_US/04_prep_voie_num_2697.md#exemple-input-1)

### Point d'attention

*  Il y a 21,144,057 observations a l’INSEE n’ayant pas de siret pour 29,3845,05 observations avec un siret. Il faut donc les écarter. Il est inutile de garder des siren sans siret

```python
query = """
WITH remove_empty_siret AS (
SELECT
siren,
siret, 
regexp_like(siret, '\d+') as test_siret,
dateCreationEtablissement,
         etablissementSiege,
         etatAdministratifEtablissement,
         complementAdresseEtablissement,
         numeroVoieEtablissement,
         indiceRepetitionEtablissement,
         typeVoieEtablissement,
         -- type_voie.voie_clean,
         libelleVoieEtablissement,
         codePostalEtablissement,
         libelleCommuneEtablissement,
         libelleCommuneEtrangerEtablissement,
         distributionSpecialeEtablissement,
         codeCommuneEtablissement,
         codeCedexEtablissement,
         libelleCedexEtablissement,
         codePaysEtrangerEtablissement,
         libellePaysEtrangerEtablissement,
         enseigne1Etablissement,
         enseigne2Etablissement,
         enseigne3Etablissement
FROM insee_rawdata 
 )
SELECT  
siren,
siret, 
dateCreationEtablissement,
         etablissementSiege,
         etatAdministratifEtablissement,
         codePostalEtablissement,
         codeCommuneEtablissement,
         libelleCommuneEtablissement,
         libelleVoieEtablissement,
         complementAdresseEtablissement,
         numeroVoieEtablissement,
         indiceRepetitionEtablissement,
         typeVoieEtablissement,
         type_voie.voie_clean,
         enseigne1Etablissement,
         enseigne2Etablissement,
         enseigne3Etablissement
FROM remove_empty_siret
LEFT JOIN type_voie 
ON remove_empty_siret.typevoieetablissement = type_voie.voie_matching
WHERE test_siret = true
LIMIT 10
"""
```

## Etape 2: Création ville_matching

Ceci est un copier coller de l'INPI avec des changements a la marge. Il y a de grande chance que de nombreuses règles ne servent a rien car déjà intégrées dans la variable brute.

```python
query = """
SELECT  
siren,
siret, 
test_siret,
dateCreationEtablissement,
         etablissementSiege,
         etatAdministratifEtablissement,
         codePostalEtablissement,
         codeCommuneEtablissement,
         libelleCommuneEtablissement,
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              REGEXP_REPLACE(
                REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    libelleCommuneEtablissement, 
                    '^\d+\s|\s\d+\s|\s\d+$', 
                    -- digit
                    ''
                  ), 
                  '^LA\s+|^LES\s+|^LE\s+|\\(.*\\)|^L(ES|A|E) | L(ES|A|E) | L(ES|A|E)$|CEDEX | CEDEX | CEDEX|^E[R*] | E[R*] | E[R*]$', 
                  ''
                ), 
                '^STE | STE | STE$|^STES | STES | STES', 
                'SAINTE'
              ), 
              '^ST | ST | ST$', 
              'SAINT'
            ), 
            'S/|^S | S | S$', 
            'SUR'
          ), 
          '/S', 
          'SOUS'
        ), 
        '[^\w\s]|\([^()]*\)|ER ARRONDISSEMENT|E ARRONDISSEMENT|" \
"|^SUR$|CEDEX|[0-9]+|\s+', 
        ''
    ) as ville_matching
"""
```

## Etape 2: Preparation `adress_reconstituee_insee`

La variable `adress_reconstituee_insee` est la concatenation de `numeroVoieEtablissement`, `voie_clean`, `libelleVoieEtablissement` et `complementAdresseEtablissement`, avec la suppression des articles

```python
query = """
REGEXP_REPLACE(
            REGEXP_REPLACE(
              REGEXP_REPLACE(
                -- REGEXP_REPLACE(
                  -- NORMALIZE(
                    -- UPPER(
                      CONCAT(
                        numeroVoieEtablissement, ' ', voie_clean, 
                        ' ', libelleVoieEtablissement, ' ',
                        complementAdresseEtablissement
                      ),
                    -- ), 
                    -- NFD
                  -- ), 
                  -- '\pM', 
                  -- ''
                -- ), 
                '[^\w\s]| +', 
                ' '
              ), 
            '\s\s+', 
            ' '
          ), 
          '^\s+|\s+$', 
          ''
      ) AS adress_reconstituee_insee
""""
```

## Etape 3: `count_initial_insee` 

```python
query = """
SELECT 
  count_initial_insee,
  concat_adress.siren,
siret, 
dateCreationEtablissement,
         etablissementSiege,
         etatAdministratifEtablissement,
         codePostalEtablissement,
         codeCommuneEtablissement,
         libelleCommuneEtablissement,
  ville_matching,
  libelleVoieEtablissement,
         complementAdresseEtablissement,
         numeroVoieEtablissement,
         indiceRepetitionEtablissement,
         typeVoieEtablissement,
  adress_reconstituee_insee,
         enseigne1Etablissement,
         enseigne2Etablissement,
         enseigne3Etablissement
FROM concat_adress
LEFT JOIN (
  SELECT siren, COUNT(siren) as count_initial_insee
  FROM concat_adress
  GROUP BY siren
  ) as count_siren
ON concat_adress.siren = count_siren.siren
"""
```
