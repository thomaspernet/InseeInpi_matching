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
# INPI ETS ajout nouvelles variables

This notebook has been generated on 18/07/2020

L'objectif de ce notebook est de préparer la data de l'INPI en vue de siretiser la séquence siren, code_greffe, nom_greffe, numero_gestion, id_etablissement

## Global steps 

The global steps to construct the dataset are the following:


 *  Préparer les variables suivantes dans la table de l’INPI:
    * enseigne 
    * ville_matching 
    * adress_nettoyee 
    * adress_regex 
    * adresse_inpi_reconstitue 
    * numero_voie_matching 
    * voie_matching 
    * last_libelle_evt 
    * status_admin 
    * status_ets 
    * index_id 

## Input data sources

The data source to construct the dataset are the following:

- Athena 
  - region: eu-west-3 
  - Database: inpi 
  -  Table: initial_partiel_evt_new_ets_status_final 
  -  Notebook construction file (data lineage) 
      -  md :[01_Athena_concatenate_ETS.md](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/01_Athena_concatenate_ETS.md)

## Output data sources

  * Athena: 
    * region: eu-west-3 
    * Database: inpi 
    * table: ets_final_sql 

     
## Things to know

* Programme preparation data:
    - [preparation_data.py](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/inpi_insee/preparation_data.py)
* ville_matching: 
    - [00_prep_ville_2613.md](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/05_redaction_US/00_prep_ville_2613.md)
    - [2613](https://tree.taiga.io/project/olivierlubet-air/us/2613)
* adress_regex:
    - [03_prep_adresse_2690.md](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/05_redaction_US/03_prep_adresse_2690.md)
* voie_matching: 
    - [04_prep_voie_num_2697.md](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/05_redaction_US/04_prep_voie_num_2697.md)
* last_libelle_evt/status_admin/status_ets: 
    - [US 04 Amelioration ETS](https://coda.io/d/CreditAgricole_dCtnoqIftTn/US-04-Amelioration-ETS_sulSU)
 
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
            'stop_word': 's3://calfdata/Parameters/STOP_WORD',
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

# Creation table `ets_final_sql`

La query met environ 5 minutes pour s'éxecuter. Il est possible d'améliorer les patterns regex

## Etapes

* `enseigne`: 
    - Mise en majuscule
* `ville_matching`:
    - Nettoyage regex de la ville et suppression des espaces
* `adress_nettoyee`: 
    - Concatenation des champs de l'adresse, nettoyage regex et suppression des types de voie et numéro
* `adress_regex`: 
    - Création pattern regex
* `adresse_inpi_reconstitue`: 
    - Concatenation des champs de l'adresse, nettoyage regex
* `numero_voie_matching`: 
    - Extraction du premier numéro de voie dans l'adresse
* `voie_matching`: 
    - Extration du type de voie dans l'adresse et match avec abbrévation type de voie de l'INSEE
* `last_libelle_evt`: 
    - Extraction du dernier libellé de l'événement connu pour une séquence, et appliquer cette information à l'ensemble de la séquence
* `status_admin`: 
    - Informe du status ouvert/fermé concernant une séquence
* `status_ets`: 
    - Informe du type d'établissement (SIE/PRI.SEC) concernant une séquence
* `index_id`: 
    - Création du numéro de ligne

```python
query = """
/*Table préparée avec nouvelles */
CREATE TABLE inpi.ets_final_sql
WITH (
  format='PARQUET'
) AS
WITH create_regex AS ( 
  SELECT 
    siren, 
    code_greffe, 
    nom_greffe, 
    numero_gestion, 
    id_etablissement, 
    status, 
    origin, 
    date_greffe, 
    file_timestamp, 
    libelle_evt, 
    type, 
    "siège_pm", 
    rcs_registre, 
    adresse_ligne1, 
    adresse_ligne2, 
    adresse_ligne3, 
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            NORMALIZE(
              UPPER(
                CONCAT(
                  adresse_ligne1, ' ', adresse_ligne2, 
                  ' ', adresse_ligne3
                )
              ), 
              NFD
            ), 
            '\pM', 
            ''
          ), 
          '[^\w\s]', 
          ' '
        ), 
        '\s\s+', 
        ' '
      ), 
      '^\s+|\s+$', 
      ''
    ) AS adress_nettoyee, 
    CONCAT(
      '(?:^|(?<= ))(', 
      -- debut regex
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              REGEXP_REPLACE(
                REGEXP_REPLACE(
                  NORMALIZE(
                    UPPER(
                      CONCAT(
                        adresse_ligne1, ' ', adresse_ligne2, 
                        ' ', adresse_ligne3
                      )
                    ), 
                    NFD
                  ), 
                  '\pM', 
                  ''
                ), 
                '[^\w\s]|\d+| +', 
                ' '
              ), 
              '(?:^|(?<= ))(AU|AUX|AVEC|CE|CES|DANS|DE|DES|DU|ELLE|EN|ET|EUX|IL|ILS|LA|LE|LES|AVENUE|BOULEVARD|CARREFOUR|CHEMIN|CITE|CORNICHE|COURS|DOMAINE|DESCENTE|ECART|ESPLANADE|FAUBOURG|GRANDE RUE|HAMEAU|HALLE|IMPASSE|LIEU DIT|LOTISSEMENT|MARCHE|MONTEE|PASSAGE|PLACE|PLAINE|PLATEAU|PROMENADE|PARVIS|QUARTIER|QUAI|RESIDENCE|RUELLE|ROCADE|ROND POINT|ROUTE|RUE|SENTIER|SQUARE|TERRE PLEIN|TRAVERSE|VILLA|VILLAGE|RN|BP|CEDEX|BIS)(?:(?= )|$)', 
              ''
            ), 
            '\s\s+', 
            ' '
          ), 
          '^\s+|\s+$', 
          ''
        ), 
        '\s', 
        '|'
      ), 
      -- milieu regex
      ')(?:(?= )|$)' -- fin regex
      ) AS adress_regex, 
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              REGEXP_REPLACE(
                NORMALIZE(
                  UPPER(
                    CONCAT(
                      adresse_ligne1, ' ', adresse_ligne2, 
                      ' ', adresse_ligne3
                    )
                  ), 
                  NFD
                ), 
                '\pM', 
                ''
              ), 
              '[^\w\s]| +', 
              ' '
            ), 
            '(?:^|(?<= ))(AU|AUX|AVEC|CE|CES|DANS|DE|DES|DU|ELLE|EN|ET|EUX|IL|ILS|LA|LE|LES|AVENUE|BOULEVARD|CARREFOUR|CHEMIN|CITE|CORNICHE|COURS|DOMAINE|DESCENTE|ECART|ESPLANADE|FAUBOURG|GRANDE RUE|HAMEAU|HALLE|IMPASSE|LIEU DIT|LOTISSEMENT|MARCHE|MONTEE|PASSAGE|PLACE|PLAINE|PLATEAU|PROMENADE|PARVIS|QUARTIER|QUAI|RESIDENCE|RUELLE|ROCADE|ROND POINT|ROUTE|RUE|SENTIER|SQUARE|TERRE PLEIN|TRAVERSE|VILLA|VILLAGE|RN|BP|CEDEX|BIS)(?:(?= )|$)', 
            ''
          ), 
          '\s\s+', 
          ' '
        ), 
        '^\s+|\s+$', 
        ''
      ), 
      '\s', 
      ' '
    ) AS adresse_inpi_reconstitue, 
    code_postal, 
    code_postal_matching, 
    ville, 
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              REGEXP_REPLACE(
                REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      NORMALIZE(
                        UPPER(ville), 
                        NFD
                      ), 
                      '\pM', 
                      ''
                    ), 
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
      ), 
      'MARSEILLEE', 
      'MARSEILLE'
    ) as ville_matching, 
    code_commune, 
    pays, 
    domiciliataire_nom, 
    domiciliataire_siren, 
    domiciliataire_greffe, 
    "domiciliataire_complément", 
    "siege_domicile_représentant", 
    nom_commercial, 
    UPPER(enseigne) as enseigne, 
    "activité_ambulante", 
    "activité_saisonnière", 
    "activité_non_sédentaire", 
    "date_début_activité", 
    "activité", 
    origine_fonds, 
    origine_fonds_info, 
    type_exploitation, 
    csv_source, 
    rn -- AS adress_nettoyee
  FROM 
    ets_test_filtered --WHERE (adresse_ligne1 IS NOT NULL and adresse_ligne2 IS NOT NULL)
    --WHERE siren = '841344229'
  --LIMIT 15
) 
SELECT 
  * 
FROM 
  (
    WITH voie_type_voie AS (
      SELECT 
        siren, 
        code_greffe, 
        nom_greffe, 
        numero_gestion, 
        id_etablissement, 
        status, 
        origin, 
        date_greffe, 
        file_timestamp, 
        libelle_evt, 
        type, 
        "siège_pm", 
        rcs_registre, 
        adresse_ligne1, 
        adresse_ligne2, 
        adresse_ligne3, 
        adress_nettoyee, 
        adresse_inpi_reconstitue, 
        adress_regex, 
        numero_voie_matching, 
        numero_voie_type_voie.voie_clean, 
        voie_matching, 
        code_postal, 
        code_postal_matching, 
        ville, 
        ville_matching, 
        code_commune, 
        pays, 
        domiciliataire_nom, 
        domiciliataire_siren, 
        domiciliataire_greffe, 
        "domiciliataire_complément", 
        "siege_domicile_représentant", 
        nom_commercial, 
        enseigne, 
        "activité_ambulante", 
        "activité_saisonnière", 
        "activité_non_sédentaire", 
        "date_début_activité", 
        "activité", 
        origine_fonds, 
        origine_fonds_info, 
        type_exploitation, 
        csv_source, 
        rn 
      FROM 
        type_voie 
        RIGHT JOIN (
          SELECT 
            siren, 
            code_greffe, 
            nom_greffe, 
            numero_gestion, 
            id_etablissement, 
            status, 
            origin, 
            date_greffe, 
            file_timestamp, 
            libelle_evt, 
            type, 
            "siège_pm", 
            rcs_registre, 
            adresse_ligne1, 
            adresse_ligne2, 
            adresse_ligne3, 
            adress_nettoyee, 
            adresse_inpi_reconstitue, 
            adress_regex, 
            regexp_extract(adress_nettoyee, '\d+') as numero_voie_matching, 
            regexp_extract(
              adress_nettoyee, '(?:^|(?<= ))(ALLEE|AVENUE|BOULEVARD|CARREFOUR|CHEMIN|CHAUSSEE|CITE|CORNICHE|COURS|DOMAINE|DESCENTE|ECART|ESPLANADE|FAUBOURG|GRANDE RUE|HAMEAU|HALLE|IMPASSE|LIEU DIT|LOTISSEMENT|MARCHE|MONTEE|PASSAGE|PLACE|PLAINE|PLATEAU|PROMENADE|PARVIS|QUARTIER|QUAI|RESIDENCE|RUELLE|ROCADE|ROND POINT|ROUTE|RUE|SENTE   SENTIER|SQUARE|TERRE PLEIN|TRAVERSE|VILLA|VILLAGE)(?:(?= )|$)'
            ) as voie_clean, 
            code_postal, 
            code_postal_matching, 
            ville, 
            ville_matching, 
            code_commune, 
            pays, 
            domiciliataire_nom, 
            domiciliataire_siren, 
            domiciliataire_greffe, 
            "domiciliataire_complément", 
            "siege_domicile_représentant", 
            nom_commercial, 
            enseigne, 
            "activité_ambulante", 
            "activité_saisonnière", 
            "activité_non_sédentaire", 
            "date_début_activité", 
            "activité", 
            origine_fonds, 
            origine_fonds_info, 
            type_exploitation, 
            csv_source, 
            rn 
          FROM 
            create_regex
        ) as numero_voie_type_voie ON numero_voie_type_voie.voie_clean = type_voie.voie_clean
    ) 
    SELECT 
      * 
    FROM 
      (
        WITH convert_date AS (
          SELECT 
            siren, 
            code_greffe, 
            nom_greffe, 
            numero_gestion, 
            id_etablissement, 
            Coalesce(
              try(
                date_parse(date_greffe, '%Y-%m-%d')
              ), 
              try(
                date_parse(
                  date_greffe, '%Y-%m-%d %hh:%mm:%ss.SSS'
                )
              ), 
              try(
                date_parse(
                  date_greffe, '%Y-%m-%d %hh:%mm:%ss'
                )
              ), 
              try(
                cast(date_greffe as timestamp)
              )
            ) as date_greffe, 
            libelle_evt 
          FROM 
            voie_type_voie
        ) 
        SELECT 
          ROW_NUMBER() OVER () as index_id,
          voie_type_voie.siren, 
          voie_type_voie.code_greffe, 
          voie_type_voie.nom_greffe, 
          voie_type_voie.numero_gestion, 
          voie_type_voie.id_etablissement, 
          status, 
          origin, 
          date_greffe, 
          file_timestamp, 
          libelle_evt, 
          last_libele_evt, 
          CASE WHEN last_libele_evt = 'Etablissement ouvert' THEN 'A' ELSE 'F' END AS status_admin,
          type, 
          CASE WHEN type = 'SIE' OR type = 'SEP' THEN 'true' ELSE 'false' END AS status_ets,
          "siège_pm", 
          rcs_registre, 
          adresse_ligne1, 
          adresse_ligne2, 
          adresse_ligne3, 
          adress_nettoyee, 
          adresse_inpi_reconstitue, 
          adress_regex, 
          numero_voie_matching, 
          voie_clean, 
          voie_matching,
          code_postal, 
          code_postal_matching, 
          ville, 
          ville_matching, 
          code_commune, 
          pays, 
          domiciliataire_nom, 
          domiciliataire_siren, 
          domiciliataire_greffe, 
          "domiciliataire_complément", 
          "siege_domicile_représentant", 
          nom_commercial, 
          enseigne, 
          "activité_ambulante", 
          "activité_saisonnière", 
          "activité_non_sédentaire", 
          "date_début_activité", 
          "activité", 
          origine_fonds, 
          origine_fonds_info, 
          type_exploitation, 
          csv_source, 
          rn 
        FROM 
          voie_type_voie 
          INNER JOIN (
            SELECT 
              convert_date.siren, 
              convert_date.code_greffe, 
              convert_date.nom_greffe, 
              convert_date.numero_gestion, 
              convert_date.id_etablissement, 
              convert_date.libelle_evt as last_libele_evt, 
              max_date_greffe 
            FROM 
              convert_date 
              INNER JOIN (
                SELECT 
                  siren, 
                  code_greffe, 
                  nom_greffe, 
                  numero_gestion, 
                  id_etablissement, 
                  MAX(
                    Coalesce(
                      try(
                        date_parse(date_greffe, '%Y-%m-%d')
                      ), 
                      try(
                        date_parse(
                          date_greffe, '%Y-%m-%d %hh:%mm:%ss.SSS'
                        )
                      ), 
                      try(
                        date_parse(
                          date_greffe, '%Y-%m-%d %hh:%mm:%ss'
                        )
                      ), 
                      try(
                        cast(date_greffe as timestamp)
                      )
                    )
                  ) as max_date_greffe 
                FROM 
                  voie_type_voie 
                GROUP BY 
                  siren, 
                  code_greffe, 
                  nom_greffe, 
                  numero_gestion, 
                  id_etablissement
              ) AS temp ON temp.siren = convert_date.siren 
              AND temp.code_greffe = convert_date.code_greffe 
              AND temp.nom_greffe = convert_date.nom_greffe 
              AND temp.numero_gestion = convert_date.numero_gestion 
              AND temp.id_etablissement = convert_date.id_etablissement 
              AND temp.max_date_greffe = convert_date.date_greffe
          ) as latest_libele ON voie_type_voie.siren = latest_libele.siren 
          AND voie_type_voie.code_greffe = latest_libele.code_greffe 
          AND voie_type_voie.nom_greffe = latest_libele.nom_greffe 
          AND voie_type_voie.numero_gestion = latest_libele.numero_gestion 
          AND voie_type_voie.id_etablissement = latest_libele.id_etablissement
      )
    ORDER BY siren, code_greffe, nom_greffe, numero_gestion, id_etablissement, date_greffe
  )
"""
```

# Details Etapes


## Etape 1: Préparation `ville_matching`

Cette étape fait référence à l'US [00_prep_ville_2613.md](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/05_redaction_US/00_prep_ville_2613.md), et la conception du regex est la suivante:

```
Select
    REGEXP_REPLACE(
    REGEXP_REPLACE(
    REGEXP_REPLACE(
    REGEXP_REPLACE(
    REGEXP_REPLACE(
    REGEXP_REPLACE(
    UPPER(TRANSLATE(
    ville,'ÀÁÂÃÄÅàáâãäåÒÓÔÕÖØòóôõöøÈÉÊËèéêëÇçÌÍÎÏìíîïÙÚÛÜùúûüÿÑñ-\'0123456789','aaaaaaaaaaaaooooooooooooeeeeeeeecciiiiiiiiuuuuuuuuynn  ')) --Remplacement accent + chiffres
    ,'\\(.*\\)|^L(ES|A|E) | L(ES|A|E) | L(ES|A|E)$|CEDEX | CEDEX | CEDEX|^E[R*] | E[R*] | E[R*]$','') --Suppression de certains patterns
    ,'^STE | STE | STE$|^STES | STES | STES$','SAINTE') -- Remplacement
    ,'^ST | ST | ST$','SAINT') -- Remplacement
    ,'S/|^S | S | S$','SUR') -- Remplacement
    ,'/S','SOUS') -- Remplacement
    ,' ','') ville_matching -- Suppression des espaces à la fin
from ${BDD_REFERENTIEL}.inpi_etablissement_historique;

```
Dans notre notebook, on a changé la facon d'enlever les accents, et on a rajouté des règles

```python
query = """
SELECT
REGEXP_REPLACE(
  REGEXP_REPLACE(
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              REGEXP_REPLACE(
                REGEXP_REPLACE(NORMALIZE(UPPER(ville), NFD) , '\pM', ''),
          '^\d+\s|\s\d+\s|\s\d+$', -- digit
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
),
  'MARSEILLEE',
  'MARSEILLE'
  ) as ville_matching
  
FROM initial_partiel_evt_new_ets_status_final  
"""
```

<!-- #region -->
## Etape 2: Preparation `adress_nettoyee` & `adress_regex` & `adresse_inpi_reconstitue`

### `adress_nettoyee`

Cette étape fait référence à l'US [03_prep_adresse_2690.md](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/05_redaction_US/03_prep_adresse_2690.md) 

- Input
    - CSV: [upper_stop.csv](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/data/input/Parameters/upper_stop.csv)
        - CSV dans S3: [Parameters/upper_stop.csv](https://s3.console.aws.amazon.com/s3/object/calfdata/Parameters/STOP_WORD/upper_stop.csv?region=eu-west-3&tab=overview)
        - A créer en table
   - Athena: stop_word


Ici, il faut faire attention à supprimer les espaces en début, et fin de ligne. Il faut ausso faire attention à bien enlever les multiples espaces
<!-- #endregion -->

```python
query = """
SELECT 
siren, adresse_ligne1, adresse_ligne2, adresse_ligne3,
REGEXP_REPLACE(
  REGEXP_REPLACE(
   REGEXP_REPLACE(
    REGEXP_REPLACE(
      NORMALIZE(
        UPPER(CONCAT(adresse_ligne1, ' ', adresse_ligne2, ' ', adresse_ligne3)),
        NFD),
      '\pM', ''),
    '[^\w\s]', ' '),
    '\s\s+',
    ' '),
  '^\s+|\s+$',''
  ) AS adress_nettoyee
  
FROM initial_partiel_evt_new_ets_status_final  
-- WHERE (adresse_ligne1 IS NOT NULL and adresse_ligne2 IS NOT NULL)
-- WHERE siren = '841344229'
""""
```

### `adress_regex`

<!-- #region -->
Pour créer le pattern regex, on utilise une liste de stop word disponible dans le Gitlab, que nous avons ensuite modifié manuellement. 

Le pattern regex devient le suivant. Par ailleurs, on utilise `(?:^|(?<= ))` et `(?:(?= )|$)` car cela semble mieux fonctionner. Cf. discussion [Stackedit](https://stackoverflow.com/questions/21448139/match-list-of-words-without-the-list-of-chars-around)

Pattern regex:


```
'(?:^|(?<= ))(AU|AUX|AVEC|CE|CES|DANS|DE|DES|DU|ELLE|EN|ET|EUX|IL|ILS|LA|LE|LES|AVENUE|BOULEVARD|CARREFOUR|CHEMIN|CITE|CORNICHE|COURS|DOMAINE|DESCENTE|ECART|ESPLANADE|FAUBOURG|GRANDE RUE|HAMEAU|HALLE|IMPASSE|LIEU DIT|LOTISSEMENT|MARCHE|MONTEE|PASSAGE|PLACE|PLAINE|PLATEAU|PROMENADE|PARVIS|QUARTIER|QUAI|RESIDENCE|RUELLE|ROCADE|ROND POINT|ROUTE|RUE|SENTIER|SQUARE|TERRE PLEIN|TRAVERSE|VILLA|VILLAGE|RN|BP|CEDEX|BIS)(?:(?= )|$)'
```

<!-- #endregion -->

```python
s3.download_file(
'Parameters/STOP_WORD/upper_stop.csv')
```

```python
#split = [i + "$" for i in pd.read_csv('upper_stop.csv', usecols = ['stop_word'])['stop_word'].to_list()]
'|'.join(pd.read_csv('upper_stop.csv', usecols = ['stop_word'])['stop_word'].to_list())
```

```python
query = """
SELECT 
siren, adresse_ligne1, adresse_ligne2, adresse_ligne3,
CONCAT(
 '(?:^|(?<= ))(', -- debut regex
 REGEXP_REPLACE(
  REGEXP_REPLACE(  
   REGEXP_REPLACE(
    REGEXP_REPLACE(
     REGEXP_REPLACE(
      REGEXP_REPLACE(
        NORMALIZE(
          UPPER(CONCAT(adresse_ligne1, ' ', adresse_ligne2, ' ', adresse_ligne3)),
        NFD),
      '\pM', ''),
  '[^\w\s]|\d+| +', ' '
  ), '(?:^|(?<= ))(AU|AUX|AVEC|CE|CES|DANS|DE|DES|DU|ELLE|EN|ET|EUX|IL|ILS|LA|LE|LES|AVENUE|BOULEVARD|CARREFOUR|CHEMIN|CITE|CORNICHE|COURS|DOMAINE|DESCENTE|ECART|ESPLANADE|FAUBOURG|GRANDE RUE|HAMEAU|HALLE|IMPASSE|LIEU DIT|LOTISSEMENT|MARCHE|MONTEE|PASSAGE|PLACE|PLAINE|PLATEAU|PROMENADE|PARVIS|QUARTIER|QUAI|RESIDENCE|RUELLE|ROCADE|ROND POINT|ROUTE|RUE|SENTIER|SQUARE|TERRE PLEIN|TRAVERSE|VILLA|VILLAGE|RN|BP|CEDEX|BIS)(?:(?= )|$)',
  ''),
    '\s\s+',
    ' '),
  '^\s+|\s+$',''),
  '\s', '|'), -- milieu regex
  ')(?:(?= )|$)' -- fin regex
  
  ) AS adress_regex
   
  -- AS adress_nettoyee
  
FROM initial_partiel_evt_new_ets_status_final  
--WHERE (adresse_ligne1 IS NOT NULL and adresse_ligne2 IS NOT NULL)
--WHERE siren = '841344229'
LIMIT 15
"""
```

On peut tester si le regex marche en faisaint le test sur la variable `adress_nettoyee` et `adress_regex`. Avec la fonction `regexp_like`, on ne devrait retrouver que des `True`

```
WITH create_regex AS (
SELECT 
siren, adresse_ligne1, adresse_ligne2, adresse_ligne3,
REGEXP_REPLACE(
  REGEXP_REPLACE(
    REGEXP_REPLACE(
      NORMALIZE(
        UPPER(CONCAT(adresse_ligne1, ' ', adresse_ligne2, ' ', adresse_ligne3)),
        NFD),
      '\pM', ''),
    '[^\w\s]', ' '),
    '\s\s+',
    ' '
  ) AS adress_nettoyee,
CONCAT(
 '(?:^|(?<= ))(', -- debut regex
 REGEXP_REPLACE(
  REGEXP_REPLACE(  
   REGEXP_REPLACE(
    REGEXP_REPLACE(
     REGEXP_REPLACE(
      REGEXP_REPLACE(
        NORMALIZE(
          UPPER(CONCAT(adresse_ligne1, ' ', adresse_ligne2, ' ', adresse_ligne3)),
        NFD),
      '\pM', ''),
  '[^\w\s]|\d+| +', ' '
  ), '(?:^|(?<= ))(AU|AUX|AVEC|CE|CES|DANS|DE|DES|DU|ELLE|EN|ET|EUX|IL|ILS|LA|LE|LES|AVENUE|BOULEVARD|CARREFOUR|CHEMIN|CITE|CORNICHE|COURS|DOMAINE|DESCENTE|ECART|ESPLANADE|FAUBOURG|GRANDE RUE|HAMEAU|HALLE|IMPASSE|LIEU DIT|LOTISSEMENT|MARCHE|MONTEE|PASSAGE|PLACE|PLAINE|PLATEAU|PROMENADE|PARVIS|QUARTIER|QUAI|RESIDENCE|RUELLE|ROCADE|ROND POINT|ROUTE|RUE|SENTIER|SQUARE|TERRE PLEIN|TRAVERSE|VILLA|VILLAGE|RN|BP|CEDEX|BIS)(?:(?= )|$)',
  ''),
    '\s\s+',
    ' '),
  '^\s+|\s+$',''),
  '\s', '|'), -- milieu regex
  ')(?:(?= )|$)' -- fin regex
  
  ) AS adress_regex
   
  -- AS adress_nettoyee
  
FROM initial_partiel_evt_new_ets_status_final  
--WHERE (adresse_ligne1 IS NOT NULL and adresse_ligne2 IS NOT NULL)
--WHERE siren = '841344229'
LIMIT 15
)
SELECT siren, adresse_ligne1, adresse_ligne2, adresse_ligne3, adress_nettoyee,adress_regex,
regexp_like(adress_nettoyee,adress_regex)
FROM create_regex   
```


### `adresse_inpi_reconstitue`

* Code pour construire adresse_inpi_reconstitue 
    - [Etape 1: Merge](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/02_siretisation/05_siretisation_new_ets_v2.md#etape-1-merge-1)
    
``` 
adresse_inpi_reconstitue = lambda x: x['adress_new'].apply(
        lambda x:' '.join([word for word in str(x).split() if word not in
        (upper_word)]))
``` 

Dans le code ci dessus, `adress_new` fait reférence ) `adress_nettoyee` dans notre code SQL

```python
query = """

REGEXP_REPLACE(
  REGEXP_REPLACE(  
   REGEXP_REPLACE(
    REGEXP_REPLACE(
     REGEXP_REPLACE(
      REGEXP_REPLACE(
        NORMALIZE(
          UPPER(CONCAT(adresse_ligne1, ' ', adresse_ligne2, ' ', adresse_ligne3)),
        NFD),
      '\pM', ''),
  '[^\w\s]| +', ' '
  ), '(?:^|(?<= ))(AU|AUX|AVEC|CE|CES|DANS|DE|DES|DU|ELLE|EN|ET|EUX|IL|ILS|LA|LE|LES|AVENUE|BOULEVARD|CARREFOUR|CHEMIN|CITE|CORNICHE|COURS|DOMAINE|DESCENTE|ECART|ESPLANADE|FAUBOURG|GRANDE RUE|HAMEAU|HALLE|IMPASSE|LIEU DIT|LOTISSEMENT|MARCHE|MONTEE|PASSAGE|PLACE|PLAINE|PLATEAU|PROMENADE|PARVIS|QUARTIER|QUAI|RESIDENCE|RUELLE|ROCADE|ROND POINT|ROUTE|RUE|SENTIER|SQUARE|TERRE PLEIN|TRAVERSE|VILLA|VILLAGE|RN|BP|CEDEX|BIS)(?:(?= )|$)',
  ''),
    '\s\s+',
    ' '),
  '^\s+|\s+$',''),
  '\s', ' '
  ) AS adresse_inpi_reconstitue

"""
```

## Etape 3: Préparation `numero_voie_matching` & `voie_matching`

Cette étape fait référence à l'US [04_prep_voie_num_2697.md](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/05_redaction_US/04_prep_voie_num_2697.md)

### `numero_voie_matching`

Le `numero_voie_matching` est l'extraction du numéro de voie dans la variable `adress_netoyee`.

Exemple de code pour recupérer les digits dans un string avec SQL [US 2622](https://tree.taiga.io/project/olivierlubet-air/us/2622):

```
REGEXP_EXTRACT(ville, '\d{5}')
``` 

```python
query = """
regexp_extract(adress_nettoyee,'\d+') as numero_voie_matching
"""
```

### `voie_matching`

Pour créer le pattern regex, on utilise une liste de type de voie disponible dans le Gitlab et à l'INSEE, que nous avons ensuite modifié manuellement. 

- Input
    - CSV: [TypeVoie.csv](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/data/input/Parameters/typeVoieEtablissement.csv)
        - CSV dans S3: [Parameters/upper_stop.csv](https://s3.console.aws.amazon.com/s3/buckets/calfdata/Parameters/TYPE_VOIE/)
        - A créer en table
   - Athena: type_voie
       - CSV dans S3: [Parameters/type_voie.csv](https://s3.console.aws.amazon.com/s3/buckets/calfdata/Parameters/TYPE_VOIE_SQL/)
- Code Python: [Exemple Input 1](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/05_redaction_US/04_prep_voie_num_2697.md#exemple-input-1)

Pattern regex

```
(?:^|(?<= ))(ALLEE|AVENUE|BOULEVARD|CARREFOUR|CHEMIN|CHAUSSEE|CITE|CORNICHE|COURS|DOMAINE|DESCENTE|ECART|ESPLANADE|FAUBOURG|GRANDE RUE|HAMEAU|HALLE|IMPASSE|LIEU DIT|LOTISSEMENT|MARCHE|MONTEE|PASSAGE|PLACE|PLAINE|PLATEAU|PROMENADE|PARVIS|QUARTIER|QUAI|RESIDENCE|RUELLE|ROCADE|ROND POINT|ROUTE|RUE|SENTE   SENTIER|SQUARE|TERRE PLEIN|TRAVERSE|VILLA|VILLAGE)(?:(?= )|$)'
```

```python
s3.download_file(
'Parameters/TYPE_VOIE/typeVoieEtablissement.csv')
```

```python
'|'.join((pd.read_csv('typeVoieEtablissement.csv')
 .assign(voie_clean = lambda x: x['possibilite'].str.normalize(
            'NFKD')
        .str.encode('ascii', errors='ignore')
        .str.decode('utf-8')
        .str.replace('[^\w\s]', ' ')
        .str.replace('^\s+|\s+$', '')
        .str.upper()
            )
          )['voie_clean'].to_list())
```

```python
(pd.read_csv('typeVoieEtablissement.csv')
 .assign(voie_clean = lambda x: x['possibilite'].str.normalize(
            'NFKD')
        .str.encode('ascii', errors='ignore')
        .str.decode('utf-8')
        .str.replace('[^\w\s]', ' ')
        .str.replace('^\s+|\s+$', '')
        .str.upper()
            )
          ).to_csv('type_voie.csv', index = False)
```

```python
s3.upload_file('type_voie.csv',
               'Parameters/TYPE_VOIE_SQL')
```

### Create `type_voie` table

```python
(pd.read_csv('typeVoieEtablissement.csv')
 .assign(voie_clean = lambda x: x['possibilite'].str.normalize(
            'NFKD')
        .str.encode('ascii', errors='ignore')
        .str.decode('utf-8')
        .str.replace('[^\w\s]', ' ')
        .str.replace('^\s+|\s+$', '')
        .str.upper()
            )
          ).head()
```

```python
dic_['global']['Parameters']['type_voie']
```

```python
query_create = """
CREATE EXTERNAL TABLE IF NOT EXISTS {0}.{1} (
voie_matching string,
possibilite string, 
voie_clean string
 )
     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
   'separatorChar' = ',',
   'quoteChar' = '"'
   )
     LOCATION '{2}'
     TBLPROPERTIES ('has_encrypted_data'='false',
              'skip.header.line.count'='1');""".format(
    dic_['global']['database'],
    'type_voie',
    dic_['global']['Parameters']['type_voie'])

output = athena.run_query(
                            query=query_create,
                            database=dic_['global']['database'],
                            s3_output=dic_['global']['output']
                        )
```

Matching entre la query de préparation des data et la table de la voie

```python
query = """
SELECT siren, adresse_ligne1, adresse_ligne2, adresse_ligne3, adress_nettoyee,adresse_inpi_reconstitue,adress_regex,
numero_voie_matching, numero_voie_type_voie.voie_clean, voie_matching
FROM type_voie   
RIGHT JOIN (
  SELECT siren, adresse_ligne1, adresse_ligne2, adresse_ligne3, adress_nettoyee,adresse_inpi_reconstitue,adress_regex,
regexp_extract(adress_nettoyee,'\d+') as numero_voie_matching,
regexp_extract(adress_nettoyee, '(?:^|(?<= ))(ALLEE|AVENUE|BOULEVARD|CARREFOUR|CHEMIN|CHAUSSEE|CITE|CORNICHE|COURS|DOMAINE|DESCENTE|ECART|ESPLANADE|FAUBOURG|GRANDE RUE|HAMEAU|HALLE|IMPASSE|LIEU DIT|LOTISSEMENT|MARCHE|MONTEE|PASSAGE|PLACE|PLAINE|PLATEAU|PROMENADE|PARVIS|QUARTIER|QUAI|RESIDENCE|RUELLE|ROCADE|ROND POINT|ROUTE|RUE|SENTE   SENTIER|SQUARE|TERRE PLEIN|TRAVERSE|VILLA|VILLAGE)(?:(?= )|$)') as voie_clean 
  FROM create_regex
  ) as numero_voie_type_voie
ON numero_voie_type_voie.voie_clean = type_voie.voie_clean
"""
```

## Etape 4: Création `last_libele_evt` & `status_admin` & `status_ets`

Le code SQL pour préparer ses deux variables:

```
### First 
/*add status etb*/
CREATE TABLE inpi.ets_preparation_python_lib
WITH (
  format='PARQUET'
) AS
WITH convert_date AS (SELECT siren, code_greffe, nom_greffe, numero_gestion, id_etablissement,Coalesce(
         try(date_parse(date_greffe, '%Y-%m-%d')),
         try(date_parse(date_greffe, '%Y-%m-%d %hh:%mm:%ss.SSS')),
         try(date_parse(date_greffe, '%Y-%m-%d %hh:%mm:%ss')),
         try(cast(date_greffe as timestamp))
  ) as date_greffe, libelle_evt
FROM ets_preparation_python
)

SELECT ets_preparation_python.siren,ets_preparation_python.code_greffe, ets_preparation_python.nom_greffe, ets_preparation_python.numero_gestion, ets_preparation_python.id_etablissement, status, origin, file_timestamp, date_greffe, libelle_evt,last_libele_evt, type, adress_new, adresse_new_clean_reg, possibilite, insee as voie_matching, digit_inpi as numero_voie_matching, code_postal_matching, ncc, code_commune, enseigne, "date_début_activité", csv_source, index 
FROM ets_preparation_python 

INNER JOIN (SELECT
convert_date.siren, convert_date.code_greffe, convert_date.nom_greffe, convert_date.numero_gestion, convert_date.id_etablissement, convert_date.libelle_evt as last_libele_evt, max_date_greffe
FROM convert_date
INNER JOIN  (SELECT siren, code_greffe, nom_greffe, numero_gestion, id_etablissement, MAX(Coalesce(
         try(date_parse(date_greffe, '%Y-%m-%d')),
         try(date_parse(date_greffe, '%Y-%m-%d %hh:%mm:%ss.SSS')),
         try(date_parse(date_greffe, '%Y-%m-%d %hh:%mm:%ss')),
         try(cast(date_greffe as timestamp))
  )) as max_date_greffe
FROM ets_preparation_python  
GROUP BY siren, code_greffe, nom_greffe, numero_gestion, id_etablissement
  ) AS temp
ON temp.siren = convert_date.siren
AND temp.code_greffe = convert_date.code_greffe
AND temp.nom_greffe = convert_date.nom_greffe
AND temp.numero_gestion = convert_date.numero_gestion
AND temp.id_etablissement = convert_date.id_etablissement
AND temp.max_date_greffe = convert_date.date_greffe) as latest_libele

ON ets_preparation_python.siren = latest_libele.siren
AND ets_preparation_python.code_greffe = latest_libele.code_greffe
AND ets_preparation_python.nom_greffe = latest_libele.nom_greffe
AND ets_preparation_python.numero_gestion = latest_libele.numero_gestion
AND ets_preparation_python.id_etablissement = latest_libele.id_etablissement

#### Seconds
CREATE TABLE inpi.ets_preparation_python_lib1
WITH (
  format='PARQUET'
) AS
SELECT *,
CASE WHEN last_libele_evt = 'Etablissement ouvert' THEN 'A' ELSE 'F' END AS status_admin,
CASE WHEN type = 'SIE' OR type = 'SEP' THEN 'true' ELSE 'false' END AS status_ets
FROM ets_preparation_python_lib
ORDER BY siren, code_greffe, nom_greffe, numero_gestion, id_etablissement, date_greffe
```


```python
query = """
SELECT 
      * 
    FROM 
      (
        WITH convert_date AS (
          SELECT 
            siren, 
            code_greffe, 
            nom_greffe, 
            numero_gestion, 
            id_etablissement, 
            Coalesce(
              try(
                date_parse(date_greffe, '%Y-%m-%d')
              ), 
              try(
                date_parse(
                  date_greffe, '%Y-%m-%d %hh:%mm:%ss.SSS'
                )
              ), 
              try(
                date_parse(
                  date_greffe, '%Y-%m-%d %hh:%mm:%ss'
                )
              ), 
              try(
                cast(date_greffe as timestamp)
              )
            ) as date_greffe, 
            libelle_evt 
          FROM 
            voie_type_voie
        ) 
        SELECT 
          voie_type_voie.siren, 
          voie_type_voie.code_greffe, 
          voie_type_voie.nom_greffe, 
          voie_type_voie.numero_gestion, 
          voie_type_voie.id_etablissement, 
          status, 
          origin, 
          date_greffe, 
          file_timestamp, 
          libelle_evt, 
          last_libele_evt, 
          CASE WHEN last_libele_evt = 'Etablissement ouvert' THEN 'A' ELSE 'F' END AS status_admin,
          type, 
          CASE WHEN type = 'SIE' OR type = 'SEP' THEN 'true' ELSE 'false' END AS status_ets,
          "siège_pm", 
          rcs_registre, 
          adresse_ligne1, 
          adresse_ligne2, 
          adresse_ligne3, 
          adress_nettoyee, 
          adresse_inpi_reconstitue, 
          adress_regex, 
          numero_voie_matching, 
          voie_clean, 
          code_postal, 
          code_postal_matching, 
          ville, 
          ville_matching, 
          code_commune, 
          pays, 
          domiciliataire_nom, 
          domiciliataire_siren, 
          domiciliataire_greffe, 
          "domiciliataire_complément", 
          "siege_domicile_représentant", 
          nom_commercial, 
          enseigne, 
          "activité_ambulante", 
          "activité_saisonnière", 
          "activité_non_sédentaire", 
          "date_début_activité", 
          "activité", 
          origine_fonds, 
          origine_fonds_info, 
          type_exploitation, 
          csv_source, 
          rn 
        FROM 
          voie_type_voie 
          INNER JOIN (
            SELECT 
              convert_date.siren, 
              convert_date.code_greffe, 
              convert_date.nom_greffe, 
              convert_date.numero_gestion, 
              convert_date.id_etablissement, 
              convert_date.libelle_evt as last_libele_evt, 
              max_date_greffe 
            FROM 
              convert_date 
              INNER JOIN (
                SELECT 
                  siren, 
                  code_greffe, 
                  nom_greffe, 
                  numero_gestion, 
                  id_etablissement, 
                  MAX(
                    Coalesce(
                      try(
                        date_parse(date_greffe, '%Y-%m-%d')
                      ), 
                      try(
                        date_parse(
                          date_greffe, '%Y-%m-%d %hh:%mm:%ss.SSS'
                        )
                      ), 
                      try(
                        date_parse(
                          date_greffe, '%Y-%m-%d %hh:%mm:%ss'
                        )
                      ), 
                      try(
                        cast(date_greffe as timestamp)
                      )
                    )
                  ) as max_date_greffe 
                FROM 
                  voie_type_voie 
                GROUP BY 
                  siren, 
                  code_greffe, 
                  nom_greffe, 
                  numero_gestion, 
                  id_etablissement
              ) AS temp ON temp.siren = convert_date.siren 
              AND temp.code_greffe = convert_date.code_greffe 
              AND temp.nom_greffe = convert_date.nom_greffe 
              AND temp.numero_gestion = convert_date.numero_gestion 
              AND temp.id_etablissement = convert_date.id_etablissement 
              AND temp.max_date_greffe = convert_date.date_greffe
          ) as latest_libele ON voie_type_voie.siren = latest_libele.siren 
          AND voie_type_voie.code_greffe = latest_libele.code_greffe 
          AND voie_type_voie.nom_greffe = latest_libele.nom_greffe 
          AND voie_type_voie.numero_gestion = latest_libele.numero_gestion 
          AND voie_type_voie.id_etablissement = latest_libele.id_etablissement
      )
    ORDER BY siren, code_greffe, nom_greffe, numero_gestion, id_etablissement, date_greffe
"""
```

## Etape 5: Création `index_id`

Dans cette dernière étape, il faut créer un index avec le numéro de ligne dans la table. Il est possible de créer un numéro de ligne, sans prendre en compte l’ordre afin d’éviter le consommer toutes les ressources du serveur:

- Discussion [StackEdit](https://stackoverflow.com/questions/51090433/select-rows-by-index-in-amazon-athena)

```python
query = """
SELECT 
          ROW_NUMBER() OVER () as index_id,
"""
```
