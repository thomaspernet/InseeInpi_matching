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

# Siretisation Version 3: SQL

## Etape 

Décrire les étapes de siretisation en SQL 

US potentielles
Creation table rapprochement INSEE/INPI

1. US 1 match sur siren + ville + code postal

  1. US: US 01 rapprochement INSEE INPI
  2. US taiga:
  
Création règles de gestion

1. US 2 Création Levenshtein 

  1. US US 02 Variables regles de gestion
  2. US taiga:
  
2. US 3 Création Jaccard

  1. US US 02 Variables regles de gestion
  2. US taiga:
  
3. US 4 Creation Regex

  1.  US US 02 Variables regles de gestion
  2. US taiga:
  
4. US 5 Récupération minimum Levenshtein + Jaccard

  1. US XX
  2. US taiga:
  
5. US 6 Tests de logique

  1. US XX
  2. US taiga:
  
Dedoublonnage

1. US 6 Filtre selon règles de séparation

  1. US XX
  2.  US taiga:
  
2. US 7 Création indicateur de doublons

  1. Meme séquence, plusieurs siret → adresse différente au cours d’une séquence (ici, l’index est différent)
  2. Meme index, plusieurs siret → adresse très similaire entre deux siret (ici, l’index est identique)
  3. US XX
  4. US taiga:
  
3. US 8 Récupération index unique et siret unique

  1. US XX
  2. US taiga:
  
Deduction siret sur séquence

1. US 9 Attribution du siret sur une séquence

  1. US XX
  2. US taiga:
  
## Input

* Athena 
    * region: eu-west-3 
    * Database: inpi 
    *  Table: ets_final_sql 
      * Notebook construction file (data lineage) 
        * md : [03_ETS_add_variables.md](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/03_ETS_add_variables.md)
    *  Table: insee_final_sql 
      * Notebook construction file (data lineage) 
        * md : [04_ETS_add_variables_insee.md](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/04_ETS_add_variables_insee.md)

## Ouput

* Athena: 
    * region: eu-west-3 
    * Database: inpi 
    *  Table: ets_insee_inpi 


## Connexion serveru

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

# Parametres et fonctions

- `split_duplication`: Split un dataframe si l'index (la variable, pas l'index) contient des doublons
- `find_regex`: Performe une recherche regex entre deux colonnes
- `jackard_distance`: Calcul l'indice de dissimilarité entre deux colonnes
- `edit_distance`: Calcul le nombre de modification a faire pour obtenir la même séquence
- `import_dask`: Charge csv en Dask DataFrame pour clusteriser les calculs 


## Etape 1: rapprochement INSEE-INPI

*  Rapprocher la table de l’INSEE avec celle de l’INPI avec les variables de matching suivantes:
   * siren
   * ville_matching  → ville_matching
   * code_postal_matching  → codepostaletablissement 
* La première query consiste à rapprocher les deux tables INPI & INSEE [NEW]
   
### test Acceptance 

1. Compter nombre de lignes après match
  1. 11957437

```python
query = """
/*matching ets inpi insee*/
CREATE TABLE inpi.ets_insee_inpi
WITH (
  format='PARQUET'
) AS
  SELECT 
  index_id, 
  sequence_id, 
  count_initial_insee,
  ets_final_sql.siren, 
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
  voie_matching type_voie_matching, 
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
  -- libellecommuneetablissement, 
  ville_matching, 
  -- libellevoieetablissement, 
  -- complementadresseetablissement, 
  numerovoieetablissement, 
  -- indicerepetitionetablissement, 
  typevoieetablissement, 
  adress_reconstituee_insee, 
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
-- LIMIT 10
"""
```
