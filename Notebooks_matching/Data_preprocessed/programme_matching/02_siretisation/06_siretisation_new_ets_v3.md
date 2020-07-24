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
        
        
Voici un tableau récapitulatif des règles appliquer sur les variables de l'adresse:

| Table | Variable | article | digit | debut/fin espace | espace | accent | Upper |
| --- | --- | --- | --- | --- | --- | --- | --- |
| INPI | adresse_regex_inpi | X | X | X | X | X | X |
|  | adress_distance_inpi | X |  | X | X | X | X |
|  | adresse_reconstituee_inpi |  |  | X | X | X | X |
| INSEE | adress_reconstituee_insee | X |  |  |  | |  |    

## Ouput

* Athena: 
    * region: eu-west-3 
    * Database: inpi 
    *  Table: ets_insee_inpi 
<!-- #endregion -->

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
   * `siren`
   * `ville_matching`  → `ville_matching`
   * `code_postal_matching`  → `codepostaletablissement`
* La première query consiste à rapprocher les deux tables INPI & INSEE [NEW]
   


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
  voie_matching as type_voie_matching, 
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
