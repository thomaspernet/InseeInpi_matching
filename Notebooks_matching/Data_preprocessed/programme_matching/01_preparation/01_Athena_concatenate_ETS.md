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
# Preparation INPI-ETS

Dans ce notebook, on prepare la donnée ETS afin d'être concatenée, puis envoyée dans le S3.
- https://docs.aws.amazon.com/athena/latest/ug/csv.html

L'idée est de récupérer les stocks initiaux, partiels,créations et les événements pour créer un csv avec tout ce qui c'est passé au cours d'une année donnée tout en remplissant les éléments manquants.

Il y a 3 dossiers source:

- [Stock initial](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/TC_1/01_donnee_source/Stock/Stock_Initial/)
- [Stock partiel](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/TC_1/01_donnee_source/Stock/Stock_Partiel/)
- [Flux](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/TC_1/01_donnee_source/Flux/)


On détaille ici le process pour les Etablissements, mais il faut noter qu'il y a un notebook qui reprend exactement la même structure pour chaque catégorie:
- ACTES
- COMPTES_ANNUELS
- ETS
- OBS
- PM
- PP
- REP


Dans chacun des dossiers, on va récupérer la catégorie qui nous intéresse, selon l'année:

```
01_donnees_source
    ├───Flux
    │   ├───2017
    │   │   ├───ACTES
    │   │   │   └───NEW
    │   │   ├───COMPTES_ANNUELS
    │   │   │   └───NEW
    │   │   ├───ETS
    │   │   │   ├───EVT
    │   │   │   └───NEW
    │   │   ├───OBS
    │   │   │   └───NEW
    │   │   ├───PM
    │   │   │   ├───EVT
    │   │   │   └───NEW
    │   │   ├───PP
    │   │   │   ├───EVT
    │   │   │   └───NEW
    │   │   └───REP
    │   │       ├───EVT
    │   │       └───NEW
    │   ├───2018
    │   │   ├───ACTES
    │   │   │   └───NEW
    │   │   ├───COMPTES_ANNUELS
    │   │   │   └───NEW
    │   │   ├───ETS
    │   │   │   ├───EVT
    │   │   │   └───NEW
    │   │   ├───OBS
    │   │   │   └───NEW
    │   │   ├───PM
    │   │   │   ├───EVT
    │   │   │   └───NEW
    │   │   ├───PP
    │   │   │   ├───EVT
    │   │   │   └───NEW
    │   │   └───REP
    │   │       ├───EVT
    │   │       └───NEW
    │   └───2019
    │   │   ├───ACTES
    │   │   │   └───NEW
    │   │   ├───COMPTES_ANNUELS
    │   │   │   └───NEW
    │   │   ├───ETS
    │   │   │   ├───EVT
    │   │   │   └───NEW
    │   │   ├───OBS
    │   │   │   └───NEW
    │   │   ├───PM
    │   │   │   ├───EVT
    │   │   │   └───NEW
    │   │   ├───PP
    │   │   │   ├───EVT
    │   │   │   └───NEW
    │   │   └───REP
    │   │       ├───EVT
    │   │       └───NEW
    └───Stock
        ├───Stock_initial
            ├───2017
            │   ├───ACTES
            │   ├───COMPTES_ANNUELS
            │   ├───ETS
            │   ├───OBS
            │   ├───PM
            │   ├───PP
            │   └───REP
        └───Stock_partiel
            ├───2018
            │   ├───ACTES
            │   ├───COMPTES_ANNUELS
            │   ├───ETS
            │   ├───OBS
            │   ├───PM
            │   ├───PP
            │   └───REP
            ├───2019
            │   ├───ACTES
            │   ├───COMPTES_ANNUELS
            │   ├───ETS
            │   ├───OBS
            │   ├───PM
            │   ├───PP
            │   └───REP
            └───2020
                ├───ACTES
                ├───COMPTES_ANNUELS
                ├───ETS
                ├───OBS
                ├───PM
                ├───PP
                └───REP
```

Exemple pour les Etablissements

- Stock:
    - [Stock initial](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/TC_1/01_donnee_source/Stock/Stock_initial/ETS/)
    - [Stock partiel 2018](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/TC_1/01_donnee_source/Stock/Stock_partiel/2018/ETS)
- Flux
    - [NEW 2017](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/TC_1/01_donnee_source/Flux/2017/ETS/NEW/)
    - [EVT 2017](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/TC_1/01_donnee_source/Flux/2017/ETS/EVT/)


## Steps: Benchmark ETS

- Step 1: Parametres et queries
	- [Préparation parametres json](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/01_Athena_concatenate_ETS.md#pr%C3%A9paration-json-parameters)
	- [Query préparation table](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/01_Athena_concatenate_ETS.md#query-pr%C3%A9paration-table)
    - [Query preparation evt](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/01_Athena_concatenate_ETS.md#query-pr%C3%A9paration-%C3%A9v%C3%A9nement)
	- [Query preparation partiel](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/01_Athena_concatenate_ETS.md#query-preparation-partiel)
	- [Query remplissage EVT](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/01_Athena_concatenate_ETS.md#query-remplissage-evt)

- [Step 2](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/01_Athena_concatenate_ETS.md#step-2-concatenation-data): Concatenation et preparation data
	- Stock
		- [Initial](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/TC_1/01_donnee_source/Stock/Stock_Initial/)
		- [Partiel](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/TC_1/01_donnee_source/Stock/Stock_Partiel/)
	- Flux
		- [NEW](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/TC_1/01_donnee_source/Flux/2017/ETS/NEW/)
		- [EVT](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/TC_1/01_donnee_source/Flux/2017/ETS/EVT/)
			- Remplissage valeurs manquantes

- Step 3: Creation table Initial/Partiel/EVT/NEW

- Step 4: Creation statut partiel
	- Création colonne status qui indique si les lignes sont a ignorer ou non

- Step 5: Remplissage observations manquantes
	- Récupération informations selon `Origin` (`Stock` ou `NEW`) pour compléter les valeurs manquantes des `EVT` 
    
    
    
## Tables Athena:

- ets_evt_2017: [Step 2](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/01_Athena_concatenate_ETS.md#step-2-concatenation-data)
- ets_evt_2018: [Step 2](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/01_Athena_concatenate_ETS.md#step-2-concatenation-data)
- ets_evt_2019: [Step 2](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/01_Athena_concatenate_ETS.md#step-2-concatenation-data)
- ets_initial: [Step 2](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/01_Athena_concatenate_ETS.md#step-2-concatenation-data)
- ets_new_2017: [Step 2](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/01_Athena_concatenate_ETS.md#step-2-concatenation-data)
- ets_new_2018: [Step 2](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/01_Athena_concatenate_ETS.md#step-2-concatenation-data)
- ets_new_2019: [Step 2](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/01_Athena_concatenate_ETS.md#step-2-concatenation-data)
- ets_partiel_2018: [Step 2](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/01_Athena_concatenate_ETS.md#step-2-concatenation-data)
- ets_partiel_2019: [Step 2](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/01_Athena_concatenate_ETS.md#step-2-concatenation-data)
- initial_partiel_evt_new_etb: [Step 3](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/01_Athena_concatenate_ETS.md#step-3-creation-table-initialpartielevtnew)
- initial_partiel_evt_new_ets_status: [Step 4](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/01_Athena_concatenate_ETS.md#step-4-creation-statut-partiel)
- initial_partiel_evt_new_ets_status_final: [Step 5](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/01_Athena_concatenate_ETS.md#step-5-remplissage-observations-manquantes)

## Regles de gestion considérées

*  Une séquence est un classement chronologique pour le triplet (quadruplet pour les Etablissements) suivant:
  * siren + code greffe + numero gestion (+ ID établissement pour les Etablissements)
  * Dans la table finale, le classement a cette forme : siren,Nom_Greffe, "code greffe",numero_gestion, (id_etablissement pour les Etablissements) , file_timestamp
* Une ligne ``partiel`` va rendre caduque l'ensemble des séquences précédentes.
* Une ligne ``événement`` ne modifie que le champs comportant la modification. Les champs non modifiés vont être remplis par la ligne t-1
    * Le remplissage doit se faire de deux façons
        * une première fois avec la date de transmission (plusieurs informations renseignées pour une meme date de transmission pour une même séquence). La dernière ligne remplie des valeurs précédentes de la séquence
        * une seconde fois en ajoutant les valeurs non renseignées pour cet évènement, en se basant sur les informations des lignes précédentes du triplet (quadruplet pour les Etablissements). Les lignes précédentes ont une date de transmission différente et/ou initial, partiel et création.
<!-- #endregion -->

```python
# Import des librairies S3
from awsPy.aws_authorization import aws_connector
from awsPy.aws_s3 import service_s3
from awsPy.aws_athena import service_athena
import os, time
from pathlib import Path
```

```python
# Connexion S3
bucket = 'calfdata'
path = os.getcwd()
parent_path = str(Path(path).parent)
path_cred = "{}/credential_AWS.json".format(parent_path)
con = aws_connector.aws_instantiate(credential = path_cred,
                                        region = 'eu-west-3')
client= con.client_boto()
s3 = service_s3.connect_S3(client = client,
                      bucket = 'calfdata') 
athena = service_athena.connect_athena(client = client,
                      bucket = 'calfdata') 
```

## Step 1: Parametres et queries

Pour faciliter l'ingestion de données en batch, on prépare un json ``dic_`` avec les paths où récupérer la data, le nom des tables, les origines, mais aussi un champ pour récupérer l'ID de l'execution dans Athena. En effet, chaque execution donne lieu a un ID. Certaines queries peuvent prendre plusieurs minutes. Athena crée un CSV dans un folder output prédéfini dont le nom est l'ID de la query. Notre process utilise la concaténation automatique d'Athena pour créer les tables. Il faut nécessairement déplacer les csv dans des dossiers destination en vue de la concatenation. Le stockage de l'ID est donc indispensable pour copier l'objet, surtout lorsque la query prend du temps d'execution.


### Préparation json parameters

```python
dic_ = {
    'global':{
        'database':'inpi',
        'output':'INPI/sql_output',
        'output_preparation':'INPI/sql_output_preparation',
        'ETS_step4_id':[],
        'table_final_id':{
            'ETS':{
            }
        }
    },
    'Stock': {
        'INITIAL':{
            'ETS': {
                'path':'s3://calfdata/INPI/TC_1/01_donnee_source/Stock/Stock_Initial/2017/ETS',
                'tables':'ets_initial',
                'origin':'INITIAL',
                'output_id':[]
            }
        },
        'PARTIEL':{
            'ETS': {
                'path':[
                    's3://calfdata/INPI/TC_1/01_donnee_source/Stock/Stock_Partiel/2018/ETS',
                    's3://calfdata/INPI/TC_1/01_donnee_source/Stock/Stock_Partiel/2019/ETS'
                       ],
                'tables':[
                    'ets_partiel_2018',
                    'ets_partiel_2019'],
                'origin':'PARTIEL',
                'output_id':[]
            }
        }
    },
    'Flux': {
        'NEW':{
            'ETS': {
                'path':[
                    's3://calfdata/INPI/TC_1/01_donnee_source/Flux/2017/ETS/NEW',
                    's3://calfdata/INPI/TC_1/01_donnee_source/Flux/2018/ETS/NEW',
                    's3://calfdata/INPI/TC_1/01_donnee_source/Flux/2019/ETS/NEW'
                       ],
                'tables':[
                    'ets_new_2017',
                    'ets_new_2018',
                    'ets_new_2019'],
                'origin':'NEW',
                'output_id':[]
            }
        },
        'EVT':{
            'ETS': {
                'path':[
                    's3://calfdata/INPI/TC_1/01_donnee_source/Flux/2017/ETS/EVT',
                    's3://calfdata/INPI/TC_1/01_donnee_source/Flux/2018/ETS/EVT',
                    's3://calfdata/INPI/TC_1/01_donnee_source/Flux/2019/ETS/EVT',
                ],
                'tables':[
                    'ets_evt_2017',
                    'ets_evt_2018',
                    'ets_evt_2019'],
                'origin':'EVT',
                'output_id':[]
            }
        }
    }
}
```

### Query préparation table

On prédéfini les requêtes qui seront à éxecuter dans Athena. Les paramètres des queries seront récupérés dans ``dic_`` au moment de l'éxecution de la query.

```python
### query_db = "CREATE DATABASE IF NOT EXISTS {};"

query_tb = \
    """CREATE EXTERNAL TABLE IF NOT EXISTS {0}.{1} (
`Code Greffe`                     string,
`Nom_Greffe`                      string,
`Numero_Gestion`                  string,
`Siren`                            string,
`Type`                            string,
`Siège_PM`                        string,
`RCS_Registre`                   string,
`Adresse_Ligne1`                  string,
`Adresse_Ligne2`                  string,
`Adresse_Ligne3`                  string,
`Code_Postal`                    string,
`Ville`                           string,
`Code_Commune`                   string,
`Pays`                            string,
`Domiciliataire_Nom`              string,
`Domiciliataire_Siren`           string,
`Domiciliataire_Greffe`           string,
`Domiciliataire_Complément`      string,
`Siege_Domicile_Représentant`     string,
`Nom_Commercial`                  string,
`Enseigne`                       string,
`Activité_Ambulante`              string,
`Activité_Saisonnière`            string,
`Activité_Non_Sédentaire`         string,
`Date_Début_Activité`             string,
`Activité`                        string,
`Origine_Fonds`                   string,
`Origine_Fonds_Info`              string,
`Type_Exploitation`               string,
`ID_Etablissement`                 string,
`Date_Greffe`                     string,
`Libelle_Evt`                     string,
`csv_source` string,
`nature` string,
`type_data` string,
`origin` string,
`file_timestamp` string
    )
     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
   'separatorChar' = '{3}',
   'quoteChar' = '"'
   )
     LOCATION '{2}'
     TBLPROPERTIES ('has_encrypted_data'='false',
              'skip.header.line.count'='1');"""

query_table_concat = \
    """CREATE EXTERNAL TABLE IF NOT EXISTS {0}.{1} (
`siren` string, 
`code greffe` int, 
`Nom_Greffe` string, 
`numero_gestion` string, 
`id_etablissement` string,  
`file_timestamp` string, 
`Libelle_Evt` string, 
`Date_Greffe` string,     
`Type` string, 
`Siège_PM` string, 
`RCS_Registre` string, 
`Adresse_Ligne1` string, 
`Adresse_Ligne2` string, 
`Adresse_Ligne3` string, 
`Code_Postal` string, 
`Ville` string, 
`Code_Commune` string, 
`Pays` string, 
`Domiciliataire_Nom` string, 
`Domiciliataire_Siren` string, 
`Domiciliataire_Greffe` string, 
`Domiciliataire_Complément` string, 
`Siege_Domicile_Représentant` string, 
`Nom_Commercial` string, 
`Enseigne` string, 
`Activité_Ambulante` string, 
`Activité_Saisonnière` string, 
`Activité_Non_Sédentaire` string, 
`Date_Début_Activité` string, 
`Activité` string, 
`Origine_Fonds` string, 
`Origine_Fonds_Info` string, 
`Type_Exploitation` string, 
`csv_source` string, 
`origin` string
    )
     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
   'separatorChar' = ',',
   'quoteChar' = '"'
   )
     LOCATION '{2}'
     TBLPROPERTIES ('has_encrypted_data'='false',
              'skip.header.line.count'='1');"""

query_drop = """ DROP TABLE `{}`;"""

query_select = """SELECT 
"siren",
"code greffe",
"Nom_Greffe",
"numero_gestion",
"id_etablissement", 
"file_timestamp",
"Libelle_Evt",
"Date_Greffe",    
"Type",
"Siège_PM",
"RCS_Registre",
"Adresse_Ligne1",
"Adresse_Ligne2",
"Adresse_Ligne3",
"Code_Postal",
"Ville",
"Code_Commune",
"Pays",
"Domiciliataire_Nom",
"Domiciliataire_Siren",
"Domiciliataire_Greffe",
"Domiciliataire_Complément",
"Siege_Domicile_Représentant",
"Nom_Commercial",
"Enseigne",
"Activité_Ambulante",
"Activité_Saisonnière",
"Activité_Non_Sédentaire",
"Date_Début_Activité",
"Activité",
"Origine_Fonds",
"Origine_Fonds_Info",
"Type_Exploitation",
"csv_source",
"origin"
FROM "inpi"."{}"
WHERE "siren" !=''
"""
```

<!-- #region -->
### Query préparation événement

La query est générée via un loop dans l'étape 3 afin d'éviter les copier/coller redondants. Dans l'ensemble, la query va reconstruire l'ensemble des valeurs manquantes pour chaque csv (ie date de transmission). A noter que la query va récupérer la dernière ligne du quadruplet `siren`,`code greffe`, `numero_gestion`, `id_etablissement`. 

La liste des champs pouvant être affectés par un changement est stockée dans `list_change`.

#### Detail

Il y a des événements ayant plusieurs entrées a des moments identiques affectant les mêmes établissements.
Quelle est la règle à appliquer lorsqu'un événement est effectué le même jour, à la même heure pour un même établissement (même quadruplet: siren + code greffe + numero gestion + ID établissement)?

Reponse INPI:
  * "Il faut modifier, en respectant l’ordre des lignes, uniquement les champs où il y a une valeur."
  * Oui, il faut additionner les champs modifiés. Attention si un même champ est modifié (avec des valeurs différentes) en ligne 2, puis en ligne 3, il faudra privilégier les valeurs de la ligne 3.

Exemple:

* SIREN: [420844656](https://calfdata.s3.eu-west-3.amazonaws.com/INPI/TC_1/02_preparation_donnee/check_csv/420844656.csv)
  * même établissement, plusieurs entrées
    - exemple SIREN 420844656, évenement effectué le 2018/01/03 a 08:48:10. Nom dans le FTP `0101_163_20180103_084810_9_ets_nouveau_modifie_EVT.csv`
    * fichier [source](https://calfdata.s3.eu-west-3.amazonaws.com/INPI/TC_1/01_donnee_source/Flux/2018/ETS/EVT/0101_163_20180103_084810_9_ets_nouveau_modifie_EVT.csv)
    
    
#### Test de vérification

* Exemple avec SIREN [513913657](https://calfdata.s3.eu-west-3.amazonaws.com/INPI/TC_1/02_preparation_donnee/check_csv/513913657.xlsx):
  * Raw data dans S3
```
    * ["INPI/TC_1/01_donnee_source/Flux/2018/ETS/EVT/3801_189_20180130_065752_9_ets_nouveau_modifie_EVT.csv",
    * "INPI/TC_1/01_donnee_source/Flux/2018/ETS/EVT/3801_190_20180131_065908_9_ets_nouveau_modifie_EVT.csv",
    * "INPI/TC_1/01_donnee_source/Flux/2018/ETS/EVT/3801_209_20180227_065600_9_ets_nouveau_modifie_EVT.csv",
    * "INPI/TC_1/01_donnee_source/Flux/2018/ETS/EVT/3801_213_20180303_064240_9_ets_nouveau_modifie_EVT.csv",
    * "INPI/TC_1/01_donnee_source/Flux/2018/ETS/EVT/3801_222_20180316_063210_9_ets_nouveau_modifie_EVT.csv",
    * "INPI/TC_1/01_donnee_source/Flux/2018/ETS/EVT/3801_293_20180627_061209_9_ets_nouveau_modifie_EVT.csv",
    * "INPI/TC_1/01_donnee_source/Flux/2018/ETS/EVT/3801_301_20180711_065600_9_ets_nouveau_modifie_EVT.csv"]
```    

* Details
    * La feuille FROM_FTP regroupe tous les événements affectants le siren  513913657 en 2018 uniquement (7 au total)
```
      *  3801_189_20180130_065752_9_ets_nouveau_modifie_EVT.csv
      *  3801_190_20180131_065908_9_ets_nouveau_modifie_EVT.csv
      *  3801_209_20180227_065600_9_ets_nouveau_modifie_EVT.csv
      *  3801_213_20180303_064240_9_ets_nouveau_modifie_EVT.csv
      *  3801_222_20180316_063210_9_ets_nouveau_modifie_EVT.csv
      *  3801_293_20180627_061209_9_ets_nouveau_modifie_EVT.csv
      *  3801_301_20180711_065600_9_ets_nouveau_modifie_EVT.csv
 ```
* En tout, il y a 83 entrées. Dans la feuille `FROM_FTP`, chaque couleur représente un csv (regroupé par date de transmission). Comme indiqué par Mr Flament, il faut remplir les entrées d’un même csv par l’entrée n-1. La dernière entrée fait foi si différente avec n-1. Dans la feuille, c’est les ligne jaunes.
* La feuille FILLIN va faire cette étape de remplissage, et la feuille FILTER récupère uniquement la dernière ligne par date de transmission. L’enseigne est indiqué comme supprimée dans la donnée brute a différentes dates de transmission mais supprimé lors de la dernière transmission.
<!-- #endregion -->

```python
list_change = [
"Libelle_Evt",
"Date_Greffe",    
"Type",
"Siège_PM",
"RCS_Registre",
"Adresse_Ligne1",
"Adresse_Ligne2",
"Adresse_Ligne3",
"Code_Postal",
"Ville",
"Code_Commune",
"Pays",
"Domiciliataire_Nom",
"Domiciliataire_Siren",
"Domiciliataire_Greffe",
"Domiciliataire_Complément",
"Siege_Domicile_Représentant",
"Nom_Commercial",
"Enseigne",
"Activité_Ambulante",
"Activité_Saisonnière",
"Activité_Non_Sédentaire",
"Date_Début_Activité",
"Activité",
"Origine_Fonds",
"Origine_Fonds_Info",
"Type_Exploitation",
"csv_source"#,
#"origin"    
#"nature",
#"type_data",
#"
]

top = """WITH createID AS (
  SELECT 
   *, 
    ROW_NUMBER() OVER (
      PARTITION BY 
      siren,
      "code greffe",
      "Nom_Greffe",
      numero_gestion,
      id_etablissement, 
      file_timestamp
    ) As row_ID, 
    DENSE_RANK () OVER (
      ORDER BY 
        siren, 
        "code greffe",
        "Nom_Greffe",
        numero_gestion, 
        id_etablissement, 
        file_timestamp
    ) As ID 
  FROM 
    "inpi"."{}" 
) 
SELECT 
  * 
FROM 
  (
    WITH filled AS (
      SELECT 
        ID, 
        row_ID, 
        siren, 
        "Nom_Greffe",
        "code greffe", 
        numero_gestion, 
        id_etablissement, 
        file_timestamp, 
"""

top_1 = """first_value("{0}") over (partition by ID, "{0}_partition" order by 
ID, row_ID
 ) as "{0}"
"""

middle = """FROM 
        (
          SELECT 
            *, """

middle_2 = """sum(case when "{0}" = '' then 0 else 1 end) over (partition by ID 
order by  row_ID) as "{0}_partition" 
"""

bottom = """ 
          FROM 
            createID 
          ORDER BY 
            ID, row_ID ASC
        ) 
      ORDER BY 
        ID, 
        row_ID
    ) 
    SELECT "siren",
"code greffe",
"Nom_Greffe",
"numero_gestion",
"id_etablissement", 
"file_timestamp",
"Libelle_Evt",
"Date_Greffe",    
"Type",
"Siège_PM",
"RCS_Registre",
"Adresse_Ligne1",
"Adresse_Ligne2",
"Adresse_Ligne3",
"Code_Postal",
"Ville",
"Code_Commune",
"Pays",
"Domiciliataire_Nom",
"Domiciliataire_Siren",
"Domiciliataire_Greffe",
"Domiciliataire_Complément",
"Siege_Domicile_Représentant",
"Nom_Commercial",
"Enseigne",
"Activité_Ambulante",
"Activité_Saisonnière",
"Activité_Non_Sédentaire",
"Date_Début_Activité",
"Activité",
"Origine_Fonds",
"Origine_Fonds_Info",
"Type_Exploitation",
"csv_source",
CASE WHEN siren IS NOT NULL THEN 'EVT' 
ELSE NULL END as origin
    FROM 
      (
        SELECT 
          *, 
          ROW_NUMBER() OVER(
            PARTITION BY ID 
            ORDER BY 
              ID, row_ID DESC
          ) AS max_value 
        FROM 
          filled
      ) AS T 
    WHERE 
      max_value = 1
  )ORDER BY siren,"Nom_Greffe", "code greffe",
      numero_gestion, id_etablissement, 
      file_timestamp
"""
```

```python
query_fillin = top.format('test')
for x, val in enumerate(list_change):

    if x != len(list_change) -1:
        query_fillin+=top_1.format(val)+ ","
    else:
        query_fillin+=top_1.format(val)
        query_fillin+= middle

for x, val in enumerate(list_change):
    if x != len(list_change) -1:
        query_fillin+=middle_2.format(val)+ ","
    else:
        query_fillin+=middle_2.format(val)
        query_fillin+=bottom
```

<!-- #region -->
### Query préparation partiel

Dans cette étape, il faut vérifier si un quadruplet `siren`,`code greffe`, `numero_gestion`, `id_etablissement` possède une ligne `Partiel`. Auquel cas, une nouvelle variable est recréée indiquant pour toutes les lignes précédant un `Partiel` les valeurs à ignorer. On prend la date maximum `date_max` des stocks partiels par quadruplet, si la date de transfert est inférieure a la `date_max`, alors on ignore.

#### Détails

* Définition Partiel:
  * si csv dans le dossier Partiel, année > 2017, alors partiel
    * la date d'ingestion est indiquée dans le path, ie comme les flux
    
1/ Est-ce que les csv dans le dossier Stock pour une date supérieure à 2017 peuvent être labélisés comme « partiel » rendant ainsi caduque toutes les valeurs précédentes d’un établissement ?
* OUI (Reponse Flament Lionel <lflament@inpi.fr>)

2/ Pour identifier un établissement, il faut bien choisir la triplette siren + numero dossier (greffe) + ID établissement ?
* → il faut choisir le quadruplet siren + code greffe + numero gestion + ID établissement  (Reponse Flament Lionel <lflament@inpi.fr>)


#### Test de vérification

On utilise dans un Excel un exemple avec les valeurs du siren 428689392 ayant des ID établissements identiques pour des adresses différentes. J’ai souligné en bleu les valeurs qui potentiellement amendent la ligne n-1 (ex ligne 10 amende la ligne 9) -> fait référence au point 1/
Pour le point 2, il y a par exemple, l’ID établissement 10 qui appartient à la fois a Rennes, mais aussi Nanterre. De fait, il faut bien distinguer le greffe, car ce sont 2 établissements différents.

* Colonne id_etablissement 
  * Pour les événements, il faut trier avec la pair nom_greffe et ou numero_gestion  
  * Exemple: [428689392](s3://calfdata/INPI/TC_1/02_preparation_donnee/check_csv/428689392.xlsx)
<!-- #endregion -->

```python
query_partiel = """WITH to_date AS (
  SELECT 
"siren",
"code greffe",
"Nom_Greffe",
"numero_gestion",
"id_etablissement", 
"Libelle_Evt",
"Date_Greffe",    
"Type",
"Siège_PM",
"RCS_Registre",
"Adresse_Ligne1",
"Adresse_Ligne2",
"Adresse_Ligne3",
"Code_Postal",
"Ville",
"Code_Commune",
"Pays",
"Domiciliataire_Nom",
"Domiciliataire_Siren",
"Domiciliataire_Greffe",
"Domiciliataire_Complément",
"Siege_Domicile_Représentant",
"Nom_Commercial",
"Enseigne",
"Activité_Ambulante",
"Activité_Saisonnière",
"Activité_Non_Sédentaire",
"Date_Début_Activité",
"Activité",
"Origine_Fonds",
"Origine_Fonds_Info",
"Type_Exploitation",
"csv_source",
"origin", Coalesce(try(cast(file_timestamp as timestamp)))  as file_timestamp
FROM "inpi"."initial_partiel_evt_new_etb"
WHERE siren !='' AND file_timestamp !=''
                 )
SELECT *
FROM (
  WITH max_date_partiel AS(
SELECT siren, "code greffe", nom_greffe, numero_gestion, id_etablissement,
MAX(file_timestamp) as max_partiel
FROM to_date
WHERE origin = 'Partiel'
GROUP BY  siren, "code greffe", nom_greffe, numero_gestion, id_etablissement
    )
  SELECT 
  to_date."siren",
to_date."code greffe",
to_date."Nom_Greffe",
to_date."numero_gestion",
to_date."id_etablissement", 
to_date."file_timestamp",
max_date_partiel.max_partiel,
CASE WHEN to_date."file_timestamp" <  max_date_partiel.max_partiel 
  THEN 'IGNORE' ELSE NULL END AS status, 
to_date."origin" ,
to_date."Libelle_Evt",
to_date."Date_Greffe",    
to_date."Type",
to_date."Siège_PM",
to_date."RCS_Registre",
to_date."Adresse_Ligne1",
to_date."Adresse_Ligne2",
to_date."Adresse_Ligne3",
to_date."Code_Postal",
to_date."Ville",
to_date."Code_Commune",
to_date."Pays",
to_date."Domiciliataire_Nom",
to_date."Domiciliataire_Siren",
to_date."Domiciliataire_Greffe",
to_date."Domiciliataire_Complément",
to_date."Siege_Domicile_Représentant",
to_date."Nom_Commercial",
to_date."Enseigne",
to_date."Activité_Ambulante",
to_date."Activité_Saisonnière",
to_date."Activité_Non_Sédentaire",
to_date."Date_Début_Activité",
to_date."Activité",
to_date."Origine_Fonds",
to_date."Origine_Fonds_Info",
to_date."Type_Exploitation",
to_date."csv_source"
  FROM to_date
  LEFT JOIN max_date_partiel on
  to_date.siren =max_date_partiel.siren AND
  to_date."code greffe" =max_date_partiel."code greffe" AND
  to_date.nom_greffe =max_date_partiel.nom_greffe AND
  to_date.numero_gestion =max_date_partiel.numero_gestion AND
  to_date.id_etablissement =max_date_partiel.id_etablissement
  ORDER BY siren, "code greffe", nom_greffe, numero_gestion,
  id_etablissement, file_timestamp
  )"""

query_table_all = """
CREATE EXTERNAL TABLE IF NOT EXISTS {0}.{1} (
`siren` string,
`code greffe` int,
`Nom_Greffe` string,
`numero_gestion` string,
`id_etablissement` string ,
`file_timestamp` string,
`max_partiel` string,
`status` string,
`origin` string,
`Libelle_Evt` string,
`Date_Greffe` string    ,
`Type` string,
`Siège_PM` string,
`RCS_Registre` string,
`Adresse_Ligne1` string,
`Adresse_Ligne2` string,
`Adresse_Ligne3` string,
`Code_Postal` string,
`Ville` string,
`Code_Commune` string,
`Pays` string,
`Domiciliataire_Nom` string,
`Domiciliataire_Siren` string,
`Domiciliataire_Greffe` string,
`Domiciliataire_Complément` string,
`Siege_Domicile_Représentant` string,
`Nom_Commercial` string,
`Enseigne` string,
`Activité_Ambulante` string,
`Activité_Saisonnière` string,
`Activité_Non_Sédentaire` string,
`Date_Début_Activité` string,
`Activité` string,
`Origine_Fonds` string,
`Origine_Fonds_Info` string,
`Type_Exploitation` string,
`csv_source` string
)
     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
   'separatorChar' = ',',
   'quoteChar' = '"'
   )
     LOCATION '{2}'
     TBLPROPERTIES ('has_encrypted_data'='false',
              'skip.header.line.count'='1');
"""
```

### Query remplissage EVT via initial, partiel, creation

Il y a deux étapes à suivre. 

Pour remplir les événements, il faut prendre la ligne t-1, et compléter les champs manquants. En effet, l'INPI ne transmet que les champs modifiés, les champs non modifiés sont transmis vides.
Dans l'[étape 2](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/01_Athena_concatenate_ETS.md#step-2-concatenation-data), nous avons pu remplir puis garder le dernier événement par date de transmission. Toutefois, dans la majeure partie des cas, les champs sont vides, car ils n'ont pas d'antécédents. L'antécédent provient soit d'un événement initial, soit d'un partiel ou création. Dans le cas de figure ou l'événement est une création.

Finalement, il faut reconstituer les valeurs manquantes des evenements en utilisant les informations qui ne sont pas communiquées dans les csv événements. En effet, le csv événement ne renseigne que les valeurs obligatoires et les modifications, laissant vides les autres champs. Pour récupérer les champs manquants, il faut utliser la valeur précédente pour le quadruplet `siren`,`code greffe`, `numero_gestion`, `id_etablissement`.


```python
middle_3 = """sum(case when origin = 'EVT' AND "{0}" = '' then 0 else 1 end) 
over (partition by ID 
order by  row_ID) as "{0}_partition" 
"""

bottom_1 = """ 
         FROM 
            createID 
          ORDER BY 
            ID, row_ID ASC
        ) 
      ORDER BY 
        ID, 
        row_ID
    ) 
    SELECT "siren",
"code greffe",
"Nom_Greffe",
"numero_gestion",
"id_etablissement", 
"status",
CASE WHEN siren IS NOT NULL THEN 'EVT' 
ELSE NULL END as origin,    
"Date_Greffe",  
"file_timestamp",
"Libelle_Evt",  
"Type",
"Siège_PM",
"RCS_Registre",
"Adresse_Ligne1",
"Adresse_Ligne2",
"Adresse_Ligne3",
"Code_Postal",
"Ville",
"Code_Commune",
"Pays",
"Domiciliataire_Nom",
"Domiciliataire_Siren",
"Domiciliataire_Greffe",
"Domiciliataire_Complément",
"Siege_Domicile_Représentant",
"Nom_Commercial",
"Enseigne",
"Activité_Ambulante",
"Activité_Saisonnière",
"Activité_Non_Sédentaire",
"Date_Début_Activité",
"Activité",
"Origine_Fonds",
"Origine_Fonds_Info",
"Type_Exploitation",
"csv_source"
    FROM filled
  )ORDER BY siren,"Nom_Greffe", "code greffe",
      numero_gestion, id_etablissement, 
      file_timestamp
  )
    )
"""
```

```python
query_evt_2 = """WITH convert AS (
  SELECT siren,
"code greffe","Nom_Greffe", numero_gestion, id_etablissement, origin, "status",
Coalesce(
         try(date_parse(file_timestamp, '%Y-%m-%d')),
         try(date_parse(file_timestamp, '%Y-%m-%d %hh:%mm:%ss.SSS')),
         try(date_parse(file_timestamp, '%Y-%m-%d %hh:%mm:%ss')),
         try(cast(file_timestamp as timestamp))
       )  as file_timestamp,

Coalesce(
         try(date_parse(date_greffe, '%Y-%m-%d')),
         try(date_parse(date_greffe, '%Y/%m/%d')),
         try(date_parse(date_greffe, '%d %M %Y')),
         try(date_parse(date_greffe, '%d/%m/%Y')),
         try(date_parse(date_greffe, '%d-%m-%Y'))
  )
  as date_greffe,libelle_evt, "Type","Siège_PM","RCS_Registre","Adresse_Ligne1",
"Adresse_Ligne2","Adresse_Ligne3","Code_Postal","Ville","Code_Commune",
"Pays","Domiciliataire_Nom","Domiciliataire_Siren","Domiciliataire_Greffe",
"Domiciliataire_Complément","Siege_Domicile_Représentant","Nom_Commercial",
"Enseigne","Activité_Ambulante","Activité_Saisonnière",
"Activité_Non_Sédentaire","Date_Début_Activité","Activité","Origine_Fonds",
"Origine_Fonds_Info","Type_Exploitation", "max_partiel","csv_source"
  FROM "inpi"."{}"
  /*WHERE siren ='449361179' AND id_etablissement = '7' AND 
  numero_gestion = '2011B00329' AND file_timestamp != ''*/
  )SELECT * 
  FROM (
    WITH temp AS (
                 SELECT siren,
                 "code greffe",
                 "Nom_Greffe",
                 numero_gestion,
                 id_etablissement,
                 origin, 
                 "status",
                 file_timestamp,
                 date_greffe, libelle_evt,"""
```

## Step 2 Concatenation data
                
### Details Steps:

L'ID de la query `creation_csv` est stocké dans le `dic_` car il faut plusieurs minutes pour lire les tables et sauvegarder en csv.

A noter que la query `query_csv` ne prend pas toutes les variables (celles crééent lors de l'extraction du FTP) car manque de mémoire lors de la préparation des événements.

- Stock
    - Initial:
        - Création table en concatenant tous les fichiers de ce dossier [Stock_initial/ETS](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/TC_1/01_donnee_source/Stock/Stock_initial/ETS/)
            - Output: [INPI/sql_output/](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/sql_output/)
        - Création csv
            - Output stocké dans le dictionaire des paramètres key `output_id`
    - Partiel:
        - Création table en concatenant tous les fichiers de ce dossier [Stock/Stock_partiel](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/TC_1/01_donnee_source/Stock/Stock_partiel/)
            - Output: [INPI/sql_output/](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/sql_output/)
        - Création csv
            - Output stocké dans le dictionaire des paramètres key `output_id`
- Flux
    - NEW:
        - Création table en concatenant tous les fichiers de ce dossier [ETS/NEW](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/TC_1/01_donnee_source/Flux/2017/ETS/NEW/)
            - Output: [INPI/sql_output/](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/sql_output/)
        - Création csv
            - Output stocké dans le dictionaire des paramètres key `output_id`
    - EVT
        - Création table en concatenant tous les fichiers de ce dossier [ETS/EVT](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/TC_1/01_donnee_source/Flux/2017/ETS/EVT/)
            - Output: [INPI/sql_output/](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/sql_output/)
        - Création csv: Run [query](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/01_Athena_concatenate_ETS.md#query-pr%C3%A9paration-%C3%A9v%C3%A9nement) pour remplir les valeurs manquantes et extraire l'entrée max par jour/heure de transmission.
            - Output stocké dans le dictionaire des paramètres key `output_id`

```python
# Drop previous tables
for i in ['ets_initial', 'ets_partiel_2018', 'ets_partiel_2019',
          'ets_new_2017', 'ets_new_2018', 'ets_new_2019', 'ets_evt_2017',
         'ets_evt_2018', 'ets_evt_2019']:
    query = "DROP TABLE `{}`".format(i)
    output = athena.run_query(
                        query=query,
                        database=dic_['global']['database'],
                        s3_output=dic_['global']['output']
                    )
    print(output['QueryExecutionId'])
```

```python
# Create tables
for nature, values in dic_.items():
    if nature != 'global':
        for origin, val in dic_[nature].items():
            for type_, v in dic_[nature][origin].items():
                if origin == 'INITIAL':
                    #### Creation table
                    create_table = query_tb.format(
                        dic_['global']['database'],
                        v['tables'],
                        v['path'],
                        ";"
                    )
                    time.sleep(2)
                    athena.run_query(
                        query=create_table,
                        database=dic_['global']['database'],
                        s3_output=dic_['global']['output'])
                    
                    #### Creation CSV
                    time.sleep(1)
                    query = query_select.format(
                        v['tables'])
                    
                    output = athena.run_query(
                        query=query,
                        database=dic_['global']['database'],
                        s3_output=dic_['global']['output']
                    )
                    
                    v['output_id'].append(output['QueryExecutionId'])

                else:
                    for i in range(0,len(v['tables'])):
                        create_table = query_tb.format(
                                dic_['global']['database'],
                                v['tables'][i],
                                v['path'][i], 
                                ";"
                            )
                        
                        time.sleep(2)
                        athena.run_query(
                        query=create_table,
                        database=dic_['global']['database'],
                        s3_output=dic_['global']['output'])
                        
                        time.sleep(1)
                        
                        if origin != 'EVT':
                            query = query_select.format(
                            v['tables'][i])
                        
                            output = athena.run_query(
                            query=query,
                            database=dic_['global']['database'],
                            s3_output=dic_['global']['output']
                        )
                            v['output_id'].append(output['QueryExecutionId'])
                        ### Dealing avec les evenements    
                        else:
                            query_fillin = top.format(v['tables'][i])
                            for x, val in enumerate(list_change):

                                if x != len(list_change) -1:
                                    query_fillin+=top_1.format(val)+ ","
                                else:
                                    query_fillin+=top_1.format(val)
                                    query_fillin+= middle

                            for x, val in enumerate(list_change):
                                if x != len(list_change) -1:
                                    query_fillin+=middle_2.format(val)+ ","
                                else:
                                    query_fillin+=middle_2.format(val)
                                    query_fillin+=bottom 
                                    
                            output = athena.run_query(
                                query=query_fillin,
                                database=dic_['global']['database'],
                                s3_output=dic_['global']['output']
                            )
                            v['output_id'].append(output['QueryExecutionId'])
```

### Step 2 Bis: Copier csv

Dans l'étape 1, nous avons stocké les ID dans le dictionaire de paramètre. Il faut environ 10/15 minutes pour préparer tous les csv. 

Dans cette étape, on va simplement récupérer les nouveaux csv du dossier [INPI/sql_output/](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/sql_output/) nommés par id d'execution, pour les déplacer dans le nouveau dossier [INPI/sql_output_preparation/](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/sql_output_preparation/) avec un nom générique.

Le [dossier](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/sql_output_preparation/) va contenir les csv suivants:

- INPI/sql_output_preparation/ets_initial.csv
- INPI/sql_output_preparation/ets_partiel_2018.csv
- INPI/sql_output_preparation/ets_partiel_2019.csv
- INPI/sql_output_preparation/ets_new_2017.csv
- INPI/sql_output_preparation/ets_new_2018.csv
- INPI/sql_output_preparation/ets_new_2019.csv
- INPI/sql_output_preparation/ets_evt_2017.csv
- INPI/sql_output_preparation/ets_evt_2018.csv
- INPI/sql_output_preparation/ets_evt_2019.csv


```python
# Move and rename files

for nature, values in dic_.items():
    if nature != 'global':
        for origin, val in dic_[nature].items():
            for type_, v in dic_[nature][origin].items():
                for i, id_ in enumerate(v['output_id']):
                    source_key = "{}/{}.csv".format(
                        dic_['global']['output'],
                        id_
                               )
                    if origin == 'INITIAL':
                        destination_key = "{}/{}.csv".format(
                        dic_['global']['output_preparation'],
                        v['tables']
                    )
                    else:
                        destination_key = "{}/{}.csv".format(
                        dic_['global']['output_preparation'],
                        v['tables'][i]
                    )
                    results = s3.copy_object_s3(
                        source_key = source_key,
                        destination_key = destination_key,
                        remove = True
                    )
```

## Step 3: Creation table Initial/Partiel/EVT/NEW

Pour cette étape, on récupère les csv de ce [dossier](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/sql_output_preparation/), qu'on aggrège avant de préparer les valeurs manquantes.

La table agrégée s'appelle `initial_partiel_evt_new_etb`.

```python
table = 'initial_partiel_evt_new_etb'
create_table = query_table_concat.format(
    dic_['global']['database'],
    table,
    "s3://calfdata/{}".format(
        dic_['global']['output_preparation'])
)
```

```python
athena.run_query(
    query=create_table,
    database=dic_['global']['database'],
    s3_output=dic_['global']['output']
)
```

## Step 4: Creation statut partiel

Dans cette étape, on crée une colonne `status`, qui indique si les lignes sont a ignorer (IGNORE) ou non (Vide). La logique c'est de prendre la date maximum des stocks partiels par quadruplet, si la date de transfert est inférieure a la date max, alors on ignore. La query prend quelques minutes.

Output de la query va dans ce dossier [INPI/sql_output_status](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/sql_output_status/?region=eu-west-3&tab=overview)
La table avec `status` s'appelle `initial_partiel_evt_new_ets_status`.

```python
output = athena.run_query(
    query=query_partiel,
    database=dic_['global']['database'],
    s3_output=dic_['global']['output']
)
```

```python
dic_['global']['ETS_step4_id'] = output['QueryExecutionId']
```

```python
table = 'initial_partiel_evt_new_ets_status'
source_key = "{}/{}.csv".format(
                        dic_['global']['output'],
                        dic_['global']['ETS_step4_id']
                               )

destination_key = "{}/{}.csv".format(
                        'INPI/sql_output_status',
                        table
                    )
results = s3.copy_object_s3(
                        source_key = source_key,
                        destination_key = destination_key,
                        remove = True
                    )
```

```python
query_status = query_table_all.format(
    dic_['global']['database'], 
    table,
     "s3://calfdata/{}".format('INPI/sql_output_status')
)

athena.run_query(
    query=query_status,
    database=dic_['global']['database'],
    s3_output=dic_['global']['output']
                )
```

## Step 5: Remplissage observations manquantes

On fait les opérations suivantes :

- Remplissage des valeurs manquantes pour les observations.
    - Si `origin` est égale a `EVT`, alors on trie sur `siren,'code greffe', numero_gestion, id_etablissement,date_greffe_temp_` et on récupère les valeurs n - 1.
    - Remplissage des champs manquants pour les événements séquentiels, uniquement dans le cas des événements
        - Output: [INPI/sql_output/](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/sql_output_final_ets/)
        - Output stocké dans le dictionaire des paramaitres key `['global']['table_final_id']['ETS']['EVT']`
- Filtre table pour le champ `origin` autre que `EVT`
   - Output: [INPI/sql_output/](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/sql_output_final_ets/)
        - Output stocké dans le dictionaire des paramètres key `['global']['table_final_id']['ETS']['Not_EVT']`
- Concaténation des deux précédentes steps `EVT` et `Not_EVT`.
    - problème de mémoire pour faire une jointure des deux steps précédentes
    - Output: [TC_1/02_preparation_donnee/ETS](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/TC_1/02_preparation_donnee/ETS/)


### EVT

```python
table = 'initial_partiel_evt_new_ets_status'
list_change = [
"Type",
"Siège_PM",
"RCS_Registre",
"Adresse_Ligne1",
"Adresse_Ligne2",
"Adresse_Ligne3",
"Code_Postal",
"Ville",
"Code_Commune",
"Pays",
"Domiciliataire_Nom",
"Domiciliataire_Siren",
"Domiciliataire_Greffe",
"Domiciliataire_Complément",
"Siege_Domicile_Représentant",
"Nom_Commercial",
"Enseigne",
"Activité_Ambulante",
"Activité_Saisonnière",
"Activité_Non_Sédentaire",
"Date_Début_Activité",
"Activité",
"Origine_Fonds",
"Origine_Fonds_Info",
"Type_Exploitation",
"max_partiel",
"csv_source"
]

for x, value in enumerate(list_change):
    query = """CASE WHEN origin = 'EVT' AND status != 'IGNORE' AND "{0}" = '' THEN 
LAG ("{0}", 1) OVER (  PARTITION BY siren,"code greffe", numero_gestion, id_etablissement 
 ORDER BY siren,'code greffe', numero_gestion, id_etablissement,file_timestamp ) ELSE "{0}" END AS "{0}" 
""".format(value)
    if  x != len(list_change)-1:
        query_evt_2 +=query +","
    else:
        query_evt_2 +=query
        end = """FROM convert
ORDER BY siren,'code greffe', numero_gestion, id_etablissement,file_timestamp)
SELECT *
FROM (
  WITH createID AS (
    SELECT  
    ROW_NUMBER() OVER (
      PARTITION BY 
      siren,
      "code greffe",
      "Nom_Greffe",
      numero_gestion,
      id_etablissement, 
      date_greffe
    ) As row_ID, 
    DENSE_RANK () OVER (
      ORDER BY 
        siren, 
        "code greffe",
        "Nom_Greffe",
        numero_gestion, 
        id_etablissement, 
        date_greffe
    ) As ID, *
    FROM temp
    WHERE origin = 'EVT'
    )
SELECT 
  * 
FROM 
  (
    WITH filled AS (
      SELECT 
        ID, 
        row_ID, 
        siren, 
        "Nom_Greffe",
        "code greffe", 
        numero_gestion, 
        id_etablissement, 
        "status",
        date_greffe,
        file_timestamp,
        libelle_evt,
        Enseigne_partition,
"""
        query_evt_2 += end
for x, val in enumerate(list_change):

    if x != len(list_change) -1:
        query_evt_2+=top_1.format(val)+ ","
    else:
        query_evt_2+=top_1.format(val)
        query_evt_2+= middle

for x, val in enumerate(list_change):
    if x != len(list_change) -1:
        query_evt_2+=middle_2.format(val)+ ","
    else:
        query_evt_2+=middle_2.format(val)
        query_evt_2+=bottom_1
```

```python
output = athena.run_query(
    query=query_evt_2.format(table),
    database=dic_['global']['database'],
    s3_output=dic_['global']['output']
)
```

```python
dic_['global']['table_final_id']['ETS']['EVT'] =  output['QueryExecutionId']
dic_['global']['table_final_id']['ETS']
```

```python
source_key = "{}/{}.csv".format(dic_['global']['output'],
                                dic_['global']['table_final_id']['ETS']['EVT']
                               )
destination_key = "{}/{}.csv".format("INPI/sql_output_final_ets",
                                          'initial_partiel_evt_new_ets_status_EVT'
                                         )
results = s3.copy_object_s3(source_key = source_key,
             destination_key = destination_key,
             remove = False
                      )
```

### Not Evt

```python
table = 'initial_partiel_evt_new_ets_status'
query = """SELECT 
"siren",
"code greffe",
"Nom_Greffe",
"numero_gestion",
"id_etablissement", 
"status",
"origin",
Coalesce(
         try(date_parse(file_timestamp, '%Y-%m-%d')),
         try(date_parse(file_timestamp, '%Y-%m-%d %hh:%mm:%ss.SSS')),
         try(date_parse(file_timestamp, '%Y-%m-%d %hh:%mm:%ss')),
         try(cast(file_timestamp as timestamp))
       )  as file_timestamp,

Coalesce(
         try(date_parse(date_greffe, '%Y-%m-%d')),
         try(date_parse(date_greffe, '%Y/%m/%d')),
         try(date_parse(date_greffe, '%d %M %Y')),
         try(date_parse(date_greffe, '%d/%m/%Y')),
         try(date_parse(date_greffe, '%d-%m-%Y'))
  ) as date_greffe,
"Libelle_Evt",  
"Type",
"Siège_PM",
"RCS_Registre",
"Adresse_Ligne1",
"Adresse_Ligne2",
"Adresse_Ligne3",
"Code_Postal",
"Ville",
"Code_Commune",
"Pays",
"Domiciliataire_Nom",
"Domiciliataire_Siren",
"Domiciliataire_Greffe",
"Domiciliataire_Complément",
"Siege_Domicile_Représentant",
"Nom_Commercial",
"Enseigne",
"Activité_Ambulante",
"Activité_Saisonnière",
"Activité_Non_Sédentaire",
"Date_Début_Activité",
"Activité",
"Origine_Fonds",
"Origine_Fonds_Info",
"Type_Exploitation",
"csv_source"
FROM {}
WHERE origin != 'EVT'
"""

output = athena.run_query(
    query=query.format(table),
    database=dic_['global']['database'],
    s3_output=dic_['global']['output']
)
```

```python
dic_['global']['table_final_id']['ETS']['Not_EVT'] =  output['QueryExecutionId']
dic_['global']['table_final_id']['ETS']
```

```python
source_key = "{}/{}.csv".format(dic_['global']['output'],
                                dic_['global']['table_final_id']['ETS']['Not_EVT']
                               )
destination_key = "{}/{}.csv".format("INPI/sql_output_final_ets",
                                          'initial_partiel_evt_new_ets_status_no_EVT'
                                         )
results = s3.copy_object_s3(source_key = source_key,
             destination_key = destination_key,
             remove = False
                      )
```

# Table finale dans Athena

La dernière étape du programme consiste a récupérer tous les csv du [dossier](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/sql_output_final_ets/) afin de recréer une table appelée `initial_partiel_evt_new_ets_status_final`. A noter que les variables sont renommées (i.e lower case, tiret du bas) puis les variables sont triées dans un nouvel ordre.

```python
table = 'initial_partiel_evt_new_ets_status_final'
list_var = [
"siren",
"code greffe",
"Nom_Greffe",
"numero_gestion",
"id_etablissement", 
"status",
"origin",
"file_timestamp",
"date_greffe",
"Libelle_Evt",  
"Type",
"Siège_PM",
"RCS_Registre",
"Adresse_Ligne1",
"Adresse_Ligne2",
"Adresse_Ligne3",
"Code_Postal",
"Ville",
"Code_Commune",
"Pays",
"Domiciliataire_Nom",
"Domiciliataire_Siren",
"Domiciliataire_Greffe",
"Domiciliataire_Complément",
"Siege_Domicile_Représentant",
"Nom_Commercial",
"Enseigne",
"Activité_Ambulante",
"Activité_Saisonnière",
"Activité_Non_Sédentaire",
"Date_Début_Activité",
"Activité",
"Origine_Fonds",
"Origine_Fonds_Info",
"Type_Exploitation",
"csv_source"
]

query_ = """CREATE EXTERNAL TABLE IF NOT EXISTS %s.%s ("""% (dic_['global']['database'],
                                                   table)
for x, value in enumerate(list_var):
    if  x != len(list_var)-1:
        q = "`{}` string,".format(value)
        query_+=q
    else:
        q = "`{}` string".format(value)
        query_+=q
        end = """)
     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
   'separatorChar' = ',',
   'quoteChar' = '"'
   )
     LOCATION '%s'
     TBLPROPERTIES ('has_encrypted_data'='false',
              'skip.header.line.count'='1')""" % ("s3://calfdata/{}".format(
                                                       "INPI/sql_output_final_ets")
                                                 )
        query_+=end
athena.run_query(
    query=query_,
    database=dic_['global']['database'],
    s3_output=dic_['global']['output']
)
```

### Create csv

```python
query = """SELECT 
"siren",
"code greffe",
"Nom_Greffe",
"numero_gestion",
"id_etablissement", 
'status',
"origin",
Coalesce(
         try(date_parse(file_timestamp, '%Y-%m-%d')),
         try(date_parse(file_timestamp, '%Y-%m-%d %hh:%mm:%ss.SSS')),
         try(date_parse(file_timestamp, '%Y-%m-%d %hh:%mm:%ss')),
         try(cast(file_timestamp as timestamp))
       )  as file_timestamp,

Coalesce(
         try(date_parse(date_greffe, '%Y-%m-%d')),
         try(date_parse(date_greffe, '%Y-%m-%d %hh:%mm:%ss.SSS')),
         try(date_parse(date_greffe, '%Y-%m-%d %hh:%mm:%ss')),
         try(cast(date_greffe as timestamp))
  ) as date_greffe,
"Libelle_Evt",  
"Type",
"Siège_PM",
"RCS_Registre",
"Adresse_Ligne1",
"Adresse_Ligne2",
"Adresse_Ligne3",
"Code_Postal",
"Ville",
"Code_Commune",
"Pays",
"Domiciliataire_Nom",
"Domiciliataire_Siren",
"Domiciliataire_Greffe",
"Domiciliataire_Complément",
"Siege_Domicile_Représentant",
"Nom_Commercial",
"Enseigne",
"Activité_Ambulante",
"Activité_Saisonnière",
"Activité_Non_Sédentaire",
"Date_Début_Activité",
"Activité",
"Origine_Fonds",
"Origine_Fonds_Info",
"Type_Exploitation",
"csv_source"
FROM {}
ORDER BY siren,"Nom_Greffe", "code greffe",
      numero_gestion, id_etablissement, 
      file_timestamp
"""
```

```python
table = 'initial_partiel_evt_new_ets_status_final'
print(query.format(table))
```

```python
output = athena.run_query(
    query=query.format(table),
    database=dic_['global']['database'],
    s3_output=dic_['global']['output']
)
```

```python
dic_['global']['table_final_id']['ETS']['combined']  =  output['QueryExecutionId']
dic_['global']['table_final_id']['ETS']
```

```python
source_key = "{}/{}.csv".format(dic_['global']['output'],
                               dic_['global']['table_final_id']['ETS']['combined']
                               )
destination_key = "{}/{}.csv".format("INPI/TC_1/02_preparation_donnee/ETS",
                                     table
                                         )
destination_key
```

```python
results = s3.copy_object_s3(source_key = source_key,
             destination_key = destination_key,
             remove = False
                      )
```
