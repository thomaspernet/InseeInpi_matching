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
# Preparation INPI-REP

Dans ce notebook, on prepare la donnée REP afin d'être concatenée, puis envoyée dans le S3.
- https://docs.aws.amazon.com/athena/latest/ug/csv.html

Le process est détaillé dans le [notebook des Etablissements](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/01_Athena_concatenate_ETS.ipynb), il est le même pour chacune des catégories.


Dossiers source pour les REP

- Stock:
    - [Stock initial](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/TC_1/01_donnee_source/Stock/Stock_initial/REP/)
    - [Stock partiel 2018](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/TC_1/01_donnee_source/Stock/Stock_partiel/2018/REP)
- Flux
    - [NEW 2017](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/TC_1/01_donnee_source/Flux/2017/REP/NEW/)
    - [EVT 2017](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/TC_1/01_donnee_source/Flux/2017/REP/EVT/)


## Steps: Benchmark REP

- Step 1: Parametre et queries
	- Préparation json parameters
	- Query préparation table
	- Query preparation partiel
	- Query remplissage EVT

- Step 2: Concatenation data
	- Stock
		- [Initial](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/TC_1/01_donnee_source/Stock/Stock_Initial/)
		- [Partiel](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/TC_1/01_donnee_source/Stock/Stock_Partiel/)
	- Flux
		- [NEW](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/TC_1/01_donnee_source/Flux/2017/REP/NEW/)
		- [EVT](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/TC_1/01_donnee_source/Flux/2017/REP/EVT/)
			- Remplissage valeur manquante

- Step 3: Creation table Initial/Partiel/EVT/NEW

- Step 4: Creation statut partiel
	- Création colonne status qui indique si les lignes sont a ignorer ou non

- Step 5: Remplissage observations manquantes
	- Récupération information selon `Origin` (`Stock` ou `NEW`) pour compléter les valeurs manquantes des `EVT` 
    
## Table Athena:

- rep_evt_2017: Step 2
- rep_evt_2018: Step 2
- rep_evt_2019: Step 2
- rep_initial: Step 2
- rep_new_2017: Step 2
- rep_new_2018: Step 2
- rep_new_2019: Step 2
- rep_partiel_2018: Step 2
- rep_partiel_2019: Step 2
- initial_partiel_evt_new_rep: Step 3
- initial_partiel_evt_new_rep_status: Step 4
- initial_partiel_evt_new_rep_status_final Step 5

<!-- #endregion -->

```python
!pip install git+git://github.com/thomaspernet/aws-python
```

```python
!pip install --upgrade git+git://github.com/thomaspernet/aws-python
```

```python
from awsPy.aws_authorization import aws_connector
from awsPy.aws_s3 import service_s3
from awsPy.aws_athena import service_athena
import os, time
from pathlib import Path
```

```python
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
        'output_preparation':'INPI/sql_output_preparation_rep',
        'REP_step4_id':[],
        'table_final_id':{
            'REP':{
            }
        }
    },
    'Stock': {
        'INITIAL':{
            'REP': {
                'path':'s3://calfdata/INPI/TC_1/01_donnee_source/Stock/Stock_Initial/2017/REP',
                'tables':'rep_initial',
                'origin':'INITIAL',
                'output_id':[]
            }
        },
        'PARTIEL':{
            'REP': {
                'path':[
                    's3://calfdata/INPI/TC_1/01_donnee_source/Stock/Stock_Partiel/2018/REP',
                    's3://calfdata/INPI/TC_1/01_donnee_source/Stock/Stock_Partiel/2019/REP'
                       ],
                'tables':[
                    'rep_partiel_2018',
                    'rep_partiel_2019'],
                'origin':'PARTIEL',
                'output_id':[]
            }
        }
    },
    'Flux': {
        'NEW':{
            'REP': {
                'path':[
                    's3://calfdata/INPI/TC_1/01_donnee_source/Flux/2017/REP/NEW',
                    's3://calfdata/INPI/TC_1/01_donnee_source/Flux/2018/REP/NEW',
                    's3://calfdata/INPI/TC_1/01_donnee_source/Flux/2019/REP/NEW'
                       ],
                'tables':[
                    'rep_new_2017',
                    'rep_new_2018',
                    'rep_new_2019'],
                'origin':'NEW',
                'output_id':[]
            }
        },
        'REP':{
            'REP': {
                'path':[
                    's3://calfdata/INPI/TC_1/01_donnee_source/Flux/2017/REP/EVT',
                    's3://calfdata/INPI/TC_1/01_donnee_source/Flux/2018/REP/EVT',
                    's3://calfdata/INPI/TC_1/01_donnee_source/Flux/2019/REP/EVT',
                ],
                'tables':[
                    'rep_evt_2017',
                    'rep_evt_2018',
                    'rep_evt_2019'],
                'origin':'REP',
                'output_id':[]
            }
        }
    }
}
```

### Query préparation table

On prédéfini les requêtes qui seront à éxecuter dans Athena. Les paramètres des queries seront récupérés dans ``dic_`` au moment de l'éxecution de la query.### Query préparation table

```python
### query_db = "CREATE DATABASE IF NOT EXISTS {};"

query_tb = \
    """CREATE EXTERNAL TABLE IF NOT EXISTS {0}.{1} (

`Code Greffe` string,
`Nom_Greffe` string,
`Numero_Gestion` string,
`Siren` string,

`Type` string,
`Nom_Patronymique` string,
`Nom_Usage` string,
`Pseudonyme` string,
`Prénoms` string,
`Dénomination` string,
`Siren_PM` string,
`Forme_Juridique` string,
`Adresse_Ligne1` string,
`Adresse_Ligne2` string,
`Adresse_Ligne3` string,
`Code_Postal` string,
`Ville` string,
`Code_Commune` string,
`Pays` string,
`Date_Naissance` string,
`Ville_Naissance` string,
`Pays_Naissance` string,
`Nationalité` string,
`Qualité` string,
`Rep_Perm_Nom` string,
`Rep_Perm_Nom_Usage` string,
`Rep_Perm_Pseudo` string,
`Rep_Perm_Prénoms` string,
`Rep_Perm_Date_Naissance` string,
`Rep_Perm_Ville_Naissance` string,
`Rep_Perm_Pays_Naissance` string,
`Rep_Perm_Nationalité` string,
`Rep_Perm_Adr_Ligne1` string,
`Rep_Perm_Adr_Ligne2` string,
`Rep_Perm_Adr_Ligne3` string,
`Rep_Perm_Code_Postal` string,
`Rep_Perm_Ville` string,
`Rep_Perm_Code_Commune` string,
`Rep_Perm_Pays` string,
`Conjoint_Collab_Nom_Patronym` string,
`Conjoint_Collab_Nom_Usage` string,
`Conjoint_Collab_Pseudo` string,
`Conjoint_Collab_Prénoms` string,
`Conjoint_Collab_Date_Fin` string,
`ID_Représentant` string,

`Date_Greffe`                   string,

`Libelle_Evt`                   string,

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
    
`Code Greffe` string,
`Nom_Greffe` string,
`Numero_Gestion` string,
`Siren` string,

`file_timestamp` string, 

`Type` string,
`Nom_Patronymique` string,
`Nom_Usage` string,
`Pseudonyme` string,
`Prénoms` string,
`Dénomination` string,
`Siren_PM` string,
`Forme_Juridique` string,
`Adresse_Ligne1` string,
`Adresse_Ligne2` string,
`Adresse_Ligne3` string,
`Code_Postal` string,
`Ville` string,
`Code_Commune` string,
`Pays` string,
`Date_Naissance` string,
`Ville_Naissance` string,
`Pays_Naissance` string,
`Nationalité` string,
`Qualité` string,
`Rep_Perm_Nom` string,
`Rep_Perm_Nom_Usage` string,
`Rep_Perm_Pseudo` string,
`Rep_Perm_Prénoms` string,
`Rep_Perm_Date_Naissance` string,
`Rep_Perm_Ville_Naissance` string,
`Rep_Perm_Pays_Naissance` string,
`Rep_Perm_Nationalité` string,
`Rep_Perm_Adr_Ligne1` string,
`Rep_Perm_Adr_Ligne2` string,
`Rep_Perm_Adr_Ligne3` string,
`Rep_Perm_Code_Postal` string,
`Rep_Perm_Ville` string,
`Rep_Perm_Code_Commune` string,
`Rep_Perm_Pays` string,
`Conjoint_Collab_Nom_Patronym` string,
`Conjoint_Collab_Nom_Usage` string,
`Conjoint_Collab_Pseudo` string,
`Conjoint_Collab_Prénoms` string,
`Conjoint_Collab_Date_Fin` string,
`ID_Représentant` string,

`Date_Greffe` string, 
`Libelle_Evt` string, 
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
"Code Greffe",
"Nom_Greffe",
"Numero_Gestion",
"Siren",

"file_timestamp", 

"Type",
"Nom_Patronymique",
"Nom_Usage",
"Pseudonyme",
"Prénoms",
"Dénomination",
"Siren_PM",
"Forme_Juridique",
"Adresse_Ligne1",
"Adresse_Ligne2",
"Adresse_Ligne3",
"Code_Postal",
"Ville",
"Code_Commune",
"Pays",
"Date_Naissance",
"Ville_Naissance",
"Pays_Naissance",
"Nationalité",
"Qualité",
"Rep_Perm_Nom",
"Rep_Perm_Nom_Usage",
"Rep_Perm_Pseudo",
"Rep_Perm_Prénoms",
"Rep_Perm_Date_Naissance",
"Rep_Perm_Ville_Naissance",
"Rep_Perm_Pays_Naissance",
"Rep_Perm_Nationalité",
"Rep_Perm_Adr_Ligne1",
"Rep_Perm_Adr_Ligne2",
"Rep_Perm_Adr_Ligne3",
"Rep_Perm_Code_Postal",
"Rep_Perm_Ville",
"Rep_Perm_Code_Commune",
"Rep_Perm_Pays",
"Conjoint_Collab_Nom_Patronym",
"Conjoint_Collab_Nom_Usage",
"Conjoint_Collab_Pseudo",
"Conjoint_Collab_Prénoms",
"Conjoint_Collab_Date_Fin",
"ID_Représentant",

"Date_Greffe",
"Libelle_Evt",
"csv_source",
"origin"
FROM "inpi"."{}"
WHERE "siren" !=''
"""
```

### Query préparation événement

La query est générée via un loop dans l'étape 3 afin d'éviter les copier/coller redondants. Dans l'ensemble, la query va reconstruire l'ensemble des valeurs manquantes pour chaque csv (ie date de transmission). A noter que la query va récupérer la dernière ligne du quadruplet `siren`,`code greffe`, `numero_gestion`, `id_etablissement`. 

La liste des champs pouvant être affectés par un changement est stockée dans `list_change`.

```python
list_change = [
"Type",
"Nom_Patronymique",
"Nom_Usage",
"Pseudonyme",
"Prénoms",
"Dénomination",
"Siren_PM",
"Forme_Juridique",
"Adresse_Ligne1",
"Adresse_Ligne2",
"Adresse_Ligne3",
"Code_Postal",
"Ville",
"Code_Commune",
"Pays",
"Date_Naissance",
"Ville_Naissance",
"Pays_Naissance",
"Nationalité",
"Qualité",
"Rep_Perm_Nom",
"Rep_Perm_Nom_Usage",
"Rep_Perm_Pseudo",
"Rep_Perm_Prénoms",
"Rep_Perm_Date_Naissance",
"Rep_Perm_Ville_Naissance",
"Rep_Perm_Pays_Naissance",
"Rep_Perm_Nationalité",
"Rep_Perm_Adr_Ligne1",
"Rep_Perm_Adr_Ligne2",
"Rep_Perm_Adr_Ligne3",
"Rep_Perm_Code_Postal",
"Rep_Perm_Ville",
"Rep_Perm_Code_Commune",
"Rep_Perm_Pays",
"Conjoint_Collab_Nom_Patronym",
"Conjoint_Collab_Nom_Usage",
"Conjoint_Collab_Pseudo",
"Conjoint_Collab_Prénoms",
"Conjoint_Collab_Date_Fin",
"ID_Représentant",
    
"Date_Greffe",
"Libelle_Evt",
"csv_source"
    
]

top = """WITH createID AS (
  SELECT 
   *, 
    ROW_NUMBER() OVER (
      PARTITION BY 
"Siren",
"Code Greffe",
"Nom_Greffe",
"Numero_Gestion",
 
      file_timestamp
    ) As row_ID, 
    DENSE_RANK () OVER (
      ORDER BY 
      
"Siren",
"Code Greffe",
"Nom_Greffe",
"Numero_Gestion",

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
        
"Siren",
"Code Greffe",
"Nom_Greffe",
"Numero_Gestion",
        
        
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
    SELECT 
    
"Code Greffe",
"Nom_Greffe",
"Numero_Gestion",
"Siren",

file_timestamp,

"Type",
"Nom_Patronymique",
"Nom_Usage",
"Pseudonyme",
"Prénoms",
"Dénomination",
"Siren_PM",
"Forme_Juridique",
"Adresse_Ligne1",
"Adresse_Ligne2",
"Adresse_Ligne3",
"Code_Postal",
"Ville",
"Code_Commune",
"Pays",
"Date_Naissance",
"Ville_Naissance",
"Pays_Naissance",
"Nationalité",
"Qualité",
"Rep_Perm_Nom",
"Rep_Perm_Nom_Usage",
"Rep_Perm_Pseudo",
"Rep_Perm_Prénoms",
"Rep_Perm_Date_Naissance",
"Rep_Perm_Ville_Naissance",
"Rep_Perm_Pays_Naissance",
"Rep_Perm_Nationalité",
"Rep_Perm_Adr_Ligne1",
"Rep_Perm_Adr_Ligne2",
"Rep_Perm_Adr_Ligne3",
"Rep_Perm_Code_Postal",
"Rep_Perm_Ville",
"Rep_Perm_Code_Commune",
"Rep_Perm_Pays",
"Conjoint_Collab_Nom_Patronym",
"Conjoint_Collab_Nom_Usage",
"Conjoint_Collab_Pseudo",
"Conjoint_Collab_Prénoms",
"Conjoint_Collab_Date_Fin",
"ID_Représentant",

"Date_Greffe",
"Libelle_Evt",
"csv_source",

CASE WHEN Siren IS NOT NULL THEN 'EVT' 
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
  )ORDER BY 
  
"Siren",
"Code Greffe",
"Nom_Greffe",
"Numero_Gestion",
      
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

### Query préparation partiel

Dans cette étape, il faut vérifier si un quadruplet `siren`,`code greffe`, `numero_gestion`, `id_etablissement` possède une ligne `Partiel`. Auquel cas, une nouvelle variable est recréée indiquant pour toutes les lignes précédant un `Partiel` les valeurs à ignorer. On prend la date maximum `date_max` des stocks partiels par quadruplet, si la date de transfert est inférieure a la `date_max`, alors on ignore.

```python
query_partiel = """WITH to_date AS (
  SELECT 
  
"Code Greffe",
"Nom_Greffe",
"Numero_Gestion",
"Siren",

"Type",
"Nom_Patronymique",
"Nom_Usage",
"Pseudonyme",
"Prénoms",
"Dénomination",
"Siren_PM",
"Forme_Juridique",
"Adresse_Ligne1",
"Adresse_Ligne2",
"Adresse_Ligne3",
"Code_Postal",
"Ville",
"Code_Commune",
"Pays",
"Date_Naissance",
"Ville_Naissance",
"Pays_Naissance",
"Nationalité",
"Qualité",
"Rep_Perm_Nom",
"Rep_Perm_Nom_Usage",
"Rep_Perm_Pseudo",
"Rep_Perm_Prénoms",
"Rep_Perm_Date_Naissance",
"Rep_Perm_Ville_Naissance",
"Rep_Perm_Pays_Naissance",
"Rep_Perm_Nationalité",
"Rep_Perm_Adr_Ligne1",
"Rep_Perm_Adr_Ligne2",
"Rep_Perm_Adr_Ligne3",
"Rep_Perm_Code_Postal",
"Rep_Perm_Ville",
"Rep_Perm_Code_Commune",
"Rep_Perm_Pays",
"Conjoint_Collab_Nom_Patronym",
"Conjoint_Collab_Nom_Usage",
"Conjoint_Collab_Pseudo",
"Conjoint_Collab_Prénoms",
"Conjoint_Collab_Date_Fin",
"ID_Représentant",

"Date_Greffe",
"Libelle_Evt",
"csv_source",

"origin", 
Coalesce(try(cast(file_timestamp as timestamp)))  as file_timestamp

FROM "inpi"."initial_partiel_evt_new_rep"
WHERE siren !='' AND file_timestamp !=''
                 )
SELECT *
FROM (
  WITH max_date_partiel AS(
SELECT 

"Siren",
"Code Greffe",
"Nom_Greffe",
"Numero_Gestion",


MAX(file_timestamp) as max_partiel
FROM to_date
WHERE origin = 'Partiel'
GROUP BY  

"Siren",
"Code Greffe",
"Nom_Greffe",
"Numero_Gestion"


    )
  SELECT 
  
to_date."Code Greffe",
to_date."Nom_Greffe",
to_date."Numero_Gestion",
to_date."Siren",

to_date."file_timestamp",
max_date_partiel.max_partiel,
CASE WHEN to_date."file_timestamp" <  max_date_partiel.max_partiel 
  THEN 'IGNORE' ELSE NULL END AS status, 
to_date."origin" ,

to_date."Libelle_Evt",
to_date."Date_Greffe",

to_date."Type",
to_date."Nom_Patronymique",
to_date."Nom_Usage",
to_date."Pseudonyme",
to_date."Prénoms",
to_date."Dénomination",
to_date."Siren_PM",
to_date."Forme_Juridique",
to_date."Adresse_Ligne1",
to_date."Adresse_Ligne2",
to_date."Adresse_Ligne3",
to_date."Code_Postal",
to_date."Ville",
to_date."Code_Commune",
to_date."Pays",
to_date."Date_Naissance",
to_date."Ville_Naissance",
to_date."Pays_Naissance",
to_date."Nationalité",
to_date."Qualité",
to_date."Rep_Perm_Nom",
to_date."Rep_Perm_Nom_Usage",
to_date."Rep_Perm_Pseudo",
to_date."Rep_Perm_Prénoms",
to_date."Rep_Perm_Date_Naissance",
to_date."Rep_Perm_Ville_Naissance",
to_date."Rep_Perm_Pays_Naissance",
to_date."Rep_Perm_Nationalité",
to_date."Rep_Perm_Adr_Ligne1",
to_date."Rep_Perm_Adr_Ligne2",
to_date."Rep_Perm_Adr_Ligne3",
to_date."Rep_Perm_Code_Postal",
to_date."Rep_Perm_Ville",
to_date."Rep_Perm_Code_Commune",
to_date."Rep_Perm_Pays",
to_date."Conjoint_Collab_Nom_Patronym",
to_date."Conjoint_Collab_Nom_Usage",
to_date."Conjoint_Collab_Pseudo",
to_date."Conjoint_Collab_Prénoms",
to_date."Conjoint_Collab_Date_Fin",
to_date."ID_Représentant",

to_date."csv_source"

  FROM to_date
  LEFT JOIN max_date_partiel on
  to_date.siren =max_date_partiel.siren AND
  to_date."code greffe" =max_date_partiel."code greffe" AND
  to_date.nom_greffe =max_date_partiel.nom_greffe AND
  to_date.numero_gestion =max_date_partiel.numero_gestion
  ORDER BY 
  
to_date."Siren",
to_date."Code Greffe",
to_date."Nom_Greffe",
to_date."Numero_Gestion",
  
  file_timestamp
  )"""

query_table_all = """
CREATE EXTERNAL TABLE IF NOT EXISTS {0}.{1} (

`Code Greffe` string,
`Nom_Greffe` string,
`Numero_Gestion` string,
`Siren` string,

`file_timestamp` string,
`max_partiel` string,
`status` string,
`origin` string,

`Libelle_Evt` string,
`Date_Greffe` string    ,

`Type` string,
`Nom_Patronymique` string,
`Nom_Usage` string,
`Pseudonyme` string,
`Prénoms` string,
`Dénomination` string,
`Siren_PM` string,
`Forme_Juridique` string,
`Adresse_Ligne1` string,
`Adresse_Ligne2` string,
`Adresse_Ligne3` string,
`Code_Postal` string,
`Ville` string,
`Code_Commune` string,
`Pays` string,
`Date_Naissance` string,
`Ville_Naissance` string,
`Pays_Naissance` string,
`Nationalité` string,
`Qualité` string,
`Rep_Perm_Nom` string,
`Rep_Perm_Nom_Usage` string,
`Rep_Perm_Pseudo` string,
`Rep_Perm_Prénoms` string,
`Rep_Perm_Date_Naissance` string,
`Rep_Perm_Ville_Naissance` string,
`Rep_Perm_Pays_Naissance` string,
`Rep_Perm_Nationalité` string,
`Rep_Perm_Adr_Ligne1` string,
`Rep_Perm_Adr_Ligne2` string,
`Rep_Perm_Adr_Ligne3` string,
`Rep_Perm_Code_Postal` string,
`Rep_Perm_Ville` string,
`Rep_Perm_Code_Commune` string,
`Rep_Perm_Pays` string,
`Conjoint_Collab_Nom_Patronym` string,
`Conjoint_Collab_Nom_Usage` string,
`Conjoint_Collab_Pseudo` string,
`Conjoint_Collab_Prénoms` string,
`Conjoint_Collab_Date_Fin` string,
`ID_Représentant` string,


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
### Query remplissage EVT

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
    SELECT 
    
"Code Greffe",
"Nom_Greffe",
"Numero_Gestion",
"Siren",

"status",
CASE WHEN siren IS NOT NULL THEN 'EVT' 
ELSE NULL END as origin,    
file_timestamp,

"Type",
"Nom_Patronymique",
"Nom_Usage",
"Pseudonyme",
"Prénoms",
"Dénomination",
"Siren_PM",
"Forme_Juridique",
"Adresse_Ligne1",
"Adresse_Ligne2",
"Adresse_Ligne3",
"Code_Postal",
"Ville",
"Code_Commune",
"Pays",
"Date_Naissance",
"Ville_Naissance",
"Pays_Naissance",
"Nationalité",
"Qualité",
"Rep_Perm_Nom",
"Rep_Perm_Nom_Usage",
"Rep_Perm_Pseudo",
"Rep_Perm_Prénoms",
"Rep_Perm_Date_Naissance",
"Rep_Perm_Ville_Naissance",
"Rep_Perm_Pays_Naissance",
"Rep_Perm_Nationalité",
"Rep_Perm_Adr_Ligne1",
"Rep_Perm_Adr_Ligne2",
"Rep_Perm_Adr_Ligne3",
"Rep_Perm_Code_Postal",
"Rep_Perm_Ville",
"Rep_Perm_Code_Commune",
"Rep_Perm_Pays",
"Conjoint_Collab_Nom_Patronym",
"Conjoint_Collab_Nom_Usage",
"Conjoint_Collab_Pseudo",
"Conjoint_Collab_Prénoms",
"Conjoint_Collab_Date_Fin",
"ID_Représentant",

"Date_Greffe",
"Libelle_Evt",
"csv_source"
    FROM filled
  )ORDER BY 
  
"Siren",
"Code Greffe",
"Nom_Greffe",
"Numero_Gestion",
  
      file_timestamp
  )
    )
"""
```

<!-- #region -->
## Step 2 Concatenation data
                
### Steps:

L'ID de la query creation_csv est stocké dans le `dic_` car il faut plusieurs minutes pour lire les tables et sauvegarder en csv.

A noter que la query `query_csv` ne prend pas toutes les variables (celles crééent lors de l'extraction du FTP) car manque de mémoire lors de la préparation des événements.


 
- Stock
    - Initial:
        - Création table en concatenant tous les fichiers de ce dossier [Initial](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/TC_1/01_donnee_source/Stock/Stock_initial/ETS/)
            - Output: [INPI/sql_output/](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/sql_output/)
        - Création csv
            - Output stocké dans le dictionaire des paramaitres key `output_id`
    - Partiel:
        - Création table en concatenant tous les fichiers de ce dossier [Partiel](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/TC_1/01_donnee_source/Stock/Stock_partiel/)
            - Output: [INPI/sql_output/](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/sql_output/)
        - Création csv
            - Output stocké dans le dictionaire des paramaitres key `output_id`
- Flux
    - NEW:
        - Création table en concatenant tous les fichiers de ce dossier [Flux-new](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/TC_1/01_donnee_source/Flux/2017/ETS/NEW/)
            - Output: [INPI/sql_output/](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/sql_output/)
        - Création csv
            - Output stocké dans le dictionaire des paramaitres key `output_id`
    - EVT
        - Création table en concatenant tous les fichiers de ce dossier [Flux-EVT](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/TC_1/01_donnee_source/Flux/2017/ETS/EVT/)
            - Output: [INPI/sql_output/](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/sql_output/)
        - Création csv: Run query pour remplir les valeurs manquantes et extraire l'entrée max par jour/heure de transmission.
            - Output stocké dans le dictionaire des paramaitres key `output_id`
<!-- #endregion -->

```python
# DROP TABLES
for i in ['rep_initial', 'rep_partiel_2018', 'rep_partiel_2019',
          'rep_new_2017', 'rep_new_2018', 'rep_new_2019', 'rep_evt_2017',
         'rep_evt_2018', 'rep_evt_2019']:
    query = "DROP TABLE `{}`".format(i)
    output = athena.run_query(
                        query=query,
                        database=dic_['global']['database'],
                        s3_output=dic_['global']['output']
                    )
    print(output['QueryExecutionId'])
```

```python
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

```python
dic_
```

### Step 2 Bis: Copier csv

Dans l'étape 1, nous avons stocké les ID dans le dictionaire de paramètre. Il faut environ 10/15 minutes pour préparer tous les csv. 

Dans cette étape, on va simplement récuperer les csv créés dans le dossier [INPI/sql_output/](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/sql_output/) pour les déplacer dans le nouveau dossier [INPI/sql_output_preparation_rep/](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/sql_output_preparation_rep/)

Le dossier va contenir les csv suivants:

- INPI/sql_output_preparation_rep/rep_initial.csv
- INPI/sql_output_preparation_rep/rep_partiel_2018.csv
- INPI/sql_output_preparation_rep/rep_partiel_2019.csv
- INPI/sql_output_preparation_rep/rep_new_2017.csv
- INPI/sql_output_preparation_rep/rep_new_2018.csv
- INPI/sql_output_preparation_rep/rep_new_2019.csv
- INPI/sql_output_preparation_rep/rep_evt_2017.csv
- INPI/sql_output_preparation_rep/rep_evt_2018.csv
- INPI/sql_output_preparation_rep/rep_evt_2019.csv


```python
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
table = 'initial_partiel_evt_new_rep'
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
dic_['global']['REP_step4_id'] = output['QueryExecutionId']
```

```python
table = 'initial_partiel_evt_new_rep_status'
source_key = "{}/{}.csv".format(
                        dic_['global']['output'],
                        dic_['global']['REP_step4_id']
                               )
print(source_key)

destination_key = "{}/{}.csv".format(
                        'INPI/sql_output_status_rep',
                        table
                    )
results = s3.copy_object_s3(
                        source_key = source_key,
                        destination_key = destination_key,
                        remove = True
                    )
```

```python
#DROP PREVIOUS EXECUTION
query = "DROP TABLE `{}`".format(table)
output = athena.run_query(
                query=query,
                database=dic_['global']['database'],
                s3_output='INPI/sql_output_status_rep'
                    )
```

```python
query_status = query_table_all.format(
    dic_['global']['database'], 
    table,
     "s3://calfdata/{}".format('INPI/sql_output_status_rep')
)

athena.run_query(
    query=query_status,
    database=dic_['global']['database'],
    s3_output=dic_['global']['output']
                )
```

## Step 5: Remplissage observations manquantes

Il y a deux étapes a suivre. Pour remplir les événements, il faut prendre la ligne t-1, et compléter les champs manquants. En effet, l'INPI ne renseigne que les modifications. Dans l'étape 2, nous avons pu remplir puis garder le dernier événement pour date de transmission. Toutefois, dans la majeur partie des cas, les champs sont vides, car ils n'ont pas d'antécédents. L'antécédent provient soit d'un événement initial, soit d'un partiel ou création. Dans le cas de figure ou l'événement est une création

- Remplissage des valeurs manquantes pour les observations.
    - Si `origin` es égale a `EVT`, alors trie sur `siren,'code greffe', numero_gestion, date_greffe_temp_` et récupère valeur - 1
    - Remplissage des champs manquants pour les événements séquentiels, uniquement événements
        - Output: [INPI/sql_output/](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/sql_output_final_rep/)
        - Output stocké dans le dictionaire des paramaitres key `['global']['table_final_id']['REP']['EVT']`
        - problème de mémoire pour calculer cette step. Message : # Query exhausted resources at this scale factor
- Filtre table XX pour le champ origin autre que EVT
   - Output: [INPI/sql_output/](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/sql_output_final_rep/)
        - Output stocké dans le dictionaire des paramètres key `['global']['table_final_id']['REP']['Not_EVT']`
- Concaténation deux précédentes step.

    - Output: [TC_1/02_preparation_donnee/REP](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/TC_1/02_preparation_donnee/REP/)
    
 


### EVT

```python
table = 'initial_partiel_evt_new_rep_status'
list_change = [

"Type",
"Nom_Patronymique",
"Nom_Usage",
"Pseudonyme",
"Prénoms",
"Dénomination",
"SIREN_PM",
"Forme_Juridique",
"Adresse_Ligne1",
"Adresse_Ligne2",
"Adresse_Ligne3",
"Code_Postal",
"Ville",
"Code_Commune",
"Pays",
"Date_Naissance",
"Ville_Naissance",
"Pays_Naissance",
"Nationalité",
"Qualité",
"Rep_Perm_Nom",
"Rep_Perm_Nom_Usage",
"Rep_Perm_Pseudo",
"Rep_Perm_Prénoms",
"Rep_Perm_Date_Naissance",
"Rep_Perm_Ville_Naissance",
"Rep_Perm_Pays_Naissance",
"Rep_Perm_Nationalité",
"Rep_Perm_Adr_Ligne1",
"Rep_Perm_Adr_Ligne2",
"Rep_Perm_Adr_Ligne3",
"Rep_Perm_Code_Postal",
"Rep_Perm_Ville",
"Rep_Perm_Code_Commune",
"Rep_Perm_Pays",
"Conjoint_Collab_Nom_Patronym",
"Conjoint_Collab_Nom_Usage",
"Conjoint_Collab_Pseudo",
"Conjoint_Collab_Prénoms",
"Conjoint_Collab_Date_Fin",
"ID_Représentant",
    
"max_partiel",
"csv_source"
]

query_ = """WITH convert AS (
  SELECT 

"Code Greffe",
"Nom_Greffe",
"Numero_Gestion",
"Siren",

origin, "status",
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
  as date_greffe,
  
  libelle_evt,
  

"Type",
"Nom_Patronymique",
"Nom_Usage",
"Pseudonyme",
"Prénoms",
"Dénomination",
"SIREN_PM",
"Forme_Juridique",
"Adresse_Ligne1",
"Adresse_Ligne2",
"Adresse_Ligne3",
"Code_Postal",
"Ville",
"Code_Commune",
"Pays",
"Date_Naissance",
"Ville_Naissance",
"Pays_Naissance",
"Nationalité",
"Qualité",
"Rep_Perm_Nom",
"Rep_Perm_Nom_Usage",
"Rep_Perm_Pseudo",
"Rep_Perm_Prénoms",
"Rep_Perm_Date_Naissance",
"Rep_Perm_Ville_Naissance",
"Rep_Perm_Pays_Naissance",
"Rep_Perm_Nationalité",
"Rep_Perm_Adr_Ligne1",
"Rep_Perm_Adr_Ligne2",
"Rep_Perm_Adr_Ligne3",
"Rep_Perm_Code_Postal",
"Rep_Perm_Ville",
"Rep_Perm_Code_Commune",
"Rep_Perm_Pays",
"Conjoint_Collab_Nom_Patronym",
"Conjoint_Collab_Nom_Usage",
"Conjoint_Collab_Pseudo",
"Conjoint_Collab_Prénoms",
"Conjoint_Collab_Date_Fin",
"ID_Représentant",

"max_partiel","csv_source"
  FROM "inpi"."{}" /*LIMIT 1000000*/

  )SELECT * 
  FROM (
    WITH temp AS (
                 SELECT           
                 
                    "Code Greffe",
                    "Nom_Greffe",
                    "Numero_Gestion",
                    "Siren",

                  origin, 
                 "status",
                 file_timestamp,
                 date_greffe, libelle_evt,"""

for x, value in enumerate(list_change):
    query = """CASE WHEN origin = 'EVT' AND status != 'IGNORE' AND "{0}" = '' THEN 
LAG ("{0}", 1) OVER (  PARTITION BY     

"Siren",
"Code Greffe",
"Nom_Greffe",
"Numero_Gestion"

 ORDER BY 
 
"Siren",
"Code Greffe",
"Nom_Greffe",
"Numero_Gestion",
 
  file_timestamp
 ) ELSE "{0}" END AS "{0}" 
""".format(value)
    if  x != len(list_change)-1:
        query_ +=query +","
    else:
        query_ +=query
        end = """FROM convert
ORDER BY 

"Siren",
"Code Greffe",
"Nom_Greffe",
"Numero_Gestion",

file_timestamp

)
SELECT *
FROM (
  WITH createID AS (
    SELECT  
    ROW_NUMBER() OVER (
      PARTITION BY 
      
"Siren",
"Code Greffe",
"Nom_Greffe",
"Numero_Gestion",

      date_greffe
    ) As row_ID, 
    DENSE_RANK () OVER (
      ORDER BY 
      
"Siren",
"Code Greffe",
"Nom_Greffe",
"Numero_Gestion",
 
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

"Siren",
"Code Greffe",
"Nom_Greffe",
"Numero_Gestion",

        "status",
        date_greffe,
        file_timestamp,
        libelle_evt,
"""
        query_ += end
for x, val in enumerate(list_change):

    if x != len(list_change) -1:
        query_+=top_1.format(val)+ ","
    else:
        query_+=top_1.format(val)
        query_+= middle

for x, val in enumerate(list_change):
    if x != len(list_change) -1:
        query_+=middle_2.format(val)+ ","
    else:
        query_+=middle_2.format(val)
        query_+=bottom_1
```

```python
output = athena.run_query(
    query=query_.format(table),
    database=dic_['global']['database'],
    s3_output=dic_['global']['output']
)
```

```python
dic_['global']['table_final_id']['REP']['EVT'] =  output['QueryExecutionId']
dic_['global']['table_final_id']['REP']
```

```python
source_key = "{}/{}.csv".format(dic_['global']['output'],
                                dic_['global']['table_final_id']['REP']['EVT']
                               )
destination_key = "{}/{}.csv".format("INPI/sql_output_final_rep",
                                          'initial_partiel_evt_new_rep_status_EVT'
                                         )
results = s3.copy_object_s3(source_key = source_key,
             destination_key = destination_key,
             remove = False
                      )
```

### Not Evt

```python
table = 'initial_partiel_evt_new_rep_status'
query = """SELECT 
"Code Greffe",
"Nom_Greffe",
"Numero_Gestion",
"Siren",

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
"Nom_Patronymique",
"Nom_Usage",
"Pseudonyme",
"Prénoms",
"Dénomination",
"SIREN_PM",
"Forme_Juridique",
"Adresse_Ligne1",
"Adresse_Ligne2",
"Adresse_Ligne3",
"Code_Postal",
"Ville",
"Code_Commune",
"Pays",
"Date_Naissance",
"Ville_Naissance",
"Pays_Naissance",
"Nationalité",
"Qualité",
"Rep_Perm_Nom",
"Rep_Perm_Nom_Usage",
"Rep_Perm_Pseudo",
"Rep_Perm_Prénoms",
"Rep_Perm_Date_Naissance",
"Rep_Perm_Ville_Naissance",
"Rep_Perm_Pays_Naissance",
"Rep_Perm_Nationalité",
"Rep_Perm_Adr_Ligne1",
"Rep_Perm_Adr_Ligne2",
"Rep_Perm_Adr_Ligne3",
"Rep_Perm_Code_Postal",
"Rep_Perm_Ville",
"Rep_Perm_Code_Commune",
"Rep_Perm_Pays",
"Conjoint_Collab_Nom_Patronym",
"Conjoint_Collab_Nom_Usage",
"Conjoint_Collab_Pseudo",
"Conjoint_Collab_Prénoms",
"Conjoint_Collab_Date_Fin",
"ID_Représentant",

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
dic_['global']['table_final_id']['REP']['Not_EVT'] =  output['QueryExecutionId']
dic_['global']['table_final_id']['REP']
```

```python
source_key = "{}/{}.csv".format(dic_['global']['output'],
                                dic_['global']['table_final_id']['REP']['Not_EVT']
                               )
destination_key = "{}/{}.csv".format("INPI/sql_output_final_rep",
                                          'initial_partiel_evt_new_rep_status_no_EVT'
                                         )
results = s3.copy_object_s3(source_key = source_key,
             destination_key = destination_key,
             remove = False
                      )
```

# Table finale dans Athena

La dernière étape du programme consiste a récupérer tous les csv du [dossier](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/sql_output_final_rep/) afin de recréer une table appelée `initial_partiel_evt_new_rep_status_final`. A noter que les variables sont renommées (i.e lower case, tiret du bas) puis les variables sont triées dans un nouvel ordre.

```python
table = 'initial_partiel_evt_new_rep_status_final'
list_var = [
    
"Code Greffe",
"Nom_Greffe",
"Numero_Gestion",
"Siren",
    
"status",
"origin",
    
"file_timestamp",
    
"date_greffe",
    
"Libelle_Evt",  
    
"Type",
"Nom_Patronymique",
"Nom_Usage",
"Pseudonyme",
"Prénoms",
"Dénomination",
"SIREN_PM",
"Forme_Juridique",
"Adresse_Ligne1",
"Adresse_Ligne2",
"Adresse_Ligne3",
"Code_Postal",
"Ville",
"Code_Commune",
"Pays",
"Date_Naissance",
"Ville_Naissance",
"Pays_Naissance",
"Nationalité",
"Qualité",
"Rep_Perm_Nom",
"Rep_Perm_Nom_Usage",
"Rep_Perm_Pseudo",
"Rep_Perm_Prénoms",
"Rep_Perm_Date_Naissance",
"Rep_Perm_Ville_Naissance",
"Rep_Perm_Pays_Naissance",
"Rep_Perm_Nationalité",
"Rep_Perm_Adr_Ligne1",
"Rep_Perm_Adr_Ligne2",
"Rep_Perm_Adr_Ligne3",
"Rep_Perm_Code_Postal",
"Rep_Perm_Ville",
"Rep_Perm_Code_Commune",
"Rep_Perm_Pays",
"Conjoint_Collab_Nom_Patronym",
"Conjoint_Collab_Nom_Usage",
"Conjoint_Collab_Pseudo",
"Conjoint_Collab_Prénoms",
"Conjoint_Collab_Date_Fin",
"ID_Représentant",
    
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
                                                       "INPI/sql_output_final_rep")
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

"Code Greffe",
"Nom_Greffe",
"Numero_Gestion",
"Siren",

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
         try(date_parse(date_greffe, '%Y-%m-%d %hh:%mm:%ss.SSS')),
         try(date_parse(date_greffe, '%Y-%m-%d %hh:%mm:%ss')),
         try(cast(date_greffe as timestamp))
  ) as date_greffe,
    
"Libelle_Evt",  

"Type",
"Nom_Patronymique",
"Nom_Usage",
"Pseudonyme",
"Prénoms",
"Dénomination",
"SIREN_PM",
"Forme_Juridique",
"Adresse_Ligne1",
"Adresse_Ligne2",
"Adresse_Ligne3",
"Code_Postal",
"Ville",
"Code_Commune",
"Pays",
"Date_Naissance",
"Ville_Naissance",
"Pays_Naissance",
"Nationalité",
"Qualité",
"Rep_Perm_Nom",
"Rep_Perm_Nom_Usage",
"Rep_Perm_Pseudo",
"Rep_Perm_Prénoms",
"Rep_Perm_Date_Naissance",
"Rep_Perm_Ville_Naissance",
"Rep_Perm_Pays_Naissance",
"Rep_Perm_Nationalité",
"Rep_Perm_Adr_Ligne1",
"Rep_Perm_Adr_Ligne2",
"Rep_Perm_Adr_Ligne3",
"Rep_Perm_Code_Postal",
"Rep_Perm_Ville",
"Rep_Perm_Code_Commune",
"Rep_Perm_Pays",
"Conjoint_Collab_Nom_Patronym",
"Conjoint_Collab_Nom_Usage",
"Conjoint_Collab_Pseudo",
"Conjoint_Collab_Prénoms",
"Conjoint_Collab_Date_Fin",
"ID_Représentant",

"csv_source"

FROM {}
ORDER BY 

"Siren",
"Code Greffe",
"Nom_Greffe",
"Numero_Gestion",

      file_timestamp      
"""
```

```python
table = 'initial_partiel_evt_new_rep_status_final'
#print(query.format(table))
```

```python
output = athena.run_query(
    query=query.format(table),
    database=dic_['global']['database'],
    s3_output=dic_['global']['output']
)
```

```python
dic_['global']['table_final_id']['REP']['combined']  =  output['QueryExecutionId']
dic_['global']['table_final_id']['REP']
```

```python
source_key = "{}/{}.csv".format(dic_['global']['output'],
                               dic_['global']['table_final_id']['REP']['combined']
                               )
destination_key = "{}/{}.csv".format("INPI/TC_1/02_preparation_donnee/REP",
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
