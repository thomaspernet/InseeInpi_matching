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

# Export CSV table INPI siretisee

* The ID is kxb88sjjt94211q

## Objective(s)

*  Dans le but de partager de la travail de siretisation, nous devons exporter la table ets_inpi_insee_no_duplicate 
* La table contient partiellement des index_id avec des doublons. Cela est du a une mauvaise préparation de la donnée ou bien a des index impossibles en l’état a dédoublonner. Dès lors, il faut retirer ses index de la table a exporter
* Nous allons créer une table finale qui contient les informations des données brutes de l’INPI et les données transformées. Pour cela, il faut utiliser la table ets_inpi_sql 
  * Il faudra ensuite exporter en csv la table complète et une seconde table avec uniquement le siren, siren, ID séquence et variables référentielles de l’établissement au sens de l’INPI.
  * Les deux CSV seront disponibles [calfdata/TEMP_PARTAGE_DATA_INPI](https://s3.console.aws.amazon.com/s3/buckets/calfdata/TEMP_PARTAGE_DATA_INPI/?region=eu-west-3&tab=overview)
* Please, update the Source URL by clicking on the button after the information have been pasted
  * US 01 CSV INPI Modify rows
  * Delete tables and Github related to the US: Delete rows
  
## Metadata

* Epic: Epic 5
* US: US 1
* Date Begin: 9/14/2020
* Duration Task: 1
* Description: Export de la base INPI siretisee sans les doublons
* Status: Active
  * Change Status task: Active
  * Update table: Modify rows
* Source URL: US 01 CSV INPI
* Task type: Jupyter Notebook
* Users: Thomas Pernet
* Watchers: Thomas Pernet
* User Account: https://937882855452.signin.aws.amazon.com/console
* Estimated Log points: 5
* Task tag: #s3,#export-csv,#siretisation,#inpi
* Toggl Tag: #share-result

## Input Cloud Storage [AWS/GCP]

If link from the internet, save it to the cloud first
Table/file

* Origin: 
    * Athena
* Name: 
    * ets_insee_inpi_no_duplicate
    * ets_inpi_sql
* Github: 
    * https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/08_US_DATUM/11_creation_table_ets_insee_inpi_no_duplicate.md
    * https://github.com/thomaspernet/InseeInpi_matching/blob/master/01_Data_preprocessing/Data_preprocessed/programme_matching/01_preparation/05_nettoyage_enseigne_inpi.md

## Destination Output/Delivery

Table/file

* Origin: 
    * Athena
* Name:
    * ets_inpi_no_doublon_siret
* GitHub:
 * https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Notebooks_matching/Data_preprocessed/programme_matching/09_export_tables/00_export_table_no_doublon_inpi_siret.ipynb



## Connexion serveur

```python
from awsPy.aws_authorization import aws_connector
from awsPy.aws_athena import service_athena
from awsPy.aws_s3 import service_s3
from pathlib import Path
import pandas as pd
import numpy as np
import seaborn as sns
import os, shutil

path = os.getcwd()
parent_path = str(Path(path).parent)
path_cred = r"{}/credential_AWS.json".format(parent_path)
con = aws_connector.aws_instantiate(credential = path_cred,
                                       region = 'eu-west-3')

region = 'eu-west-3'
bucket = 'calfdata'
```

```python
bucket = 'calfdata'
con = aws_connector.aws_instantiate(credential = path_cred,
                                       region = region)
client= con.client_boto()
s3 = service_s3.connect_S3(client = client,
                      bucket = bucket, verbose = False) 
```

```python
pandas_setting = True
if pandas_setting:
    cm = sns.light_palette("green", as_cmap=True)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', None)
```

```python
s3_output = 'inpi/sql_output'
database = 'siretisation'
```

# Brief analysis

Il y a certains `index_id` qui peuvent avoir des doublons après avoir merger avec la table `ets_inpi_sql` car la date de transmission est la même (ie le timestamp) Lors de nos développements, nous n'avons pas envisagé ce cas de figure, toutefois lors de la mise en production, cet aspect a été pris en compte.

Dans la query si dessous, nous allons imprimer les lignes ayant des doublons:

```python
query = """
WITH merge_inpi AS (
  SELECT 
    ROW_NUMBER() OVER (PARTITION BY ets_insee_inpi_no_duplicate.index_id ORDER BY file_timestamp) AS row_id_group,
    ets_insee_inpi_no_duplicate.index_id, 
    ets_insee_inpi_no_duplicate.siren, 
    ets_insee_inpi_no_duplicate.siret, 
    ets_insee_inpi_no_duplicate.sequence_id
  FROM 
    siretisation.ets_insee_inpi_no_duplicate 
    INNER JOIN siretisation.ets_inpi_sql ON ets_insee_inpi_no_duplicate.index_id = siretisation.ets_inpi_sql.index_id 
  WHERE 
    count_index = 1
) 
SELECT 

  index_id, COUNT(*) AS cnt
  
FROM 
  merge_inpi 
GROUP BY  index_id
ORDER BY cnt DESC
LIMIT 20
"""
s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = "nb_index", ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

# Creation tables

Nous avons constaté dans avec la query précédente qu'il y avait 9 lignes ayant des timestamps de transmission identique. Pour ne pas avoir de doublons lors de la création de la table `ets_inpi_no_doublon_siret`, nous décidons de ne récupérer la première ligne. Ce n'est pas optimal comme solution!

{
	"StorageDescriptor": {
		"cols": {
			"FieldSchema": [
				{
					"name": "row_id_group",
					"type": "bigint",
					"comment": "Nombre de lignes par index_id. Normalement que des 1"
				},
				{
					"name": "index_id",
					"type": "bigint",
					"comment": "Identification "
				},
				{
					"name": "siren",
					"type": "string",
					"comment": ""
				},
				{
					"name": "siret",
					"type": "string",
					"comment": ""
				},
				{
					"name": "sequence_id",
					"type": "bigint",
					"comment": ""
				},
				{
					"name": "code_greffe",
					"type": "string",
					"comment": ""
				},
				{
					"name": "nom_greffe",
					"type": "string",
					"comment": ""
				},
				{
					"name": "numero_gestion",
					"type": "string",
					"comment": ""
				},
				{
					"name": "id_etablissement",
					"type": "string",
					"comment": ""
				},
				{
					"name": "status",
					"type": "string",
					"comment": ""
				},
				{
					"name": "origin",
					"type": "string",
					"comment": ""
				},
				{
					"name": "date_greffe",
					"type": "string",
					"comment": ""
				},
				{
					"name": "file_timestamp",
					"type": "string",
					"comment": ""
				},
				{
					"name": "libelle_evt",
					"type": "string",
					"comment": ""
				},
				{
					"name": "last_libele_evt",
					"type": "string",
					"comment": ""
				},
				{
					"name": "status_admin",
					"type": "varchar(1)",
					"comment": ""
				},
				{
					"name": "type",
					"type": "string",
					"comment": ""
				},
				{
					"name": "status_ets",
					"type": "varchar(5)",
					"comment": ""
				},
				{
					"name": "siège_pm",
					"type": "string",
					"comment": ""
				},
				{
					"name": "rcs_registre",
					"type": "string",
					"comment": ""
				},
				{
					"name": "adresse_ligne1",
					"type": "string",
					"comment": ""
				},
				{
					"name": "adresse_ligne2",
					"type": "string",
					"comment": ""
				},
				{
					"name": "adresse_ligne3",
					"type": "string",
					"comment": ""
				},
				{
					"name": "adresse_reconstituee_inpi",
					"type": "string",
					"comment": ""
				},
				{
					"name": "adresse_distance_inpi",
					"type": "string",
					"comment": ""
				},
				{
					"name": "list_numero_voie_matching_inpi",
					"type": "array<string>",
					"comment": ""
				},
				{
					"name": "numero_voie_matching",
					"type": "string",
					"comment": ""
				},
				{
					"name": "voie_clean",
					"type": "string",
					"comment": ""
				},
				{
					"name": "type_voie_matching",
					"type": "string",
					"comment": ""
				},
				{
					"name": "code_postal",
					"type": "string",
					"comment": ""
				},
				{
					"name": "code_postal_matching",
					"type": "string",
					"comment": ""
				},
				{
					"name": "ville",
					"type": "string",
					"comment": ""
				},
				{
					"name": "ville_matching",
					"type": "string",
					"comment": ""
				},
				{
					"name": "code_commune",
					"type": "string",
					"comment": ""
				},
				{
					"name": "pays",
					"type": "string",
					"comment": ""
				},
				{
					"name": "domiciliataire_nom",
					"type": "string",
					"comment": ""
				},
				{
					"name": "domiciliataire_siren",
					"type": "string",
					"comment": ""
				},
				{
					"name": "domiciliataire_greffe",
					"type": "string",
					"comment": ""
				},
				{
					"name": "domiciliataire_complément",
					"type": "string",
					"comment": ""
				},
				{
					"name": "siege_domicile_représentant",
					"type": "string",
					"comment": ""
				},
				{
					"name": "nom_commercial",
					"type": "string",
					"comment": ""
				},
				{
					"name": "enseigne",
					"type": "string",
					"comment": ""
				},
				{
					"name": "activité_ambulante",
					"type": "string",
					"comment": ""
				},
				{
					"name": "activité_saisonnière",
					"type": "string",
					"comment": ""
				},
				{
					"name": "activité_non_sédentaire",
					"type": "string",
					"comment": ""
				},
				{
					"name": "date_début_activité",
					"type": "string",
					"comment": ""
				},
				{
					"name": "activité",
					"type": "string",
					"comment": ""
				},
				{
					"name": "origine_fonds",
					"type": "string",
					"comment": ""
				},
				{
					"name": "origine_fonds_info",
					"type": "string",
					"comment": ""
				},
				{
					"name": "type_exploitation",
					"type": "string",
					"comment": ""
				},
				{
					"name": "csv_source",
					"type": "string",
					"comment": ""
				}
			]
		},
		"location": "s3://calfdata/inpi/sql_output/tables/bf7473f3-4aab-4389-abed-ccc92e1d42ec/",
		"inputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
		"outputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
		"compressed": "false",
		"numBuckets": "0",
		"SerDeInfo": {
			"name": "ets_inpi_no_doublon_siret",
			"serializationLib": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
			"parameters": {}
		},
		"bucketCols": [],
		"sortCols": [],
		"parameters": {},
		"SkewedInfo": {},
		"storedAsSubDirectories": "false"
	},
	"parameters": {
		"EXTERNAL": "TRUE",
		"has_encrypted_data": "false"
	}
}

## Steps

```python
query = """
DROP TABLE `ets_inpi_no_doublon_siret`;
"""
s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = None, ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

```python
query = """
CREATE TABLE siretisation.ets_inpi_no_doublon_siret
WITH (
  format='PARQUET'
) AS
WITH merge_inpi AS (
  SELECT 
    ROW_NUMBER() OVER (PARTITION BY ets_insee_inpi_no_duplicate.index_id ORDER BY file_timestamp) AS row_id_group,
    ets_insee_inpi_no_duplicate.index_id, 
    ets_insee_inpi_no_duplicate.siren, 
    ets_insee_inpi_no_duplicate.siret, 
    ets_insee_inpi_no_duplicate.sequence_id,
    code_greffe, 
    nom_greffe, 
    numero_gestion, 
    id_etablissement, 
    status, 
    origin, 
    date_greffe, 
    file_timestamp, 
    libelle_evt, 
    last_libele_evt, 
    ets_insee_inpi_no_duplicate.status_admin, 
    type, 
    ets_insee_inpi_no_duplicate.status_ets, 
    "siège_pm", 
    rcs_registre, 
    adresse_ligne1, 
    adresse_ligne2, 
    adresse_ligne3, 
    adresse_reconstituee_inpi, 
    ets_insee_inpi_no_duplicate.adresse_distance_inpi, 
    ets_insee_inpi_no_duplicate.list_numero_voie_matching_inpi, 
    numero_voie_matching, 
    voie_clean, 
    type_voie_matching, 
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
    ets_insee_inpi_no_duplicate.enseigne, 
    "activité_ambulante", 
    "activité_saisonnière", 
    "activité_non_sédentaire", 
    ets_insee_inpi_no_duplicate."date_début_activité", 
    "activité", 
    origine_fonds, 
    origine_fonds_info, 
    type_exploitation, 
    csv_source 
  FROM 
    siretisation.ets_insee_inpi_no_duplicate 
    INNER JOIN siretisation.ets_inpi_sql ON ets_insee_inpi_no_duplicate.index_id = siretisation.ets_inpi_sql.index_id 
  WHERE 
    count_index = 1
) 
SELECT 

  *
  
FROM 
  merge_inpi 
WHERE row_id_group = 1    
"""

s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = None, ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

Maintenant que la table est créée, nous pouvons la copier dans le dossier [calfdata/TEMP_PARTAGE_DATA_INPI](https://s3.console.aws.amazon.com/s3/buckets/calfdata/TEMP_PARTAGE_DATA_INPI/?region=eu-west-3&tab=overview)

```python
query = """
SELECT index_id, siren, siret, sequence_id, code_greffe, nom_greffe, numero_gestion, id_etablissement
FROM ets_inpi_no_doublon_siret 
"""
output = s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = None, ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

```python
source_key =  '{}/{}.csv'.format(s3_output, output['QueryID'])
destination_key_filename = '{}/{}.csv'.format('TEMP_PARTAGE_DATA_INPI', 'inpi_siret')
s3.copy_object_s3(source_key = source_key,
                              destination_key = destination_key_filename,
                              remove = True
                                                )
```

```python
query = """
SELECT *
FROM ets_inpi_no_doublon_siret 
"""
output = s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = None, ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

```python
source_key =  '{}/{}.csv'.format(s3_output, output['QueryID'])
destination_key_filename = '{}/{}.csv'.format('TEMP_PARTAGE_DATA_INPI', 'inpi_siret_full')
s3.copy_object_s3(source_key = source_key,
                              destination_key = destination_key_filename,
                              remove = True )
```

# Analyse table


Nombre de lignes

```python
query = """
SELECT COUNT(*) as cnt
FROM ets_inpi_no_doublon_siret
"""
s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = "analyse_1", ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

Nombre de siren

```python
query = """
SELECT COUNT(DISTINCT(siren)) as CNT
FROM ets_inpi_no_doublon_siret
"""
s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = "analyse_2", ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

Nombre de siret

```python
query = """
SELECT COUNT(DISTINCT(siret)) as CNT
FROM ets_inpi_no_doublon_siret
"""
s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = "analyse_3", ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

Nombre d'établissements par ville

```python
query = """
SELECT ville_matching, COUNT(DISTINCT(siret)) as CNT
FROM ets_inpi_no_doublon_siret
GROUP BY ville_matching
ORDER BY CNT DESC
LIMIT 25
"""
(
    s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = "analyse_4", ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
    .set_index('ville_matching')
    .style
    .format("{:,.0f}")
    .bar(subset= ['CNT' ],
                       color='#d65f5f')
)
```

Nombre d"établissements par Greffe

```python
query = """
SELECT nom_greffe, COUNT(DISTINCT(siret)) as CNT
FROM ets_inpi_no_doublon_siret
GROUP BY nom_greffe
ORDER BY CNT DESC
LIMIT 25
"""
(
    s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = "analyse_4", ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
    .set_index('nom_greffe')
    .style
    .format("{:,.0f}")
    .bar(subset= ['CNT' ],
                       color='#d65f5f')
)
```

Nombre d'établissements créés par année

```python
query = """
SELECT YEAR(
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
    ) 
) as date_debut_activite,


COUNT(DISTINCT(siret)) as CNT
FROM ets_inpi_no_doublon_siret
GROUP BY YEAR(
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
    ) 
)
ORDER BY CNT DESC
LIMIT 25
"""
(
    s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = "analyse_4", ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
    .dropna()
    .style
    .format("{:,.0f}")
    .bar(subset= ['CNT' ],
                       color='#d65f5f')
)
```

Nombre de siret par événements

A verifier pourquoi nombre de lignes différents du nombre de siret par ville

```python
query = """
SELECT ville_matching, libelle_evt,
COUNT(DISTINCT(siret)) as CNT
FROM ets_inpi_no_doublon_siret
GROUP BY ville_matching, libelle_evt
ORDER BY CNT DESC
"""

output = (
    s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = "analyse_4", ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
    #.dropna()
    #.style
    #.format("{:,.0f}")
    #.bar(subset= ['CNT' ],
    #                   color='#d65f5f')
)
```

```python
(output
 #.dropna()
 .set_index(['ville_matching','libelle_evt'])
 .unstack(-1)
 .assign(total = lambda x: x.sum(axis = 1))
 .sort_values(by = 'total', ascending = False)
 .head(25)
 .fillna(0)
 .style
 .format("{:,.0f}")
 .bar(subset= ['total' ],
                       color='#d65f5f')
)
```

# Generation report

```python
import os, time, shutil, urllib, ipykernel, json
from pathlib import Path
from notebook import notebookapp
```

```python
def create_report(extension = "html", keep_code = False):
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
    if keep_code:
        os.system('jupyter nbconvert --to {} {}'.format(
    extension,notebookname))
    else:
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
