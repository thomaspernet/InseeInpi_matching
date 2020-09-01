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

# Nettoyage variable enseigne inpi 

Copy paste from Coda to fill the information

## Objective(s)

*   Nous avons réalisé des tests de similarité entre l’enseigne de l’INPI et l’enseigne de l’adresse et il est avéré que l’INPI ne normalise pas suffisamment la variable pour être comparé en l’état.
* Dans cette US, nous allons préparer la variable enseigne en supprimant les caractères spéciaux puis en mettant le texte en majuscule 

## Metadata 

* Metadata parameters are available here: 
* US Title: Nettoyage variable enseigne inpi 
* Epic: Epic 5
* US: US 7
* Date Begin: 8/31/2020
* Duration Task: 1
* Status:  
* Source URL:[US 07 Preparation siretisation](https://coda.io/d/_dCtnoqIftTn/US-07-Preparation-siretisation_suFb9)
* Task type:
  * Jupyter Notebook
* Users: :
  * Thomas Pernet
* Watchers:
  * Thomas Pernet
* Estimated Log points:
  * One being a simple task, 15 a very difficult one
  *  3
* Task tag
  *  #sql-query,#preparation-inpi
* Toggl Tag
  * #data-preparationtance [AWS]
  *   
  
## Input Cloud Storage [AWS]

If link from the internet, save it to the cloud first

### Tables [AWS]

1. Batch 1:
  * Select Provider: Athena
  * Select table(s): ets_final_sql 
    * Select only tables created from the same notebook, else copy/paste selection to add new input tables
    * If table(s) does not exist, add them: Add New Table
    * Information:
      * Region: 
        * NameEurope (Paris)
        * Code: eu-west-3
      * Database: inpi
      * Notebook construction file: 
        * https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/03_ETS_add_variables.md
    
## Destination Output/Delivery

  * AWS
    * Athena: 
      * Region: Europe (Paris)
      * Database: siretisation
      * Tables (Add name new table): ets_inpi_sql
      * List new tables
           * ets_inpi_sql

## Things to know (Steps, Attention points or new flow of information)

### Sources of information  (meeting notes, Documentation, Query, URL)

1. Jupyter Notebook (Github Link)
  1. md :https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/02_siretisation/07_pourcentage_siretisation_v3.md#connexion-serveur



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

<!-- #region -->
# Creation tables

## Steps

- Prendre la table  `ets_final_sql` de l'inpi
- Nettoyer la variable `enseigne`:
    - enlever les accents
    - Mise en majuscule

```
REGEXP_REPLACE(
      NORMALIZE(enseigne, NFD), 
      '\pM', 
      ''
    ) AS enseigne
```

Mise a jour tableau des règles de nettoyage:


| Table | Variables | Article | Digit | Debut/fin espace | Espace | Accent | Upper |
| --- | --- | --- | --- | --- | --- | --- | --- |
| INPI | adresse_regex_inpi | X | X | X | X | X | X |
| INPI | adresse_distance_inpi | X | X | X | X | X | X |
| INPI | adresse_reconstituee_inpi |  |  | X | X | X | X |
|INPI| enseigne |  |  |  |  | X |X |
| INSEE | adresse_reconstituee_insee |  |  | X | X | X | X |
| INSEE | adresse_distance_insee | X | X | X | X | X | X |
<!-- #endregion -->

```python
s3_output = 'INPI/sql_output'
database = 'inpi'
```

### Exemple INPUT / output

```python
query = """
WITH data_ AS (
SELECT index_id, enseigne
FROM inpi.ets_final_sql 
WHERE enseigne != ''

)
SELECT data_.index_id, data_.enseigne as input, output
FROM data_
INNER JOIN (
  SELECT index_id, 
  REGEXP_REPLACE(
      NORMALIZE(enseigne, NFD), 
      '\pM', 
      ''
    ) AS output
            FROM 
  data_
            ) as tb_enseigne
            ON data_.index_id = tb_enseigne.index_id
LIMIT 10
"""
print(s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = 'exemple_enseigne', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        ).to_markdown())
```

```python
query = """
CREATE DATABASE IF NOT EXISTS siretisation
  COMMENT 'DB avec tb pour la siretisation'
  LOCATION 's3://calfdata/inpi/SIRETISATION/'
""""

query ="""
DROP TABLE `siretisation.ets_inpi_sql`;
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
CREATE TABLE siretisation.ets_inpi_sql
WITH (
  format='PARQUET'
) AS
SELECT index_id, sequence_id, siren, code_greffe, nom_greffe, numero_gestion, id_etablissement, status, origin, date_greffe, file_timestamp, libelle_evt, last_libele_evt, status_admin, type, status_ets, "siège_pm", rcs_registre, adresse_ligne1, adresse_ligne2, adresse_ligne3, adresse_reconstituee_inpi, adresse_regex_inpi, adresse_distance_inpi, list_numero_voie_matching_inpi, numero_voie_matching, voie_clean, type_voie_matching, code_postal, code_postal_matching, ville, ville_matching, code_commune, pays, domiciliataire_nom, domiciliataire_siren, domiciliataire_greffe, "domiciliataire_complément", "siege_domicile_représentant", nom_commercial, REGEXP_REPLACE(
      NORMALIZE(enseigne, NFD), 
      '\pM', 
      ''
    ) AS enseigne, "activité_ambulante" , "activité_saisonnière", "activité_non_sédentaire", "date_début_activité", "activité", origine_fonds, origine_fonds_info, type_exploitation, csv_source
FROM inpi.ets_final_sql 
"""

s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = None, ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

### Validation

- Imprimer 10 lignes avec des enseignes différentes de null, avec deux colonnes, une colonne input et une colonne output

```python
query = """
WITH data_ AS (
SELECT index_id, enseigne
FROM siretisation.ets_inpi_sql
WHERE enseigne != ''

)
SELECT data_.index_id, data_.enseigne as input, output
FROM data_
INNER JOIN (
  SELECT index_id, 
  REGEXP_REPLACE(
      NORMALIZE(enseigne, NFD), 
      '\pM', 
      ''
    ) AS output
            FROM 
  data_
            ) as tb_enseigne
            ON data_.index_id = tb_enseigne.index_id
LIMIT 10
"""
print(s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = 'exemple_enseigne', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        ).to_markdown()
     )
```

# Generation report

```python
import os, time, shutil, urllib, ipykernel, json
from pathlib import Path
from notebook import notebookapp
```

```python

```

```python


```python

```


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
