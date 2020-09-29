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

# Creation table merge INSEE INPI filtree 

Copy paste from Coda to fill the information

# Objective(s)

*  Création d’une table contenant la jointure des tables de l’INSEE et de l’INPI transformées. 
* Dans cette jointure, il n’est pas nécessaire de récupérer toutes les variables. Seules les variables énumérées ci-dessous seront utilisées:
  * `row_id`
  * `index_id` 
  * `sequence_id`  
  * `count_initial_insee`  
  * `ets_inpi_sql.siren` 
  * `siret` 
  * `datecreationetablissement` 
  * `date_debut_activite` 
  * `etatadministratifetablissement` 
  * `status_admin` 
  * `etablissementsiege` 
  * `status_ets` 
  * `adresse_distance_inpi` 
  * `adresse_distance_insee` 
  * `list_numero_voie_matching_inpi` 
  * `list_numero_voie_matching_insee` 
  * `ets_inpi_sql.code_postal_matching` 
  * `ets_inpi_sql.ville_matching` 
  * `codecommuneetablissement` 
  * `code_commune` 
  * `enseigne` 
  * `list_enseigne` 

# Metadata

* Epic: Epic 6
* US: US 4
* Date Begin: 9/28/2020
* Duration Task: 0
* Description: Merger les tables insee et inpi afin d’avoir une table prête pour la réalisation des tests pour la siretisation
* Step type: Transform table
* Status: Active
  * Change Status task: Active
  * Update table: Modify rows
* Source URL: US 04 Merge table INSEE INPI
* Task type: Jupyter Notebook
* Users: Thomas Pernet
* Watchers: Thomas Pernet
* User Account: https://937882855452.signin.aws.amazon.com/console
* Estimated Log points: 10
* Task tag: #athena,#sql,#data-preparation,#inpi,#insee
* Toggl Tag: #documentation

# Input Cloud Storage [AWS/GCP]

## Table/file

* Origin: 
* Athena
* Name: 
* ets_inpi_transformed
* ets_insee_transformed
* Github: 
  * https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/10_sumup_preparation/02_creation_variables_siretisation_inpi.md
  * https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/10_sumup_preparation/03_creation_variables_siretisation_insee.md

# Destination Output/Delivery

## Table/file
* Origin: 
* Athena
* Name:
* ets_insee_inpi
* GitHub:
* https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/11_sumup_siretisation/00_merge_ets_insee_inpi.md


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

# Creation tables

## Steps

- Merger la table `` avec `` 
    - utiliser `inner join`
- Filtrer lorsque statut est différent de 'IGNORE'

```python
s3_output = 'inpi/sql_output'
database = 'inpi'
```

```python
query = """
DROP TABLE `siretisation.ets_insee_inpi`;
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
CREATE TABLE siretisation.ets_insee_inpi 
WITH (
  format='PARQUET'
) AS
 SELECT 
    ROW_NUMBER() OVER () AS row_id, 
    index_id, 
    sequence_id, 
    count_initial_insee, 
    ets_inpi_sql.siren, 
    siret, 
    datecreationetablissement, 
    "date_début_activité", 
    etatadministratifetablissement, 
    status_admin, 
    etablissementsiege, 
    status_ets, 
    adresse_distance_inpi, 
    adresse_distance_insee, 
    list_numero_voie_matching_inpi, 
    list_numero_voie_matching_insee, 
    typevoieetablissement, 
    type_voie_matching, 
    ets_inpi_sql.code_postal_matching, 
    ets_inpi_sql.ville_matching, 
    codecommuneetablissement, 
    code_commune, 
    enseigne, 
    list_enseigne,
    enseigne1etablissement, 
    enseigne2etablissement, 
    enseigne3etablissement 
  FROM 
    siretisation.ets_inpi_sql 
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
        list_enseigne,
        enseigne1etablissement, 
        enseigne2etablissement,
        enseigne3etablissement 
      FROM 
        siretisation.ets_insee_sql
    ) as insee ON ets_inpi_sql.siren = insee.siren 
    AND ets_inpi_sql.ville_matching = insee.ville_matching 
    AND ets_inpi_sql.code_postal_matching = insee.codepostaletablissement 
  WHERE 
    status != 'IGNORE'

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

1. Imprimer 10 lignes aléatoirement
2. Compter le nombre d'observation

```python
query = """
SELECT *
FROM siretisation.ets_insee_inpi
limit 10
"""
s3.run_query(
            query=query,
            database='siretisation',
            s3_output=s3_output,
  filename = 'exemple_siretisation', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

```python
query = """
SELECT COUNT(*)
FROM siretisation.ets_insee_inpi
"""
s3.run_query(
            query=query,
            database='siretisation',
            s3_output=s3_output,
  filename = 'count_siretisation', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
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
create_report(extension = "html", keep_code = True)
```
