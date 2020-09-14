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
  * Les deux CSV seront disponibles calfdata/TEMP_PARTAGE_DATA_INPI
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
parent_path = str(Path(path).parent.parent.parent)


name_credential = 'XXX_credentials.csv'
region = ''
bucket = ''
path_cred = "{0}/creds/{1}".format(parent_path, name_credential)
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

```python
s3_output = 'XX'
database = ''
```

```python
query = """

"""

output = s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = None, ## Add filename to print dataframe
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
create_report(extension = "html")
```
