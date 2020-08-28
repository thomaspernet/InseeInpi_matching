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

# Test nombre lignes siretise

Copy paste from Coda to fill the information

## Objective(s)

- Lors de [l’US 7: Test nombre lignes siretise avec nouvelles regles de gestion](https://coda.io/d/CreditAgricole_dCtnoqIftTn/US-07-ETS-version-3_su0VF), nous avons créé une table avec l’ensemble des possibilités de tests, trié par ordre de préférence. 

  - Toutefois, il manque deux variables:

    - max_distance_cosine
    - levhenstein_distance

  - Dans cette US, nous allons créer ses deux variables et les ajouter à la table ets_inpi_insee_cases. Une nouvelle table sera créé, appelée ets_inpi_insee_cases_distance . Les nouvelles variables a ajouter sont les suivantes:

  - unzip_inpi, 

    - mot ayant servi coté inpi pour trouver la distance

  -  unzip_insee, 

    - mot ayant servi coté inse pour trouver la distance

  - max_cosine_distance, 

    - distance maximum de l’index

  -  test as key_except_to_test

    - liste contenant les clés valeurs des mots non communs

  - Une table intermédiaire contenant le max de la distance sera calculé, avec la Levhenstein aussi. La table s’appelle  ets_inpi_distance_max_word2vec 

## Metadata 

- Metadata parameters are available here: [Ressources_suDYJ#_luZqd](http://Ressources_suDYJ#_luZqd)

  - Task type:

     - Jupyter Notebook

  - Users: :

      - [Thomas Pernet](mailto:t.pernetcoudrier@gmail.com)

  - Watchers:

      - [Thomas Pernet](mailto:t.pernetcoudrier@gmail.com)

  - Estimated Log points:

      - One being a simple task, 15 a very difficult one
        -  10

  - Task tag

      - \#machine-learning,#sql-query,#computation,#word2vec

  - Toggl Tag

      - \#variable-computation
  
## Input Cloud Storage [AWS/GCP]

If link from the internet, save it to the cloud first

### Tables [AWS/BigQuery]

- Batch 1:

  - Select Provider: Athena

    - Select table(s): ets_inpi_insee_cases

    - Select only tables created from the same notebook, else copy/paste selection to add new input tables

      - If table(s) does not exist, add them: 

        Add New Table

      - Information:

      - Region: 

        - NameEurope (Paris)
          - Code: eu-west-3

        - Database: inpi

        - Notebook construction file: [07_pourcentage_siretisation_v3](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/02_siretisation/07_pourcentage_siretisation_v3.md)
    
## Destination Output/Delivery

- AWS

    - Athena: 

      - Region: Europe (Paris)
        - Database: inpi
        - Tables (Add name new table): ets_inpi_distance_max_word2vec,ets_inpi_insee_cases_distance
        - List new tables
        - ets_inpi_distance_max_word2vecets_inpi_insee_cases_distance

## Things to know (Steps, Attention points or new flow of information)

### Sources of information  (meeting notes, Documentation, Query, URL)



## Connexion serveur

```python
from awsPy.aws_authorization import aws_connector
from awsPy.aws_athena import service_athena
from awsPy.aws_s3 import service_s3
from pathlib import Path
import pandas as pd
import numpy as np
import os, shutil
bucket = 'calfdata'
path = os.getcwd()
parent_path = str(Path(path).parent)
path_cred = r"{}/credential_AWS.json".format(parent_path)

```

```python
con = aws_connector.aws_instantiate(credential = path_cred,
                                       region = region)
client= con.client_boto()
s3 = service_s3.connect_S3(client = client,
                      bucket = bucket, verbose = False) 
#athena = service_athena.connect_athena(client = client,
#                      bucket = bucket) 
region = 'eu-west-3'
bucket = 'calfdata'
s3_output = 'INPI/sql_data'
```

```python
import seaborn as sns

cm = sns.light_palette("green", as_cmap=True)
pd.set_option('display.max_columns', None)
```

# Creation tables

## Steps

```python
query = """

"""

output = s3.run_query(
            query=query,
            database='',
            s3_output='',
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
def create_report(extension = "html"):
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
