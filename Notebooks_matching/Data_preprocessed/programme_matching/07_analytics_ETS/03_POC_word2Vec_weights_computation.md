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
# Creation méthodologie calcul poids similarité adresse via word2vec

Objective(s)

- Lors du POC de l’US [US 06 Union et Intersection](https://coda.io/d/CreditAgricole_dCtnoqIftTn/US-06-Union-et-Intersection_sucns), nous avons besoin d’une table contenant les poids indiquant la similarité entre deux mots. Dès lors, il est indispensable de créer un notebook avec de la documentation sur la création de ces poids et de la technique utilisée. 

  - Une table sera créé dans Athena avec deux colonnes pour chacun des mots et un poids. Les trois nouvelles variables seront appelées:

  - Mot_A
  - Mot_B
  - Index_relation

- Pour calculer les poids, il faut utiliser la table suivante XX avec les variables:

  -  
  -  

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
    -  7

- Task tag

  - \#machine-learning,#word2vec,#documentation,#similarite

- Toggl Tag

  - \#variable-computation
 
  
## Input Cloud Storage [AWS/GCP]

If link from the internet, save it to the cloud first

### Tables [AWS/BigQuery]

1. Batch 1:
  * Select Provider: Athena
  * Select table(s): ets_insee_inpi
    * Select only tables created from the same notebook, else copy/paste selection to add new input tables
    * If table(s) does not exist, add them: Add New Table
    * Information:
      * Region: 
        * NameEurope (Paris)
        * Code: eu-west-3
      * Database: inpi
      * Notebook construction file: https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/03_ETS_add_variables.md
    
## Destination Output/Delivery

1. Athena: 
      * Region: Europe (Paris)
      * Database: machine_learning
      * Tables (Add name new table):
          - list_mots_insee_inpi_ngrams
          - list_mots_insee_inpi_word2vec_weights
* list_mots_insee_inpi_word2vec_weights

  
## Things to know (Steps, Attention points or new flow of information)

### Sources of information  (meeting notes, Documentation, Query, URL)

- Query [Athena/BigQuery]

  1. Link 1: [Liste ngrams](https://eu-west-3.console.aws.amazon.com/athena/home?region=eu-west-3#query/history/79a481c2-df9c-4785-993b-4c6813947770)

    - Description: Query utilisée précédemment pour créer la liste des combinaisons INSEE-INPI
<!-- #endregion -->

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
con = aws_connector.aws_instantiate(credential = path_cred,
                                       region = 'eu-west-3')
client= con.client_boto()
s3 = service_s3.connect_S3(client = client,
                      bucket = 'calfdata', verbose = False) 
athena = service_athena.connect_athena(client = client,
                      bucket = 'calfdata') 
```

# Creation Tables


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
