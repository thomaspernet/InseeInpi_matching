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
# Test similarite exception list mots INSEE et INPI siretisation

Objective(s)

*  L’objectif de cette tache est de trouver une solution pour retourner la distance donnée par le Word2Vec entre 2 listes contenant des mots qui ne sont pas communs dans l’adresse INSEE et INPI
* Il faut faire le test lorsque la variable status_cas est egal a CAS_5,6 ou 7
* Par exemple:
    * inpi_except: [A, B]
    * insee_except: [A,C]
    * Le test: [[A,A], [A,C], [B,A],[B,C]]
    * Output: [p1, p2, p3, p4]
    * Recupération max list output
    * Variables nécéssaire:
        * inpi_except 
        * insee_except 
        * status_cas

## Metadata

* Metadata parameters are available here: Ressources_suDYJ#_luZqd
* Task type:
  * Jupyter Notebook
* Users: :
  * Thomas Pernet
* Watchers:
  * Thomas Pernet
* Estimated Log points:
  * One being a simple task, 15 a very difficult one
  *  14
* Task tag
  *  #sql-query,#matching,#siretisation,#machine-learning,#word2vec
* Toggl Tag
  * #poc
  
## Input Cloud Storage [AWS/GCP]

If link from the internet, save it to the cloud first

### Tables [AWS/BigQuery]

1. Batch 1:
    * Select Provider: Athena
      * Select table(s): ets_inpi_insee_cases
        * Select only tables created from the same notebook, else copy/paste selection to add new input tables
        * If table(s) does not exist, add them: Add New Table
        * Information:
          * Region: 
            * NameEurope (Paris)
            * Code: eu-west-3
          * Database: inpi
          * Notebook construction file: [07_pourcentage_siretisation_v3](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/02_siretisation/07_pourcentage_siretisation_v3.md)
2. Batch 2:
  * Select Provider: Athena
  * Select table(s): list_mots_insee_inpi_word2vec_weights
    * Select only tables created from the same notebook, else copy/paste selection to add new input tables
    * If table(s) does not exist, add them: Add New Table
    * Information:
      * Region: 
        * NameEurope (Paris)
        * Code: eu-west-3
      * Database: machine_learning
      * Notebook construction file: 03_POC_word2Vec_weights_computation
    
## Destination Output/Delivery

* Athena: 
    * Region: Europe (Paris)
    * Database: inpi
    * Tables (Add name new table): ets_inpi_inse_wordvec

  
## Things to know (Steps, Attention points or new flow of information)

### Sources of information  (meeting notes, Documentation, Query, URL)

1. Jupyter Notebook (Github Link)
  1. md : [Test_word2Vec.md](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/02_siretisation/Test_word2Vec.md)
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

# Creation table analyse



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
