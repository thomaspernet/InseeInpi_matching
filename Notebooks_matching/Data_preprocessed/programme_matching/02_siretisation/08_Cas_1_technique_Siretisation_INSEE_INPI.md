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

- Lors de [l’US 7: Test nombre lignes siretise avec nouvelles regles de gestion](https://coda.io/d/CreditAgricole_dCtnoqIftTn/US-07-ETS-version-3_su0VF), une batterie d’analyse a été réalisé ce qui a permit de mettre en évidence des règles de gestion pour discriminer les lignes qui ont des doublons, mais aussi ce qui n’en n’ont pas. Effectivement, certaines lignes sans doublons n’ ont pas nécessairement une relation (i.e. l’adresse n’est pas celle du siret). 

  - Nous avons pu distinguer 4 grandes catégories pour séparer les lignes. Le tableau ci dessous récapitule les possibilités

![00_ensemble_cas_siretisation.jpeg](https://codahosted.io/docs/CtnoqIftTn/blobs/bl-vmJJOeJ__6/455e18296e4076d4b2ec1dbcb5f1364af4b7824c8e96262f78095b67a3db4fd10422ee1adc6e06779452eeb0a47003a6f4a1e29995c559aa9531a8e6499db04a0e089b7e173ed187259fc4aa4a799935f8ead28ed9dfe555a307bdac2eb840e4991b963c)

  - Dans cette US, nous allons traiter du cas ou il n’y a pas de doublon, et la relation entre l’adresse INSEE et INPI appartient au cas de figure 1,3 ou 4. Pour filtrer la table avec les lignes qui satisfont ses deux critères, il faut utiliser les variables:

   - index_id_duplicate
   - test_adresse_cas_1_3_4

  - Les variables à utiliser pour les deux sont:

   - test_siege
   - test_enseigne

  - Des que les lignes sont écartées, il faut réaliser un document json avec les informations sur la siretisation, qui indique les informations suivantes:

  - Un csv avec le siret par index sera créé. Le CSV doit contenir les informations suivantes:

  - Il faut aussi filtrer les lignes qui sont à éjecter du test test_duplicates_is_in_cas_1_3_4 a savoir les TO_REMOVE 

  - Include DataStudio (tick if yes): false

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
    -  \#sql-query,#matching,#siretisation,#regle-de-gestion,#cas-1
- Toggl Tag
   - \#data-analysis 
  
## Input Cloud Storage [AWS/GCP]

If link from the internet, save it to the cloud first

### Tables [AWS/BigQuery]

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
      - Tables (Add name new table): ets_inpi_insee_cases_filtered
      - List new tables
      - ets_inpi_insee_cases_filtered
- S3(Add new filename to Database: [Ressources](https://coda.io/d/CreditAgricole_dCtnoqIftTn/Ressources_suDYJ))

    - Origin: Jupyter notebook

    - Bucket: calfdata

    - Key: RESULTATS_SIRETISATION/LOGS_ANALYSE

    - Filename(s): LOGS_CAS_1.json,RESULTAT_CAS_1.csv
        - [RESULTATS_SIRETISATION/LOGS_ANALYSE/LOGS_CAS_1.json](https://s3.console.aws.amazon.com/s3/buckets/calfdata/RESULTATS_SIRETISATION/LOGS_ANALYSE)-
        - [RESULTATS_SIRETISATION/TABLES_SIRET/RESULTAT_CAS_1.csv](https://s3.console.aws.amazon.com/s3/buckets/calfdata/RESULTATS_SIRETISATION/TABLES_SIRET)




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

region = ''
bucket = ''
```

```python
con = aws_connector.aws_instantiate(credential = path_cred,
                                       region = region)
client= con.client_boto()
s3 = service_s3.connect_S3(client = client,
                      bucket = bucket, verbose = False) 
athena = service_athena.connect_athena(client = client,
                      bucket = bucket) 
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

output = athena.run_query(
            query=query,
            database='',
            s3_output=''
        )

results = False
filename = 'XX.csv'

while results != True:
    source_key = "{}/{}.csv".format(
                                'XX/XX',
                                output['QueryExecutionId']
                                       )
    destination_key = "{}/{}".format(
                                    'XX',
                                    filename
                                )

    results = s3.copy_object_s3(
                                    source_key = source_key,
                                    destination_key = destination_key,
                                    remove = True
                                )
    
table = (s3.read_df_from_s3(
            key = 'XX/{}'.format(filename), sep = ',')
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
