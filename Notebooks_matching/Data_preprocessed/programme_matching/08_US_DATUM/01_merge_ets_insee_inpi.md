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

## Objective(s)

  *   Maintenant que la table INSEE et INPI sont prêtes, il faut faire la jointure en vue de créer des règles de différentiations
  * Nous savons qu’un établissement peut être rattaché via le sirent et le siret. Pour cela, nous allons merger la table de l’inpi et de l’inpi via le siren, la ville et le code postale. 
  * Différentes règles supplémentaires sont a prendre en considérations:
    * Le join doit etre INNER  → inutile de dédoublonner des siren non matché
    * le status IGNORE doit être filtré
  * La nouvelle table doit contenir les variables suivantes: 
    *  index_id, 
    *     sequence_id, 
    *     count_initial_insee, 
    *     ets_inpi_sql.siren, 
    *     siret, 
    *     datecreationetablissement, 
    *     "date_début_activité", 
    *     etatadministratifetablissement, 
    *     status_admin, 
    *     etablissementsiege, 
    *     status_ets, 
    *     adresse_distance_inpi, 
    *     adresse_distance_insee, 
    *     list_numero_voie_matching_inpi, 
    *     list_numero_voie_matching_insee, 
    *     typevoieetablissement, 
    *     type_voie_matching, 
    *     ets_inpi_sql.code_postal_matching, 
    *     ets_inpi_sql.ville_matching, 
    *     codecommuneetablissement, 
    *     code_commune, 
    *     enseigne, 
    *      list_enseigne
    *     enseigne1etablissement, 
    *     enseigne2etablissement, 
    *     enseigne3etablissement

## Metadata 

* Metadata parameters are available here: 
* US Title: Creation table merge INSEE INPI filtree 
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
  *  5
* Task tag
  *  #sql-query,#matching,#siretisation,#inpi  ,#insee
* Toggl Tag
  * #data-preparation
  
## Input Cloud Storage [AWS]

If link from the internet, save it to the cloud first

### Tables [AWS]

1. Batch 1:
  * Select Provider: Athena
  * Select table(s): ets_insee_sql ,ets_inpi_sql
    * Select only tables created from the same notebook, else copy/paste selection to add new input tables
    * If table(s) does not exist, add them: Add New Table
    * Information:
      * Region: 
        * NameEurope (Paris)
        * Code: eu-west-3
      * Database: inpi
      * Notebook construction file: 
        * https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/03_ETS_add_variables.md
        * [05_nettoyage_enseigne_inpi](https://github.com/thomaspernet/InseeInpi_matching/blob/master/01_Data_preprocessing/Data_preprocessed/programme_matching/01_preparation/05_nettoyage_enseigne_inpi.md)

## Destination Output/Delivery

  * AWS
    1. Athena: 
      * Region: Europe (Paris)
      * Database: siretisation
      * Tables (Add name new table): ets_insee_inpi 
      * List new tables
      *  ets_insee_inpi 

## Things to know (Steps, Attention points or new flow of information)

### Sources of information  (meeting notes, Documentation, Query, URL)

1. Jupyter Notebook (Github Link)
  1. md : https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/02_siretisation/06_analyse_pre_siretisation_v3.md#full-pipeline


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
DROP TABLE `ets_insee_inpi`;
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
create_report(extension = "html")
```
