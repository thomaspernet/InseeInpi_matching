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

<!-- #region -->
# Filtrage et enrichissement de la donnée de l’INPI

Copy paste from Coda to fill the information

## Objective(s)

Filtrage et enrichissement de la donnée de l’INPI
Select the US you just created →Filtrage et enrichissement de la donnée de l’INPI

* The ID is ued26xzfy75910v
* Add notebook Epic Epic 6 US US 2 Filtrage et enrichissement de la donnée de l’INPI

# Objective(s)

*  La préparation de la donnée de l’INPI requière plusieurs étapes de filtrate et d’enrichissement de la donnée. Dans cette US, nous allons détailler comment procéder pour préparer la donnée de l’INPI mais aussi mettre en avant les “problèmes” et points d’attention rencontrées.
* Le schéma se résume au diagramme ci-dessous

![](https://app.lucidchart.com/publicSegments/view/9e73b3ff-1648-4cda-ab7c-204290721629/image.png)


# Metadata

* Epic: Epic 6
* US: US 2
* Date Begin: 9/21/2020
* Duration Task: 0
* Description: Création d’un notebook pour expliquer comment préparer la donnée de l’INPI 
* Status: Active
  * Change Status task: Active
  * Update table: Modify rows
* Source URL: US 02 Filtrage et enrichissement data INPI
* Task type: Jupyter Notebook
* Users: Thomas Pernet
* Watchers: Thomas Pernet
* User Account: https://937882855452.signin.aws.amazon.com/console
* Estimated Log points: 10
* Task tag: #data-preparation,#documentation,#inpi
* Toggl Tag: #documentation


# Destination Output/Delivery

## Table/file

* GitHub:
  * https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/10_sumup_preparation/00_filtrage_enrichissement_inpi.md

 
<!-- #endregion -->

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

# Input/output

```python
s3_output = 'inpi/sql_output'
database = 'inpi'
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
