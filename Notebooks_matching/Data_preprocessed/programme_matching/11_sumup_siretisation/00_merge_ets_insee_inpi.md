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

<!-- #region -->
# Introduction 


La seconde partie du projet consiste a utiliser la table transformée de l'INSEE afin d'attribuer un siret au établissement présent dans la table transformée de l'INPI. Pour réaliser ce processus dit de siretisation, il faut tout d'abord joindre les tables créés durant la partie précédente. La table de l'INSEE ne contient qu'un seul siret (établissement), avec les informations les plus récentes uniquement. La table de l'INPI, quant à elle, rassemble toutes les informations connues des greffes. Autrement dit, la table a une profondeur historique. Nous devons donc utiliser les informations les plus récentes pour déduire le siret d'un établissement à l'INPI. La façon dont nous réalisons la jointure (sur le siren et le code postal) va déboucher sur des doublons. Effectivement, une entreprise peut avoir plus d'un établissement pour un code postal donné. 

Le dédoublonnement de cette table va être faite par l'intermédiaire de règles de gestion (tests) que nous avons élaboré. Nous allons utiliser une matrice dans lequel figure un ensemble de règles de gestion. Cette matrice est un produit cartésien de toutes les variables que nous avons transformé lors de la partie une, trié de manière ordonnée. Plus précisément, les variables générées lors de la partie précédente permettent la création de règles, majoritairement booléennes. Les règles sont ensuite triées par ordre d'importance, ou la règle une est prioritaire sur la règle deux, la règles deux est prioritaire sur la règle trois et ainsi de suite. Le produit cartésien va produire une matrice carrée avec toutes les possibilités, dans lequel figure le rang. Le rang un est la meilleure combinaison possible des tests. Chaque ligne de la table insee-inpi contient une batterie de variables tests qui va être comparée avec la matrice des règles de gestion. Cette comparaison fait sens pour les lignes doublonnées car c'est elles que nous cherchons a filtrer. Nous allons comparer les tests de chacune des lignes et ne garder uniquement la ligne dont le rang est le plus faible. Cette technique va nous permettre de dédoublonner les établissements en ne gardant que le meilleur des probables sachant les informations que nous avons. Les autres lignes doublonées pour le même établissement n'ont pas des informations suffisament pertinentes. Ainsi, chacun des établissements au sens de l'INPI se voit attribuer un siret le plus probable sachant l'information contenu dans les deux tables.

Le processus de siretisation est découpé en 3 ensembles (figure ci dessous). Premièrement, il faut joindre les tables de l'INSEE et de l'INPI. Ensuite, il est nécéssaire de créer les variables qui vont être utiliser lors des tests puis bien sur générer les tests. Troisièmenent, le rapprochement avec la matrice des règles de gestion va permettre de dédoublonner les lignes avec plusieurs probables.
<!-- #endregion -->

![](https://app.lucidchart.com/publicSegments/view/53fcea7f-7468-4aaf-967f-8ab0f24cdea4/image.png)


## Connexion serveur

```python
from awsPy.aws_authorization import aws_connector
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

# Jointure INSEE-INPI

La jointure entre la table de l'INSEE et de l'INPI se fait sur le siren, la ville et le code postal. A noter que nous devons créer une variable avec le numéro de lignes, qui va nous être util ultérieurement pour merger les tables intermédiaires.

Dans cette jointure, il n’est pas nécessaire de récupérer toutes les variables. Seules les variables énumérées ci-dessous seront utilisées:

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

Nous ne mettons pas les informations dans le fichier JSON de configuration car cette partie n'a pas été intégrer dans la mise en production.

```python
s3_output = 'SQL_OUTPUT_ATHENA'
database = 'ets_siretisation'
```

```python
query = """
DROP TABLE `ets_siretisation.ets_insee_inpi`;
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
CREATE TABLE ets_siretisation.ets_insee_inpi 
WITH (
  format='PARQUET'
) AS
  SELECT 
    ROW_NUMBER() OVER () AS row_id, 
    index_id, 
    sequence_id, 
    count_initial_insee, 
    ets_inpi_transformed.siren, 
    siret, 
    datecreationetablissement, 
    date_debut_activite, 
    etatadministratifetablissement, 
    status_admin, 
    etablissementsiege, 
    status_ets, 
    adresse_distance_inpi, 
    adresse_distance_insee, 
    list_numero_voie_matching_inpi, 
    list_numero_voie_matching_insee, 
    ets_inpi_transformed.code_postal_matching, 
    ets_inpi_transformed.ville_matching, 
    codecommuneetablissement, 
    code_commune, 
    enseigne, 
    list_enseigne
  FROM 
    ets_inpi.ets_inpi_transformed 
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
        adresse_reconstituee_insee, 
        adresse_distance_insee, 
        list_enseigne
      FROM 
        ets_insee.ets_insee_transformed
    ) as insee 
    ON ets_inpi_transformed.siren = insee.siren 
    AND ets_inpi_transformed.ville_matching = insee.ville_matching 
    AND ets_inpi_transformed.code_postal_matching = insee.codepostaletablissement 
    WHERE status is NULL

"""

s3.run_query(
    query=query,
    database=database,
    s3_output=s3_output,
    filename = None, ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

## Analyse

1. Imprimer 10 lignes aléatoirement
2. Compter le nombre d'observation

```python
query = """
SELECT *
FROM ets_siretisation.ets_insee_inpi
limit 10
"""
s3.run_query(
            query=query,
            database='ets_siretisation',
            s3_output=s3_output,
  filename = 'exemple_siretisation', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

```python
query = """
SELECT COUNT(*) AS CNT
FROM ets_siretisation.ets_insee_inpi
"""
s3.run_query(
            query=query,
            database='ets_siretisation',
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
