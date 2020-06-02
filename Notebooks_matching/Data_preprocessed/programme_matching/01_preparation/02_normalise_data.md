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

# Prepare ETS Data : normalize address

La préparation de la donnée se fait en deux étapes.

1. Préparation de l'INPI
2. Préparation de l'INSEE

L'étape 1 va mettre en conformité la data de l'INPI en vue d'une siretisation. L'étape 2 va utiliser les siren présents lors de l'étape 1 pour ne préparer que ce sous ensemble dans la donnée de l'INSEE.

Pour faire la préparation en une seule fois, une machine de 64gb de RAM est nécéssaire!

## Detail

1. Préparation de l'INPI
    input: `data/RawData/INPI/` -> ex: `data/RawData/INPI/Stock/initial_partiel_evt_new_ets_status_final.csv`
    output: 
        - directory: `data/input/INPI/` + `ORIGIN`
            - ex:`data/input/INPI/NEW`
        - filename: `XX_ORIGIN_0.csv`
            - ex `initial_partiel_evt_new_ets_status_final_NEW_0.csv`
        - directory: `data/input/SIREN_INPI/` + `ORIGIN`
            - ex:`data/input/SIREN_INPI/NEW`
        - filename: `XX_ORIGIN_0.csv`
            - ex `initial_partiel_evt_new_ets_status_final_NEW_0.csv`
2. Préparation de l'INSEE
    input: 
        - CSV INSEE`data/RawData/INSEE/Stock/ETS/` -> ex: `data/RawData/INSEE/Stock/ETS/StockEtablissement_utf8.csv`
        - CSV INPI step 1 `data/input/SIREN_INPI/` + `ORIGIN`
    output: 
        - directory: `data/input/INSEE/` + `ORIGIN`
            - ex:`data/input/INSEE/NEW`
        - filename: `XX_SIZE_ORIGIN.csv`
            - ex `insee_1745311_NEW.csv`

```python
import os, shutil
os.chdir('../')
current_dir = os.getcwd()
from inpi_insee import preparation_data
from awsPy.aws_authorization import aws_connector
from awsPy.aws_s3 import service_s3
from pathlib import Path
import pandas as pd
%load_ext autoreload
%autoreload 2
```

## Download from S3

```python
bucket = 'calfdata'
path = os.getcwd()
parent_path = str(Path(path).parent)
path_cred = r"{}/programme_matching/credential_AWS.json".format(parent_path)
con = aws_connector.aws_instantiate(credential = path_cred,
                                        region = 'eu-west-3')
client= con.client_boto()
s3 = service_s3.connect_S3(client = client,
                      bucket = 'calfdata') 

```

Download INPI

```python
s3.download_file(key= 'INPI/TC_1/02_preparation_donnee/PP/initial_partiel_evt_new_pp_status_final.csv')
```

Download INSEE

```python
s3.download_file(key= 'INSEE/Stock/ETS/StockEtablissement_utf8.csv')
```

```python
os.mkdir("data/RawData/INPI/Stock/PP")
```

```python
shutil.move("initial_partiel_evt_new_pp_status_final.csv",
            "data/RawData/INPI/Stock")
```

```python
os.mkdir("data/RawData/INSEE/Stock")
```

```python
os.mkdir("data/RawData/INSEE/Stock/ETS")
```

```python
shutil.move("StockEtablissement_utf8.csv",
            "data/RawData/INSEE/Stock/ETS")
```

```python
etb_ex = 'https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/raw'\
'/master/Notebooks_matching/Data_preprocessed/programme_matching/data/RawData' \
'/INPI/Stock/initial_partiel_evt_new_pp_status_final_exemple.csv'

commune = 'https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/raw' \
'/master/Notebooks_matching/Data_preprocessed/programme_matching/data/input' \
'/Parameters/communes_france.csv'

voie = 'https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/raw' \
'/master/Notebooks_matching/Data_preprocessed/programme_matching/data/input' \
'/Parameters/voie.csv'

param = {
    'communes_insee': commune,
    'upper_word':'data/input/Parameters/upper_stop.csv',
     "voie": voie,
    'insee':  "data/RawData/INSEE/Stock/ETS/StockEtablissement_utf8.csv",
    'inpi_etb': etb_ex,
    'date_end':"2020-01-01"
}
prep_data = preparation_data.preparation(param)
```

# Step by step approach

Le code est lent car le VPN ralentie le téléchargement de la donnée. Le fichier commune fait 7MO


## Creation NCC

Le détail de la fonction pour créer la variable ncc est disponible [ici](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/inpi_insee/preparation_data.py#L131) 

```python
pd.set_option('display.max_columns', None)
```

```python
df_inpi = pd.read_csv(param['inpi_etb'])
df_inpi.head()
```

```python
prep_data.clean_commune(df_inpi).head()[['siren','ville', 'ncc']]
```

## Creation INSEE

Le détail de la fonction pour créer la variable INSEE est disponible [ici](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/inpi_insee/preparation_data.py#L325) 

```python
voie = pd.read_csv(param['voie'])
voie.head()
```

```python

```

## Full pipeline


Origin:

- Initial: 7,575,462
- Partiel: 550,544
- NEW: 3,989,209
- EVT: 1,332,466

Il faut faire un par un, puis relancer le notebook pour relacher la mémoire


Clean dossiers

```python
import glob

files = glob.glob('data/input/INPI/InitialPartielEVTNEW/*')
for f in files:
    os.remove(f)
os.rmdir('data/input/INPI/InitialPartielEVTNEW')
```

```python
files = glob.glob('data/output/InitialPartielEVTNEW/*')
for f in files:
    os.remove(f)
try:
    os.rmdir('data/output/InitialPartielEVTNEW')
except:
    pass
```

```python
files = glob.glob('data/input/INPI/special_treatment/InitialPartielEVTNEW/*')
for f in files:
    os.remove(f)
try:
    os.rmdir('data/input/INPI/special_treatment/InitialPartielEVTNEW')
except:
    pass
```

```python
files = glob.glob('data/input/SIREN_INPI/InitialPartielEVTNEW/*')
for f in files:
    os.remove(f)
try:
    os.rmdir('data/input/SIREN_INPI/InitialPartielEVTNEW')
except:
    pass
```

```python
files = glob.glob('data/input/INSEE/InitialPartielEVTNEW/*')
for f in files:
    os.remove(f)
try:
    os.rmdir('data/input/INSEE/InitialPartielEVTNEW')
except:
    pass
```

Préparation de la normalisation. Il faut être patient. Cela durent environ 25 minutes

```python
%%time
prep_data.normalize_inpi(
    origin =['Initial','Partiel','EVT','NEW'],
    save_gz = True)
```

```python
%%time
path = 'data/input/SIREN_INPI/InitialPartielEVTNEW/' \
'inpi_SIREN_initial_partiel_evt_new_pp_status_final_InitialPartielEVTNEW.csv'

prep_data.normalize_insee(
   path,
    save_gz = True)
```
