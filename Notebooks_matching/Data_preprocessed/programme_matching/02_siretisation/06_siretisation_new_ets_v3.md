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

# Siretisation Version 3: SQL

## Algorithme

### origin

Comme la taille de la donnée est trop élevée, il faut prendre un sous échantillon pour faire la siretisation. Le sous échantillonage se fait avec l'origine. 

Input:
- INSEE:
    - Athena: `insee_final_sql` 
- INPI:
    - Athena: `ets_final_sql` 

Output:

- 

```python
import os 
os.getcwd()
```

```python
#from itertools import compress, product

#def combinations(items):
#    return ( set(compress(items,mask)) for 
#            mask in product(*[[0,1]]*len(items)))

#all_list = ['ncc',
#             'Code_Postal','Code_Commune',
#             'INSEE','digit_inpi']
#test = list(combinations(items = all_list))[1:]
#sort_list = sorted(test[1:], key=lambda k: len(k), reverse=True) 
```

```python
list_inpi = ['ville_matching','code_postal_matching','code_commune','voie_matching','numero_voie_matching',
             'date_début_activité', 'status_admin', 'status_ets']

list_insee = ['ville_matching',
            'codepostaletablissement', 'codecommuneetablissement',
            'typevoieetablissement','numerovoieetablissement',
             'datecreationetablissement', 'etatadministratifetablissement', 'etablissementsiege']

sort_list = [
 {'ville_matching', 'code_postal_matching', 'code_commune', 'voie_matching', 'numero_voie_matching',
  'date_début_activité', 'status_admin', 'status_ets'},
    
 {'ville_matching', 'code_postal_matching', 'code_commune', 'voie_matching',
  'date_début_activité', 'status_admin', 'status_ets'},
    
 {'ville_matching', 'code_postal_matching', 'code_commune', 'numero_voie_matching',
 'date_début_activité', 'status_admin', 'status_ets'},
    
 {'ville_matching', 'code_postal_matching', 'code_commune','date_début_activité', 'status_admin', 'status_ets'},   
 {'ville_matching', 'code_postal_matching','date_début_activité', 'status_admin', 'status_ets'},
    
 {'ville_matching', 'date_début_activité', 'status_admin', 'status_ets'},
    
 {'code_postal_matching', 'date_début_activité', 'status_admin', 'status_ets'},
    
 {'code_commune', 'date_début_activité', 'status_admin', 'status_ets'},
]
len(sort_list)
```

```python
list_possibilities = []
for i in sort_list:
    left =[]
    right = []
    for j in i:
        left.append(j)
        right.append(list_insee[list_inpi.index(j)])
    left.insert(0,'siren')
    right.insert(0,'siren')
    
    dic_ = {
    'match':{
        'inpi':left,
        'insee':right,
    }
}
    list_possibilities.append(dic_)
list_possibilities
```

Indiquer le fichiers a siretiser. Si pas en local, le télécharger depuis le S3

```python
from awsPy.aws_authorization import aws_connector
from awsPy.aws_athena import service_athena
from awsPy.aws_s3 import service_s3
from pathlib import Path
import pandas as pd
import os, shutil
bucket = 'calfdata'
path = os.getcwd()
parent_path = str(Path(path).parent)
path_cred = r"{}/credential_AWS.json".format(parent_path)
con = aws_connector.aws_instantiate(credential = path_cred,
                                       region = 'eu-west-3')
client= con.client_boto()
s3 = service_s3.connect_S3(client = client,
                      bucket = 'calfdata') 
athena = service_athena.connect_athena(client = client,
                      bucket = 'calfdata') 
```

## Recuperation ETS INPI

```python
filename = 'ets_final_sql'
shutil.move("{}.csv".format(filename),
            "data/input/INPI")
```

# Parametres et fonctions

- `split_duplication`: Split un dataframe si l'index (la variable, pas l'index) contient des doublons
- `find_regex`: Performe une recherche regex entre deux colonnes
- `jackard_distance`: Calcul l'indice de dissimilarité entre deux colonnes
- `edit_distance`: Calcul le nombre de modification a faire pour obtenir la même séquence
- `import_dask`: Charge csv en Dask DataFrame pour clusteriser les calculs 
