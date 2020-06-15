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

# Programme de Matching

## Agenda

1. Demonstration full pipeline
2. Demonstration pas a pas

```python
import os, shutil
os.chdir('../')
current_dir = os.getcwd()
from inpi_insee import preparation_data
import pandas as pd
%load_ext autoreload
%autoreload 2
```

# Preparation INPI ETS

Afin d'accelerer les calcules, nous avons pris un échantillon aléatoire de 100.000 observations depuis la donnée préparée de l'INPI.


Preparation des dossiers destination

```python
import glob

files = glob.glob('data/output/InitialPartielEVTNEW/*')
for f in files:
    os.remove(f)
try:
    os.rmdir('data/output/InitialPartielEVTNEW')
except:
    pass

files = glob.glob('data/input/INPI/special_treatment/InitialPartielEVTNEW/*')
for f in files:
    os.remove(f)
try:
    os.rmdir('data/input/INPI/special_treatment/InitialPartielEVTNEW')
except:
    pass

files = glob.glob('data/input/SIREN_INPI/InitialPartielEVTNEW/*')
for f in files:
    os.remove(f)
try:
    os.rmdir('data/input/SIREN_INPI/InitialPartielEVTNEW')
except:
    pass

files = glob.glob('data/input/INSEE/InitialPartielEVTNEW/*')
for f in files:
    os.remove(f)
try:
    os.rmdir('data/input/INSEE/InitialPartielEVTNEW')
except:
    pass
```

```python
#etb_ex = 'https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/raw'\
#'/master/Notebooks_matching/Data_preprocessed/programme_matching/data/RawData' \
#'/INPI/Stock/initial_partiel_evt_new_ets_status_final_exemple_1.csv'

commune = 'https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/raw' \
'/master/Notebooks_matching/Data_preprocessed/programme_matching/data/input' \
'/Parameters/communes_france.csv'

voie = 'https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/raw' \
'/master/Notebooks_matching/Data_preprocessed/programme_matching/data/input' \
'/Parameters/voie.csv'

stopword ='https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/raw' \
'/master/Notebooks_matching/Data_preprocessed/programme_matching/data/input' \
'/Parameters/upper_stop.csv'

test = 'C:\\Users\\PERNETTH\\Documents\\Projects\\InseeInpi_matching\\' \
'Notebooks_matching\\Data_preprocessed\\programme_matching\\data\\RawData\\INPI\\Stock\\' \
'inpi_ets_exemple_2.csv'

param = {
    'communes_insee': commune,
    'upper_word':stopword,
     "voie": voie,
    'insee':  "data/RawData/INSEE/Stock/ETS/StockEtablissement_utf8.csv",
    'inpi_etb': test,
    'date_end':"2020-01-01"
}
prep_data = preparation_data.preparation(param)
```

La préparation de la donnée exclue tous les sirens qui n'ont pas de valeurs pour l'ensemble des champs de matching. De plus nous allons récupérer à l'INSEE, seulement les SIREN qui nous interessent

```python
%%time
prep_data.normalize_inpi(
    origin =['Initial','Partiel','EVT','NEW'],
    save_gz = True)
```

```python
%%time
path = 'data\\input\\SIREN_INPI\\InitialPartielEVTNEW\\' \
'inpi_SIREN_inpi_ets_exemple_2_InitialPartielEVTNEW.csv'

prep_data.normalize_insee(
   path,
    save_gz = True)
```

# Detail siretisation

L'algorithme de SIRETISATION fonctionne avec l'aide de trois fonctions:

- `step_one`: permet d'écarter les doublons du merge et d'appliquer les premières règles afin de connaitre l'origine de la siretisation
- `step_two_assess_test`: détermine l'origine du matching, a savoir la date, adresse, voie, numéro de voie
- `step_two_duplication`: permet de récuperer des SIRET sur les doublons émanant du merge avec l'INSEE

Dans premier temps, on crée un dictionnaire avec toutes les variables de matching. L'algorithme va utiliser séquentiellement les variables suivantes:

```
 {'ncc', 'Code_Postal', 'Code_Commune', 'INSEE', 'digit_inpi'},
 {'ncc', 'Code_Postal', 'Code_Commune', 'INSEE'},
 {'ncc', 'Code_Postal', 'Code_Commune', 'digit_inpi'},
 {'ncc', 'Code_Postal', 'Code_Commune'},   
 {'ncc', 'Code_Postal'},
 {'ncc'},
 {'Code_Postal'},
 {'Code_Commune'}
```

L'algorithme fonctionne de manière séquentielle, et utilise comme input un fichier de l'INPI a siretiser. De fait, après chaque séquence, l'algorithme sauvegarde un fichier gz contenant les siren a trouver. Cette étape de sauvegarde en gz permet de loader le fichier gz en input.

```python
import os
#os.chdir('../')
#current_dir = os.getcwd()
from inpi_insee import siretisation

%load_ext autoreload
%autoreload 2
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
list_inpi = ['ncc','code_postal','code_commune','INSEE','digit_inpi']
list_insee = ['libelleCommuneEtablissement',
            'codePostalEtablissement', 'codeCommuneEtablissement',
            'typeVoieEtablissement','numeroVoieEtablissement']

sort_list = [
 {'ncc', 'code_postal', 'code_commune', 'INSEE', 'digit_inpi'},
 {'ncc', 'code_postal', 'code_commune', 'INSEE'},
 {'ncc', 'code_postal', 'code_commune', 'digit_inpi'},
 {'ncc', 'code_postal', 'code_commune'},   
 {'ncc', 'code_postal'},
 {'ncc'},
 {'code_postal'},
 {'code_commune'}
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
list_possibilities[0]
```

```python
len(list_possibilities)
```

Indiquer le fichiers a siretiser. Si pas en local, le télécharger depuis le S3

```python
#filename = 'inpi_initial_partiel_evt_new_ets_status_final_test_1'
```

```python
#from awsPy.aws_authorization import aws_connector
#from awsPy.aws_s3 import service_s3
#from pathlib import Path
#import pandas as pd
#bucket = 'calfdata'
#path = os.getcwd()
#parent_path = str(Path(path).parent)
#path_cred = r"{}/programme_matching/credential_AWS.json".format(parent_path)
#con = aws_connector.aws_instantiate(credential = path_cred,
#                                        region = 'eu-west-3')
#client= con.client_boto()
#s3 = service_s3.connect_S3(client = client,
#                      bucket = 'calfdata') 
#s3.download_file(
#    key= 'INPI/TC_1/02_preparation_donnee/Stock/ETB/{}_0.csv'.format(filename))
```

```python
#df_ets = 'data/input/INPI/{}_{}.csv'.format(filename, key)
#df_ets
#'data/input/INPI/{}_{}.csv'.format(filename, 0)
```

```python
#import shutil
#try:
#    os.remove("data/input/INPI/{}_0.gz".format(filename))
#except:
#    pass
#shutil.move("{}_0.gz".format(filename),
#            "data/input/INPI")
```

Il faut prendre l'`origin` et `filename` que l'on souhaite sitetiser

```python
import glob
files = glob.glob(os.path.join(current_dir,'data/logs/InitialPartielEVTNEW/*'))
for f in files:
    os.remove(f)
```

```python
os.path.join(current_dir,'programme_matching','data\logs')
```

```python
origin = "InitialPartielEVTNEW"
#filename = "inpi_initial_partiel_evt_new_ets_status_final_InitialPartielEVTNEW" ####ETS
filename = "inpi_ets_exemple_2_InitialPartielEVTNEW"
#origin = "NEW"
#filename = "inpi_initial_partiel_evt_new_ets_status_final_NEW"
#### make dir
parent_dir = os.path.join(current_dir,'data\output')
parent_dir_1 = os.path.join(current_dir,'data\input\INPI\special_treatment')
parent_dir_2 = os.path.join(current_dir,'data\logs')

for d in [parent_dir,parent_dir_1,parent_dir_2]:
    path = os.path.join(d, origin) 
    try:
        os.mkdir(path) 
    except:
        pass
```

```python
#current_dir = os.getcwd()
current_dir
```

```python
param = {
    'insee': 'data\\input\\INSEE\\InitialPartielEVTNEW\\insee_418560_InitialPartielEVTNEW.csv' ### ETS
}
# 4824158 SIREN a trouver!
al_siret = siretisation.siretisation_inpi(param)
```

```python
%%time
import json
inpi_col = ['siren',
            'index',
            'type',
            'code_postal',
            'ville',
            'code_commune',
            'pays',
            'count_initial_inpi',
            'ncc',
            'adresse_new_clean_reg',
            'adress_new',
            'INSEE',
            'date_debut_activite',
            'digit_inpi',
            'len_digit_address_inpi',
            'list_digit_inpi'
            ]

inpi_dtype = {
    'siren': 'object',
    'index': 'int',
    'type': 'object',
    'code_postal': 'object',
    'ville': 'object',
    'code_commune': 'object',
    'pays': 'object',
    'count_initial_inpi': 'int',
    'ncc': 'object',
    'adresse_new_clean_reg': 'object',
    'adress_new':'object',
    'INSEE': 'object',
    'date_debut_activite': 'object',
    'digit_inpi': 'object',
    'len_digit_address_inpi':'object'
}


### Debut du programme
for key, values in enumerate(list_possibilities):
    df_ets = 'data\\input\\INPI\\{0}\\{1}_{2}.csv'.format(origin, filename, key)

    inpi = al_siret.import_dask(file=os.path.join(current_dir,
                                                  #'Data_preprocessed\programme_matching' ,
                                                  df_ets),
                                usecols=inpi_col,
                                dtype=inpi_dtype,
                                parse_dates=False)

    df_no_duplication, df_duplication = al_siret.step_one(
        df_input=inpi,
        left_on=values['match']['inpi'],
        right_on=values['match']['insee']
    )

    # Step 2: No duplication
    pure_match = al_siret.step_two_assess_test(df=df_no_duplication,
                                               var_group=values['match']['inpi'])
    
    path_pm = os.path.join(current_dir,
                 #'Data_preprocessed\programme_matching',
                 'data\\output\\{0}\\{1}_{2}_pure_match.gz'.format(
        origin,
        key,
        filename)
                          )

    pure_match.to_csv(path_pm,
                      compression='gzip', index= False)
    # Step 2: duplication
    df_not_duplicate, sp = al_siret.step_two_duplication(df_duplication,
                                                        var_group = 
                                                         values['match']['inpi'])
                           
    path_not_dup = os.path.join(current_dir,
                #'Data_preprocessed\programme_matching',
                                'data\\output\\{0}\\{1}_{2}_not_duplicate.gz'.format(
            origin,
            key,
            filename)
                               )
    
    (df_not_duplicate
        .to_csv(path_not_dup,
                compression='gzip', index= False))
    
    path_sp = os.path.join(current_dir,
                 #'Data_preprocessed\programme_matching',
                           'data\\input\\INPI\\special_treatment\\{0}\\{1}_{2}_special_treatment.gz'.format(
        origin,key, filename)
                          )

    (sp.to_csv(
        path_sp
        ,compression='gzip', index= False))

    # Input -> Save for the next loop 
    path_next = os.path.join(current_dir,
                # 'Data_preprocessed\programme_matching',
                             'data\\input\\INPI\\{0}\\{1}_{2}.csv'.format(
        origin,
        filename,
        key+1)
                            )
                             
    inpi.loc[
        (~inpi['index'].isin(pure_match['index'].unique()))
        & (~inpi['index'].isin(df_not_duplicate['index'].unique()))
        & (~inpi['index'].isin(sp['index'].unique()))
    ].compute().to_csv(path_next,
                       index= False)

    #### Creation LOG
    if key ==0:
        total_to_siret_intial = inpi.compute().shape[0]
        total_siren_initial = inpi.compute()['siren'].nunique()
    
    ### Total rows in df inpi to match
    total_to_siret_current = inpi.compute().shape[0]
    total_siren_current = inpi.compute()['siren'].nunique() # unique siren 
    
    ### DF with no duplication after merge INSEE
    total_rows_no_dup = df_no_duplication["index"].nunique()
    total_rows_no_dup_unique_siren = df_no_duplication['siren'].nunique()
    
    ### DF with duplication after merge INSEE
    total_rows_dup = df_duplication["index"].nunique() # total duplication
    total_rows_dup_unique_siren = df_duplication["siren"].nunique()
    
    total_rows_dup_matched = df_not_duplicate["index"].nunique() #no duplication
    total_rows_dup_matched_unique_siren = df_not_duplicate["siren"].nunique()
    
    total_rows_dup_not_matched = sp["index"].nunique() # special treatmnent
    total_rows_dup_not_matched_unique_siren = sp["siren"].nunique()
    
    ### compare with initial
    total_match_rows_current = total_rows_no_dup + total_rows_dup_matched
    perc_total_match_rows_initial = total_match_rows_current / \
    total_to_siret_intial
    
    total_match_siren_current = total_rows_no_dup_unique_siren + \
    total_rows_dup_matched_unique_siren
    
    perc_total_match_siren_initial = total_match_siren_current / \
    total_siren_initial 
    
    ### compare with current
    perc_total_match_rows_current = total_match_rows_current / \
    total_to_siret_current

    perc_total_match_siren_current = total_match_siren_current / \
    total_siren_current
    
    
    dic_ = {
        'key':key,
        'total_to_siret_intial':total_to_siret_intial,
        'total_stotal_siren_initialiren': total_siren_initial,
        'total_to_siret_current':total_to_siret_current,
        'total_siren_current': total_siren_current,
        'total_match_rows_current':total_match_rows_current,
        'perc_total_match_rows_initial':perc_total_match_rows_initial,
        'total_match_siren_current':total_match_siren_current,
        'perc_total_match_siren_initial':perc_total_match_siren_initial,
        'perc_total_match_rows_current':perc_total_match_rows_current,
        'perc_total_match_siren_current':perc_total_match_siren_current,
        'df_no_duplication': {
            'nb_index': total_rows_no_dup,
            'unique_siren':total_rows_no_dup_unique_siren
        },
        'df_duplication': {
            'nb_index': total_rows_dup,
            'unique_siren':total_rows_dup_unique_siren,
            'df_not_duplicate_index': {
                'nb_index':total_rows_dup_matched,
               'unique_siren':total_rows_dup_unique_siren
            },
            'df_sp_index': {
                'nb_index':total_rows_dup_not_matched,
               'unique_siren':total_rows_dup_not_matched_unique_siren
            }
        },
        'check': total_to_siret_current -
        total_rows_no_dup +
        total_rows_dup
    }

    path_log = os.path.join(current_dir,
                 #'Data_preprocessed\programme_matching',
                            'data\\logs\\{0}\\{1}_{2}_logs.json'.format(origin,
                                                                        key,filename)
                           )
                            
    with open(path_log, 'w') as f:
        json.dump(dic_, f)
```

# Reconstruction log

```python
data = []
for file in glob.glob(
    os.path.join(current_dir,"data\logs\InitialPartielEVTNEW\*.json")):
    with open(file) as f:
        for line in f:
            data.append(json.loads(line))
```

```python
logs = pd.json_normalize(data).sort_values(by = 'key')
logs
```

```python
logs[['total_match_rows_current']].sum()
```

```python
np.round(logs[['perc_total_match_rows_initial']].sum(), 2)
```

```python
np.round(logs[['perc_total_match_siren_initial']].sum(), 2)
```

```
{'ncc', 'Code_Postal', 'Code_Commune', 'INSEE', 'digit_inpi'}, 0
 {'ncc', 'Code_Postal', 'Code_Commune', 'INSEE'},1
 {'ncc', 'Code_Postal', 'Code_Commune', 'digit_inpi'}, 2
 {'ncc', 'Code_Postal', 'Code_Commune'},    3
 {'ncc', 'Code_Postal'}, 4
 {'ncc'}, 5
 {'Code_Postal'}, 6
 {'Code_Commune'} 7
```

```python
logs[['key',
    'perc_total_match_rows_initial',
      'perc_total_match_siren_initial']].set_index('key').plot.bar(stacked=False)
```

```python
logs[['total_match_rows_current']].plot.bar(stacked=False)
```

# Pas a pas

L'algorithme de SIRETISATION fonctionne avec l'aide de trois fonctions:

- `step_one`: permet d'écarter les doublons du merge et d'appliquer les premières règles afin de connaitre l'origine de la siretisation
- `step_two_assess_test`: détermine l'origine du matching, a savoir la date, adresse, voie, numéro de voie
- `step_two_duplication`: permet de récuperer des SIRET sur les doublons émanant du merge avec l'INSEE

Dans premier temps, on crée un dictionnaire avec toutes les variables de matching. L'algorithme va utiliser séquentiellement les variables suivantes:

```
 {'ncc', 'Code_Postal', 'Code_Commune', 'INSEE', 'digit_inpi'},
 {'ncc', 'Code_Postal', 'Code_Commune', 'INSEE'},
 {'ncc', 'Code_Postal', 'Code_Commune', 'digit_inpi'},
 {'ncc', 'Code_Postal', 'Code_Commune'},   
 {'ncc', 'Code_Postal'},
 {'ncc'},
 {'Code_Postal'},
 {'Code_Commune'}
```

**Vue d'ensemble du programme**

![](https://www.lucidchart.com/publicSegments/view/5a8cb28f-dc42-4708-babd-423962514878/image.png)


## Step One

La première étape de la séquence est l'ingestion d'un fichier gz contenant les SIREN a trouver. L'ingestion va se faire en convertissant le dataframe en Dask. L'algorithme tout d'abord utiliser la fonction `step_one` et produire deux dataframes selon si le matching avec l'INSEE a débouté sur des doublons ou non.

Les doublons sont générés si pour un même nombre de variables de matching, il existe plusieurs possibilités à l'INSEE. Par exemple, pour un siren, ville, adressse donnée, il y a plusieurs possibilités. Cela constitue un doublon et il sera traité ultérieurement, dans la mesure du possible.

Les étapes déroulées lors du premier processus est le suivant:

```
- Test 1: doublon
        - non: Save-> `test_1['not_duplication']`
        - oui:
            - Test 2: Date equal
                - oui:
                    - Test 2 bis: doublon
                        - non: Save-> `test_2_bis['not_duplication']`
                        - oui: Save-> `test_2_bis['duplication']`
                - non:
                    - Test 3: Date sup
                        - oui:
                            - Test 2 bis: doublon
                                - non: Save-> `test_3_oui_bis['not_duplication']`
                                - oui: Save-> `test_3_oui_bis['duplication']`
                        - non: Save-> `test_3_non`
```

Deux dataframe sont créés, un ne contenant pas de doublon et un deuxième contenant les doublons. L'algorithme va réaliser les tests sur le premier et faire d'avantage de recherche sur le second.

```python
df_ets = 'data\\input\\INPI\\{0}\\{1}_{2}.csv'.format(origin, filename, 0)

inpi = al_siret.import_dask(file=os.path.join(current_dir,
                                                  #'Data_preprocessed\programme_matching' ,
                                                  df_ets),
                                usecols=inpi_col,
                                dtype=inpi_dtype,
                                parse_dates=False)
inpi
```

Creation de deux dataframe:

1. df_no_duplication: Pure match
2. df_duplication: Doublon

### Partie  1 fonction: [Merge](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/inpi_insee/siretisation.py#L240)

```
 temp = df_input.merge(insee,
                          how='left',
                          left_on=left_on,
                          right_on= right_on,
                          indicator=True,
                          suffixes=['_insee', '_inpi'])

#### Recupere les merges
to_check = temp[temp['_merge'].isin(['both'])].drop(columns= '_merge')

```

```python
list_possibilities[0]['match']['inpi']
```

```python
list_possibilities[0]['match']['insee']
```

```python
import json, os, re
from dask.diagnostics import ProgressBar
from dask.multiprocessing import get
import dask.dataframe as dd
import pandas as pd
import numpy as np
#from nltk.corpus import stopwords
import matplotlib.pyplot as plt
pbar = ProgressBar()
pbar.register()

insee_col = ['siren',
         'siret',
         'dateCreationEtablissement',
         "etablissementSiege",
         "etatAdministratifEtablissement",
         'complementAdresseEtablissement',
         'numeroVoieEtablissement',
         'indiceRepetitionEtablissement',
         'typeVoieEtablissement',
         'libelleVoieEtablissement',
         'codePostalEtablissement',
         'libelleCommuneEtablissement',
         'libelleCommuneEtrangerEtablissement',
         'distributionSpecialeEtablissement',
         'codeCommuneEtablissement',
         'codeCedexEtablissement',
         'libelleCedexEtablissement',
         'codePaysEtrangerEtablissement',
         'libellePaysEtrangerEtablissement',
         'count_initial_insee','len_digit_address_insee','list_digit_insee']

insee_dtype = {
             'siren': 'object',
             'siret': 'object',
             "etablissementSiege": "object",
             "etatAdministratifEtablissement": "object",
             #'dateCreationEtablissement': 'object',
             'complementAdresseEtablissement': 'object',
             'numeroVoieEtablissement': 'object',
             'indiceRepetitionEtablissement': 'object',
             'typeVoieEtablissement': 'object',
             'libelleVoieEtablissement': 'object',
             'codePostalEtablissement': 'object',
             'libelleCommuneEtablissement': 'object',
             'libelleCommuneEtrangerEtablissement': 'object',
             'distributionSpecialeEtablissement': 'object',
             'codeCommuneEtablissement': 'object',
             'codeCedexEtablissement': 'object',
             'libelleCedexEtablissement': 'object',
             'codePaysEtrangerEtablissement': 'object',
             'libellePaysEtrangerEtablissement': 'object',
             'count_initial_insee': 'int',
             'len_digit_address_insee':'object'
         }


insee = dd.read_csv(param['insee'], usecols = insee_col, dtype = insee_dtype,
        blocksize=None, low_memory = True,
        parse_dates = ['dateCreationEtablissement'])
insee
```

```python
temp = inpi.merge(insee,
                          how='left',
                          left_on=list_possibilities[0]['match']['inpi'],
                          right_on= list_possibilities[0]['match']['insee'],
                          indicator=True,
                          suffixes=['_insee', '_inpi'])
to_check = temp[temp['_merge'].isin(['both'])].drop(columns= '_merge')
```

```python
pd.set_option('display.max_columns', None)
temp.compute().head()
```

<!-- #region -->
### Partie 2 fonction: Doublons vs non doublons

On test les doublons via la fonction [`split_duplication`](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/inpi_insee/siretisation.py#L109)

Split un dataframe si l'index (la variable, pas l'index) contient des doublons.

L'idée est de distinguer les doublons resultants du merge avec l'INSEE


```
test_1 = self.split_duplication(df = to_check)
        # Test 1: doublon -> non
        test_1['not_duplication'] = test_1['not_duplication'].assign(
        origin_test = 'test_1_no_duplication'
        )

```

Returns:
        - Un Dictionary avec:
            - not_duplication: Dataframe ne contenant pas les doublons
            - duplication: Dataframe contenant les doublons
            - report_dup: Une Serie avec le nombres de doublon

<!-- #endregion -->

```python
to_check.compute().shape
```

```python
### Solution temporaire
to_check["date_debut_activite"] = \
        to_check["date_debut_activite"].map_partitions(
        pd.to_datetime,
        format='%Y/%m/%d',
        errors = 'coerce',
        meta = ('datetime64[ns]')
        )

test_1 = al_siret.split_duplication(df = to_check)
        # Test 1: doublon -> non
test_1['not_duplication'] = test_1['not_duplication'].assign(
        origin_test = 'test_1_no_duplication'
        )

```

Pas de doublon lors de la première passe

```python
test_1['not_duplication'].shape
```

Nombre de doublons nécéssitants des tests, avec le nombre de siren concerné

```python
test_1['duplication'].shape
```

```python
# Nombre siren
test_1['report_dup'].shape
```

### Partie 3 fonction: premiers tests

On applique la première règle qui est de savoir si la date de création est égale à l'INSEE et à l'INPI.

Si c'est égale, on applique de nouveau la fonction des doublons.

Pour les doublons restants, on test un nouvelle règle, a savoir si la date de l'INSEE est supérieure à L'INPI. En effet, l'INPI est la première informée des dates de créations car c'est elle qui les réalise.

```python
## Test 2: Date equal -> oui
test_2_oui = test_1['duplication'][
        (test_1['duplication']['date_debut_activite'] ==
                     test_1['duplication']['dateCreationEtablissement'])
                     ]
### Test 2: Date equal -> oui, Test 2 bis: doublon
test_2_bis = al_siret.split_duplication(df = test_2_oui)
```

```python
test_2_bis['not_duplication'].shape
```

```python
test_2_bis['duplication'].shape
```

```python
test_2_bis['report_dup'].shape
```

Full step

```python
df_ets = 'data\\input\\INPI\\{0}\\{1}_{2}.csv'.format(origin, filename, 0)

inpi = al_siret.import_dask(file=os.path.join(current_dir,
                                                  #'Data_preprocessed\programme_matching' ,
                                                  df_ets),
                                usecols=inpi_col,
                                dtype=inpi_dtype,
                                parse_dates=False)

df_no_duplication, df_duplication = al_siret.step_one(
        df_input=inpi,
        left_on=list_possibilities[0]['match']['inpi'],
        right_on=list_possibilities[0]['match']['insee']
    )
```

365 lignes récupérées sur les 1556

```python
### 365 observations récupérées
60275-59910
```

```python
df_no_duplication.shape
```

l'objectif plus tard est de récuperer les informations des 357 observations restantes.

```python
df_duplication.shape 
```

Il y a des observations qui ne statisfont pas les règles de gestion et sont écarté, a savoir si la date de l'INSEE est supérieure à l'INPI


## Conformité des règles: [step_two_assess_test](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/inpi_insee/siretisation.py#L331)

Le premier dataframe ne contient pas de doublon, il est donc possible de réaliser différents tests afin de mieux déterminer l'origine du matching. Plus précisement, si le matching a pu se faire sur la date, l'adresse, la voie, numéro de voie et le nombre unique d'index. Les règles sont définies ci-dessous.

```
- Test 1: address libelle
            - Si mots dans inpi est contenu dans INSEE, True
        - Test 1 bis: address complement
            - Si mots dans inpi est contenu dans INSEE, True
        - Test 2: Date
            - dateCreationEtablissement >= Date_Début_Activité OR
            Date_Début_Activité = NaN OR (nombre SIREN a l'INSEE = 1 AND nombre
            SIREN des variables de matching = 1), True
        - Test 3: siege
            - Type = ['SEP', 'SIE'] AND siege = true, True
        - Test 4: voie
            - Type voie INPI = Type voie INSEE, True
        - Test 5: numero voie
            - Numero voie INPI = Numero voie INSEE, True
```

Un premier fichier csv est enregistré contenant les "pure matches"


### Step 1: Calculer nombre de SIREN

On utilise cette variable pour le test de la date

```python
## Calcul nb siren/siret
df_ = (df_no_duplication
        .merge(
        (df_no_duplication
        .groupby(list_possibilities[0]['match']['inpi'])['siren']
             .count()
             .rename('count_siren_siret')
             .reset_index()
             ),how = 'left'
             )
             )
df_.head(2)
```

### Step 1: Verification adresse

```python
## Test 1: address
df_ = dd.from_pandas(df_, npartitions=10)
df_['test_address_libelle'] = df_.map_partitions(
            lambda df:
                df.apply(lambda x:
                    al_siret.find_regex(
                     x['adresse_new_clean_reg'],
                     x['libelleVoieEtablissement']), axis=1)
                     ).compute()
```

```python
df_.head(2)
```

```python
df_['test_address_libelle'].value_counts().compute()
```

D'autres tests sont réalisés. 

- Test 1 bis: address complement
            - Si mots dans inpi est contenu dans INSEE, True
        - Test 2: Date
            - dateCreationEtablissement >= Date_Début_Activité OR
            Date_Début_Activité = NaN OR (nombre SIREN a l'INSEE = 1 AND nombre
            SIREN des variables de matching = 1), True
        - Test 3: siege
            - Type = ['SEP', 'SIE'] AND siege = true, True
        - Test 4: voie
            - Type voie INPI = Type voie INSEE, True
        - Test 5: numero voie
            - Numero voie INPI = Numero voie INSEE, True


Full pipeline

```python
pure_match = al_siret.step_two_assess_test(df=df_no_duplication,
                                               var_group=values['match']['inpi'])
```

```python
test_ = ['test_address_libelle', 'test_address_complement',
        'test_join_address', 'test_date', 'test_siege',
         'test_voie', 'test_numero']

for i in test_:
    print('Test: {0}:\n {1}'.format(i,pure_match[i].value_counts()))
```

## step_two_duplication

Les seconds dataframes contiennent les doublons obtenus après le matching avec l'INSEE. L'algorithme va travailler sur différentes variables de manière séquentielle pour tenter de trouver le bons SIRET. Plus précisément, 3 variables qui ont été récemment créées sont utilisées:

- test_join_address -> True si la variable test_address_libelle = True (ie mot INPI trouvé dans INSEE) et test_join_address =  True
- test_address_libelle ->  True si la variable test_address_libelle = True (ie mot INPI trouvé dans INSEE)
- test_address_complement -> True si la variable test_join_address =  True

Pour chaque séquence, on réalise les tests suivants:

```
- Si test_join_address = True:
        - Test 1: doublon:
            - Oui: append-> `df_not_duplicate`
            - Non: Pass
            - Exclue les `index` de df_duplication
            - then go next
        - Si test_address_libelle = True:
            - Test 1: doublon:
                - Oui: append-> `df_not_duplicate`
                - Non: Pass
                - Exclue les `index` de df_duplication
                - then go next
        - Si test_address_complement = True:
            - Test 1: doublon:
                - Oui: append-> `df_not_duplicate`
                - Non: Pass
                - Exclue les `index` de df_duplication
```

Dernière étape de l'algorithme permettant de récuperer des SIRET sur les
        doublons émanant du merge avec l'INSEE. Cette étape va utliser l'étape
        précédante, a savoir les variables 'test_join_address',
        'test_address_libelle', 'test_address_complement'. Le résultat du test
        distingue 2 différents dataframe. Un premier pour les doublons
        fraichement siretisés, un deuxième contenant des SIREN qui feront
        l'objet d'un traitement spécial.


### Partie 1: Realisation des tests sur les doublons

```python
df_duplication.shape
```

```python
duplicates_ = al_siret.step_two_assess_test(df = df_duplication,
        var_group=list_possibilities[0]['match']['inpi'])

df_not_duplicate = pd.DataFrame()
copy_duplicate = duplicates_.copy()
```

### Partie 2: Tests sur 'test_join_address'

Le test join adresse:

- Si regex trouvé dans libelleVoieEtablissement et complementAdresseEtablissement, alors True (réalisé a l'étape précédente)

On va donc récuperer les doublons pour lequel le test join adresse est TRUE et on applique la fonction de séparation des doublons

```python
test_1 = al_siret.split_duplication(
            copy_duplicate[
            copy_duplicate['test_join_address'].isin([True])]
    )
```

```python
test_1['not_duplication'].shape
```

```python
test_1['duplication'].shape
```

On récupere les siret trouvés (ie sans les doublons), et on les ajoute au dataframe

```python
### append unique
df_not_duplicate = (
            df_not_duplicate
            .append(test_1['not_duplication']
            .assign(test = 'test_join_address')
            )
            )
```

On exclue du dataframe `copy_duplicate` les valeurs trouvées et non trouvées en prenant le soin d'enlever les doublons

```python
copy_duplicate.shape
```

```python
pd.concat([
                           test_1['duplication'],
                           test_1['not_duplication']
                       ], axis = 0).shape
```

```python
copy_duplicate.shape[0]- (test_1['duplication'].shape[0] +  test_1['not_duplication'].shape[0])
```

Vérifier pourquoi on enlève les duplicates.

```python
(copy_duplicate
                   .loc[~copy_duplicate['index'].isin(
                       pd.concat([
                           test_1['duplication'],
                           test_1['not_duplication']
                       ], axis = 0)['index']
                       .drop_duplicates())]).shape
```

Full pipeline

```python
df_not_duplicate, sp = al_siret.step_two_duplication(df_duplication,
                                                        var_group = 
                                                         list_possibilities[0]['match']['inpi']
                                                    )
```

```python
df_not_duplicate.shape
```

```python
sp.shape
```
