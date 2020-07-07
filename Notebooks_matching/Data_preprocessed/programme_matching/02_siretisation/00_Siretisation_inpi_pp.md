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

## Algorithme

### origin

Comme la taille de la donnée est trop élevée, il faut prendre un sous échantillon pour faire la siretisation. Le sous échantillonage se fait avec l'origine. 

Input:
- INSEE:
    - NEW: `data/input/INSEE/NEW/insee_1745311_NEW.csv`
    - Initial, Partiel, EVT: `data/input/INSEE/InitialPartielEVT/insee_8272605_InitialPartielEVT.csv`
- INPI:
    - NEW: `data/input/SIREN_INPI/NEW/inpi_initial_partiel_evt_new_ets_status_final_NEW_0.csv`
    - Initial, Partiel, EVT: `data/input/SIREN_INPI/InitialPartielEVT/inpi_initial_partiel_evt_new_ets_status_final_InitialPartielEVT_0`

Output:
- `data/output/` + `ORIGIN`
    - contient les matches
- `data/input/INPI/special_treatment/`+ `ORIGIN`
    - contient les non matches
- `data/logs/`+ `ORIGIN`
    - contient les logs

```python
import os
os.chdir('../')
current_dir = os.getcwd()
from inpi_insee import siretisation

%load_ext autoreload
%autoreload 2

param = {
    'insee': 'data/input/INSEE/InitialPartielEVTNEW/insee_1557220_InitialPartielEVTNEW.csv' ### PP
    #'insee': 'data/input/INSEE/InitialPartielEVTNEW/insee_9428972_InitialPartielEVTNEW.csv'  ### ETS
    #'insee': 'data/input/INSEE/NEW/insee_1745311_NEW.csv' ### ETS
}
# 4824158 SIREN a trouver!
al_siret = siretisation.siretisation_inpi(param)
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
list_possibilities
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
files = glob.glob('data/logs/InitialPartielEVTNEW/*')
for f in files:
    os.remove(f)
```

```python
origin = "InitialPartielEVTNEW"
#filename = "inpi_initial_partiel_evt_new_ets_status_final_InitialPartielEVTNEW" ####ETS
filename = "inpi_initial_partiel_evt_new_pp_status_final_InitialPartielEVTNEW"
#origin = "NEW"
#filename = "inpi_initial_partiel_evt_new_ets_status_final_NEW"
#### make dir
parent_dir = 'data/output/'
parent_dir_1 = 'data/input/INPI/special_treatment/'
parent_dir_2 = 'data/logs/'

for d in [parent_dir,parent_dir_1,parent_dir_2]:
    path = os.path.join(d, origin) 
    try:
        os.mkdir(path) 
    except:
        pass
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

for key, values in enumerate(list_possibilities):
    df_ets = 'data/input/INPI/{0}/{1}_{2}.csv'.format(origin, filename, key)

    inpi = al_siret.import_dask(file=df_ets,
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

    pure_match.to_csv('data/output/{0}/{1}_{2}_pure_match.gz'.format(
        origin,
        key,
        filename),
                      compression='gzip', index= False)
    # Step 2: duplication
    df_not_duplicate, sp = al_siret.step_two_duplication(df_duplication,
                                                        var_group = 
                                                         values['match']['inpi'])
    
    (df_not_duplicate
        .to_csv('data/output/{0}/{1}_{2}_not_duplicate.gz'.format(
            origin,
            key,
            filename),
                compression='gzip', index= False))

    (sp.to_csv(
        'data/input/INPI/special_treatment/{0}/{1}_{2}_special_treatment.gz'.format(
        origin,key, filename),compression='gzip', index= False))

    # Input -> Save for the next loop 
    inpi.loc[
        (~inpi['index'].isin(pure_match['index'].unique()))
        & (~inpi['index'].isin(df_not_duplicate['index'].unique()))
        & (~inpi['index'].isin(sp['index'].unique()))
    ].compute().to_csv('data/input/INPI/{0}/{1}_{2}.csv'.format(
        origin,
        filename,
        key+1),
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

    with open('data/logs/{0}/{1}_{2}_logs.json'.format(origin, key,filename), 'w') as f:
        json.dump(dic_, f)
```

# Special Treatment

exemple:

- 752085324
- 342122546: libelle type dans l'adresse complementaire a l'insee


### Recuperation via list_digit_ INPI/INSEE

```python
import pandas as pd
```

```python
for i in range(0,8):
    test = pd.read_csv(
        r'data\input\INPI\special_treatment\{}_special_treatment.gz'.format(i),
                   low_memory=False)
    print((test
 .loc[lambda x:
      (x['list_digit_inpi'] == x['list_digit_insee'])
     ]
     )['siret'].nunique())
```

```python
test = pd.read_csv(
        r'data\input\INPI\special_treatment\{}_special_treatment.gz'.format(1),
                   low_memory=False)
```

```python
test['siren'].nunique()
```

```python
97*97
```

```python
test.loc[lambda x: 
         x['siren'].isin(['752085324'])
        & (x['list_digit_inpi'] == x['list_digit_insee'])
        ][['list_digit_inpi', 'list_digit_insee', 'siret']]
```

```python
#.assign(special_digit = lambda x:x['libelleVoieEtablissement'].str.findall(r"(\d+)").apply(
#        lambda x:'&'.join([i for i in x])))
```
