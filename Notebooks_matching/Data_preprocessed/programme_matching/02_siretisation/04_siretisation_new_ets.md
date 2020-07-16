---
jupyter:
  jupytext:
    formats: ipynb,md
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.2'
      jupytext_version: 1.5.1
  kernelspec:
    display_name: Python 3
    language: python
    name: python3
---

# Programme de Matching: Version 2 Nouvelles règles de gestion

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
import os, re
os.chdir('../')
current_dir = os.getcwd()
from inpi_insee import siretisation
import pandas as pd

%load_ext autoreload
%autoreload 2

param = {
    #'insee': 'data/input/INSEE/InitialPartielEVTNEW/insee_1557220_InitialPartielEVTNEW.csv' ### PP
    'insee': 'data/input/INSEE/InitialPartielEVTNEW/insee_9368683_InitialPartielEVTNEW.csv'  ### ETS
    #'insee': 'data/input/INSEE/NEW/insee_1745311_NEW.csv' ### ETS
}
# 4824158 SIREN a trouver!
al_siret = siretisation.siretisation_inpi(param)
```

```python
pd.set_option('display.max_columns', None)
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
list_inpi = ['ncc','code_postal_matching','code_commune','voie_matching','numero_voie_matching',
             'date_début_activité', 'status_admin', 'status_ets']

list_insee = ['libelleCommuneEtablissement',
            'codePostalEtablissement', 'codeCommuneEtablissement',
            'typeVoieEtablissement','numeroVoieEtablissement',
             'dateCreationEtablissement', 'etatAdministratifEtablissement', 'etablissementSiege']

sort_list = [
 {'ncc', 'code_postal_matching', 'code_commune', 'voie_matching', 'numero_voie_matching',
  'date_début_activité', 'status_admin', 'status_ets'},
 {'ncc', 'code_postal_matching', 'code_commune', 'voie_matching',
  'date_début_activité', 'status_admin', 'status_ets'},
 {'ncc', 'code_postal_matching', 'code_commune', 'numero_voie_matching',
 'date_début_activité', 'status_admin', 'status_ets'},
 {'ncc', 'code_postal_matching', 'code_commune','date_début_activité', 'status_admin', 'status_ets'},   
 {'ncc', 'code_postal_matching','date_début_activité', 'status_admin', 'status_ets'},
 {'ncc', 'date_début_activité', 'status_admin', 'status_ets'},
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

```

```python
from awsPy.aws_authorization import aws_connector
from awsPy.aws_athena import service_athena
from awsPy.aws_s3 import service_s3
from pathlib import Path
import pandas as pd
import os
bucket = 'calfdata'
path = os.getcwd()
parent_path = str(Path(path).parent)
path_cred = r"{}/programme_matching/credential_AWS.json".format(parent_path)
con = aws_connector.aws_instantiate(credential = path_cred,
                                       region = 'eu-west-3')
client= con.client_boto()
s3 = service_s3.connect_S3(client = client,
                      bucket = 'calfdata') 
athena = service_athena.connect_athena(client = client,
                      bucket = 'calfdata') 
```

```python
query = """
SELECT * 
FROM ets_preparation_python_lib1   
"""
```

```python
output = athena.run_query(
    query=query,
    database='inpi',
    s3_output='INPI/sql_output'
)
```

```python
table = 'ets_preparation_python_lib1_1'
source_key = "{}/{}.csv".format(
                        'INPI/sql_output',
                        output['QueryExecutionId']
                               )

destination_key = "{}/{}.csv".format(
                        'INPI/TC_1/02_preparation_donnee/ETS_TEMP',
                        table
                    )

```

```python
results = s3.copy_object_s3(
                        source_key = source_key,
                        destination_key = destination_key,
                        remove = False
                    )
```

```python

```

```python
#s3.download_file(
#    key= destination_key)
```

```python
import shutil
#try:
#    os.remove("data/input/INPI/InitialPartielEVTNEW/ets_preparation_python_1.csv")
#except:
#    pass
#os.getcwd()
```

```python
#filename = 'ets_preparation_python_lib1_1'
#shutil.move("{}.csv".format(filename),
#            "data/input/INPI/InitialPartielEVTNEW")
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
filename = "ets_preparation_python_lib1" ####ETS
#filename = "inpi_initial_partiel_evt_new_ets_status_final_InitialPartielEVTNEW"
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

# Parametres et fonctions

- `import_dask`: Charge csv en Dask DataFrame pour clusteriser les calculs 

```python
def split_duplication(df):
        """
        Split un dataframe si l'index (la variable, pas l'index) contient des
        doublons.

        L'idée est de distinguer les doublons resultants du merge avec l'INSEE

        Args:
        - df: Pandas dataframe contenant au moins une variable "index"

        Returns:
        - Un Dictionary avec:
            - not_duplication: Dataframe ne contenant pas les doublons
            - duplication: Dataframe contenant les doublons
            - report_dup: Une Serie avec le nombres de doublons
        """
        if 'count_duplicates_' in df.columns:
            df = df.drop(columns = 'count_duplicates_')

        df = df.merge(
            (df
                .groupby('index')['index']
                .count()
                .rename('count_duplicates_')
                .reset_index()
                )
                )
        
        dic_ = {
            'not_duplication':df[df['count_duplicates_'].isin([1])],
            'duplication' : df[~df['count_duplicates_'].isin([1])],
            'report_dup':df[
            ~df['count_duplicates_'].isin([1])
            ]['count_duplicates_'].value_counts()
            }

        return dic_
```

```python
def find_regex(regex, test_str):
        """
        Performe une recherche regex entre deux colonnes.

        Args:
        - regex: string: charactère contenant la formule regex
        - test_str: String: String contenant le regex a trouver

        Return:
        Boolean, True si le regex est trouvé, sinon False

        """
        try:
            matches = re.search(regex, test_str)
            if matches:
                return True
            else:
                return False
        except:
            return False
```

```python
inpi_col = ['siren',
            'code_greffe',
            'nom_greffe',
            'numero_gestion',
            'id_etablissement',
            'origin',
            'file_timestamp',
            'date_greffe',
            #'nom_commercial',
            'enseigne',
            'libelle_evt',
            'last_libele_evt',
            'index',
            'type',
            'status_admin', 'status_ets',
            'code_postal_matching',
            #'ville',
            'code_commune',
            #'pays',
            #'count_initial_inpi',
            'ncc',
            'adresse_new_clean_reg',
            'adress_new',
            'voie_matching',
            'date_début_activité',
            'numero_voie_matching',
            #'len_digit_address_inpi',
            #'list_digit_inpi'
            ]

#['count_initial_inpi', 'list_digit_inpi', 'pays', 'len_digit_address_inpi', 'nom_commercial', 'ville']

inpi_dtype = {
    'siren': 'object',
    'code_greffe': 'object',
            'nom_greffe': 'object',
            'numero_gestion': 'object',
            'id_etablissement': 'object',
            'origin': 'object',
            'file_timestamp': 'object',
            'date_greffe': 'object',
    #'nom_commercial': 'object',
            'enseigne': 'object',
            'libelle_evt': 'object',
    'id_etablissement': 'object',
    'index': 'object',
    'type': 'object',
    'status_admin': 'object',
    'status_ets': 'object',
    'code_postal_matching': 'object',
    'ville': 'object',
    'code_commune': 'object',
    'pays': 'object',
    #'count_initial_inpi': 'int',
    'ncc': 'object',
    'adresse_new_clean_reg': 'object',
    'adress_new':'object',
    'voie_matching': 'object',
    'date_début_activité': 'object',
    'numero_voie_matching': 'object',
    #'len_digit_address_inpi':'object'
}

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
         'count_initial_insee',
             'len_digit_address_insee',
             'list_digit_insee',
            "enseigne1Etablissement",
            "enseigne2Etablissement",
            "enseigne3Etablissement"]

insee_dtype = {
             'siren': 'object',
             'siret': 'object',
             "etablissementSiege": "object",
             "etatAdministratifEtablissement": "object",
             'dateCreationEtablissement': 'object',
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
             'len_digit_address_insee':'object',
            "enseigne1Etablissement":'object',
            "enseigne2Etablissement":'object',
            "enseigne3Etablissement":'object'
         }
```

```python
## Calcul nb siren/siret
upper_word = pd.read_csv('data/input/Parameters/upper_stop.csv'
        ).iloc[:,0].to_list()
```

```python
voie = (pd.read_csv('data/input/Parameters/voie.csv').assign(upper = lambda x: 
             x['possibilite'].str.isupper()).loc[lambda x: 
                                                 x['upper'].isin([True])])
```

```python
import nltk
import dask.dataframe as dd
import numpy as np
import sidetable
def jackard_distance(inpi, insee):
    """
    
    """
    
    
    
    try:
        w1 = set(inpi)
        w2 = set(insee)
        return nltk.jaccard_distance(w1, w2)
    except:
        pass
```

```python
def edit_distance(inpi, insee):
    """
    
    """
    
    try:
        return nltk.edit_distance(inpi, insee)
    except:
        pass
```

```python
def count_conformite_test(df):
    """
    """
    
    dic_ = {
       # 'test_siege_siege':
    #df['test_siege_siege'].value_counts(),
        
    #    'test_siege_pri_sec':
    #df['test_siege_pri_sec'].value_counts(),
        
    #    'divergence_siege':
    #df['divergence_siege'].value_counts(),
        
    #    'test_etb_ferme':
    #df['test_status_ets_ferme'].value_counts(),
        
    #    'test_etb_ouvert':
    #df['test_status_ets_ouvert'].value_counts(),
        
    #    'divergence_etatadmin':
    #df['divergence_etatadmin'].value_counts(),
        
        'test_join_address':
    df['test_join_address'].value_counts(),
        
        'test_address_libelle':
    df['test_address_libelle'].value_counts(),
        
        'test_address_complement':
    df['test_address_complement'].value_counts(),
        
        
    #    'test_debut_activite_egal':
    #(df.loc[lambda x: 
    #  ~x['date_début_activité'].isin([np.datetime64('NaT')])
    # ]['test_date_egal']
 #.value_counts()
#),
        
 # 'test_debut_activite_sup':      
 #   (df.loc[lambda x: 
 #     ~x['date_début_activité'].isin([np.datetime64('NaT')])
 #    ]['test_date_sup']
 #.value_counts()
#),
        'jacquard':      
    (df['jacquard'].describe()
),
        'edit':      
    (df['edit'].describe())
    }
    return dic_
```

# Règle de gestion

Dans la première étape, nous allons merger les données de l'INSEE avec les données de l'INPI. Dans le cadre de ce notebook, nous allons utiliser uniquement les variables de matching suivantes:

- Siren
- Numéro de voie
- Code postal
- type de voie
- Code commune
- Ville

``` 
'siren', 'digit_inpi', 'code_postal_matching', 'INSEE', 'code_commune', 'ncc' 
``` 

Des lors que la table a été matché et soustrait des index qui n'ont pas de correspondance a l'INSEE, nous pouvons effectuer les tests suivants:

## Test Adresse

- `test_address_libelle` :
    - Si un des mots contenus dans la variable `adresse_new_clean_reg` est présente dans la variable `libelleVoieEtablissement` alors True
- `test_address_complement`:
    - Si un des mots contenus dans la variable `adresse_new_clean_reg` est présente dans la variable `libelleVoieEtablissement` alors True
- `test_join_address`:
    - Si `test_address_libelle` et `test_address_complement` sont égales a True, alors True
- `jacquard`:
    - Calcul de la distance entre `adresse_inpi_clean` et `adress_insee_reconstitue`

## Test Siege

- `test_siege_principal`:
    - Si `type` contient `SIE` ou `SEP`  et `etablissementSiege` est égal a True, alors True
- `test_siege_secondaire`:
    - Si `type` contient `SEC` ou `PRI` et `etablissementSiege` est égal a False, alors True
- `divergence_siege`:
    - Si `test_siege_principal` est égale à False et `test_siege_secondaire` est égal à False, alors True

## Test Etat 

- `test_status_ets_ferme`:
    - Si `last_libele_evt` est égale à `Etablissement supprimé` et `etatAdministratifEtablissement` est égal à `F` alors True
- `test_status_ets_ouvert`:
    - Si `last_libele_evt` est égale à `Etablissement ouvert` et `etatAdministratifEtablissement` est égal à `A` alors True
- `divergence_etatadmin`: Si `test_status_ets_ferme` est égal à False et `test_status_ets_ouvert` est égal à False, alors True

## Test Date

- `test_date_egal`:
    -  `date_début_activité` est égale à `dateCreationEtablissement` alors True 
- `test_date_sup`:
    - `date_début_activité` est supérieure à `dateCreationEtablissement` alors True 


```python
list_possibilities[0]['match']['inpi']
```

```python
list_possibilities[0]['match']['insee']
```

Dans un premier temps, on import et on merge les dataframes Insee et INPI. Pour rappel, il y a 10,648,546 d'observations à siretiser. Pour le vérifier, il suffit de regarder la variable index, qui est le numéro de la ligne dans la base initiale de l'INPI.

A noté que nous convertissons la variable `date_début_activité` en format date, car nous avons besoin de comparer cette variable avec `dateCreationEtablissement` lors de nos tests

```python
df_ets = 'data/input/INPI/{0}/{1}_{2}.csv'.format(origin, filename, 1)

inpi = (al_siret.import_dask(file=df_ets,
                                usecols=inpi_col,
                                dtype=inpi_dtype,
                                parse_dates=False)
       )
insee = al_siret.import_dask(
        file=al_siret.insee,
        usecols=insee_col,
        dtype=insee_dtype,
        #parse_dates = ['dateCreationEtablissement']
)

temp = inpi.merge(insee,
                          how='left',
                          left_on=list_possibilities[0]['match']['inpi'],
                          right_on= list_possibilities[0]['match']['insee'],
                          indicator=True,
                          suffixes=['_insee', '_inpi'])
temp = temp.compute()
```

Il est assez simple de voir que le merge a abouti a la création de doublon 

```python
temp.shape[0] - temp['index'].max() + 1
```

Nous allons appliquer des règles de gestion sur les combinaisons matchées (les `both`)

```python
temp['_merge'].value_counts()
```

```python
to_check = (temp[temp['_merge']
                 .isin(['both'])]
            .drop(columns= '_merge')
            .merge(voie, left_on = 'typeVoieEtablissement', right_on ='INSEE', how = 'left')
            #### création addresse
            .assign(
                numeroVoieEtablissement_ = lambda x: x['numeroVoieEtablissement'].fillna(''),
                possibilite = lambda x: x['possibilite'].fillna('')
                   )
            .assign(
            date_début_activité = lambda x: pd.to_datetime(
            x['date_début_activité'], errors = 'coerce'),   
        adresse_insee_clean=lambda x: x['libelleVoieEtablissement'].str.normalize(
                'NFKD')
            .str.encode('ascii', errors='ignore')
            .str.decode('utf-8')
            .str.replace('[^\w\s]|\d+', ' ')
            .str.upper(),
    adress_insee_reconstitue = lambda x: 
            x['numeroVoieEtablissement_'] + ' '+ \
            x['possibilite'] + ' ' + \
            x['libelleVoieEtablissement']   
        )
            .assign(
            
        adresse_insee_clean = lambda x: x['adresse_insee_clean'].apply(
        lambda x:' '.join([word for word in str(x).split() if word not in
        (upper_word)])),
            
        adresse_inpi_reconstitue = lambda x: x['adress_new'].apply(
        lambda x:' '.join([word for word in str(x).split() if word not in
        (upper_word)])),   
            
        adress_insee_reconstitue = lambda x: x['adress_insee_reconstitue'].apply(
        lambda x:' '.join([word for word in str(x).split() if word not in
        (upper_word)])), 
                enseigne = lambda x: x['enseigne'].str.upper()
        )
            )

## Test 1: address
df_2 = dd.from_pandas(to_check, npartitions=10)
df_2['test_address_libelle'] = df_2.map_partitions(
            lambda df:
                df.apply(lambda x:
                    find_regex(
                     x['adresse_new_clean_reg'],
                     x['libelleVoieEtablissement']), axis=1)
                     ).compute()

df_2['test_address_complement'] = df_2.map_partitions(
            lambda df:
                df.apply(lambda x:
                    find_regex(
                     x['adresse_new_clean_reg'],
                     x['complementAdresseEtablissement']), axis=1)
                     ).compute()

df_2['jacquard'] = df_2.map_partitions(
            lambda df:
                df.apply(lambda x:
                    jackard_distance(
                     x['adresse_inpi_reconstitue'],
                     x['adress_insee_reconstitue']), axis=1)
                     ).compute()

df_2['edit'] = df_2.map_partitions(
            lambda df:
                df.apply(lambda x:
                    edit_distance(
                     x['adresse_inpi_reconstitue'],
                     x['adress_insee_reconstitue']), axis=1)
                     ).compute()

df_2['jacquard_enseigne1'] = df_2.map_partitions(
            lambda df:
                df.apply(lambda x:
                    jackard_distance(
                     x['enseigne'],
                     x['enseigne1Etablissement']), axis=1)
                     ).compute()
df_2['jacquard_enseigne2'] = df_2.map_partitions(
            lambda df:
                df.apply(lambda x:
                    jackard_distance(
                     x['enseigne'],
                     x['enseigne2Etablissement']), axis=1)
                     ).compute()
df_2['jacquard_enseigne3'] = df_2.map_partitions(
            lambda df:
                df.apply(lambda x:
                    jackard_distance(
                     x['enseigne'],
                     x['enseigne3Etablissement']), axis=1)
                     ).compute()

df_2['edit_enseigne1'] = df_2.map_partitions(
            lambda df:
                df.apply(lambda x:
                    edit_distance(
                     x['enseigne'],
                     x['enseigne1Etablissement']), axis=1)
                     ).compute()
df_2['edit_enseigne2'] = df_2.map_partitions(
            lambda df:
                df.apply(lambda x:
                    edit_distance(
                     x['enseigne'],
                     x['enseigne2Etablissement']), axis=1)
                     ).compute()
df_2['edit_enseigne3'] = df_2.map_partitions(
            lambda df:
                df.apply(lambda x:
                    edit_distance(
                     x['enseigne'],
                     x['enseigne3Etablissement']), axis=1)
                     ).compute()

df_2 = df_2.compute()
## test join Adress
df_2.loc[
        (df_2['test_address_libelle'] == True)
        &(df_2['test_address_complement'] == True),
        'test_join_address'] = True

df_2.loc[
        (df_2['test_join_address'] != True),
        'test_join_address'] = False

df_2 = df_2.assign(
    min_jacquard = lambda x:
    x.groupby(['siren', 'code_greffe', 'nom_greffe', 'numero_gestion',
               'id_etablissement'])['jacquard'].transform('min'),
    min_edit = lambda x:
    x.groupby(['siren', 'code_greffe', 'nom_greffe', 'numero_gestion',
               'id_etablissement'])['edit'].transform('min'))

## test join Adress
df_2.loc[
        (df_2['test_address_libelle'] == False)
        &(df_2['test_address_complement'] == False)
        &(df_2['test_join_address'] == False),
        'test_regex_adress'] = False

df_2.loc[
        (df_2['test_regex_adress'] != False),
        'test_regex_adress'] = True
```

```python
df_2 = df_2.assign(
    
    test_distance_diff = lambda x:
    np.where(
        np.logical_or(
            x['jacquard'] != x['min_jacquard'],
            x['edit'] != x['min_edit']    
        ),
        True, False
    
    ),
    
    min_jacquard_enseigne1 = lambda x:
    x.groupby(['siren', 'code_greffe', 'nom_greffe', 'numero_gestion',
               'id_etablissement'])['jacquard_enseigne1'].transform('min'),
    
    min_jacquard_enseigne2 = lambda x:
    x.groupby(['siren', 'code_greffe', 'nom_greffe', 'numero_gestion',
               'id_etablissement'])['jacquard_enseigne2'].transform('min'),
    
    min_jacquard_enseigne3 = lambda x:
    x.groupby(['siren', 'code_greffe', 'nom_greffe', 'numero_gestion',
               'id_etablissement'])['jacquard_enseigne3'].transform('min'),
    
    min_edit_enseigne1 = lambda x:
    x.groupby(['siren', 'code_greffe', 'nom_greffe', 'numero_gestion',
               'id_etablissement'])['edit_enseigne1'].transform('min'),
    
    min_edit_enseigne2 = lambda x:
    x.groupby(['siren', 'code_greffe', 'nom_greffe', 'numero_gestion',
               'id_etablissement'])['edit_enseigne2'].transform('min'),
    
    min_edit_enseigne3 = lambda x:
    x.groupby(['siren', 'code_greffe', 'nom_greffe', 'numero_gestion',
               'id_etablissement'])['edit_enseigne3'].transform('min')
)
```

```python
#df_2 = df_2.drop(columns =  ['test_enseigne_insee','test_enseigne_edit',
#                                               'test_enseigne_jacquard'])

df_2.loc[
    (df_2['enseigne1Etablissement'].isin([np.nan]))
    &(df_2['enseigne2Etablissement'].isin([np.nan]))
    &(df_2['enseigne3Etablissement'].isin([np.nan])),
    'test_enseigne_insee'
] = True

df_2.loc[
        (df_2['test_enseigne_insee'] != True),
        'test_enseigne_insee'] = False

df_2.loc[
    (df_2['enseigne'].isin([np.nan]))
    |(df_2['test_enseigne_insee'].isin([True]))
    |(df_2['jacquard_enseigne1'] == 0)
    |(df_2['jacquard_enseigne2'] == 0)
    |(df_2['jacquard_enseigne3'] == 0),
    'test_enseigne_jacquard'
] = True

df_2.loc[
    (df_2['enseigne'].isin([np.nan]))
    |(df_2['test_enseigne_insee'].isin([True]))
    |(df_2['edit_enseigne1'] == 0)
    |(df_2['edit_enseigne2'] == 0)
    |(df_2['edit_enseigne3'] == 0),
    'test_enseigne_edit'
] = True

df_2.loc[
        (df_2['test_enseigne_edit'] != True),
        'test_enseigne_edit'] = False

df_2.loc[
        (df_2['test_enseigne_jacquard'] != True),
        'test_enseigne_jacquard'] = False
```

```python
df_2.loc[lambda x: x['siren'].isin(['400534020'])]
```

```python
df_2.head()
```

```python
### Nombre de duplicate
df_2.shape[0] -df_2['index'].nunique()
```

## Etape 5: Selection meilleurs candidats

Dans cette dernière étape, on ne garde que les valeurs ont les tests ont été concluant sur le regex de l'adresse

```python
#count_conformite_test(df = df_2)
```

```python
test = split_duplication(df = df_2)
```

```python
test['not_duplication'].shape
```

```python
test['duplication'].shape
```

```python
test['duplication'].head(10)
```

## Recupération

```python
test['duplication']['index'].nunique()
```

```python
df_filre = test['duplication'].copy()
index_found = pd.DataFrame()
df_filre.shape#.head()
```

```python
for i in ['test_join_address', 'test_address_libelle',
          'test_address_complement', 'jacquard', 'edit']:
        
        if i in ['jacquard']:
            
            index_to_keep = split_duplication(
                (df_filre.loc[lambda x:                     
                                             (x[i] == x['min_jacquard'])
                                              ]
                                             )
                                             )['not_duplication']#['index']
            
        elif i in ['edit']:
            
            index_to_keep = split_duplication(
                (df_filre.loc[lambda x:             
                                             (x[i] == x['min_edit'])
                                              ]
                                             )
                                             )['not_duplication']#['index']
     
        else:
            
            index_to_keep = split_duplication(
                (df_filre.loc[lambda x: 
                                             (x[i].isin([True]))
                                            ]
                                             )
                                             )['not_duplication']#['index']
                
        index_found = index_found.append(index_to_keep)
        df_filre = df_filre.loc[lambda x: (~x['index'].isin(index_to_keep['index'].values))]
```

```python
df_filre.shape
```

```python
index_found.shape
```

```python
index_found['index'].nunique() + df_filre['index'].nunique() == test['duplication']['index'].nunique()
```

## Règles de conformité

- Test 1: - > ?
    - Plusieurs SIRET/sequence
- Test 2:
    - Ensemble des regex faux
        - `test_regex_adress`
- Test 3
    - Test sur l'enseigne
        - Si `test_enseigne_edit` = True (-> Enseigne INSEE & INPI, match parfait)
        - Si `test_enseigne_jacquard` = True
- Test 4:
    - Test sur la distance:
        - Si `test_distance_diff` =  True & `min_jacquard` < .2 
        - Si `test_distance_diff` =  True & `min_edit` < 20 

```python
import sidetable
```

```python
df_conformite = (pd.concat(
[test['not_duplication'],
 index_found
]#, axis = 1
))
```

```python
df_conformite.shape
```

```python
sequence = ['siren', 'code_greffe', 'nom_greffe', 'numero_gestion', 'id_etablissement']
df_conformite = df_conformite.assign(
    total_siret = lambda x: x.groupby(sequence)['siret'].transform('nunique')
)
df_conformite.loc[lambda x: x['total_siret'].isin([2])].to_excel('plusieurs_siret.xlsx')
```

```python
#df_conformite.dtypes.to_frame().T#.unstack()
```

```python
#df_conformite.loc[lambda x: x['siren'].isin(['451041172'])]
```

```python
#df_conformite.loc[lambda x: x['test_enseigne_jacquard'].isin([False])]
```

```python
df_conformite.stb.freq(['test_regex_adress'])
```

```python
df_conformite.stb.freq(['test_distance_diff'])
```

```python
df_conformite.stb.freq(['test_enseigne_jacquard'])
```

```python
df_conformite.stb.freq(['test_enseigne_edit'])
```

### Filtrage

```python
df_conformite.shape
```

```python
test_1 = (df_conformite.loc[
    
    lambda x: 
    (x['test_regex_adress'].isin([True]))
    &
    (x['test_enseigne_edit'].isin([True]))
    |
    (x['test_enseigne_jacquard'].isin([True]))
    |
    (x['count_initial_insee'].isin([1]))
    & 
    (x['total_siret'].isin([1]))
    #&
    #(x['test_distance_diff'].isin([False])) 
    #& ((x['min_jacquard'] < .2) 
    # | (x['min_edit'] < 20))
]#['index']
)
```

Besoin de compter le nombre de ligne à l'INSEE pour les variables utilisées pour matcher

```python
#df_conformite.loc[lambda x: (x['siren'].isin(['998823504'])) 
#           & (x['id_etablissement'].isin(['185']))
#                      ].sort_values(by = ['id_etablissement'])
```

```python
df_conformite.shape[0] - len(test_1)
```

```python
test_1[sequence].drop_duplicates() 
```

Recupération de l'historique

```python
df_to_match = inpi.compute().loc[lambda x: 
                                 ~x['index'].isin(df_conformite['index'])
                                ]
df_to_match.shape
```

```python
df_to_match.head()
```

```python
seq = ['siren','code_greffe', 'nom_greffe', 'numero_gestion', 'id_etablissement']
sequence = ['siren', 'siret','code_greffe', 'nom_greffe', 'numero_gestion', 'id_etablissement', 'total_siret']

columns_to_keep = ['siren', 'siret', 'code_greffe', 'nom_greffe', 'numero_gestion',
       'id_etablissement', 'total_siret', 'origin', 'file_timestamp',
       'date_greffe', 'libelle_evt', 'last_libele_evt', 'type', 'adress_new',
       'adresse_new_clean_reg', 'voie_matching', 'numero_voie_matching',
       'code_postal_matching', 'ncc', 'code_commune', 'enseigne',
       'date_début_activité', 'index', 'status_admin', 'status_ets', '_merge']

df_match_2 = (
    (test_1[sequence]
 .drop_duplicates()  
    )
 .merge(df_to_match, right_on = seq, left_on = seq, how = 'left', indicator = True)
)
```

```python
df_match_2.stb.freq(['_merge'])
```

```python
df_match_2.head()
```

```python
#df_match_2.loc[lambda x: x['_merge'].isin(['both'])].dtypes
```

```python
df_final = (pd.concat(
    [
        df_match_2.loc[lambda x: x['_merge'].isin(['both'])],
        df_conformite.reindex(columns  = columns_to_keep)
        
    ])
 .sort_values(by = [
     'siren','code_greffe', 'nom_greffe', 'numero_gestion', 'id_etablissement', 'date_greffe'
 ])
)
```

```python
df_final.groupby("index")['index'].count().sort_values().loc[lambda x: x>1]
```

```python
df_conformite.loc[lambda x: x['index'].isin(['000735785'])]
```

```python
#df_conformite.loc[lambda x: x['siren'].isin(['814217535'])]
```

```python
test_1.loc[lambda x: x['siren'].isin(['814217535'])]
```

```python
split_duplication(
                df_final)['not_duplication'].shape
```

```python
split_duplication(
                df_final)['duplication'].shape
```

```python

```

```python

```
