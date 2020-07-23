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

# Programme de Matching: Nouvelles règles de gestion

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
list_inpi = ['ncc','code_postal_matching','code_commune','voie_matching','numero_voie_matching']
list_insee = ['libelleCommuneEtablissement',
            'codePostalEtablissement', 'codeCommuneEtablissement',
            'typeVoieEtablissement','numeroVoieEtablissement']

sort_list = [
 {'ncc', 'code_postal_matching', 'code_commune', 'voie_matching', 'numero_voie_matching'},
 {'ncc', 'code_postal_matching', 'code_commune', 'voie_matching'},
 {'ncc', 'code_postal_matching', 'code_commune', 'numero_voie_matching'},
 {'ncc', 'code_postal_matching', 'code_commune'},   
 {'ncc', 'code_postal_matching'},
 {'ncc'},
 {'code_postal_matching'},
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
from awsPy.aws_authorization import aws_connector
from awsPy.aws_s3 import service_s3
from pathlib import Path
import pandas as pd
bucket = 'calfdata'
path = os.getcwd()
parent_path = str(Path(path).parent)
path_cred = r"{}/programme_matching/credential_AWS.json".format(parent_path)
con = aws_connector.aws_instantiate(credential = path_cred,
                                       region = 'eu-west-3')
client= con.client_boto()
s3 = service_s3.connect_S3(client = client,
                      bucket = 'calfdata') 
#s3.download_file(
#    key= 'INPI/TC_1/02_preparation_donnee/ETS_TEMP/ets_preparation_python_1.csv')
```

```python
#df_ets = 'data/input/INPI/{}_1.csv'.format(filename)
#df_ets
#'data/input/INPI/{}_{}.csv'.format(filename, 0)
```

```python
import shutil
try:
    os.remove("data/input/INPI/{}_0.gz".format(filename))
except:
    pass
os.getcwd()
```

```python
#filename = 'ets_preparation_python_1'
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
filename = "ets_preparation_python" ####ETS
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
    'index': 'int',
    'type': 'object',
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
             'list_digit_insee']

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
    
    w1 = set(inpi)
    w2 = set(insee)
    
    try:
        return nltk.jaccard_distance(w1, w2)
    except:
        pass
```

```python
def count_conformite_test(df):
    """
    """
    
    dic_ = {
        'test_siege_principale':
    df['test_siege_principal'].value_counts(),
        
        'test_siege_secondaire':
    df['test_siege_secondaire'].value_counts(),
        
        'divergence_siege':
    df['divergence_siege'].value_counts(),
        
        'test_etb_ferme':
    df['test_status_ets_ferme'].value_counts(),
        
        'test_etb_ouvert':
    df['test_status_ets_ouvert'].value_counts(),
        
        'divergence_etatadmin':
    df['divergence_etatadmin'].value_counts(),
        
        'test_join_address':
    df['test_join_address'].value_counts(),
        
        'test_address_libelle':
    df['test_address_libelle'].value_counts(),
        
        'test_address_complement':
    df['test_address_complement'].value_counts(),
        
        
        'test_debut_activite_egal':
    (df.loc[lambda x: 
      ~x['date_début_activité'].isin([np.datetime64('NaT')])
     ]['test_date_egal']
 .value_counts()
),
        
  'test_debut_activite_sup':      
    (df.loc[lambda x: 
      ~x['date_début_activité'].isin([np.datetime64('NaT')])
     ]['test_date_sup']
 .value_counts()
)
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
    - Si `type` contient `SIE` ou `SEP` ou `PRI` et `etablissementSiege` est égal a True, alors True
- `test_siege_secondaire`:
    - Si `type` contient `SEC` et `etablissementSiege` est égal a False, alors True
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
- `divergence_etatadmin`:
    - Si `test_date_egal` est égal à False et `test_date_sup` est égal à False, alors True


```python
list_possibilities[0]['match']['inpi']
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
        parse_dates = ['dateCreationEtablissement'])

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
            date_début_activité = lambda x: pd.to_datetime(
            x['date_début_activité'], errors = 'coerce'),
                numeroVoieEtablissement_ = lambda x: x['numeroVoieEtablissement'].fillna(''),   
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
        )
            #### création tests
            .assign(   
                test_siege_principal = lambda x: np.where(
        np.logical_and(
        x['type'].isin(['SIE', 'SEP', 'PRI']),
        x['etablissementSiege'].isin(['true'])
        ),
        True, False
        ),
                test_siege_secondaire = lambda x:np.where(
        np.logical_and(
        x['type'].isin(['SEC']),
        x['etablissementSiege'].isin(['false'])
        ),
        True, False
        ),
                test_status_ets_ferme = lambda x: np.where(
        np.logical_and(
        x['last_libele_evt'].isin(['Etablissement supprimé']),
        x['etatAdministratifEtablissement'].isin(['F'])
        ),
        True, False
        ),
                test_status_ets_ouvert = lambda x: np.where(
        np.logical_and(
        x['last_libele_evt'].isin(['Etablissement ouvert']),
        x['etatAdministratifEtablissement'].isin(['A'])
        ),
        True, False
        ),
                test_date_egal = lambda x:np.where(
        x['date_début_activité'] ==
                     x['dateCreationEtablissement']
        ,
        True, False
        ),
                test_date_sup = lambda x: np.where(
        x['date_début_activité'] <=
                     x['dateCreationEtablissement']
        ,
        True, False
        ),
                divergence_siege = lambda x: np.where(
        np.logical_and(
        x['test_siege_principal'].isin([False]),
        x['test_siege_secondaire'].isin([False])
        ),
        True, False
        ),
        divergence_etatadmin = lambda x: np.where(
        np.logical_and(
        x['test_status_ets_ferme'].isin([False]),
        x['test_status_ets_ouvert'].isin([False])
        ),
        True, False
        )
            )
            )
to_check.dtypes
```

```python
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
               'id_etablissement'])['jacquard'].transform('min'))
```

```python
df_2['jacquard'] = df_2.map_partitions(
            lambda df:
                df.apply(lambda x:
                    jackard_distance(
                     x['adresse_inpi_reconstitue'],
                     x['adress_insee_reconstitue']), axis=1)
                     ).compute()
```

```python
df_2.head()
```

```python
### Nombre de duplicate
df_2.shape[0] -df_2['index'].nunique()
```

```python
#df_2[['adress_insee_reconstitue', 'adresse_inpi_reconstitue', 'jacquard',
#      'test_address_libelle', 'min_jacquard']].head(22)
```

## Etape 5: Selection meilleurs candidats

Dans cette dernière étape, on ne garde que les valeurs ont les tests ont été concluant sur le regex de l'adresse

```python
count_conformite_test(df = df_2)
```
