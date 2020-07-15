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

# Programme de Matching: Version 2 nouvelles règles de gestion

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

# Pas a pas

Dans l'ensemble, le processus de siretisation fonctionne de la manière suivante:

1. Merge INSEE-INPI
2. Differenciation entre les doublons et non doublons
    - Recupération non doublons -> matched
    - Recupération doublons -> tests à réaliser
3. Batterie de tests via des regles de gestions sur la date
4. Batterie de tests via des regles de gestions sur l'adresse et type d'établissement
5. Selection des meilleurs candidats via les règles de gestion





## Etape 1: Merge INSEE-INPI

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
            .assign(
            date_début_activité = lambda x: pd.to_datetime(
            x['date_début_activité'], errors = 'coerce')
            )
                 )
to_check.dtypes
```

```python
### Nombre de duplicate
to_check.shape[0] -to_check['index'].nunique()
```

## Etape 2: Separation des doublons

Comme nous venons de le voir, sur les lignes matchées, il y a à peu près 85k lignes de doublons. De fait, nous devons écarter les lignes avec des doublons de ceux qui ne le sont pas. Nous avons créer une fonction, appelée `split_duplication` qui va compter le nombre de lignes ayant le même index (la variable). 

La fonction retourne un dictionnaire, avec un dataframe contenant uniquement les lignes sans doublon, un dataframe avec les doublons et enfin, un rapport sur le nombre d'index avec des doublons

```python
test_1 = split_duplication(df = to_check)
```

Ci dessous, un tableau avec le nombre de doublons

```python
test_1['report_dup']
```

Le dictionnaire est composé d'un DataFrame avec les valeurs sans doublons.

```python
# Test 1: doublon -> non
test_1['not_duplication'] = test_1['not_duplication'].assign(
        origin_test = 'test_1_no_duplication'
        )
test_1['not_duplication'].shape
```

Il y a environ 98% des lignes de siren sur d'avoir le bon siret. Toutefois, nous devons quand même effectuer des tests pour etre certains du résultat

```python
test_1['not_duplication'].shape[0] / to_check['index'].nunique()
```

### Conformité données

1. test sur le siège:
    - Si `type` différent de `SEC` mais `etablissementSiege` est True, alors Flag, mais pas correct. Un Siège ou principal ne peut pas être un secondaire
2. 

```python
import numpy as np
```

```python
test_1['not_duplication']['type'].value_counts()
```

```python
test_1['not_duplication']['etatAdministratifEtablissement'].value_counts()
```

```python
def test_conformite(df):
    """
    """
    
    df['test_siege_principal']  = np.where(
        np.logical_and(
        df['type'].isin(['SIE', 'SEP', 'PRI']),
        df['etablissementSiege'].isin(['false'])
        ),
        True, False
        )
    
    df['test_siege_secondaire']  = np.where(
        np.logical_and(
        df['type'].isin(['SEC']),
        df['etablissementSiege'].isin(['true'])
        ),
        True, False
        )
    
    df['test_status_ets_ferme']  = np.where(
        np.logical_and(
        df['last_libele_evt'].isin(['Etablissement supprimé']),
        df['etatAdministratifEtablissement'].isin(['A'])
        ),
        True, False
        )
    
    df['test_status_ets_ouvert']  = np.where(
        np.logical_and(
        df['last_libele_evt'].isin(['Etablissement ouvert']),
        df['etatAdministratifEtablissement'].isin(['F'])
        ),
        True, False
        )
    
    df['test_date_egal'] = np.where(
        df['date_début_activité'] ==
                     df['dateCreationEtablissement']
        ,
        True, False
        )
    
    df['test_date_sup'] = np.where(
        df['date_début_activité'] <=
                     df['dateCreationEtablissement']
        ,
        True, False
        )
    
    return df
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
        
        'test_etb_ferme':
    df['test_status_ets_ferme'].value_counts(),
        'test_etb_ouvert':
    df['test_status_ets_ouvert'].value_counts(),
        
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

```python
import sidetable
```

```python
test_1['not_duplication'] = test_conformite(df = test_1['not_duplication'])
```

```python
count_conformite_test(df = test_1['not_duplication'])
```

```python
(test_1['not_duplication']
 .loc[lambda x:x['test_siege_principal'].isin([True])]
 .stb.freq(['test_siege_principal', 'type'])
)
```

```python
(test_1['not_duplication']
 .loc[lambda x: 
      (x['test_siege_principal'].isin([True]))
      &
      (x['type'].isin(['SEP']))
     ].tail()
)
```

```python
(test_1['not_duplication']
 .loc[lambda x: 
      (x['test_status_ets'].isin([True]))
     ].tail())
```

```python
(test_1['not_duplication']
 .loc[lambda x: 
      
 (~x['date_début_activité'].isin([np.datetime64('NaT')])) 
      &
(x['test_date_sup'].isin([False]))
     ]
 .tail()
)
```

### Etape 3: Test sur les doublons, via la date

Dans cette étape la, nous allons procéder a des tests sur la date afin d'écarter les établissements enregistrés avant la date de création de l'activité. En effet, la variable `date_début_activité` indique la date de création de l'entreprise (ou activité si personne physique), alors que la variable `dateCreationEtablissement` informe sur la date de création de l'établissement. Il n'est donc pas possible que la création de l'établissement se fasse avant la date de création de l'entreprise. 

Pour chaque règle, nous appliquons la fonction `split_duplication` pour être sur que nous avons bien écarté les doublons.

Tout d'abord, nous ne gardons que les lignes ou la date de début d'activité est identique à la date de création de l'établissement.


On a deux dataframes. Le `df_no_duplication` contient des siren/siret correctement matchés, alors que le dataframe `df_duplication` a besoin de davatantage de traitement.


test

```python
test = test_conformite(df = test_1['duplication'])
```

```python
count_conformite_test(df = test)
```

### Etape 4: Test sur les doublons, via l'adresse et autre regles specifiques

Dans cette dernière étape, nous allons effectuer un dernière ensemble de tests avant de faire le trie sur les éléments que nous gardons. 

Tout d'abord, nous allons compter le nombre de siren qu'il y a selon les variables utilisées lors du matching. Ensuite, nous appliquons le regex sur les variables de l'adresse à l'INPI. Puis on réalise les tests sur le siège, la date.

```python
## Calcul nb siren/siret
upper_word = pd.read_csv('data/input/Parameters/upper_stop.csv'
        ).iloc[:,0].to_list()
df_ = (test
        .merge(
        (test
        .groupby(list_possibilities[0]['match']['inpi'])['siren']
             .count()
             .rename('count_siren_siret')
             .reset_index()
             ),how = 'left'
             )
       .sort_values(by = ['siren', 'code_greffe',
                          'nom_greffe', 'numero_gestion',
                          'id_etablissement'])
       .assign(
        numeroVoieEtablissement_ = lambda x: x['numeroVoieEtablissement'].fillna(''),   
        adresse_insee_clean=lambda x: x['libelleVoieEtablissement'].str.normalize(
                'NFKD')
            .str.encode('ascii', errors='ignore')
            .str.decode('utf-8')
            .str.replace('[^\w\s]|\d+', ' ')
            .str.upper(),
    adress_insee_reconstitue = lambda x: 
            x['numeroVoieEtablissement_'] + ' '+ \
            #x['typeVoieEtablissement'] + ' ' + \
            x['libelleVoieEtablissement']   
        )
        .assign(
            
        adresse_insee_clean = lambda x: x['adresse_insee_clean'].apply(
        lambda x:' '.join([word for word in str(x).split() if word not in
        (upper_word)])),
            
        adresse_inpi_clean = lambda x: x['adress_new'].apply(
        lambda x:' '.join([word for word in str(x).split() if word not in
        (upper_word)])),   
            
        adress_insee_reconstitue = lambda x: x['adress_insee_reconstitue'].apply(
        lambda x:' '.join([word for word in str(x).split() if word not in
        (upper_word)])),
            
        )   
             )
df_.shape
```

```python
df_.loc[lambda x: x['siren'].isin(['829840859'])]
```

Prepare adresse INSEE

```python
df_['index'].nunique()
```

```python
(df_
 .groupby(['siren', 'code_greffe', 'nom_greffe', 'numero_gestion', 'id_etablissement'])
 ['siren']
 .nunique()
).sum()
```

```python
import nltk
```

```python
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
#df_.apply(lambda x: jackard_distance(
#    x['adress_insee_reconstitue'],
#    x['adresse_inpi_clean']
#)
#)
```

### Regex sur l'adresse

```python
import dask.dataframe as dd
import numpy as np
```

```python
## Test 1: address
df_2 = dd.from_pandas(df_, npartitions=10)
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
                     x['adress_insee_reconstitue'],
                     x['adresse_inpi_clean']), axis=1)
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
df_2.head(10)
```

### Test sur la date

En plus de la date, on filtre les lignes qui ont seulement un établissement par siren


### Verification

```python
## Final test: count unique index
df_2 = df_2.merge(
        (df_2
        .groupby('index')['index']
        .count()
        .rename('count_duplicates_final')
        .reset_index()
        )
        )
```

```python
#df_2['count_duplicates_final'].value_counts()
```

```python
for i in ['test_join_address',
          'test_address_libelle',
          'test_address_complement']:
    print(df_2[i].value_counts())
```

## Etape 5: Selection meilleurs candidats

Dans cette dernière étape, on ne garde que les valeurs ont les tests ont été concluant sur le regex de l'adresse

```python
df_not_duplicate = pd.DataFrame()
copy_duplicate = df_2.copy()
duplicates_ = df_2.copy()
```

```python
for i in ['test_join_address','test_address_libelle',
         'test_address_complement']:
         ### split duplication
            test_1 = split_duplication(
            copy_duplicate[
            copy_duplicate[i].isin([True])]
    )

            ### append unique
            df_not_duplicate = (
            df_not_duplicate
            .append(test_1['not_duplication']
            .assign(test = i)
            )
            )

            copy_duplicate = (copy_duplicate
                   .loc[~copy_duplicate['index'].isin(
                       pd.concat([
                           test_1['duplication'],
                           test_1['not_duplication']
                       ], axis = 0)['index']
                       .drop_duplicates())])

            # Special treatment
            sp = (df_2[
            ~df_2['index']
            .isin(df_not_duplicate['index'])])
```

```python
df_not_duplicate.shape
```

```python
df_not_duplicate = split_duplication(df = df_not_duplicate)
```

```python
df_not_duplicate['not_duplication'].shape
```

```python
df_not_duplicate['not_duplication']['siren'].nunique()
```

```python
count_conformite_test(df = df_not_duplicate['not_duplication'])
```

# test autre regle

- Le test sur la distance de Jacquard
- Les tests sur pri/secondaire/ferme/ouvert

```python
test_jacquard = split_duplication(sp.loc[lambda x: 
       x['jacquard'] == x['min_jacquard']]
                 )['not_duplication']
```

```python
test_full_test = split_duplication(
(sp.loc[lambda x: 
        (~x['index'].isin(test_jacquard['index']))
       & 
       (x['test_siege_principal'].isin([False]))
 & 
        (x['test_siege_secondaire'].isin([False]))
&
        (x['test_status_ets_ferme'].isin([False]))
        &
         (x['test_status_ets_ouvert'].isin([False]))
       ]
 
)
)['not_duplication']
```

```python
split_duplication(
(sp.loc[lambda x: 
        (~x['index'].isin(test_jacquard['index']))
        & 
        (~x['index'].isin(test_full_test['index']))
        &
 (x['test_siege_secondaire'].isin([False]))
 &
 (x['test_status_ets_ferme'].isin([False]))
 ]
        )
)['not_duplication']
```

## Filtre matché

On sauvegarde pour le prochain loop

```python
# Input -> Save for the next loop 
inpi.loc[
        (~inpi['index'].isin(df_no_duplication['index'].unique()))
        & (~inpi['index'].isin(df_not_duplicate['index'].unique()))
        & (~inpi['index'].isin(sp['index'].unique()))
    ].compute().shape
```
