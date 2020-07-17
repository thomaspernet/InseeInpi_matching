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

# Siretisation: Version 2 

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

- `split_duplication`: Split un dataframe si l'index (la variable, pas l'index) contient des doublons
- `find_regex`: Performe une recherche regex entre deux colonnes
- `jackard_distance`: Calcul l'indice de dissimilarité entre deux colonnes
- `edit_distance`: Calcul le nombre de modification a faire pour obtenir la même séquence
- `import_dask`: Charge csv en Dask DataFrame pour clusteriser les calculs 

```python
import nltk
import dask.dataframe as dd
import numpy as np
import sidetable
```

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

## Calcul nb siren/siret
upper_word = pd.read_csv('data/input/Parameters/upper_stop.csv'
        ).iloc[:,0].to_list()

voie = (pd.read_csv('data/input/Parameters/voie.csv').assign(upper = lambda x: 
             x['possibilite'].str.isupper()).loc[lambda x: 
                                                 x['upper'].isin([True])])
```

<!-- #region -->
# Processus de siretisation

## Création variables supplémentaire

Lors du processus de siretisation, nous avons besoin de créer de nouvelles variables, qui seront, faites lors de la préparation de la donnée. 

Les nouvelles variables sont les suivantes:

- `status_admin`: Si last_libele_evt = 'Etablissement ouvert' THEN 'A' ELSE 'F
- `status_ets`: Si type = 'SIE' OR type = 'SEP' THEN 'true' ELSE 'false'
- `numeroVoieEtablissement_`: Remplace les na par des blancs. Cela évite d'avoir des problèmes lors du calcul de la distance
- `possibilite`: Conversion des abbrévations des types de voie de l'INSEE 
- `adresse_insee_clean`: Nettoyage de l'adresse de l'INSEE (`libelleVoieEtablissement`) de la même manière que l'INPI
- `adress_insee_reconstitue`: Reconstitution de l'adresse à l'INSEE en utilisant le numéro de voie `numeroVoieEtablissement_`, le type de voie non abbrégé `possibilite` et l'adresse `libelleVoieEtablissement`
- `enseigne`: Mise en majuscule de l'adresse


### variables nécéssaires aux tests

Les variables ci dessous sont des nouvelles variables résultant du merge entre les deux tables

- `test_address_libelle`: 
    - Si un des mots contenus dans la variable `adresse_new_clean_reg` est présente dans la variable `libelleVoieEtablissement` alors True
- `test_address_complement`:
    - Si un des mots contenus dans la variable `adresse_new_clean_reg` est présente dans la variable `libelleVoieEtablissement` alors True
- `jacquard`:
    - Calcul de la distance (dissimilarité) entre `adresse_inpi_clean` et `adress_insee_reconstitue`
- `edit`:
    - Calcul de la distance (Levhenstein) entre `adresse_inpi_clean` et `adress_insee_reconstitue`
- `jacquard_enseigne1`:
    - Jaccard distance entre `enseigne` et `enseigne1Etablissement1`
- `jacquard_enseigne2`:
    - Jaccard distance entre `enseigne` et `enseigne1Etablissement2`
- `jacquard_enseigne3`:
    - Jaccard distance entre `enseigne` et `enseigne1Etablissement3`
- `edit_enseigne1`:
    - Edit distance entre `enseigne` et `enseigne1Etablissement1`
- `edit_enseigne2`:
    - Edit distance entre `enseigne` et `enseigne1Etablissement2`
- `edit_enseigne3`:
    - Edit distance entre `enseigne` et `enseigne1Etablissement3`
- `min_jacquard`:
    - Jaccard distance minimum sur l'adresse pour la séquance `siren`, `code_greffe`, `nom_greffe`, `numero_gestion`, `id_etablissement`
- `min_edit`:
    - Edit distance minimum sur l'adresse pour la séquance `siren`, `code_greffe`, `nom_greffe`, `numero_gestion`, `id_etablissement`
- `min_jacquard_enseigne1`:
    - Jaccard distance minimum sur l'enseigne 1 pour la séquance `siren`, `code_greffe`, `nom_greffe`, `numero_gestion`, `id_etablissement`
- `min_jacquard_enseigne2`:
    - Jaccard distance minimum sur l'enseigne 2 pour la séquance `siren`, `code_greffe`, `nom_greffe`, `numero_gestion`, `id_etablissement`
- `min_jacquard_enseigne3`:
    - Jaccard distance minimum sur l'enseigne 3 pour la séquance `siren`, `code_greffe`, `nom_greffe`, `numero_gestion`, `id_etablissement`
- `min_edit_enseigne1`:
    - Edit distance minimum sur l'enseigne 1 pour la séquance `siren`, `code_greffe`, `nom_greffe`, `numero_gestion`, `id_etablissement`
- `min_edit_enseigne2`:
    - Edit distance minimum sur l'enseigne 2 pour la séquance `siren`, `code_greffe`, `nom_greffe`, `numero_gestion`, `id_etablissement`
- `min_edit_enseigne3`:
    - Edit distance minimum sur l'enseigne 3 pour la séquance `siren`, `code_greffe`, `nom_greffe`, `numero_gestion`, `id_etablissement`


### Variables Tests 

- test_join_address:
    - Si `test_address_libelle` et `test_address_complement` sont égales a True, alors True
- test_regex_adress:
    - Si `test_address_libelle`, `test_address_complement` et `test_join_address` sont égales a False, Alors False
- test_jacquard_adress:
    - Si `jacquard` est égale à `min_jacquard` alors, True
- test_edit_adress:
    - Si `edit` est égale à `min_edit` alors, True
- test_distance_diff:
    - Vérification des désacords entre distance de Jaccard et Levhenstein
        - Si (`test_jacquard_adress` est True et `test_edit_adress` est False) OU (`test_jacquard_adress` est False et `test_edit_adress` est True), alors True
- test_enseigne_insee:
    - Si `enseigne1Etablissement1` et `enseigne1Etablissement2` et `enseigne1Etablissement3` ne sont pas renseignées, alors True
- test_enseigne_jacquard: 
    - Si l'enseigne à l'INSEE ou à l'INPI est renseignée et que la distance de jaccard est égale à 0, alors True
- test_enseigne_edit:
    - Si l'enseigne à l'INSEE ou à l'INPI est renseignée et que la distance de Levensthein est égale à 0, alors True
<!-- #endregion -->

```python
df_3_bis['not_duplication'].head()
```

```python
list_possibilities[0]['match']['inpi']
```

```python
list_possibilities[0]['match']['insee']
```

## Etape 1: Merge

Dans un premier temps, nous allons merger la table de l'INSEE et de l'INPI sur un ensemble de variable très contraignante: 

- `siren`
- `status_ets`: Etablissement ouvert/fermé
- `numero_voie_matching`: Numéro de voie
- `voie_matching`:  Type de voie
- `ncc`: ville
- `code_commune`:  Code commune
- `status_admin`: Type d'entreprise
- `date_début_activité`: Date de création de l'établissement
- `code_postal_matching`: Code postal

L'idée principale est de trouver le siret d'une séquence avec le plus d'exactitude pour ensuite appliquer le siret à la séquence. Cette technique permet d'être plus sur sur l'historique. En effet, l'INSEE donne le dernier état connu, il peut donc avoir des différences avec les valeurs historisées de l'INPI, surtout sur le type ou l'enseigne.

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
```

```python
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

```python
temp['_merge'].value_counts()
```

Nous allons appliquer des règles de gestion sur les combinaisons matchées (les `both`)


## Etape 2: Création variables tests

Dans cette étape, nous allons créer toutes les variables de test, comme évoqué précédement, a savoir sur l'adresse et l'enseigne.

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

df_2 = df_2.assign(
    test_jacquard_adress = lambda x: np.where(
        x['jacquard'] == x['min_jacquard'],
        True, False
    ),
    test_edit_adress = lambda x: np.where(
        x['edit'] == x['min_edit'],
        True, False
    ),
    test_distance_diff = lambda x:
    np.where(
        np.logical_or(
            np.logical_and(
            x['jacquard'] == x['min_jacquard'],
            x['edit'] != x['min_edit']
            ),
            np.logical_and(
            x['jacquard'] != x['min_jacquard'],
            x['edit'] == x['min_edit']
            )
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
df_2.to_csv('temp.csv', index= False)#.loc[lambda x: x['siren'].isin(['400534020'])]
```

```python
df_2.head()
```

```python
### Nombre de duplicate
df_2.shape[0] -df_2['index'].nunique()
```

```python
df_2.stb.freq(['test_regex_adress'])

```

```python
df_2.stb.freq(['test_jacquard_adress'])
df_2.stb.freq(['test_edit_adress'])
df_2.stb.freq(['test_distance_diff'])
df_2.stb.freq(['test_enseigne_jacquard'])
df_2.stb.freq(['test_enseigne_edit'])
```

## Etape 3: Dedoublonnage

Cette étape permet de dédoublonner les lignes matchées via la variable `index`. En effet, il est possible d'avoir des doublons lorsque l'entreprise à plusieurs établissements dans la même adresse. C'est le cas pour les sièges et principals.

On va appliquer le filtre sur l'ensemble de la table matchée, puis compter le nombre de siret par séquence. Si le nombre de siret est égal à 1, c'est un bon match, sinon, il y a encore des doublons même après le filtre. Nous allons appliquer un deuxième filtre sur les doublons puis concatener avec les séquences ayant 1 siret. Dès lors, on applique la fonction `split_duplication` pour séparer les doublons des valeurs uniques. Si il y a encore des doublons, il n'y a pas suffisamment d'information pour distinguer le bon siret. Il faudra prendre plus de précaution avec des séquences

Les règles sons les suivantes:

### Filtre 1

- Si `test_regex_adress` est égal a True, ET `test_jacquard_adress` est égal à True, ET `test_edit_adress` est égal à True, ET `test_enseigne_edit` est égal a True OU `test_enseigne_jacquard` est égal a True OU `count_initial_insee` est égal à 1

### Filtre 2

Le filtre deux ne s'applique que sur les lignes dont la séquence a plus de deux sirets. Le filtre est le suivant 

- Si `jacquard` est egal 0 et `edit` est egal a 0, alors on garde. Autrement dit, on ne garde que les lignes dont l'adresse est correcte dans les deux cas. On pourrait potentiellement lever la contrainte.


```python
df_2.head()
```

```python
df_2 = pd.read_csv('temp.csv', dtype = {'siren': 'O',
 'code_greffe': 'O',
 'nom_greffe': 'O',
 'numero_gestion': 'O',
 'id_etablissement': 'O',
 'origin': 'O',
 'file_timestamp': 'O',
 'date_greffe': 'O',
 'libelle_evt': 'O',
 'last_libele_evt': 'O',
 'type': 'O',
 'adress_new': 'O',
 'adresse_new_clean_reg': 'O',
 'voie_matching': 'O',
 'numero_voie_matching': 'O',
 'code_postal_matching': 'O',
 'ncc': 'O',
 'code_commune': 'O',
 'enseigne': 'O',
 'date_début_activité': 'O',
 'index': 'O',
 'status_admin': 'O',
 'status_ets': 'bool',
 'siret': 'O',
 'dateCreationEtablissement': 'O',
 'count_initial_insee': 'float64',
 'etablissementSiege': 'bool',
 'complementAdresseEtablissement': 'O',
 'numeroVoieEtablissement': 'O',
 'indiceRepetitionEtablissement': 'O',
 'typeVoieEtablissement': 'O',
 'libelleVoieEtablissement': 'O',
 'len_digit_address_insee': 'float64',
 'list_digit_insee': 'O',
 'codePostalEtablissement': 'O',
 'libelleCommuneEtablissement': 'O',
 'libelleCommuneEtrangerEtablissement': 'O',
 'distributionSpecialeEtablissement': 'O',
 'codeCommuneEtablissement': 'O',
 'codeCedexEtablissement': 'O',
 'libelleCedexEtablissement': 'O',
 'codePaysEtrangerEtablissement': 'O',
 'libellePaysEtrangerEtablissement': 'O',
 'etatAdministratifEtablissement': 'O',
 'enseigne1Etablissement': 'O',
 'enseigne2Etablissement': 'O',
 'enseigne3Etablissement': 'O',
 'INSEE': 'O',
 'possibilite': 'O',
 'upper': 'O',
 'numeroVoieEtablissement_': 'O',
 'adresse_insee_clean': 'O',
 'adress_insee_reconstitue': 'O',
 'adresse_inpi_reconstitue': 'O',
 'test_address_libelle': 'bool',
 'test_address_complement': 'bool',
 'jacquard': 'float64',
 'edit': 'int64',
 'jacquard_enseigne1': 'float64',
 'jacquard_enseigne2': 'float64',
 'jacquard_enseigne3': 'float64',
 'edit_enseigne1': 'float64',
 'edit_enseigne2': 'float64',
 'edit_enseigne3': 'float64',
 'test_join_address': 'bool',
 'min_jacquard': 'float64',
 'min_edit': 'int64',
 'test_regex_adress': 'bool',
 'test_distance_diff': 'bool',
 'min_jacquard_enseigne1': 'float64',
 'min_jacquard_enseigne2': 'float64',
 'min_jacquard_enseigne3': 'float64',
 'min_edit_enseigne1': 'float64',
 'min_edit_enseigne2': 'float64',
 'min_edit_enseigne3': 'float64',
 'test_enseigne_insee': 'bool',
 'test_enseigne_jacquard': 'bool',
 'test_enseigne_edit': 'bool'}
                  )
```

```python
reindex = ['index','total_siret','count_duplicates_','siren','siret', 'code_greffe', 'nom_greffe', 'numero_gestion',
       'id_etablissement', 'origin', 'file_timestamp', 'date_greffe',
       'libelle_evt', 'last_libele_evt','status_admin','etatAdministratifEtablissement',
        'type', 'etablissementSiege', 'status_ets',
       'adress_new','libelleVoieEtablissement','complementAdresseEtablissement',
       'voie_matching','typeVoieEtablissement', 'INSEE','possibilite', 'upper',
           'numero_voie_matching','numeroVoieEtablissement','numeroVoieEtablissement_',
       'code_postal_matching','codePostalEtablissement',
        'ncc','libelleCommuneEtablissement',
        'code_commune', 'codeCommuneEtablissement',
       'date_début_activité', 
       'dateCreationEtablissement',
       'count_initial_insee',
       'indiceRepetitionEtablissement',
       'len_digit_address_insee', 'list_digit_insee',
       'libelleCommuneEtrangerEtablissement',
       'distributionSpecialeEtablissement', 
       'codeCedexEtablissement', 'libelleCedexEtablissement',
       'codePaysEtrangerEtablissement', 'libellePaysEtrangerEtablissement', 
       'adresse_insee_clean',
       'adress_insee_reconstitue',
       'adresse_inpi_reconstitue',
       'adresse_new_clean_reg',
       'jacquard',
       'edit',
       'test_address_libelle',
       'test_address_complement',
       'test_join_address',
        'test_regex_adress',
       'min_jacquard',
           'test_jacquard_adress',
       'min_edit',
       'test_edit_adress',
       'test_distance_diff',
        'enseigne',
       'enseigne1Etablissement',
        'jacquard_enseigne1',
        'min_jacquard_enseigne1',
        'edit_enseigne1',
        'min_edit_enseigne1',
           
        'enseigne2Etablissement',
       'jacquard_enseigne2',
       'min_jacquard_enseigne2',
        'edit_enseigne2',
       'min_edit_enseigne2',   
           
           'enseigne3Etablissement',
       'jacquard_enseigne3',
       'min_jacquard_enseigne3',
        'test_enseigne_jacquard',
       'edit_enseigne3',
       'min_edit_enseigne3',
       'test_enseigne_edit', 
        'test_enseigne_insee']
len(reindex)
```

```python
sequence = ['siren', 'code_greffe', 'nom_greffe', 'numero_gestion', 'id_etablissement']
df_3 = (df_2.loc[
    
    lambda x: 
    (x['test_regex_adress'].isin([True]))
    &
    (x['test_jacquard_adress'].isin([True]))
    &
    (x['test_edit_adress'].isin([True]))
    &
    (x['test_enseigne_edit'].isin([True]))
    |
    (x['test_enseigne_jacquard'].isin([True]))
    |
    (x['count_initial_insee'].isin([1])) ### 1 seul etb a l'INSEE
]
          .assign(
    total_siret = lambda x: 
    x.groupby(sequence)['siret'].transform('nunique')
)
          .reindex(columns = reindex)
)
```

```python
df_3.stb.freq(['total_siret'])
```

Maintenant, nous allons faire une dédoublonnage sur les séquences avec plusieurs siret, puis on concatene avec les lignes ayant qu'un seul siret par séquence. Finalement, on applique la fonction `split_duplication` pour écarter les doublons restant

```python
df_3.loc[lambda x: 
         (x['total_siret'] > 1)].shape
```

```python
df_3.loc[lambda x: 
         (x['total_siret'] > 1)
        &
         (
         (x['jacquard'] == 0) 
         |
         (x['edit'] == 0) 
         )].shape
```

```python
3117/11047
```

```python
df_3_bis = split_duplication(pd.concat(
    [df_3.loc[lambda x: 
         (x['total_siret'] > 1)
        &
         (
         (x['jacquard'] == 0) 
         |
         (x['edit'] == 0) 
         )]
        ,
     df_3.loc[lambda x: 
         (x['total_siret'] == 1)
             ]
    ]
     )
                             
                 )
```

```python
#df_2.loc[lambda x:x['test_distance_diff'].isin([True])].head()
# test_distance_diff  A REVOIR
```

```python
df_3_bis['not_duplication'] = df_3_bis['not_duplication'].sort_values(by = [
     'siren','code_greffe', 'nom_greffe', 'numero_gestion', 'id_etablissement', 'date_greffe'
 ])
df_3_bis['not_duplication'].head()
```

```python
df_3_bis['duplication'].head()
```

```python
df_3_bis['not_duplication'].loc[lambda x: x['siret'].isin(['05480654204949'])]
```

```python
df_2.loc[lambda x: x['siret'].isin(['05480654204949'])]
```

## Etape 4: Récupération sequence dans table INPI

Dans cette étape, nous allons utiliser les siret que nous venons de récupérer et les matcher avec la table de l'INPI. Cela évite de refaire tous les tests sur des séquences dont on a déjà récupérer le siret.

Tout d'abord, nous devons récupérer les siret sur la séquence `siren`, `code_greffe`, `nom_greffe`, `numero_gestion`, et `id_etablissement`. Attention, il faut enlever les doublons du aux valeurs historiques, puis on merge avec la table de l'INSEE. 

```python
import sidetable
```

```python
seq_siret = ['siren', 'siret', 'code_greffe', 'nom_greffe', 'numero_gestion', 'id_etablissement']
df_3_bis['not_duplication'][seq_siret].drop_duplicates() 
```

### Recupération de l'historique

```python
#seq = ['siren','code_greffe', 'nom_greffe', 'numero_gestion', 'id_etablissement']
columns_to_keep = ['siren', 'siret', 'code_greffe', 'nom_greffe', 'numero_gestion',
       'id_etablissement', 'total_siret', 'origin', 'file_timestamp',
       'date_greffe', 'libelle_evt', 'last_libele_evt', 'type', 'adress_new',
       'adresse_new_clean_reg', 'voie_matching', 'numero_voie_matching',
       'code_postal_matching', 'ncc', 'code_commune', 'enseigne',
       'date_début_activité', 'index', 'status_admin', 'status_ets', '_merge']

df_match_2 = (
    (df_3_bis['not_duplication'][seq_siret + ['total_siret']]
 .drop_duplicates()  
    )
 .merge(inpi.compute().loc[lambda x: 
                                 ~x['index'].isin(df_3_bis['not_duplication']['index'])],
                           on = sequence, how = 'left', indicator = True)
)
```

```python
df_match_2.stb.freq(['_merge'])
```

```python
df_match_2.head()
```

## Etape 5: Concatenation des sequences

Maintenant que nous avons réussi a récuperer les siret dans la table INPI depuis les valeurs connues lors de nos tests, nous pouvons concatener les deux tables et ne prendre que les colonnes d'origines.

Il faut tout de même refaire la fonction `split_duplication` pour enlever les siret multiples

```python
df_final = (pd.concat(
    [
        df_match_2.loc[lambda x: x['_merge'].isin(['both'])],
        df_3_bis['not_duplication'].reindex(columns  = columns_to_keep)
        
    ])
 .sort_values(by = [
     'siren','code_greffe', 'nom_greffe', 'numero_gestion', 'id_etablissement', 'date_greffe'
 ])
 #.loc[lambda x: x['total_siret'] == 1]            
)
```

```python
### Sequence trouvée
df_final_no_duplicate = split_duplication(
                df_final)['not_duplication']
```

```python
df_final_no_duplicate.shape
```

```python
df_final_duplicate = split_duplication(
                df_final)['duplication']
```

```python
df_final_duplicate.shape
```

## Etape 6: Ecarte les séquences trouvées de la table INPI

La dernière étape consiste a enlever les index des séquences siretisées de la table de l'INPI. On va sauvegarder la nouvelle table de l'INPI mais aussi, la table que a servi a trouver le siret, et la table siretiser.

On sauvegarde aussi un table de log

```python
df_a_trouver = inpi.compute().loc[lambda x: ~x['index'].isin(df_final['index'].values)]
```

```python
{'Nombre de lignes': df_final_no_duplicate.shape[0],
'detail': {
    'siren':df_final['siren'].nunique(),
    'siret':{
        'unique':df_final_no_duplicate['siret'].nunique(),
        'multiple':df_final_duplicate['siret'].nunique(),
        'siren_multiple':df_final_duplicate['siren'].nunique()
    },
    'merge_step_4': df_match_2.stb.freq(['_merge']).to_dict()
},
'reste a trouver':{
    'siren': df_a_trouver['siren'].nunique(),
    'size': df_a_trouver.shape[0],
    'sequence':df_a_trouver.groupby(sequence)['siren'].nunique().sum()
}
}
```

```python
# 10648545
9245473 + 1403065 + 7
```

```python
#df_a_trouver.groupby(sequence)['siren'].nunique().sum()
```
