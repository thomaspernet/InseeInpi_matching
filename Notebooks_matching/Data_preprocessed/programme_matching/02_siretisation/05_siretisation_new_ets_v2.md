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

# Siretisation: Version 2 

## Algorithme

### origin

Comme la taille de la donnée est trop élevée, il faut prendre un sous échantillon pour faire la siretisation. Le sous échantillonage se fait avec l'origine. 

Input:
- INSEE:
    - Athena: `insee_final_sql` 
- INPI:
    - Athena: `ets_final_sql` 

Output:

- ets_siretise
    * Table siretisée: [INPI/TC_1/04_table_siretisee](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/TC_1/04_table_siretisee/?region=eu-west-3) 

- ets_non_siretise
    * Table non siretisée: [INPI/TC_1/05_table_non_siretisee](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/TC_1/05_table_non_siretisee/?region=eu-west-3)

- ets_rules
    * Table règles: [INPI/TC_1/06_table_regles/ETS](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/TC_1/06_table_regles/ETS/?region=eu-west-3&tab=overview)

- logs ETS: [INPI/TC_1/04_table_siretisee/ETS_logs](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/TC_1/04_table_siretisee/ETS_logs/?region=eu-west-3&tab=overview)

```python
import os 
os.getcwd()
```

```python
#import os, re
#os.chdir('../')
#from inpi_insee import siretisation
import json, os, re
from dask.diagnostics import ProgressBar
from dask.multiprocessing import get
import dask.dataframe as dd
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
pbar = ProgressBar()
pbar.register()
current_dir = os.getcwd()
%load_ext autoreload
%autoreload 2
pd.set_option('display.max_columns', None)
#param = {
    #'insee': 'data/input/INSEE/InitialPartielEVTNEW/insee_1557220_InitialPartielEVTNEW.csv' ### PP
#    'insee': 'data/input/INSEE/InitialPartielEVTNEW/insee_9368683_InitialPartielEVTNEW.csv'  ### ETS
    #'insee': 'data/input/INSEE/NEW/insee_1745311_NEW.csv' ### ETS
#}
# 4824158 SIREN a trouver!
#al_siret = siretisation.siretisation_inpi(param)
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
csv_a_cree = False
if csv_cree:
    query = """
  SELECT 
  index_id, 
  siren, 
  code_greffe, 
  nom_greffe, 
  numero_gestion, 
  id_etablissement, 
  status, 
  origin, 
  date_greffe, 
  file_timestamp, 
  libelle_evt, 
  last_libele_evt, 
  status_admin, 
  type, 
  status_ets, 
  adress_reconstituee_inpi, 
  adress_regex_inpi, 
  adress_distance_inpi, 
  numero_voie_matching, 
  voie_clean, 
  voie_matching, 
  code_postal_matching, 
  ville_matching , 
  code_commune, 
  enseigne, 
  "date_début_activité", 
  csv_source 
FROM 
  ets_final_sql 
WHERE 
  status != 'IGNORE'
"""
    
    output = athena.run_query(
        query=query,
        database='inpi',
        s3_output='INPI/sql_output'
    )
```

```python
if csv_cree:
    table = 'ets_final_sql'
    source_key = "{}/{}.csv".format(
                            'INPI/sql_output',
                            output['QueryExecutionId']
                                   )

    destination_key = "{}/{}.csv".format(
                            'INPI/TC_1/02_preparation_donnee/ETS_SQL',
                            table
                        )
    results = s3.copy_object_s3(
                            source_key = source_key,
                            destination_key = destination_key,
                            remove = False
                        )
    
    s3.download_file(
    key= destination_key)
```

```python
filename = 'ets_final_sql'
shutil.move("{}.csv".format(filename),
            "data/input/INPI")
```

## Recuperation ETS INSEE

```python
csv_cree = True
if csv_cree:
    query = """SELECT 
  count_initial_insee, 
  insee_final_sql.siren, 
  siret, 
  datecreationetablissement, 
  etablissementsiege, 
  etatadministratifetablissement, 
  codepostaletablissement, 
  codecommuneetablissement, 
  libellecommuneetablissement, 
  ville_matching, 
  libellevoieetablissement, 
  complementadresseetablissement, 
  numerovoieetablissement, 
  indicerepetitionetablissement, 
  typevoieetablissement, 
  adress_reconstituee_insee, 
  enseigne1etablissement, 
  enseigne2etablissement, 
  enseigne3etablissement
FROM 
  insee_final_sql 
  INNER JOIN (
    select 
      distinct(siren) 
    FROM 
      ets_final_sql
  ) as ets ON insee_final_sql.siren = ets.siren
"""
    output = athena.run_query(
        query=query,
        database='inpi',
        s3_output='INPI/sql_output'
    )
```

```python
if csv_cree:
    table = 'insee_final_sql'
    source_key = "{}/{}.csv".format(
                            'INPI/sql_output',
                            output['QueryExecutionId']
                                   )

    destination_key = "{}/{}.csv".format(
                            'INPI/TC_1/02_preparation_donnee/INSEE_SQL',
                            table
                        )
    results = s3.copy_object_s3(
                            source_key = source_key,
                            destination_key = destination_key,
                            remove = False
                        )
    
    s3.download_file(
    key= destination_key)
```

```python
shutil.move("insee_final_sql.csv",
            "../data/input/INSEE")
```

Il faut prendre l'`origin` et `filename` que l'on souhaite sitetiser

```python
import glob
files = glob.glob('data/logs/InitialPartielEVTNEW/*')
for f in files:
    os.remove(f)
```

```python
##origin = "InitialPartielEVTNEW"
##filename = "ets_preparation_python_lib1" ####ETS
#filename = "inpi_initial_partiel_evt_new_ets_status_final_InitialPartielEVTNEW"
#origin = "NEW"
#filename = "inpi_initial_partiel_evt_new_ets_status_final_NEW"
#### make dir
#parent_dir = 'data/output/'
#parent_dir_1 = 'data/input/INPI/special_treatment/'
#parent_dir_2 = 'data/logs/'

#for d in [parent_dir,parent_dir_1,parent_dir_2]:
#    path = os.path.join(d, origin) 
#    try:
#        os.mkdir(path) 
#    except:
#        pass
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
def import_dask(file, usecols = None, dtype=None, parse_dates = False):
        """
        Import un fichier gzip ou csv en format Dask

        Deja dans preparation data

        Args:Merge
        - file: String, Path pour localiser le fichier, incluant le nom et
        l'extension
        - usecols: List: les noms des colonnes a importer. Par defaut, None
        - dtype: Dictionary: La clé indique le nom de la variable, la valeur
        indique le type de la variable
        - parse_dates: bool or list of int or names or list of lists or dict,
         default False
        """
        #extension = os.path.splitext(file)[1]
        if usecols == None:
            low_memory = False
        else:
            low_memory = True
        dd_df = dd.read_csv(file, usecols = usecols, dtype = dtype,
        blocksize=None, low_memory = low_memory,parse_dates = parse_dates)

        return dd_df
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
                .groupby('index_id')['index_id']
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
inpi_col = ["index_id",
            "siren",
            "code_greffe",
            "nom_greffe",
            "numero_gestion",
            "id_etablissement",
            "status",
            "origin",
            "date_greffe",
            "file_timestamp",
            "libelle_evt",
            "last_libele_evt", 
            "status_admin",
            "type",
            "status_ets",
            "adress_reconstituee_inpi",
            "adress_regex_inpi",
            "adress_distance_inpi",
            "numero_voie_matching",
            "voie_clean",
            "voie_matching",
            "code_postal_matching",
            "ville_matching",
            "code_commune",
            "enseigne",
            "date_début_activité",
            "csv_source"
            ]

#['count_initial_inpi', 'list_digit_inpi', 'pays', 'len_digit_address_inpi', 'nom_commercial', 'ville']

inpi_dtype = {
    "index_id":"object",
"siren":"object",
"code_greffe":"object",
"nom_greffe":"object",
"numero_gestion":"object",
"id_etablissement":"object",
"status":"object",
"origin":"object",
"date_greffe":"object",
"file_timestamp":"object",
"libelle_evt":"object",
"last_libele_evt":"object", 
"status_admin":"object",
"type":"object",
"status_ets":"object",
"adress_reconstituee_inpi":"object",
"adress_regex_inpi":"object",
"adress_distance_inpi":"object",
"numero_voie_matching":"object",
"voie_clean":"object",
"voie_matching":"object",
"code_postal_matching":"object",
"ville_matching":"object",
"code_commune":"object",
"enseigne":"object",
"date_début_activité":"object",
"csv_source":"object"
}

insee_col = [
    "count_initial_insee",
    "siren",
    "siret",
    "datecreationetablissement",
    "etablissementsiege",
    "etatadministratifetablissement",
    "codepostaletablissement",
    "codecommuneetablissement",
    "libellecommuneetablissement",
    "ville_matching",
    "libellevoieetablissement",
    "complementadresseetablissement",
    "numerovoieetablissement",
    "indicerepetitionetablissement",
    "typevoieetablissement",
    "adress_reconstituee_insee",
    "enseigne1etablissement",
    "enseigne2etablissement",
    "enseigne3etablissement" 
    
]

insee_dtype = {
             "count_initial_insee":"object",
"siren":"object",
"siret":"object",
"datecreationetablissement":"object",
"etablissementsiege":"object",
"etatadministratifetablissement":"object",
"codepostaletablissement":"object",
"codecommuneetablissement":"object",
"libellecommuneetablissement":"object",
"ville_matching":"object",
"libellevoieetablissement":"object",
"complementadresseetablissement":"object",
"numerovoieetablissement":"object",
"indicerepetitionetablissement":"object",
"typevoieetablissement":"object",
"adress_reconstituee_insee":"object",
"enseigne1etablissement":"object",
"enseigne2etablissement":"object",
"enseigne3etablissement" :"object"
         }
```

```python
reindex = ['index_id',
 'siren',
 'siret',
 'count_initial_insee',
 'date_début_activité',
 'datecreationetablissement',
 'code_greffe',
 'nom_greffe',
 'numero_gestion',
 'id_etablissement',
 'status',
 'origin',
 'date_greffe',
 'file_timestamp',
 'libelle_evt',
 'last_libele_evt',
 'status_admin',
 'etatadministratifetablissement',
 'type',
 'status_ets',
 'etablissementsiege',
 'numero_voie_matching',
 'numerovoieetablissement',
 'voie_clean',
 'voie_matching',
 'typevoieetablissement',
 'libellevoieetablissement',
 'complementadresseetablissement',
 'adress_reconstituee_inpi',
 'adress_reconstituee_insee',
 'adress_regex_inpi',
 'adress_distance_inpi',
 'test_address_regex',
 'jacquard',
 'edit',
 'min_jacquard',
 'min_edit',
 'test_jacquard_adress',
 'test_edit_adress',
 'test_distance_diff',
 'code_postal_matching',
 'codepostaletablissement',
 'ville_matching',
 'libellecommuneetablissement',
 'code_commune',
 'codecommuneetablissement',
 'enseigne',
 'enseigne1etablissement',
 'enseigne2etablissement',
 'enseigne3etablissement',
 'jacquard_enseigne1',
 'jacquard_enseigne2',
 'jacquard_enseigne3',
 'edit_enseigne1',
 'edit_enseigne2',
 'edit_enseigne3',
 'min_jacquard_enseigne1',
 'min_jacquard_enseigne2',
 'min_jacquard_enseigne3',
 'min_edit_enseigne1',
 'min_edit_enseigne2',
 'min_edit_enseigne3',
 'test_enseigne_insee',
 'test_enseigne_jacquard',
 'test_enseigne_edit',
 'csv_source',
 'indicerepetitionetablissement'
 ]
```

<!-- #region -->
# Processus de siretisation

## Variables utilisées pour les tests

Lors du processus de siretisation, nous avons besoin de créer de nouvelles variables, qui seront, faites lors de la préparation de la donnée. 

Les nouvelles variables sont les suivantes:

- `adress_regex_inpi`: Concatenation des champs de l'adresse, suppression des espaces, des articles et des numéros et ajout de `(?:^|(?<= ))(` et `)(?:(?= )|$)`
- `adress_distance_inpi`: Concatenation des champs de l'adresse, suppression des espaces et des articles
- `adress_reconstituee_insee`: Reconstitution de l'adresse à l'INSEE en utilisant le numéro de voie `numeroVoieEtablissement`, le type de voie non abbrégé, `voie_clean`, l'adresse `libelleVoieEtablissement` et le `complementAdresseEtablissement` et suppression des articles
- `enseigne`
- `enseigne1etablissement`
- `enseigne2etablissement`
- `enseigne3etablissement`


### variables nécéssaires aux tests

Les variables ci dessous sont des nouvelles variables résultant du merge entre les deux tables

- `test_address_regex`: 
    - Si un des mots contenus dans la variable `adress_regex_inpi` est présente dans la variable `adress_reconstituee_insee` alors True
- `jacquard`:
    - Calcul de la distance (dissimilarité) entre `adress_distance_inpi` et `adress_reconstituee_insee`
- `edit`:
    - Calcul de la distance (Levhenstein) entre `adress_distance_inpi` et `adress_reconstituee_insee`
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
    
# Processus

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

## Etape 2: Création variables tests

Dans cette étape, nous allons créer toutes les variables de test, comme évoqué précédement, a savoir sur l'adresse et l'enseigne.

## Etape 3: Dedoublonnage

Cette étape permet de dédoublonner les lignes matchées via la variable `index`. En effet, il est possible d'avoir des doublons lorsque l'entreprise à plusieurs établissements dans la même adresse. C'est le cas pour les sièges et principals.

On va appliquer le filtre sur l'ensemble de la table matchée, puis compter le nombre de siret par séquence. Si le nombre de siret est égal à 1, c'est un bon match, sinon, il y a encore des doublons même après le filtre. Nous allons appliquer un deuxième filtre sur les doublons puis concatener avec les séquences ayant 1 siret. Dès lors, on applique la fonction `split_duplication` pour séparer les doublons des valeurs uniques. Si il y a encore des doublons, il n'y a pas suffisamment d'information pour distinguer le bon siret. Il faudra prendre plus de précaution avec des séquences

Les règles sons les suivantes:

### Filtre 1

- Si `test_address_regex` est égal a True, ET `test_jacquard_adress` est égal à True, ET `test_edit_adress` est égal à True, ET `test_enseigne_edit` est égal a True OU `test_enseigne_jacquard` est égal a True OU `count_initial_insee` est égal à 1

### Filtre 2

Le filtre deux ne s'applique que sur les lignes dont la séquence a plus de deux sirets. Le filtre est le suivant 

- Si `jacquard` est egal 0 et `edit` est egal a 0, alors on garde. Autrement dit, on ne garde que les lignes dont l'adresse est correcte dans les deux cas. On pourrait potentiellement lever la contrainte.


## Etape 4: Récupération sequence dans table INPI

Dans cette étape, nous allons utiliser les siret que nous venons de récupérer et les matcher avec la table de l'INPI. Cela évite de refaire tous les tests sur des séquences dont on a déjà récupérer le siret.

Tout d'abord, nous devons récupérer les siret sur la séquence `siren`, `code_greffe`, `nom_greffe`, `numero_gestion`, et `id_etablissement`. Attention, il faut enlever les doublons du aux valeurs historiques, puis on merge avec la table de l'INSEE. 

## Etape 5: Concatenation des sequences

Maintenant que nous avons réussi a récuperer les siret dans la table INPI depuis les valeurs connues lors de nos tests, nous pouvons concatener les deux tables et ne prendre que les colonnes d'origines.

Il faut tout de même refaire la fonction `split_duplication` pour enlever les siret multiples
<!-- #endregion -->

```python
list_possibilities[0]['match']['inpi']
```

```python
list_possibilities[0]['match']['insee']
```

# Siretisation

```python
os.chdir('../')
#os.getcwd()
```

```python
for key, values in enumerate(list_possibilities[:1]):
    df_ets = 'data/input/INPI/ets_final_sql_{0}.csv'.format(key)
    #print(df_ets)
    
    ## Etape 1
    inpi = import_dask(file=df_ets,
                                usecols=inpi_col,
                                dtype=inpi_dtype,
                                parse_dates=False)
       
    insee = import_dask(
        file= 'data/input/INSEE/insee_final_sql.csv',
        usecols=insee_col,
        dtype=insee_dtype
)
    
    temp = (inpi
            .merge(insee,
                          how='inner',
                   #left_on = ['siren','code_postal_matching'],
                   #right_on = ['siren','codepostaletablissement']
                          left_on=list_possibilities[0]['match']['inpi'],
                          right_on= list_possibilities[0]['match']['insee']
                  )
           ).compute()
    
    ### Etape 2
    ## Test 1: address
    df_2 = dd.from_pandas(temp, npartitions=10)
    df_2['test_address_regex'] = df_2.map_partitions(
                lambda df:
                    df.apply(lambda x:
                        find_regex(
                         x['adress_regex_inpi'],
                         x['adress_reconstituee_insee']), axis=1)
                         ).compute()

    df_2['jacquard'] = df_2.map_partitions(
                lambda df:
                    df.apply(lambda x:
                        jackard_distance(
                         x['adress_distance_inpi'],
                         x['adress_reconstituee_insee']), axis=1)
                         ).compute()

    df_2['edit'] = df_2.map_partitions(
                lambda df:
                    df.apply(lambda x:
                        edit_distance(
                         x['adress_distance_inpi'],
                         x['adress_reconstituee_insee']), axis=1)
                         ).compute()
    
    df_2['jacquard_enseigne1'] = df_2.map_partitions(
            lambda df:
                df.apply(lambda x:
                    jackard_distance(
                     x['enseigne'],
                     x['enseigne1etablissement']), axis=1)
                     ).compute()
    df_2['jacquard_enseigne2'] = df_2.map_partitions(
                lambda df:
                    df.apply(lambda x:
                        jackard_distance(
                         x['enseigne'],
                         x['enseigne2etablissement']), axis=1)
                         ).compute()
    df_2['jacquard_enseigne3'] = df_2.map_partitions(
                lambda df:
                    df.apply(lambda x:
                        jackard_distance(
                         x['enseigne'],
                         x['enseigne3etablissement']), axis=1)
                         ).compute()

    df_2['edit_enseigne1'] = df_2.map_partitions(
                lambda df:
                    df.apply(lambda x:
                        edit_distance(
                         x['enseigne'],
                         x['enseigne1etablissement']), axis=1)
                         ).compute()
    df_2['edit_enseigne2'] = df_2.map_partitions(
                lambda df:
                    df.apply(lambda x:
                        edit_distance(
                         x['enseigne'],
                         x['enseigne2etablissement']), axis=1)
                         ).compute()
    df_2['edit_enseigne3'] = df_2.map_partitions(
                lambda df:
                    df.apply(lambda x:
                        edit_distance(
                         x['enseigne'],
                         x['enseigne3etablissement']), axis=1)
                         ).compute()

    df_2 = df_2.compute()

    df_2 = df_2.assign(
        min_jacquard = lambda x:
        x.groupby(['siren', 'code_greffe', 'nom_greffe', 'numero_gestion',
                   'id_etablissement'])['jacquard'].transform('min'),
        min_edit = lambda x:
        x.groupby(['siren', 'code_greffe', 'nom_greffe', 'numero_gestion',
                   'id_etablissement'])['edit'].transform('min'))

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
        (df_2['enseigne1etablissement'].isin([np.nan]))
        &(df_2['enseigne2etablissement'].isin([np.nan]))
        &(df_2['enseigne3etablissement'].isin([np.nan])),
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
    
    df_2 = df_2.reindex(columns = reindex)

    
    
```

```python
df_2.head(20)
```

```python
df_2.loc[lambda x: x['test_address_regex'].isin([False])].head(10)
```

Il est assez simple de voir que le merge a abouti a la création de doublon 


Nous allons appliquer des règles de gestion sur les combinaisons matchées (les `both`)


## Etape 2: Création variables tests

Dans cette étape, nous allons créer toutes les variables de test, comme évoqué précédement, a savoir sur l'adresse et l'enseigne.

```python
df_2.to_csv('temp.csv', index= False)#.loc[lambda x: x['siren'].isin(['400534020'])]
```

```python
df_2.head()
```

```python
### Nombre de duplicate
df_2.shape[0] -df_2['index_id'].nunique()
```

```python
df_2.stb.freq(['test_address_regex'])
```

```python
df_2.stb.freq(['test_jacquard_adress'])
```

```python
df_2.stb.freq(['test_edit_adress'])
```

```python
df_2.stb.freq(['test_distance_diff'])
```

```python
df_2.stb.freq(['test_enseigne_jacquard'])
```

```python
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
#df_2 = pd.read_csv('temp.csv', dtype = {'siren': 'O',
```

```python
sequence = ['siren', 'code_greffe', 'nom_greffe', 'numero_gestion', 'id_etablissement']
df_3 = (df_2.loc[
    
    lambda x: 
    (x['test_address_regex'].isin([True]))
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
          #.reindex(columns = reindex)
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
#df_3_bis['not_duplication'].columns
```

## Etape 4: Récupération sequence dans table INPI

Dans cette étape, nous allons utiliser les siret que nous venons de récupérer et les matcher avec la table de l'INPI. Cela évite de refaire tous les tests sur des séquences dont on a déjà récupérer le siret.

Tout d'abord, nous devons récupérer les siret sur la séquence `siren`, `code_greffe`, `nom_greffe`, `numero_gestion`, et `id_etablissement`. Attention, il faut enlever les doublons du aux valeurs historiques, puis on merge avec la table de l'INSEE. 

```python
seq_siret = ['siren', 'siret', 'code_greffe', 'nom_greffe', 'numero_gestion', 'id_etablissement']
df_3_bis['not_duplication'][seq_siret].drop_duplicates() 
```

### Recupération de l'historique

```python
#seq = ['siren','code_greffe', 'nom_greffe', 'numero_gestion', 'id_etablissement']
#columns_to_keep = ['siren', 'siret', 'code_greffe', 'nom_greffe', 'numero_gestion',
#       'id_etablissement', 'total_siret', 'origin', 'file_timestamp',
#       'date_greffe', 'libelle_evt', 'last_libele_evt', 'type', 'adress_new',
#       'adresse_new_clean_reg', 'voie_matching', 'numero_voie_matching',
#       'code_postal_matching', 'ncc', 'code_commune', 'enseigne',
#       'date_début_activité', 'index_id', 'status_admin', 'status_ets', '_merge']

df_match_2 = (
    (df_3_bis['not_duplication'][seq_siret + ['total_siret']]
 .drop_duplicates()  
    )
 .merge(inpi.compute().loc[lambda x: 
                                 ~x['index_id'].isin(df_3_bis['not_duplication']['index_id'])],
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
        df_3_bis['not_duplication']#.reindex(columns  = columns_to_keep)
        
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

## Creation CSV pour CACD2

Temporairement, on sauvergarde la table sequence + siret + index_id pour donner la table a CACD2

```python
df_final_no_duplicate.head()
```

```python
df_temp = (df_final_no_duplicate.reindex(columns = [
     'index_id','siren','siret','code_greffe', 'nom_greffe', 'numero_gestion', 'id_etablissement', 'date_greffe'
 ])
           .sort_values(by = 
['siren','siret','code_greffe', 'nom_greffe', 'numero_gestion', 'id_etablissement', 'date_greffe']
)
 .drop_duplicates(keep = 'last', subset = ['siren','siret','code_greffe', 'nom_greffe', 'numero_gestion',
                                           'id_etablissement'])
          )
```

```python
df_temp.dtypes
```

```python
#df_temp.loc[lambda x: x['siret'].isin(['82517574800011'])]
```

```python
df_temp.to_csv('temp.csv', index = False)
```

```python
s3.upload_file('temp.csv',
               'INPI/TC_1/03_siretisation/ETS_SIRET_TEMP')
```

```python
query_create = """
CREATE EXTERNAL TABLE IF NOT EXISTS {0}.{1} (
index_id string,
siren string, 
siret string,
code_greffe string,
nom_greffe string, 
numero_gestion string,
id_etablissement string, 
date_greffe string
 )
     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
   'separatorChar' = ',',
   'quoteChar' = '"'
   )
     LOCATION '{2}'
     TBLPROPERTIES ('has_encrypted_data'='false',
              'skip.header.line.count'='1');""".format(
    'inpi',
    'ets_sequence_siret',
    's3://calfdata/INPI/TC_1/03_siretisation/ETS_SIRET_TEMP'
)

output = athena.run_query(
                            query=query_create,
                            database='inpi',
                            s3_output='INPI/sql_output'
                        )
```

Créer la table et on l'envoie dans le S3

```python
query = """
SELECT 
  seq.siren, 
  siret, 
  seq.code_greffe, 
  seq.nom_greffe, 
  seq.numero_gestion, 
  seq.id_etablissement, 
  seq.date_greffe, 
  file_timestamp, 
  libelle_evt, 
  "siège_pm", 
  rcs_registre, 
  adresse_ligne1, 
  adresse_ligne2, 
  adresse_ligne3, 
  adress_reconstituee_inpi, 
  code_postal_matching, 
  ville, 
  code_commune, 
  pays, 
  domiciliataire_nom, 
  domiciliataire_siren, 
  domiciliataire_greffe, 
  "domiciliataire_complément", 
  "siege_domicile_représentant", 
  nom_commercial, 
  enseigne, 
  "activité_ambulante", 
  "activité_saisonnière", 
  "activité_non_sédentaire", 
  "date_début_activité", 
  "activité", 
  origine_fonds, 
  origine_fonds_info, 
  type_exploitation 
FROM 
  ets_sequence_siret 
  INNER JOIN (
    SELECT 
      CAST(index_id AS varchar) as index_id, 
      siren, 
      code_greffe, 
      nom_greffe, 
      numero_gestion, 
      id_etablissement, 
      date_greffe, 
      file_timestamp, 
      libelle_evt, 
      "siège_pm", 
      rcs_registre, 
      adresse_ligne1, 
      adresse_ligne2, 
      adresse_ligne3, 
      adress_reconstituee_inpi, 
      code_postal_matching, 
      ville, 
      code_commune, 
      pays, 
      domiciliataire_nom, 
      domiciliataire_siren, 
      domiciliataire_greffe, 
      "domiciliataire_complément", 
      "siege_domicile_représentant", 
      nom_commercial, 
      enseigne, 
      "activité_ambulante", 
      "activité_saisonnière", 
      "activité_non_sédentaire", 
      "date_début_activité", 
      "activité", 
      origine_fonds, 
      origine_fonds_info, 
      type_exploitation 
    FROM 
      "inpi"."ets_final_sql"
  ) as seq ON ets_sequence_siret.index_id = seq.index_id 
  AND ets_sequence_siret.siren = seq.siren
"""
output = athena.run_query(
                            query=query,
                            database='inpi',
                            s3_output='INPI/sql_output'
                        )
```

```python
name_s3 = 'ETS_SIRET_INPI'
source_key = "{}/{}.csv".format('INPI/sql_output',
                               output['QueryExecutionId']
                               )
destination_key = "{}/{}.csv".format("INPI/TC_1/03_siretisation/ETS_SIRET_TEMP_CAC2D",
                                     name_s3
                                         )

results = s3.copy_object_s3(source_key = source_key,
             destination_key = destination_key,
             remove = False
                      )
```

## Etape 6: Ecarte les séquences trouvées de la table INPI

La dernière étape consiste a enlever les index des séquences siretisées de la table de l'INPI. On va sauvegarder la nouvelle table de l'INPI mais aussi, la table que a servi a trouver le siret, et la table siretiser.

On sauvegarde aussi un table de log

```python
df_a_trouver = inpi.compute().loc[lambda x: ~x['index_id'].isin(df_final['index_id'].values)]
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
