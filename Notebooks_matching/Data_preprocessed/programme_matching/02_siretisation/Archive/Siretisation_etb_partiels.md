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

<!-- #region Collapsed="false" -->
# Programme de Matching

Regarder si l'établissement est fermé ou pas

## Moteur de recherche TEST

* Insee
  * http://avis-situation-sirene.insee.fr/IdentificationListeSiret.action
* INPI/TC
  * https://data.inpi.fr/
* Infogreffe
  * https://www.infogreffe.fr/

# Presentation Algorithme

# Class preparation Data

Le coude source est disponible [ici](https://github.com/thomaspernet/InseeInpi_matching/blob/dev_thomas/Notebooks_matching/programme_matching/inpi_insee/preparation_data.py) et le notebook pour lancer le programme est disponible [ici](https://github.com/thomaspernet/InseeInpi_matching/blob/dev_thomas/Notebooks_matching/programme_matching/Preparation_data.ipynb)

## INSEE

Les données sources de l'INSEE proviennent de [Data Gouv](https://www.data.gouv.fr/fr/datasets/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret/)

- communes_insee:
    - Le fichier source pour les communes se trouvent à cette [URL](https://www.insee.fr/fr/information/3720946)
    - Le notebook pour reconstituer le csv est disponible a cette [URL](https://github.com/thomaspernet/InseeInpi_matching/blob/dev_thomas/Notebooks_matching/programme_matching/Source_intermediates.ipynb). ⚠️ Repo privé + branche
- voie:
    - Le fichier source pour les communes se trouvent à cette [URL](https://www.sirene.fr/sirene/public/variable/libelleVoieEtablissement)
    - Le notebook pour reconstituer le csv est disponible a cette [URL](https://github.com/thomaspernet/InseeInpi_matching/blob/dev_thomas/Notebooks_matching/programme_matching/Source_intermediates.ipynb). ⚠️ Repo privé + branche
- upper_word:
    - La liste des upper word (stop word capitalisé) provient de la librarie [NLTK](https://www.nltk.org/) avec un ajout manuel.
    
## INPI 

Les données de l'INPI proviennent de ses différents Notebooks:

- [inpi_etb](https://github.com/thomaspernet/InseeInpi_matching/blob/dev_thomas/Notebooks_matching/programme_matching/Source_intermediates.ipynb)

## Normalisation du fichier INPI.

Le fichier INPI doit contenir un seul fichier gz avant d'être ingéré par le programme. Le fichier va être importé dans un format Dask, ce qui permet de paralléliser les calcules et bien sur d'éviter les problèmes de mémoire. 

La normalisation du fichier de l'INPI se fait en plusieurs étapes:

1) Exclusion des observations contenant des NaN pour chacune des variables candidates, à savoir:

    - Adresse_Ligne1
    - Adresse_Ligne2
    - Adresse_Ligne3
    - Code_Postal
    - Ville
    - Code_Commune
2) Extraction des SIREN a SIRETISER -> cela evite d'utiliser toute la base INSEE pour la sirétisation. I.e Speedup le process
3) Calcule du nombre de SIRET par SIREN via la fonction `nombre_siret_siren`
4) Normalisation de la variable commune via la fonction `clean_commune`

    - Extraction des digits dans la ville. En effet, certaines communes incluent l'arrondissement dans la variable.
    - Extraction des caractères spéciaux et espaces
    - Capitalisation du nom de la commune
    - Matching avec le fichier commune pour avoir le nom de la commune de l'INSEE.
5) Préparation de l'adresse via la fonction `prepare_adress`
    - Concatenation des variables `Adresse_Ligne1` + `Adresse_Ligne2` + `Adresse_Ligne3`
    
    - Normalisation de la variable concatenée -> Extraction des caractères speciaux, espace, digit puis capitalisation
    - Extraction de tous les stop words du fichier `upper_word`
    - Split de chaque mot restant de l'adresse 
    - Creation du regex de la forme suivante:  `MOT1$|MOT2$` 
    - Extration des digits:
        - Première variable avec le premier digit
        - Seconde variable avec une liste de digit et jointure -> DIGIT1|DIGIT2
    - Merge avec le fichier `voie` pour obtenir le type de voie de l'INSEE
    - Calcule du nombre de digit dans l'adresse
        - Si len inférieure a 2, alors NaN. C'est une variable utlisée pendant le matching des règles spéciales
    - Creation d'une variable `index` correspondant à l'index du dataframe. Indispensable
 
Le fichier est sauvegardé en format gz, et dans un table SQL

    - inpi_etb_stock_0.gz
    - inpi_origine.db
    
Un appercu de la table est disponible via cette application `App_inpi`.

## Normalisation du fichier INSEE

Pour l'étape de siretisation, les variables candidates sont les suivantes:

- 'siren',
- 'siret',
- "etablissementSiege",
- "etatAdministratifEtablissement",
- "numeroVoieEtablissement",
- "indiceRepetitionEtablissement",
- "typeVoieEtablissement",
- "libelleVoieEtablissement",
- "complementAdresseEtablissement",
- "codeCommuneEtablissement",
- "libelleCommuneEtablissement",
- "codePostalEtablissement",
- "codeCedexEtablissement",
- "libelleCedexEtablissement",
- "distributionSpecialeEtablissement",
- "libelleCommuneEtrangerEtablissement",
- "codePaysEtrangerEtablissement",
- "libellePaysEtrangerEtablissement",
- "dateCreationEtablissement"

Comme pour le fichier de l'INPI, le fichier csv est importé en Dask Dataframe. Les étapes sont les suivantes:

1) Filtre les SIREN à sirétiser uniquement

2) Filtre la date limite à l'INSEE. Cette étape sert essentiellement pour siretiser les bases de stocks. Cela évite d'utiliser des valeurs "dans le future" -> inconnu à l'INPI

3) Remplacement des "-" par des " " dans la variable `libelleCommuneEtablissement`

4) Extraction des digits en format liste de la variable `libelleVoieEtablissement`

5) Calcule du nombre de SIRET par SIREN

6) Calcule du nombre de digit dans la variable `libelleCommuneEtablissement`

    - Si len inférieure a 2, alors NaN. C'est une variable utlisée pendant le matching des règles spéciales
    
Le fichier est sauvegardé en format gz, et dans un table SQL

    - insee_2017_SIZE.gz
    - App_insee.db
    
Un appercu de la table est disponible via cette application `App_insee`.

# Algorithme Siretisation

Le code source est disponible [ici](https://github.com/thomaspernet/InseeInpi_matching/blob/dev_thomas/Notebooks_matching/programme_matching/inpi_insee/siretisation.py) et le notebook pour lancer le programme est disponible [ici](https://github.com/thomaspernet/InseeInpi_matching/blob/dev_thomas/Notebooks_matching/programme_matching/Siretisation.ipynb)

L'algorithme de SIRETISATION fonctionne avec l'aidre de trois fonctions:

- `step_one`: permet d'écarter les doublons du merge et d'appliquer les premières règles afin de connaitre l'origine de la siretisation
- `step_two_assess_test`: détermine l'origine du matching, a savoir la date, adresse, voie, numéro de voie
- `step_two_duplication`: permet de récuperer des SIRET sur les doublons émanant du merge avec l'INSEE

Dans premier temps, on créer un dictionnaire avec toutes les variables de matching. Toutefois, l'algorithme va utiliser séquentiellement les variables suivantes:

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

Pour connaitre l'ensemble des variables de matching INSEE/INPI, veuillez vous rendre en [annexe](#annexe).

Dans la mesure ou l'algorithme fonctionne de manière séquentielle, et utilise comme input un fichier de l'INPI a siretiser. De fait, après chaque séquence, l'algorithme sauvegarde un fichier gz contenant les siren a trouver. Cette étape de sauvegarde en gz permet de loader le fichier gz en input en Dataframe Dask.

## Step One

La première étape de la séquence est l'ingestion d'un fichier gz contenant les SIREN a trouver. L'ingestion va se faire en convertissant le dataframe en Dask. L'algorithme tout d'abord utiliser la fonction `step_one` et produit deux dataframes selon si le matching avec l'INSEE a débouté sur des doublons ou non. 

Les doublons sont générés si pour un même nombre de variables de matching, il existe plusieurs possibilités à l'INSEE. Par exemple, pour un siren, ville, adressse donnée, il y a plusieurs possibilité. Cela constitue un double et il sera traiter ultérieurement, dans la mesure du possible. 

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

Deux dataframe sont crées, un ne contenant pas de doublon pas les doublons et un deuxième contenant les doublon. L'algorithme va réaliser les tests sur le premier et faire d'avantage de recherche sur le second

## step_two_assess_test

Le premier dataframe ne contient pas de doublon, il est donc possible de réaliser différents tests afin de mieux déterminer
l'origine du matching. Plus précisement, si le matching a pu se faire sur la date, l'adresse, la voie, numéro de voie et le nombre unique d'index. Les règles sont définies ci-dessous.

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

Un premier fichier gz est enregistré contenant les "pure matches"

## step_two_duplication

Les second dataframe contient les doublons obtenus après le matching avec l'INSEE. L'algorithme va travailler sur différentes variables de manière séquencielle pour tenter de trouver les bons siret. Plus précisément, 3 variables qui ont été récemment créé sont utilisées:

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
 
On peut sauvegarder le `df_not_duplicate` et le restant en tant que `special_treatment`


![](https://www.lucidchart.com/publicSegments/view/5a8cb28f-dc42-4708-babd-423962514878/image.png)


<!-- #endregion -->

<!-- #region Collapsed="false" -->
## Algorithme
<!-- #endregion -->

```python Collapsed="false"
import os
os.chdir('../')
current_dir = os.getcwd()
from inpi_insee import siretisation

%load_ext autoreload
%autoreload 2

param = {
    'insee': r'data\input\INSEE\insee_2017_7627977.gz'
}
# 4824158 SIREN a trouver!
al_siret = siretisation.siretisation_inpi(param)
```

```python Collapsed="false"
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

```python Collapsed="false"
list_inpi = ['ncc','code_postal','code_commune','insee','digit_inpi']
list_insee = ['libelleCommuneEtablissement',
            'codePostalEtablissement', 'codeCommuneEtablissement',
            'typeVoieEtablissement','numeroVoieEtablissement']

sort_list = [
 {'ncc', 'code_postal', 'code_commune', 'insee', 'digit_inpi'},
 {'ncc', 'code_postal', 'code_commune', 'insee'},
 {'ncc', 'code_postal', 'code_commune', 'digit_inpi'},
 {'ncc', 'code_postal', 'code_commune'},   
 {'ncc', 'code_postal'},
 {'ncc'},
 {'code_postal'},
 {'code_commune'}
]
len(sort_list)
```

```python Collapsed="false"
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
filename = 'inpi_stock_partiel'
```

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
s3.download_file(
    key= 'INPI/TC_1/02_preparation_donnee/Stock/ETB/{}_0.gz'.format(filename))
```

```python
import shutil
try:
    os.remove(r"data\input\INPI\{}_0.gz'.format(filename))
except:
    pass
shutil.move(r"{}_0.gz".format(filename),
            r"data\input\INPI")
```

```python
list(pd.read_csv( r'data\input\INPI\{}_{}.gz'.format(filename, key)))
```

```python Collapsed="false"
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
            'insee',
            'date_début_activité',
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
    'insee': 'object',
    'date_début_activité': 'object',
    'digit_inpi': 'object',
    'len_digit_address_inpi':'object'
}

for key, values in enumerate(list_possibilities):
    df_ets = r'data\input\INPI\{}_{}.gz'.format(filename, key)

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

    pure_match.to_csv(r'data\output\{}_{}_pure_match.gz'.format(key, filename),
                      compression='gzip', index= False)
    # Step 2: duplication
    df_not_duplicate, sp = al_siret.step_two_duplication(df_duplication,
                                                        var_group = 
                                                         values['match']['inpi'])
    
    (df_not_duplicate
        .to_csv(r'data\output\{}_{}_not_duplicate.gz'.format(key, filename),
                compression='gzip', index= False))

    (sp.to_csv(
        r'data\input\INPI\special_treatment\{}_{}_special_treatment.gz'.format(
        key, filename),compression='gzip', index= False))

    # Input -> Save for the next loop 
    inpi.loc[
        (~inpi['index'].isin(pure_match['index'].unique()))
        & (~inpi['index'].isin(df_not_duplicate['index'].unique()))
        & (~inpi['index'].isin(sp['index'].unique()))
    ].compute().to_csv(r'data\input\INPI\{}_{}.gz'.format(filename, key+1),
                       compression='gzip', index= False)

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

    with open(r'data\logs\{}_{}_logs.json'.format(key,filename), 'w') as f:
        json.dump(dic_, f)
```

## AWS Service

```python

```

<!-- #region Collapsed="false" -->
# Special Treatment

exemple:

- 752085324
- 342122546: libelle type dans l'adresse complementaire a l'insee
<!-- #endregion -->

<!-- #region Collapsed="false" -->
### Recuperation via list_digit_ INPI/INSEE
<!-- #endregion -->

```python Collapsed="false"
import pandas as pd
```

```python Collapsed="false"
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

```python Collapsed="false"
test = pd.read_csv(
        r'data\input\INPI\special_treatment\{}_special_treatment.gz'.format(1),
                   low_memory=False)
```

```python Collapsed="false"
test['siren'].nunique()
```

```python Collapsed="false"
97*97
```

```python Collapsed="false"
test.loc[lambda x: 
         x['siren'].isin(['752085324'])
        & (x['list_digit_inpi'] == x['list_digit_insee'])
        ][['list_digit_inpi', 'list_digit_insee', 'siret']]
```

```python Collapsed="false"
#.assign(special_digit = lambda x:x['libelleVoieEtablissement'].str.findall(r"(\d+)").apply(
#        lambda x:'&'.join([i for i in x])))
```
