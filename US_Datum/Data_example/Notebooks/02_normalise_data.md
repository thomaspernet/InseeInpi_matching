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

# Exemple Prepare ETS Data : normalize address

La préparation de la donnée se fait en deux étapes.

1. Préparation de l'INPI
2. Préparation de l'INSEE

L'étape 1 va mettre en conformité la data de l'INPI en vue d'une siretisation. L'étape 2 va utiliser les siren présents lors de l'étape 1 pour ne préparer que ce sous ensemble dans la donnée de l'INSEE.

Pour la présentation de la création de la donnée, nous allons utiliser une donnée synthétique qui est un sous ensemble de la table finale (US [2264](https://tree.taiga.io/project/olivierlubet-air/us/2464)). La donnée est disponible dans le Gitlab [initial_partiel_evt_new_pp_status_final_exemple.csv](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/data/RawData/INPI/Stock/initial_partiel_evt_new_pp_status_final_exemple.csv)

# Introduction

Avant de commencer, il faut rappeler pourquoi nous avons besoin de ses nouvelles variables. La finalité de la table inpi_etablissement_historique  est de pouvoir faire le rapprochement avec les établissements à l’INSEE. Ce rapprochement va permettre de récupérer le numéro SIRET de l’établissement qui figure à l’INSEE mais pas à l’INPI.

L’étape de sirétisation repose sur un algorithme assez simple qui cherche a matcher des variables communes dans les deux bases puis vérifie la conformité du matching.

L'algorithme de SIRETISATION fonctionne avec l'aide de trois fonctions:

* [step_one](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/tree/master/Notebooks_matching/Data_preprocessed/programme_matching/02_siretisation#step-one) : permet d'écarter les doublons du merge et d'appliquer les premières règles afin de connaitre l'origine de la siretisation
* [step_two_assess_test](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/tree/master/Notebooks_matching/Data_preprocessed/programme_matching/02_siretisation#step_two_assess_test) : détermine l'origine du matching, a savoir la date, adresse, voie, numéro de voie
* [step_two_duplication](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/tree/master/Notebooks_matching/Data_preprocessed/programme_matching/02_siretisation#step_two_duplication) : permet de récupérer des SIRET sur les doublons émanant du merge avec l'INSEE

L'algorithme va utiliser séquentiellement les variables suivantes:

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
 
Chacune des variables ci dessus proviennent de l’INPI, et sont disponibles a l’INSEE sous les noms suivants:

| Source | Method        | Preparation                 | URL                         | INPI_INSEE_equiv            | Detail création                 |
|--------|---------------|-----------------------------|-----------------------------|-----------------------------|---------------------------------|
| INSEE  | normalisation | libelleCommuneEtablissement | [libelleCommuneEtablissement](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/inpi_insee/preparation_data.py#L708) | ncc                         |                                 |
| INPI   | Creation      | ncc                         | [ncc](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/inpi_insee/preparation_data.py#L131)                         | libelleCommuneEtablissement | Detail preparation siretisation |
| INPI   | Creation      | adresse_new_clean_reg       | [adresse_new_clean_reg](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/inpi_insee/preparation_data.py#L311)       | libelleVoieEtablissement    | Detail preparation siretisation |
| INPI   | Creation      | digit_inpi                  | [digit_inpi](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/inpi_insee/preparation_data.py#L315)                  | numeroVoieEtablissement     | Detail preparation siretisation |
| INPI   | Creation      | INSEE                       | [INSEE](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/inpi_insee/preparation_data.py#L325)                       | typeVoieEtablissement       | Detail preparation siretisation |
| INPI   | normalisation | code_commune                |                             | codeCommuneEtablissement    |                                 |
| INPI   | normalisation | code_postal                 |                             | codePostalEtablissement     |                                 |

## Detail


Nous allons préciser les étapes a suivre pour créer chacune des variables suivantes:

* ncc
* code_postal
* code_commune
* INSEE
* digit_inpi
* adresse_new_clean_reg
* Pays

```python
import os, shutil
os.chdir('../')
current_dir = os.getcwd()
from inpi_insee import preparation_data
import pandas as pd
%load_ext autoreload
%autoreload 2
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

stopword ='https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/raw' \
'/master/Notebooks_matching/Data_preprocessed/programme_matching/data/input' \
'/Parameters/upper_stop.csv'

param = {
    'communes_insee': commune,
    'upper_word':stopword,
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

La variable ncc  correspond a la valeur normalisée du libellé de la commune. La même variable à l’INSEE s’appelle libelleCommuneEtablissement .

La création de cette variable s’opère en 2 étapes:
1. Normalisation du fichier des communes de France
  1. Input [CSV] `communes-01012019.csv` & ville  [Champs]
    1. Fichier source: [Code officiel géographique au 1er janvier 2019 et au 1er avril 2019 | Insee](https://www.insee.fr/fr/information/3720946) → [communes-01012019.csv](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/data/input/RawParameters/communes-01012019.csv) (Gitlab)
    2. Code normalisation: [possibilités communes](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/data/Source_intermediates.md#cr%C3%A9er-fichier-toutes-les-possibilit%C3%A9s-communes)
  2. Output [CSV]
    1.  [communes_france.csv](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/data/input/Parameters/communes_france.csv) (Gitlab)
2. Ajout ncc dans la table historique
  1. input [Table] : `inpi_etablissement_historique_` 
    1. A noter que dans notre exemple, la source est un csv , [initial_partiel_evt_new_pp_status_final_exemple.csv](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/data/RawData/INPI/Stock/initial_partiel_evt_new_pp_status_final_exemple.csv)
    2. Code préparation: [Ligne 131](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/inpi_insee/preparation_data.py#L131)
  2. Output [Champs]
    1. ncc provenant de  [communes_france.csv](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/data/input/Parameters/communes_france.csv) (Gitlab)

```python
pd.set_option('display.max_columns', None)
```

```python
df_inpi = pd.read_csv(param['inpi_etb'])
df_inpi.head()
```

```python
prep_data.clean_commune(df_inpi).head(20)[['siren','ville', 'ncc']]
```

## Creation adresse_new_clean_reg, digit_inpi & INSEE

### Creation adresse_new_clean_reg

La variable `adresse_new_clean_reg` correspond a un pattern regex englobant les différentes formes courantes de l’adresse à l’INPI. La variable de recherche à l’INSEE s’appelle `libelleVoieEtablissement`.

La création de cette variable s’opère en une seule étape, via un fichier intermédiaire, appelé upper_stop.csv. Ce fichier contient des termes a ignorer lors du parsing de l’adresse

1. Création du pattern regex via la concaténation des variables de l’adresse
  1. Input [CSV]: [upper_stop.csv](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/data/input/Parameters/upper_stop.csv)
  2. input [Table] : `inpi_etablissement_historique_` & [Champs] `adresse_ligne_1`, `adresse_ligne_2`, & `adresse_ligne_3`  
    1. A noter que dans notre exemple, la source est un csv , [initial_partiel_evt_new_pp_status_final_exemple.csv](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/data/RawData/INPI/Stock/initial_partiel_evt_new_pp_status_final_exemple.csv)
    2. Code préparation: [Ligne 270](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/inpi_insee/preparation_data.py#L270)
  3. Output [Champs]
    1. `adress_new_clean_reg`

### Creation digit_inpi

La variable digit_inpi correspond au numéro de la voie, si applicable. La variable de recherche à l’INSEE s’appelle `numeroVoieEtablissement`. 

La création de cette variable s’opère en une seule étape, via le champs créé précédemment, adress_new 

1. Extraction du premier digit de l’adresse reconstitué
  1. input [Table] : `inpi_etablissement_historique_` & [Champs] `adress_new` 
    1. A noter que dans notre exemple, la source est un csv , [initial_partiel_evt_new_pp_status_final_exemple.csv](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/data/RawData/INPI/Stock/initial_partiel_evt_new_pp_status_final_exemple.csv) 
    2. Code préparation: [Ligne 315](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/inpi_insee/preparation_data.py#L315)
  2. Output [Champs]
    1. `digit_inpi`
    
### Creation INSEE

La variable INSEE correspond a la valeur normalisé du type de voie. La même variable à l’INSEE s’appelle `typeVoieEtablissement`.

La création de cette variable s’opère en 2 étapes:

1. Normalisation du fichier des communes de France
  1. Input [CSV]
    1. Fichier source: [Liste des variables - sirene.fr](https://www.sirene.fr/sirene/public/variable/libelleVoieEtablissement) →  [libelleVoieEtablissement.csv](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/data/input/RawParameters/libelleVoieEtablissement.csv) (Gitlab)
    2. Code normalisation: [Libellé voie](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/data/Source_intermediates.md#creation-libellevoieetablissement)
  2. Output [CSV]
    1.  [voie.csv](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/data/input/Parameters/voie.csv) (Gitlab)
2. Ajout INSEE dans la table historique
  1. input [Table] : `inpi_etablissement_historique_` 
    1. A noter que dans notre exemple, la source est un csv , [initial_partiel_evt_new_pp_status_final_exemple.csv](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/data/RawData/INPI/Stock/initial_partiel_evt_new_pp_status_final_exemple.csv) 
    2. Code préparation: [Ligne 325](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/inpi_insee/preparation_data.py#L325)
  2. Output [Champs]
    1. `INSEE` provenant de [libelleVoieEtablissement.csv](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/data/input/RawParameters/libelleVoieEtablissement.csv) (Gitlab)

```python
pd.read_csv(param['upper_word']).head()
```

```python
pd.read_csv(param['voie']).head()
```

```python
prep_data.prepare_adress(df_inpi).head(20)[['siren',
                                          'adresse_ligne1',
                                          'adresse_ligne2',
                                          'adresse_ligne3',
                                          'adress_new',
                                          'adresse_new_clean_reg',  ## Target
                                          'digit_inpi',  ## Target
                                          'INSEE', ## Target
                                          'possibilite']]
```

```python
prep_data.prepare_adress(df_inpi).head(1)[
                                          'adresse_new_clean_reg']
```
