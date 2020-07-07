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

# Exemple Prepare ETS Data : Normalisation Ville

La préparation de la donnée se fait en deux étapes.

1. Préparation de l'INPI
2. Préparation de l'INSEE

L'étape 1 va mettre en conformité la data de l'INPI en vue d'une siretisation. L'étape 2 va utiliser les siren présents lors de l'étape 1 pour ne préparer que ce sous ensemble dans la donnée de l'INSEE.

Pour la présentation de la création de la donnée, nous allons utiliser une donnée synthétique qui est l'extraction de toutes les possibilités uniques de dénomination de ville à l'INPI (US [2264](https://tree.taiga.io/project/olivierlubet-air/us/2464)). La donnée est disponible dans le Gitlab [ville_inpi.csv](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/raw/master/Notebooks_matching/Data_preprocessed/programme_matching/data/input/RawParameters/ville_inpi.csv)

# Introduction

Avant de commencer, il faut rappeler pourquoi nous avons besoin de ses nouvelles variables. La finalité de la table inpi_etablissement_historique  est de pouvoir faire le rapprochement avec les établissements à l’INSEE. Ce rapprochement va permettre de récupérer le numéro SIRET de l’établissement qui figure à l’INSEE mais pas à l’INPI.

L’étape de sirétisation repose sur un algorithme assez simple qui cherche a matcher des variables communes dans les deux bases puis vérifie la conformité du matching.

L'algorithme de SIRETISATION fonctionne avec l'aide de trois fonctions:

* [step_one](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/tree/master/Notebooks_matching/Data_preprocessed/programme_matching/02_siretisation#step-one) : permet d'écarter les doublons du merge et d'appliquer les premières règles afin de connaitre l'origine de la siretisation
* [step_two_assess_test](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/tree/master/Notebooks_matching/Data_preprocessed/programme_matching/02_siretisation#step_two_assess_test) : détermine l'origine du matching, a savoir la date, adresse, voie, numéro de voie
* [step_two_duplication](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/tree/master/Notebooks_matching/Data_preprocessed/programme_matching/02_siretisation#step_two_duplication) : permet de récupérer des SIRET sur les doublons émanant du merge avec l'INSEE

L'algorithme va utiliser séquentiellement les variables suivantes, en plus du siren:

```
 {'ville_matching', 'Code_Postal', 'Code_Commune', 'INSEE', 'digit_inpi'},
 {'ville_matching', 'Code_Postal', 'Code_Commune', 'INSEE'},
 {'ville_matching', 'Code_Postal', 'Code_Commune', 'digit_inpi'},
 {'ville_matching', 'Code_Postal', 'Code_Commune'},   
 {'ville_matching', 'Code_Postal'},
 {'ville_matching'},
 {'Code_Postal'},
 {'Code_Commune'}
 ```
 
Chacune des variables ci dessus proviennent de l’INPI, et sont disponibles a l’INSEE sous les noms suivants:

| Source | Method        | Preparation                 | URL                         | INPI_INSEE_equiv            | Detail création                 |
|--------|---------------|-----------------------------|-----------------------------|-----------------------------|---------------------------------|
| INSEE  | normalisation | libelleCommuneEtablissement | [libelleCommuneEtablissement](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/inpi_insee/preparation_data.py#L708) | ville_matching                         |                                 |
| INPI   | Creation      | ville_matching                         | [ncc](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/inpi_insee/preparation_data.py#L131)                         | libelleCommuneEtablissement | Detail preparation siretisation |
| INPI   | Creation      | adresse_new_clean_reg       | [adresse_new_clean_reg](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/inpi_insee/preparation_data.py#L311)       | libelleVoieEtablissement    | Detail preparation siretisation |
| INPI   | Creation      | digit_inpi                  | [digit_inpi](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/inpi_insee/preparation_data.py#L315)                  | numeroVoieEtablissement     | Detail preparation siretisation |
| INPI   | Creation      | INSEE                       | [INSEE](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/inpi_insee/preparation_data.py#L325)                       | typeVoieEtablissement       | Detail preparation siretisation |
| INPI   | normalisation | code_commune                |                             | codeCommuneEtablissement    |                                 |
| INPI   | normalisation | code_postal                 |                             | codePostalEtablissement     |                                 |
| INSEE   | normalisation | ville_matching                 |                             | ville_matching     |                                 |



```python
import os, shutil
os.chdir('../')
current_dir = os.getcwd()
#from inpi_insee import preparation_data
import pandas as pd
%load_ext autoreload
%autoreload 2
```

```python
inpi_ville = 'https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/raw' \
'/master/Notebooks_matching/Data_preprocessed/programme_matching/data/input' \
'/RawParameters/ville_inpi.csv'

path_commune = 'https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/raw' \
'/master/Notebooks_matching/Data_preprocessed/programme_matching/data/input' \
'/RawParameters/communes-01012019.csv'

param = {
    #'communes_insee': commune,
    #'upper_word':stopword,
    # "voie": voie,
    'insee_ville':  path_commune,
   # 'inpi_etb': etb_ex,
    'inpi_ville':inpi_ville,
    #'date_end':"2020-01-01"
}
#prep_data = preparation_data.preparation(param)
```

La technique de normalisation a été proposé par Jonathan Collet. L'idée est de nettoyer suiffisant les villes à l'INPI afin de les faire correspondre à l'INSEE. La technique utilise un trick, qui est d'enlever tous les espaces dans les deux tables afin d'avoir une "clé" unique. Par exemple, la ville suivante à l'INPI est `Soisy-sur-Seine`, alors qu'à l'INSEE, c'est `SOISY SUR SEINE`. En nettoyant la ville à l'INPI et en enlevant les espaces dans les deux champs, on peut créer une clé unique pour faire le matching, donnant lieu à la valeur suivante: `SOISYSURSEINE`


# Step by step approach


<!-- #region -->
## Creation ville_matching

 
La variable `ville_matching` correspond a la valeur normalisée du libellé de la commune. La même variable sera a créer a l'INSEE, appelé aussi `ville_matching`.

La création de cette variable s’opère en 1 étape:

1. Ajout `ville_matching` dans la table historique
  1. input [Table] : `inpi_etablissement_historique_` 
    2. Code préparation: [Jupyter notebook 00_prep_ville_matching](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/US_Datum/Data_example/Notebooks/00_prep_ville_matching.md#regex-nettoyage)
  2. Output [Champs]
    1. `ville_matching`
<!-- #endregion -->

```python
pd.set_option('display.max_columns', None)
```

```python
inpi = pd.read_csv(param['inpi_ville'])
inpi.shape
```

### Regex nettoyage

Le regex opère de manière séquentiel:

1. Extraction des accents
2. Extraction des digits
3. Mettre en lettre majuscule le string
4. Extraction de "LA", "LES" et "LE" (methode imparfaite actuellement)
5. Normalisation "ST", "ST " à "SAINT"
6. Normalisation "S", "S/ " à "SUR"
7. Extraction pattern regex:
    1. charactère sepciaux
    2. Espace debut de string
    3. Parenthèse
    4. ER ARRONDISSEMENT, E ARRONDISSEMENT
    5. SUR
    6. CEDEX
    7. Digit
    8. espace
8. Remplacement 'MARSEILLEE' à "MARSEILLE"
    - Ceci est du au pattern suivant:
        - MARSEILLE (15E)
        - Regex enlève "(", ")"," " et "15"
            - output: "MARSEILLEE"
            
le code sous forme de notebook est disponible dans le Gitlab: [Jupyter notebook 00_prep_ville_matching](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/US_Datum/Data_example/Notebooks/00_prep_ville_matching.md#regex-nettoyage)

```python
regex = r"[^\w\s]|\([^()]*\)|ER ARRONDISSEMENT|E ARRONDISSEMENT|" \
"|^SUR$|CEDEX|[0-9]+|\s+"
test = (inpi
 .assign(
ville_matching =lambda x: x['ville']
     .str.normalize('NFKD') ### Supprimer accents
     .str.encode('ascii', errors='ignore')
     .str.decode('utf-8')
     .str.replace("^\d+\s|\s\d+\s|\s\d+$", '') #### digit
     .str.upper() ### Majuscule
     .str.replace("^LA\s+|^LES\s+|^LE\s+", '') #### Pas de LE/LA/LES a l'INSEE
     .str.replace('^ST$|^ST\s+', 'SAINT')  #### Normalise SAINT
     .str.replace('^S$|S/', 'SUR')  #### Normaliser SUR
     .str.replace(regex, '') ### pattern regex
     .str.replace('MARSEILLEE', 'MARSEILLE') #### Probleme dans le regex, donc modification temporaire
 )
 )
test.loc[lambda x: x['ville_matching'].isin(['MARSEILLE'])][
    ['ville','ville_matching']]
```

```python
#FRÉJUS/ST AYGULF
```

```python
test.loc[lambda x: x['ville_matching'].isin(['FREJUSSAINTAYGULF'])]
```

```python

```

### Test acceptance

Pour vérifier si le nettoyage est plus ou moins correcte, nous pouvons faire les tests suivants:

1. Compter le nombre de villes unique à L'INPI.Au moment du test, il y avait 76545 patterns uniques a l'INPI
2. Compter le nombre uniques de patterns uniques à l'INPI. Au moment du test, il y avait 38802 patterns uniques a l'INPI
3. Utiliser le fichier des communes à l'INSEE, [Code officiel géographique au 1er janvier 2019 et au 1er avril 2019 | Insee](https://www.insee.fr/fr/information/3720946), et le merger avec toutes les valeurs uniques possibles de l'INPI. Lors du test, nous avons matché 90% des valeurs à l'INPI, soit 76543 observations, laissant de coté 8796 valeurs possibles. Lors de nos tests, nous avons stocké un fichier Excel avec les villes non matchées. Il est disponible à l'adresse [suivante](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/US_Datum/Data_example/ville_non_matchees.xlsx). Un tel fichier peut être créer afin d'améliorer les règles de gestion dans l'avenir. 


1. Compter le nombre uniques de ville à l'INPI

```python
test['ville'].shape[0]
```

2. Compter le nombre uniques de patterns uniques à l'INPI

```python
test['ville_matching'].nunique()
```

2. Test match INSEE et INPI 

Pour réaliser ce test, il est possible de récupérer les valeurs uniques possibles de `ville_matching` à l'INPI, puis d'utiliser le csv [communes-01012019.csv](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/raw/master/Notebooks_matching/Data_preprocessed/programme_matching/data/input/RawParameters/communes-01012019.csv) et d'appliquer le pattern regex et enlever les espaces. Dès que la variable est prête à l'INSEE, un simple right_join (INSEE -> INPI), en gardant les valeurs non matchées permet de vérifier l'exactitude du matching. 

```python

insee = pd.read_csv(param['insee_ville'])
```

```python
test_merge = (insee
 .assign(ncc = lambda x: x['ncc'].str.replace(r'\s+', '').str.replace(regex, ''))       
 .merge(test, how = 'right', indicator = True, left_on = 'ncc', 
        right_on = 'ville_matching')
)

pd.concat([
    test_merge['_merge'].value_counts().rename('count'),
    test_merge['_merge'].value_counts(normalize = True).rename('pct')
], axis = 1)

```

```python
test_merge.loc[lambda x: x['ville_matching'].isin(['FREJUSSAINTAYGULF'])]
```

```python

```

Exemple avec la ville `Soisy-sur-Seine`

```python
insee.loc[lambda x: x['nccenr'].isin(['Soisy-sur-Seine'])]
```

```python
insee.loc[lambda x: x['nccenr'].isin(['Fréjus'])]
```

```python
test_merge.loc[lambda x: x['ville'].isin(['Soisy-sur-Seine'])]
```

Sauvegarde des non matchées dans un fichier Excel, appellé, "ville_non_matchees.xlsx".

```python
(test_merge
 .loc[lambda x: x['_merge'].isin(['right_only'])]
 .drop_duplicates(subset = ["ville_matching"])[['ville', 'ville_matching']]
 .sort_values(by = 'ville')
 .to_excel('ville_non_matchees.xlsx')
)
```

## Test Distance based metrics

Nous allons tester 3 algorithmes différents:

- Edit distance: similarity
- Jaccards: Dissimilarity
- Tokenization: Cleaning + Jaccards

```python
import nltk
```

```python
test.head()
```

```python
insee.head()
```

```python
def edit_distance(city_inpi, cities_insee):
    """
    Compute and sort the lowest distance (edit distance based metrics)
    
    Args:
    city_inpi: str. A city from INPI dataframe
    cities_insee: list of string: Cities from INSEE dataframe
    
    return 
    """
    i = 0
    length  = len(cities_insee)
    list_distance = []
    while i < length:
        d = nltk.edit_distance(
                city_inpi,
                cities_insee[i]
            )
        if d == 0:
            #distance = (
            #    city_inpi,
            #    cities_insee[i]
            #)
            break
            #return distance
        elif d == 1:
            distance = (
                city_inpi,
                cities_insee[i],
                d
            )
            list_distance.append(distance)
        i += 1 
    
    if d ==0:
        return cities_insee[i]
    else:
        #return sorted(
        return list_distance
        #,key=lambda x: x[-1]
    #)

```

```python
edit_distance(city_inpi = test['ville_matching'].iloc[0],
              cities_insee = insee['ncc'].to_list())
```

```python
cities_insee = (insee
 .assign(ncc = lambda x:
         x['ncc'].str.replace(r'\s+', '').str.replace(regex, '')
        )
 ['ncc'].to_list()
)
```

```python
test.iloc[:10, 1].apply(lambda x: 
                       edit_distance(city_inpi = x,
              cities_insee = cities_insee
                                    )
                       )
```

```python
non_match = (test_merge
 .loc[lambda x: x['_merge'].isin(['right_only'])]
 .drop_duplicates(subset = ["ville_matching"])[['ville', 'ville_matching']]
 .sort_values(by = 'ville')
)
```

```python
edit_distance(city_inpi = non_match['ville_matching'].iloc[3],
              cities_insee = cities_insee)
```

```python
non_match#.iloc[:10, 1]
```

```python
non_match = non_match.assign(
    test_dist = lambda x: 
    x['ville_matching'].apply(
        lambda x: 
                       edit_distance(city_inpi = x,
              cities_insee = cities_insee
                                    )
                       )
)
```

```python
non_match.iloc[:10, 1].apply(lambda x: 
                       edit_distance(city_inpi = x,
              cities_insee = cities_insee
                                    )
                       )
```
