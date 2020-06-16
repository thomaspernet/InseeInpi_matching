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

# Normaliser la variable Pays

```
Entant que {X} je souhaite {normaliser la variable pays} afin de {pouvoir la faire correspondre à l'INSEE}
```

# Contexte

Nous pouvons considérer une nouvelle variable de matching afin de faire correspondre les deux tables sources, INSEE et INPI. Dans les US précédents, nous avons normalisé la ville et le code postal. Dans cette US, nous allons nous interessé au pays. Cette variable ne fait pas exception, aucune normalisation n'est appliquée alors que l'INSEE affiche toujours la même rigueur. A vrai dire, il y a 107 valeurs possibles à l'INPI à l'heure ou nous écrivons l'US. L'INSEE recence précisément 286.

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

avec potentiellement la variable pays que l'on va normaliser dans cette US.

Workflow US (via stock)

![workflow](https://www.lucidchart.com/publicSegments/view/d9e4494d-bfaf-4d0e-9e0f-53011cda7eb9/image.png)

# US / ISSUES liées

[PO & DEV : s'il existe des références, les inscrire]

# Besoin

Dans cette US, le besoin est le suivant:

- Mettre en conformité la variable pays de l'INPI




# Spécifications

Pour récupérer les codes pays de l'INSEE, il faut se rendre sur cet [URL](https://www.insee.fr/fr/information/2028273). Pour récupérer les codes, nous pouvons télécharger la data directement sur le site de l'[INSEE](https://www.insee.fr/fr/information/3720946#titre-bloc-21):

Source GitLab:

- PDF: [Codification des pays et territoires étrangers _ Insee](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/data/input/RawParameters/Codification%20des%20pays%20et%C2%A0territoires%20%C3%A9trangers%20_%20Insee.pdf)

## Input

[PO : dans le cas de transformation de données, préciser ,les sources :

*   Applications
*   Schémas
*   Tables: `inpi_etablissement_historique`
* CSV: https://www.insee.fr/fr/information/3720946#titre-bloc-21
*   Champs: `pays`




### Exemple codification pays INSEE

Ci dessous, un apperçu des codes pays à l'INSEE

```python
country_code = 'https://www.insee.fr/fr/statistiques/fichier/' \
'3720946/pays2019-csv.zip'
```

```python
import pandas as pd
import numpy as np
```

```python
country_insee = pd.read_csv(country_code, sep = ",")

```

```python
print(country_insee.head().to_markdown())
```

### Exemple de possibilité INPI

Ci dessous, un apperçu des codes pays à l'INPI

```python
country_inpi = 'https://scm.saas.cagip.group.gca/PERNETTH/' \
'inseeinpi_matching/raw/master/Notebooks_matching/Data_preprocessed/' \
'programme_matching/data/input/RawParameters/test_inpi_country.csv'
country_inpi = pd.read_csv(country_inpi)
```

```python
print(country_inpi.head().to_markdown())
```

## Output

[PO : dans le cas de transformation de données, préciser les sorties :

*   BDD cibles
*   Tables: `inpi_etablissement_historique`
*   Champs: `pays_matching`

]

La table ci-dessous donne un apperçu de l'output résultant du merge entre la codification des pays à l'INSEE et l'INPI. Notre variable d'intérêt est `pays_matching`, qui est le code de référence à l'INSEE.

Nous le verrons dans la partie d'après mais un nettoyage avant le merge est nécéssaire.

```python
pd.set_option('display.max_rows', None)
```

```python
regex = r"[^\w\s]|\([^()]*\)+"

country_insee = (country_insee
                 .assign(pays_regex = lambda x:
    x['libcog']
 .str.upper()
 .drop_duplicates()
 .str.replace(" ","")
 .str.normalize('NFKD') ### Supprimer accents
 .str.encode('ascii', errors='ignore')
 .str.decode('utf-8')
 .str.replace(regex, '')
 .sort_values()
)
)
```

```python
regex = r"[^\w\s]|\([^()]*\)+"

match = (country_inpi
 .apply(lambda x:
         x
 .str.upper()
 .drop_duplicates()
 .str.replace(" ","")
 .str.normalize('NFKD') ### Supprimer accents
 .str.encode('ascii', errors='ignore')
 .str.decode('utf-8')
 .str.replace(regex, '')
 .str.replace('ETATSUNISDAMERIQUE', 'ETATSUNIS')
 .str.replace('BURKINAFASO', 'BURKINA')
        .str.replace('CONGOKINSHASAREPDEMODUCONGO', 'CONGOREPUBLIQUEDEMOCRATIQUE')
        .str.replace('COREEDUSUD', 'COREEREPUBLIQUEDE')
        .str.replace('ILEMAURICE', 'MAURICE')
        .str.replace('IRLANDE', 'IRLANDEOUEIRE')
        .str.replace('REPUBLIQUETCHEQUE', 'TCHEQUEREPUBLIQUE')
        .str.replace('SWAZILAND', 'ESWATINI')
 .drop_duplicates()
        )
.merge(country_insee[['pays_regex', 'cog']], 
        how = 'left', 
        left_on = 'pays', 
        right_on = 'pays_regex', 
        indicator = True)
 .loc[lambda x: x['_merge'].isin(['both']) & ~x['pays'].isin([np.nan])]
 .rename(columns = {"cog":'pays_matching'})
)
print(match.head().to_markdown())
```

```
 (country_inpi
 .assign(pays_regex = lambda x:
         x['pays']
 .str.upper()
 #.drop_duplicates()
 .str.replace(" ","")
 .str.normalize('NFKD') ### Supprimer accents
 .str.encode('ascii', errors='ignore')
 .str.decode('utf-8')
 .str.replace(regex, '')
 .str.replace('ETATSUNISDAMERIQUE', 'ETATSUNIS')
 .str.replace('BURKINAFASO', 'BURKINA')
        .str.replace('CONGOKINSHASAREPDEMODUCONGO', 'CONGOREPUBLIQUEDEMOCRATIQUE')
        .str.replace('COREEDUSUD', 'COREEREPUBLIQUEDE')
        .str.replace('ILEMAURICE', 'MAURICE')
        .str.replace('IRLANDE', 'IRLANDEOUEIRE')
        .str.replace('REPUBLIQUETCHEQUE', 'TCHEQUEREPUBLIQUE')
        .str.replace('SWAZILAND', 'ESWATINI')
 #.drop_duplicates()
        )
.merge(country_insee[['pays_regex', 'cog']], 
        how = 'left', 
        #left_on = 'pays', 
        #right_on = 'pays_regex', 
        indicator = True)
 .loc[lambda x: x['_merge'].isin(['both']) & ~x['pays'].isin([np.nan])]
 .rename(columns = {"cog":'pays_matching'})
.to_csv('pays_insee_inpi.csv', index = False)
)

CREATE EXTERNAL TABLE IF NOT EXISTS inpi.pays_insee_inpi (
  `pays` string,
  `pays_regex` string,
  `pays_matching` string,
  `merge` string 
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
) LOCATION 's3://calfdata/INPI/TC_1/02_preparation_donnee/intermediate_file/pays/'
TBLPROPERTIES ('has_encrypted_data'='false');


<!-- #region -->
## Règles de gestion applicables

[PO : Formules applicables]

# Charges de l'équipe

[

PO : Si des étapes particulières / des points d'attention sont attendus, être aussi explicite que possible

Spécifiquement pour l'intégration de nouvelles données dans DATUM :

*   Nombre de lignes chargées pour chaque nouvelle table
*   Poids de chaque nouvelle table
*   Durée du traitement ajouté (+ durée avant et après)

]

La variable pays_matching correspond au code pays normalisée du pays à l'INSEE.

### Regex nettoyage

**INPI**

[Snippet Regex INPI](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/snippets/59)

1. Mettre en lettre majuscule le string
2. Suppression des doublons
3. Enlever les espaces
4. Extraction des accents
5. Extraction pattern regex:
    - caractères spéciaux
    - Espace debut de string
    - Parenthèse
6. Mise en forme des pays INPI - INSEE dont on sait la différence:
    - ETATSUNISDAMERIQUE vers ETATSUNIS
    - BURKINAFASO vers BURKINA
    - CONGOKINSHASAREPDEMODUCONGO vers CONGOREPUBLIQUEDEMOCRATIQUE
    - COREEDUSUD vers COREEREPUBLIQUEDE
    - ILEMAURICE vers MAURICE
    - IRLANDE vers IRLANDEOUEIRE
    - REPUBLIQUETCHEQUE vers TCHEQUEREPUBLIQUE
    - SWAZILAND vers ESWATINI

**INSEE**

[Snippet Regex INSEE](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/snippets/58)

1. Mettre en lettre majuscule le string
2. Suppression des doublons
3. Enlever les espaces
4. Extraction des accents
5. Extraction pattern regex:
    - caractères spéciaux
    - Espace debut de string
    - Parenthèse


# Tests d'acceptance

[PO : comment contrôler que la réalisation est conforme]

- Compter le nombre de pays: normalement environ 79
- Verifiez que la variable `pays_matching` à une seule 
- Récuperer le pourcentage de ligne par pays à l'inpi. Utiliser la variable `pays_matching`. Doit correspondre plus ou moins au tableau ci-dessous.

```
SELECT pays_matching,pays_regex, count(*) as count
FROM 
(SELECT pays_matching, pays_regex
 FROM initial_partiel_evt_new_ets_status_final 
LEFT JOIN pays_insee_inpi 
ON initial_partiel_evt_new_ets_status_final.pays = pays_insee_inpi.pays
)
GROUP BY pays_matching, pays_regex
ORDER BY count
```


<!-- #endregion -->

```python
count_pays= "https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/raw/" \
"master/US_Datum/00_Preparation_data/Acceptance_test/pays_inpi_count.csv"
print(pd.read_csv(count_pays).sort_values(by = 'count', 
                                          ascending = False).to_markdown())
```
