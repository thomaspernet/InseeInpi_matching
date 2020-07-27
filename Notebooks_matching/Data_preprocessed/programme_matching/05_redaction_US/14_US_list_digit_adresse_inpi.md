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

<!-- #region -->
# Création list digit [INPI] 

```
Entant que {X} je souhaite {récuperer une liste de numéros présent dans les champs de l'adresse de l'INPI} afin de {pouvoir comparer cette même liste du coté de l'INSEE}
```

**Metadatab**

- Taiga:
    - Numero US: [3004](https://tree.taiga.io/project/olivierlubet-air/us/3004)
- Gitlab
    - Notebook: [15_US_list_digit_n_distance_adresse_insee](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/05_redaction_US/15_US_list_digit_n_distance_adresse_insee.md)
    - Markdown: [15_US_list_digit_n_distance_adresse_insee](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/05_redaction_US/15_US_list_digit_n_distance_adresse_insee.ipynb)
    - Data:
        - []()
        - 

# Contexte

Une adresse à l'INPI est formée de trois variables `adresse_1`, `adresse_2` et `adresse_3`. Chacun des champs peut contenir un ou plusieurs numéros. Dans un précédent US, nous avons décider de récupérer le premier numéro pour le faire correspondre à avec le numéro de voie de l'INSEE. Dans certains cas, cette règle de va pas fonctionner. Par exemple, une adresse peut être composée de 2 numéros si elle a deux entrées. Pour faire face a ce type de situation, nous allons créer une liste de numéro depuis les champs de l'adresse. 

## Règles de gestion

*   Définition partiel

    *   si csv dans le dossier Stock, année > 2017, alors partiel, c'est a dire, modification complète du dossier due a une anomalie.
    *   la date d’ingestion est indiquée dans le path, ie comme les flux
*   Une séquence est un classement chronologique pour le quadruplet suivant:

    *   _siren_ + _code greffe_ + _numero gestion_ + _ID établissement_
*   Une création d'une séquence peut avoir plusieurs transmission a des intervalles plus ou moins long

    *   Si plusieurs transmissions avec le libellé “création établissement” ou “création" , alors il faut prendre la dernière date de transmission
    *   Il y a certains cas ou les lignes de créations doublons sont de faux événements (mauvais envoie de la part du greffier)
        *   Si le timestamp entre la première ligne et dernière ligne est supérieures a 31 jour (exclut), il faut:
            *   Récupération de la dernière ligne, et créer une variable flag, comme pour le statut
*   Evénement 1

    *   Les événements doivent impérativement suivre l'ordre d'apparition dans le csv du FTP et de la numérotation des fichiers: 8, 9 et 10 pour les établissements
        *   Pour les événements, il est possible d'avoir plusieurs informations renseignées pour une même date de transmission pour une même séquence
    *   Le remplissage doit se faire de la manière suivante pour la donnée brute
        *   Pour une date de transmission donnée, c'est la dernière ligne de la séquence qui doit être utilisée, remplie des valeurs manquantes extraites des lignes précédentes. Si la dernière ligne de la séquence contient un champs non vide, il ne faut pas la remplacer par la ligne précédente.
*   Partiel

    *   En cas de corrections majeures, la séquence annule et remplace la création et événements antérieurs. Dans ce cas, toutes les données qui ont pu être transmises antérieurement via le stock initial ou le flux doivent donc être ignorées (prendre en compte la date de transmission indiquée dans le nom des sous-répertoires du stock et des fichiers
*   Siren sans Siège ou Principal

*   Il est possible qu'un SIREN n'ai pas de siege/principal. Normalement, cela doit être corrigé par un partiel

*   Etablissement sans création

*   Il arrive que des établissements soient supprimés (EVT) mais n'ont pas de ligne "création d'entreprise". Si cela, arrive, Infogreffe doit envoyer un partiel pour corriger. Il arrive que le greffe envoie seulement une ligne pour SEP, lorsque le Principal est fermé, le siège est toujours ouvert. Mais pas de nouvelle ligne dans la base. Le partiel devrait corriger cela.

*   La variable `ville` de l'INPI n'est pas normalisée. C'est une variable libre de la créativité du greffier, qui doit être formalisée du mieux possible afin de permettre la jointure avec l'INSEE. Plusieurs règles regex ont été recensé comme la soustraction des numéros, caractères spéciaux, parenthèses, etc. Il est possible d'améliorer les règles si nécessaire

*   Le code postal doit être formalisé correctement, a savoir deux longueurs possibles: zero (Null) ou cinq. Dans certains cas, le code postal se trouve dans la variable de la ville.

*   La variable pays doit être formalisée, a savoir correspondre au code pays de l'INSEE. Bien que la majeure partie des valeurs soit FRANCE ou France, il convient de normaliser la variable pour récuperer les informations des pays hors France.

*   Les variables de l'adresse de l'INPI ne sont pas normalisées, et ne peuvent être utilisées en l'état. Il est donc indispensable de retravailler les variables adresse pour pouvoir les comparer avec l'INSEE. Nous utilisons une règle (pattern) regex pour vérifier si les mots contenus dans l'adresse de l'INPI sont aussi contenus à l'INSEE.

*   [NEW] L'INSEE codifie le type de voie de la manière suivante:

    *   Si le type de voie est d'une longueur inférieure ou égale à 4 caractères, le type de voie n'est pas abrégé. Ainsi, RUE ou QUAI sont écrits tels quels, alors que AVENUE est abrégée en AV.
    *   La codification de l'INSEE va donc être utilisé ppur créer la variable `voie_matching`
*   [NEW] Pour ne pas complexifié le processus de siretisation, seule le premier numéro de voie contenu dans l'adresse nettoyée est extraite pour ensuite etre matché avec l'INSEE.

Workflow US (via stock)

![workflow](https://www.lucidchart.com/publicSegments/view/d9e4494d-bfaf-4d0e-9e0f-53011cda7eb9/image.png)

# Ensemble des variables INSEE/INPI

La table ci dessous récapitule l’ensemble des variables a créer pour siretiser la table des établissements

| Tables | Variables                          | Commentaire                                                                                                                                                                                                        | Bullet_inputs                                                                                                                 | Bullet_point_regex                                                  | US_md                                                          | query_md_gitlab                                                                                                                                                                                                                                                                                        |
|--------|------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------|----------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| INPI   | sequence_id                        | ID unique pour la séquence suivante: siren + code greffe + nom greffe + numero gestion +ID établissement                                                                                                           | siren code_greffe nom_greffe numero_gestion id_etablissement                                                                  |                                                                     | [2976](https://tree.taiga.io/project/olivierlubet-air/us/2976) | [create-id-and-id-sequence](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/01_Athena_concatenate_ETS.md#create-id-and-id-sequence)                                                                    |
| INPI   | adresse_reconstituee_inpi          | Concatenation des champs de l'adresse et suppression des espace                                                                                                                                                    | adresse_ligne1 adresse_ligne2 adresse_ligne3                                                                                  | debut/fin espace espace accent Upper                                | [2690](https://tree.taiga.io/project/olivierlubet-air/us/2690) | [adress_reconsitituee_inpi](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/03_ETS_add_variables.md#adress_reconsitituee_inpi)                                                                         |
| INPI   | adresse_distance_inpi              | Concatenation des champs de l'adresse, suppression des espaces et des articles. Utilisé pour calculer le score permettant de distinguer la similarité/dissimilarité entre deux adresses (INPI vs INSEE)            | adresse_ligne1 adresse_ligne2 adresse_ligne3                                                                                  | article digit debut/fin espace espace accent Upper                  | [2949](https://tree.taiga.io/project/olivierlubet-air/us/2949) | [adresse_distance_inpi](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/03_ETS_add_variables.md#adresse_distance_inpi)                                                                                 |
| INPI   | adresse_regex_inpi                 | Concatenation des champs de l'adresse, suppression des espaces, des articles et des numéros et ajout de (?:^\|(?<= ))( et )(?:(?= )\|$)                                                                            | adresse_ligne1 adresse_ligne2 adresse_ligne3                                                                                  | article digit debut/fin espace espace accent Upper                  | [2690](https://tree.taiga.io/project/olivierlubet-air/us/2690) | [adress_regex_inpi](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/03_ETS_add_variables.md#adress_regex_inpi)                                                                                         |
| INPI   | ville_matching                     | Nettoyage regex de la ville et suppression des espaces. La même logique de nettoyage est appliquée coté INSEE                                                                                                      | ville                                                                                                                         | article digit debut/fin espace espace accent Upper Regles_speciales | [2613](https://tree.taiga.io/project/olivierlubet-air/us/2613) | [etape-1-pr%C3%A9paration-ville_matching](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/03_ETS_add_variables.md#etape-1-pr%C3%A9paration-ville_matching)                                             |
| INPI   | voie_clean                         | Extraction du type de voie contenu dans l’adresse. Variable pivot servant à reprendre l’abrevation du type de voie comme à l’INSEE                                                                                 | adresse_reconstituee_inpi                                                                                                     | Regles_speciales                                                    | [2697](https://tree.taiga.io/project/olivierlubet-air/us/2697) | [voie_matching](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/03_ETS_add_variables.md#voie_matching)                                                                                                 |
| INPI   | type_voie_matching                 | Extration du type de voie dans l'adresse et match avec abbrévation type de voie de l'INSEE                                                                                                                         | voie_clean                                                                                                                    |                                                                     | [2697](https://tree.taiga.io/project/olivierlubet-air/us/2697) | [voie_matching](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/03_ETS_add_variables.md#voie_matching)                                                                                                 |
| INPI   | numero_voie_matching               | Extraction du premier numéro de voie dans l'adresse. Besoin table externe (type_voie) pour créer la variable                                                                                                       | adresse_reconstituee_inpi                                                                                                     | digit                                                               | [2697](https://tree.taiga.io/project/olivierlubet-air/us/2697) | [numero_voie_matching](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/03_ETS_add_variables.md#numero_voie_matching)                                                                                   |
| INPI   | list_numero_voie_matching_inpi     | Liste contenant tous les numéros de l'adresse dans l'INPI                                                                                                                                                          | adresse_ligne1 adresse_ligne2 adresse_ligne3                                                                                  | digit debut/fin espace espace                                       | [A CREER](A CREER)                                             | [etape-5-creation-liste-num%C3%A9ro-de-voie](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/03_ETS_add_variables.md#etape-5-creation-liste-num%C3%A9ro-de-voie)                                       |
| INPI   | last_libele_evt                    | Extraction du dernier libellé de l'événement connu pour une séquence, et appliquer cette information à l'ensemble de la séquence                                                                                   | libelle_evt                                                                                                                   |                                                                     | [2950](https://tree.taiga.io/project/olivierlubet-air/us/2950) | [etape-4-cr%C3%A9ation-last_libele_evt-status_admin-status_ets](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/03_ETS_add_variables.md#etape-4-cr%C3%A9ation-last_libele_evt-status_admin-status_ets) |
| INPI   | status_admin                       | Informe du status ouvert/fermé concernant une séquence                                                                                                                                                             | last_libele_evt                                                                                                               | Regles_speciales                                                    | [2951](https://tree.taiga.io/project/olivierlubet-air/us/2951) | [etape-4-cr%C3%A9ation-last_libele_evt-status_admin-status_ets](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/03_ETS_add_variables.md#etape-4-cr%C3%A9ation-last_libele_evt-status_admin-status_ets) |
| INPI   | status_ets                         | Informe du type d'établissement (SIE/PRI/SEC) concernant une séquence                                                                                                                                              | type                                                                                                                          | Regles_speciales                                                    | [2951](https://tree.taiga.io/project/olivierlubet-air/us/2951) | [etape-4-cr%C3%A9ation-last_libele_evt-status_admin-status_ets](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/03_ETS_add_variables.md#etape-4-cr%C3%A9ation-last_libele_evt-status_admin-status_ets) |
| INSEE  | voie_clean                         | Extraction du type de voie contenu dans l’adresse. Variable type voie nom complet. Exemple, l'INSEE indique CH, pour chemin, il faut donc indiquer CHEMIN. Besoin table externe (type_voie) pour créer la variable |                                                                                                                               |                                                                     | [2953](https://tree.taiga.io/project/olivierlubet-air/us/2953) | [etape-1-pr%C3%A9paration-voie_clean](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/04_ETS_add_variables_insee.md#etape-1-pr%C3%A9paration-voie_clean)                                               |
| INSEE  | indiceRepetitionEtablissement_full | Récupération du nom complet des indices de répétion; par exemple B devient BIS, T devient TER                                                                                                                      | indiceRepetitionEtablissement                                                                                                 | Regles_speciales                                                    | [2953](https://tree.taiga.io/project/olivierlubet-air/us/2953) | []()                                                                                                                                                                                                                                                                                                   |
| INSEE  | adresse_reconstituee_insee         | Concatenation des champs de l'adresse et suppression des espace                                                                                                                                                    | numeroVoieEtablissement indiceRepetitionEtablissement_full voie_clean libelleVoieEtablissement complementAdresseEtablissement | debut/fin espace espace Upper                                       | [2954](https://tree.taiga.io/project/olivierlubet-air/us/2954) | [etape-2-preparation-adress_reconstituee_insee](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/04_ETS_add_variables_insee.md#etape-2-preparation-adress_reconstituee_insee)                           |
| INSEE  | adresse_distance_insee             | Concatenation des champs de l'adresse, suppression des espaces et des articles. Utilisé pour calculer le score permettant de distinguer la similarité/dissimilarité entre deux adresses (INPI vs INSEE)            | numeroVoieEtablissement indiceRepetitionEtablissement_full voie_clean libelleVoieEtablissement complementAdresseEtablissement | article digit debut/fin espace espace Upper                         | []()                                                           | [etape-3-adresse_distance_insee](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/04_ETS_add_variables_insee.md#etape-3-adresse_distance_insee)                                                         |
| INSEE  | list_numero_voie_matching_insee    | Liste contenant tous les numéros de l'adresse dans l'INSEE                                                                                                                                                         | numeroVoieEtablissement indiceRepetitionEtablissement_full voie_clean libelleVoieEtablissement complementAdresseEtablissement | article digit debut/fin espace                                      | []()                                                           | [etape-4-creation-liste-num%C3%A9ro-de-voie](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/04_ETS_add_variables_insee.md#etape-4-creation-liste-num%C3%A9ro-de-voie)                                 |
| INSEE  | ville_matching                     | Nettoyage regex de la ville et suppression des espaces. La même logique de nettoyage est appliquée coté INPI                                                                                                       | libelleCommuneEtablissement                                                                                                   | article digit debut/fin espace espace Regles_speciales              | [2954](https://tree.taiga.io/project/olivierlubet-air/us/2954) | [etape-2-cr%C3%A9ation-ville_matching](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/04_ETS_add_variables_insee.md#etape-2-cr%C3%A9ation-ville_matching)                                             |
| INSEE  | count_initial_insee                | Compte du nombre de siret (établissement) par siren (entreprise)                                                                                                                                                   | siren                                                                                                                         |                                                                     | [2955](https://tree.taiga.io/project/olivierlubet-air/us/2955) | [etape-5-count_initial_insee](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/04_ETS_add_variables_insee.md#etape-5-count_initial_insee)                                                               |



# US / ISSUES liées

[PO & DEV : s'il existe des références, les inscrire]

# Besoin

Dans cette US, le besoin est le suivant:

- créer une variable appelée `list_numero_voie_matching_inpi` qui correspond à une liste contenant l'ensemble des numéros présent dans les champs de l'adresse

<!-- #endregion -->

# Spécifications

### Origine information (si applicable) 

- Metadata:
    - Type
    - Source
    - Summary
    
## Input

[PO : dans le cas de transformation de données, préciser ,les sources :

*   Applications
*   Schémas
*   Tables: `inpi_etablissement_historique`
*   CSV: 
*   Champs: 
    - `adresse_ligne1`
    - `adresse_ligne2`
    - `adresse_ligne3`




### Exemple Input 1

Exemple avec un seul numéro présent dans les trois champs

| adresse_ligne1 | adresse_ligne2                 | adresse_ligne3 | adresse_reconstituee_inpi             |
|----------------|--------------------------------|----------------|---------------------------------------|
|                | 16 RUE DU GENERAL LECLERC      | VANNES         | 16 RUE DU GENERAL LECLERC VANNES      |
|                | 42 RUE DU MENE                 | VANNES         | 42 RUE DU MENE VANNES                 |
|                | 12 RUE DU CAPITAINE LABORDETTE | VANNES         | 12 RUE DU CAPITAINE LABORDETTE VANNES |
|                | 5 RUE SAINT VINCENT            | VANNES         | 5 RUE SAINT VINCENT VANNES            |
|                | 2 rue Aimé Jéglot              |                | 2 RUE AIME JEGLOT                     |


### Exemple Input 2

Exemple avec deux ou plus de numéros dans les trois champs

| adresse_ligne1                         | adresse_ligne2                     | adresse_ligne3 | adresse_reconstituee_inpi                 |
|----------------------------------------|------------------------------------|----------------|-------------------------------------------|
|                                        | 17/19 boulevard du Général Leclerc |                | 17 19 BOULEVARD DU GENERAL LECLERC        |
|                                        | 1 - 5 rue Talleyrand               |                | 1 5 RUE TALLEYRAND                        |
| 3-5 rue Paul Duez                      |                                    |                | 3 5 RUE PAUL DUEZ                         |
| 6 Rue du Pelvoux - Immeuble la Vanoise | 6-18                               |                | 6 RUE DU PELVOUX IMMEUBLE LA VANOISE 6 18 |
| 350 rue Arthur Brunet                  | BP 50017                           |                | 350 RUE ARTHUR BRUNET BP 50017            |


## Output

[PO : dans le cas de transformation de données, préciser les sorties :

*   BDD cibles
*   Tables: `inpi_etablissement_historique`
*   Champs: 
    - `list_numero_voie_matching_inpi`

]

La variable `list_numero_voie_matching_inpi` est un array contenant tous les numéros.

### Exemple Output 1

Exemple avec un seul numéro présent dans les trois champs

| adresse_ligne1                    | adresse_ligne2  | adresse_ligne3 | adresse_reconstituee_inpi         | list_numero_voie_matching_inpi |
|-----------------------------------|-----------------|----------------|-----------------------------------|--------------------------------|
| 66 avenue du Général de Gaulle    |                 |                | 66 AVENUE DU GENERAL DE GAULLE    | [66]                           |
| 2 avenue Barbara                  |                 |                | 2 AVENUE BARBARA                  | [2]                            |
| 10 boulevard Rocheplatte          |                 |                | 10 BOULEVARD ROCHEPLATTE          | [10]                           |
| 29 rue du Faubourg de la Chaussée |                 |                | 29 RUE DU FAUBOURG DE LA CHAUSSEE | [29]                           |
|                                   | 12B rue Diderot |                | 12B RUE DIDEROT                   | [12]                           |

### Exemple Output 2

Exemple avec deux ou plus de numéros dans les trois champs

| adresse_ligne1                         | adresse_ligne2                     | adresse_ligne3 | adresse_reconstituee_inpi                 | list_numero_voie_matching_inpi |
|----------------------------------------|------------------------------------|----------------|-------------------------------------------|--------------------------------|
|                                        | 17/19 boulevard du Général Leclerc |                | 17 19 BOULEVARD DU GENERAL LECLERC        | [17, 19]                       |
|                                        | 1 - 5 rue Talleyrand               |                | 1 5 RUE TALLEYRAND                        | [1, 5]                         |
| 3-5 rue Paul Duez                      |                                    |                | 3 5 RUE PAUL DUEZ                         | [3, 5]                         |
| 6 Rue du Pelvoux - Immeuble la Vanoise | 6-18                               |                | 6 RUE DU PELVOUX IMMEUBLE LA VANOISE 6 18 | [6, 18]                        |
| 350 rue Arthur Brunet                  | BP 50017                           |                | 350 RUE ARTHUR BRUNET BP 50017            | [350, 50017]                   |

<!-- #region -->
## Règles de gestion applicables

[PO : Formules applicables]

Si nouvelle règle, ajouter ici.

- Pattern regex pour récupérer tous les digits -> `[0-9]+`
- Pensez a ne garder que les digits uniques. Si [10,10,5] alors cela devient [10,5]

# Charges de l'équipe

[

PO : Si des étapes particulières / des points d'attention sont attendus, être aussi explicite que possible

Spécifiquement pour l'intégration de nouvelles données dans DATUM :

*   Nombre de lignes chargées pour chaque nouvelle table
*   Poids de chaque nouvelle table
*   Durée du traitement ajouté (+ durée avant et après)

]

Query SQL utilisée lors de nos tests

```
SELECT 
  adresse_ligne1, 
  adresse_ligne2, 
  adresse_ligne3, 
  array_distinct(
    regexp_extract_all(
        trim(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              NORMALIZE(
                UPPER(
                  CONCAT(
                    adresse_ligne1, ' ', adresse_ligne2, 
                    ' ', adresse_ligne3
                  )
                ), 
                NFD
              ), 
              '\pM', 
              ''
            ), 
            '[^\w\s]| +', 
            ' '
          )
      ), 
      '[0-9]+'
    )
  ) AS list_numero_voie_matching_inpi 
```

# Tests d'acceptance

[PO : comment contrôler que la réalisation est conforme]

 * Donner la liste des pairs  (top 10) avec le plus de fréquence
 * Donner la liste des pairs (top 10) avec le plus de fréquence lorsque la taille de l’array est égale à 
   * 2
   * 3
   * 4
*  Compter le nombre occurrences sachant la taille de la liste:
   * Exemple, combien de fois la liste est égale à 1, 2, etc

**Code reproduction**

```
```


# CONCEPTION

Conception réalisée par ............. et ..................

[DEV :

Important :

*   Ce chapitre doit impérativement être complété **avant de basculer l'US à 'développement en cours'**
*   La conception doit systématiquement être **faite à deux**
*   Il ne doit **pas y avoir de code dans ce chapitre**
*   Tout au long du développement, ce chapitre doit être enrichi
*   Le nom du binôme ayant participé à la conception doit être précisé dans l'US

Contenu :

*   Décrire les traitements nouveaux / modifiés : emplacement des fichiers (liens vers GIT), mise en avant des évolutions fortes, impacts dans la chaîne d'exploitation
*   Points d'attention spécifiques : notamment sur les règles de gestion et leur mise en oeuvre technique

]

# Evolution de la documentation

[DEV :

*   Identifier les champs enrichis dans le dictionnaire de données
*   Identifier les impacts dans les documents pérennes DTA, DEXP, Consignes de supervision
*   Identifier les impacts dans les documents de MEP (FI)

]

# Tests réalisés

[DEV : préciser les tests réalisés pour contrôler le bon fonctionnement, et les résultats obtenus]

# Tests automatiques mis en oeuvre

[DEV : préciser les TA et expliciter leur fonctionnement]

# Démonstration

[DEV : suivant le cas, publier sur le sharepoint et mettre un lien ici soit :

*   Capture d'écran
*   Vidéo publiée

]
<!-- #endregion -->

# Creation markdown

```python
import os, time, shutil, urllib, ipykernel, json
from pathlib import Path
from notebook import notebookapp
```

```python
def create_report(extension = "html"):
    """
    Create a report from the current notebook and save it in the 
    Report folder (Parent-> child directory)
    
    1. Exctract the current notbook name
    2. Convert the Notebook 
    3. Move the newly created report
    
    Args:
    extension: string. Can be "html", "pdf", "markdown"
    
    
    """
    
    ### Get notebook name
    connection_file = os.path.basename(ipykernel.get_connection_file())
    kernel_id = connection_file.split('-', 1)[1].split('.')[0]

    for srv in notebookapp.list_running_servers():
        try:
            if srv['token']=='' and not srv['password']:  
                req = urllib.request.urlopen(srv['url']+'api/sessions')
            else:
                req = urllib.request.urlopen(srv['url']+ \
                                             'api/sessions?token=' + \
                                             srv['token'])
            sessions = json.load(req)
            notebookname = sessions[0]['name']
        except:
            pass  
    
    sep = '.'
    path = os.getcwd()
    parent_path = str(Path(path).parent)
    
    ### Path report
    #path_report = "{}/Reports".format(parent_path)
    #path_report = "{}/Reports".format(path)
    
    ### Path destination
    name_no_extension = notebookname.split(sep, 1)[0]
    if extension == 'markdown':
        #extension = 'md'
        os.remove(name_no_extension +'.{}'.format('md'))
        source_to_move = name_no_extension +'.{}'.format('md')
    else:
        source_to_move = name_no_extension +'.{}'.format(extension)
    dest = os.path.join(path,'US_md', source_to_move)
    
    print('jupyter nbconvert --no-input --to {} {}'.format(
    extension,notebookname))
    
    ### Generate notebook
    os.system('jupyter nbconvert --no-input --to {} {}'.format(
    extension,notebookname))
    
    ### Move notebook to report folder
    #time.sleep(5)
    shutil.move(source_to_move, dest)
    print("Report Available at this adress:\n {}".format(dest))
```

```python
create_report(extension = "markdown")
```
