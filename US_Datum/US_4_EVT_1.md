[

Usage : copier-coller le texte ci-dessous (en "_mode markdown_") à la création de nouvelles US, et retenir les paragraphe applicables

Grammaire des US :

```
En tant que {X} je souhaite {Remplir et filter data EVT 1} afin de {pouvoir préparer la donnée en vue de la siretisation}
```

*   Y est une fonctionnalité à valeur ajoutée <-- c'est **le TITRE de l'US**, afin de garder une cohérence, commencer par un **verbe à l'infinitif**
*   Z est le bénéfice attendu de la fonctionnalité <-- à placer dans le champ consacré en bas d'US
*   X est la personne (ou le rôle) qui va en bénéficier <-- à placer dans le champ consacré en bas d'US

]

# Contexte

[PO :

La donnée brute de l'INPI est constitutée de deux branches. Une première avec des données de stock (initial et partiel) et une seconde avec des données de flux. Dans cette dernière, nous pouvons distinguer les créations d'établissements mais aussi les modifications ou suppressions. Dans cet US, nous allons nous intéresser aux modifications et suppressions. La manière d'opérer de l'INPI est plutot originale.

Les fichiers transmis permettent d’exploiter les données de flux en provenance des greffes des tribunaux de commerce.

En cas de mise à jour d’un dossier suite à un événement (modification, radiation), les fichiers transmis ont une structure identique aux fichiers créés à l’immatriculation avec la présence de 2 champs spécifiques : la date de l’événement (Date_Greffe) et le libellé de l’événement (Libelle_Evt).

Attention, il peut arriver que le même dossier fasse l’objet de plusieurs événements (création et modification) dans la même transmission. Il est impératif d’intégrer les événements dans l’ordre d’apparition. Cela veut dire que pour une même transmission, pour une même séquence, il peut avoir plusieurs lignes.

Le flux de créations, modifications, suppressions est en “différentiel”, c’est à dire qu’une ligne de CSV contiendra des colonnes vides si la donnée n’a pas changé depuis sa dernière version.

### Exemple

- SIREN: 420844656
  - même établissement, plusieurs entrées
  - exemple SIREN 420844656, évenement effectué le 2018/01/03 a 08:48:10. Nom dans le FTP
    - [0101_163_20180103_084810_9_ets_nouveau_modifie_EVT.csv](US_Datum/Data_example/US_2172/0101_163_20180103_084810_9_ets_nouveau_modifie_EVT.csv)

    | code greffe | nom_greffe      | numero_gestion | siren     | type | siège_pm | rcs_registre | adresse_ligne1 | adresse_ligne2 | adresse_ligne3 | code_postal | ville  | code_commune | pays   | domiciliataire_nom | domiciliataire_siren | domiciliataire_greffe | domiciliataire_complément | siege_domicile_représentant | nom_commercial | enseigne | activité_ambulante | activité_saisonnière | activité_non_sédentaire | date_début_activité | activité                                                                        | origine_fonds | origine_fonds_info | type_exploitation    | id_etablissement | date_greffe | libelle_evt                                | file_timestamp      | libelle_evt2    |
    |-------------|-----------------|----------------|-----------|------|----------|--------------|----------------|----------------|----------------|-------------|--------|--------------|--------|--------------------|----------------------|-----------------------|---------------------------|-----------------------------|----------------|----------|--------------------|----------------------|-------------------------|---------------------|---------------------------------------------------------------------------------|---------------|--------------------|----------------------|------------------|-------------|--------------------------------------------|---------------------|-----------------|
    | 101         | Bourg-en-Bresse | 1998D00387     | 420844656 | SEP  |          |              |                | le Devin       |                | 01851       | Marboz | 01232        | FRANCE |                    |                      |                       |                           |                             |                |          | non                | non                  | non                     | 1998-11-04          | Acquisition propriété gestion de tous biens et droits mobiliers et Immobi Liers | Création      |                    | Exploitation directe | 1                | 2018-01-02  | Modifications relatives à un établissement | 2018-01-03 08:48:10 | nouveau_modifie |
    | 101         | Bourg-en-Bresse | 1998D00387     | 420844656 | SEP  |          |              |                | le Devin       |                | 01851       | Marboz | 01232        | FRANCE |                    |                      |                       |                           |                             |                |          |                    |                      |                         |                     | Acquisition propriété gestion de tous biens et droits mobiliers et Immobi Liers |               |                    |                      | 1                | 2018-01-02  | Modifications relatives à un établissement | 2018-01-03 08:48:10 | nouveau_modifie |


Dès lors, nous pouvons dégager une nouvelle règle de gestion.

## Règles de gestion

 - Definition partiel
   - si csv dans le dossier Stock, année > 2017, alors partiel, c'est a dire, modification complète du dossier due a une anomalie.
   - la date d’ingestion est indiquée dans le path, ie comme les flux

 - Une séquence est un classement chronologique pour le quadruplet suivant:

   - *siren* + *code greffe* + *numero gestion* + *ID établissement*

- [NEW] Evénement 1
  - Les événements doivent impérativement suivre l'ordre d'apparition dans le csv du FTP
    - Pour les événements, il est possible d'avoir plusieurs informations renseignées pour une meme date de transmission pour une même séquence
  - Le remplissage doit se faire de la manière suivante pour la donnée brute
    - Pour une date de transmission donnée, c'est la dernière ligne de la séquence qui doit être utilisée remplie des valeurs manquantes extraites des lignes précédentes. Si la dernière ligne de la séquence contient un champs non vide, il ne faut pas la remplacer par la ligne précédente.

]

# US / ISSUES liées

[PO & DEV : s'il existe des références, les inscrire]

# Besoin

[PO : Situation attendue]

# Spécifications

## Input

[PO : dans le cas de transformation de données, préciser, les sources :

*   inpi_flux_etablissement_nouveau_modifie
*   inpi_flux_etablissement_supprim

]

## Output

[PO : dans le cas de transformation de données, préciser les sorties :

*   BDD cibles
*   Table: Ets_evt
*   Champs

]

## Règles de gestion applicables

[PO : Formules applicables]

- [NEW] Evénement 1
  - Les événements doivent impérativement suivre l'ordre d'apparition dans le csv du FTP
    - Pour les événements, il est possible d'avoir plusieurs informations renseignées pour une meme date de transmission pour une même séquence
  - Le remplissage doit se faire de la manière suivante pour la donnée brute
    - Pour une date de transmission donnée, c'est la dernière ligne de la séquence qui doit être utilisée remplie des valeurs manquantes extraites des lignes précédentes. Si la dernière ligne de la séquence contient un champs non vide, il ne faut pas la remplacer par la ligne précédente.

# Charges de l'équipe

[

PO : Si des étapes particulières / des points d'attention sont attendus, être aussi explicite que possible

Spécifiquement pour l'intégration de nouvelles données dans DATUM :

*   Nombre de lignes chargées pour chaque nouvelle table
*   Poids de chaque nouvelle table
*   Durée du traitement ajouté (+ durée avant et après)

]

- Enrichissement des champs manquants et filtre sur la dernière ligne d'une séquence pour une date de transmission donnée

Le detail de la query est dispnible [ici](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/01_Athena_concatenate_ETS.md#query-pr%C3%A9paration-%C3%A9v%C3%A9nement) et la query est disponible [ici](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/snippets/34)

## Exemple avec SIREN: 513913657

- Un fichier Excel appelé [513913657.xlsx](US_Datum/Data_example/US_2172/513913657.xlsx) vient a l'appui de l'exemple

**Ensemble fichier Data brute: `FROM_FTP`**

- Dans le FTP, 7 transmissions ont été réalisée en 2018.
   *  3801_189_20180130_065752_9_ets_nouveau_modifie_EVT.csv
   *  3801_190_20180131_065908_9_ets_nouveau_modifie_EVT.csv
   *  3801_209_20180227_065600_9_ets_nouveau_modifie_EVT.csv
   *  3801_213_20180303_064240_9_ets_nouveau_modifie_EVT.csv
   *  3801_222_20180316_063210_9_ets_nouveau_modifie_EVT.csv
   *  3801_293_20180627_061209_9_ets_nouveau_modifie_EVT.csv
   *  3801_301_20180711_065600_9_ets_nouveau_modifie_EVT.csv

Toutes les données relatives aux 7 transmissions sont disponibles dans l'onglet `FROM_FTP` (83 entrées), et chaque couleur représente un csv (regroupé par date de transmission). Comme indiqué par l'INPI, il faut remplir les entrées d’un même csv par l’entrée n-1. La dernière entrée fait foi si différente avec n-1. Dans la feuille, c’est les ligne jaunes.

**Etape remplissage: `FILLIN`**

La feuille `FILLIN` se charge du remplissage des valeurs manquantes. La ligne jaune étant celle que nous devons garder

**Etape Filtre: `FILTER`**

La feuille `FILTER` récupère uniquement la dernière ligne par date de transmission.

Au final, il ne reste plus que 8 lignes sur les 83 initiales.

# Tests d'acceptance

[PO : comment contrôler que la réalisation est conforme]

1. Vérifier que le nombre de lignes entre `inpi_flux_etablissement_nouveau_modifie` + `inpi_flux_etablissement_supprim` soit supérieur au nombre de lignes de la table `Ets_evt`
2. Vérifier que le siren `513913657` a été rempli puis filtrer. Normalement, l'output doit correspondre a la feuille Excel `FILTER`.
3.

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
