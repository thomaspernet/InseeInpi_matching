[

Usage : copier-coller le texte ci-dessous (en "_mode markdown_") à la création de nouvelles US, et retenir les paragraphe applicables

Grammaire des US :

```
En tant que {X} je souhaite {remplir et filtrer les événements de la base INPI établissements} afin de {pouvoir préparer la donnée en vue de la siretisation}
```

*   Y est une fonctionnalité à valeur ajoutée <-- c'est **le TITRE de l'US**, afin de garder une cohérence, commencer par un **verbe à l'infinitif**
*   Z est le bénéfice attendu de la fonctionnalité <-- à placer dans le champ consacré en bas d'US
*   X est la personne (ou le rôle) qui va en bénéficier <-- à placer dans le champ consacré en bas d'US

]

# Contexte

[PO :

La donnée brute de l'INPI est constitutée de deux branches. Une première avec des données de stock (initial et partiel) et une seconde avec des données de flux. Dans cette dernière, nous pouvons distinguer les créations d'établissements mais aussi les modifications ou suppression. Dans cet US, nous allons nous intéresser au modifications et suppressions dans la mesure ou leur création est plutot originale. Pour rappel, les fichiers transmis permettent d’exploiter les données de flux en provenance des greffes des tribunaux de commerce.

En cas de mise à jour d’un dossier suite à un événement (modification, radiation), les fichiers transmis ont une structure identique aux fichiers créés à l’immatriculation avec la présence de 2 champs spécifiques : la date de l’événement (Date_Greffe) et le libellé de l’événement (Libelle_Evt).

Attention, il peut arriver que le même dossier fasse l’objet de plusieurs événements (création et modification) dans la même transmission. Il est impératif d’intégrer les événements dans l’ordre d’apparition. Cela veut dire que pour une même transmission, pour une même séquence, il peut avoir plusieurs lignes.

Le flux de créations, modifications, suppressions est en “différentiel”, c’est à dire qu’une ligne de CSV contiendra des colonnes vides si la donnée n’a pas changé depuis sa dernière version.

### Exemple

- SIREN: 420844656
  - même établissement, plusieurs entrées
  - exemple SIREN 420844656, évenement effectué le 2018/01/03 a 08:48:10. Nom dans le FTP [0101_163_20180103_084810_9_ets_nouveau_modifie_EVT.csv](https://calfdata.s3.eu-west-3.amazonaws.com/INPI/TC_1/01_donnee_source/Flux/2018/ETS/EVT/0101_163_20180103_084810_9_ets_nouveau_modifie_EVT.csv)
 - [cvs](https://calfdata.s3.eu-west-3.amazonaws.com/INPI/TC_1/02_preparation_donnee/check_csv/420844656.csv) pour un exemple.

Dès lors, nous pouvons dégager une nouvelle règle de gestion.

## Règles de gestion

 - Definition partiel
   - si csv dans le dossier Partiel, année > 2017, alors partiel, c'est a dire, modification complète du dossier due a une anomalie.
   - la date d’ingestion est indiquée dans le path, ie comme les flux

 - Une séquence est un classement chronologique pour le quadruplet suivant:

   - *siren* + *code greffe* + *numero gestion* + *ID établissement*

- [NEW] Evénement
  - Une ligne événement ne modifie que le champs comportant la modification. Les champs non modifiés vont être remplis par la ligne t-1
  - Le remplissage doit se faire de la manière suivate pour la donnée brute
    - une première fois avec la date de transmission (plusieurs informations renseignées pour une meme date de transmission pour une même séquence). La dernière ligne remplie des valeurs précédentes de la séquence

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

Le detail de la query est dispnible [ici](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/01_Athena_concatenate_ETS.md#query-pr%C3%A9paration-%C3%A9v%C3%A9nement) et la query est disponible [ici](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/snippets/34)

]

## Output

[PO : dans le cas de transformation de données, préciser les sorties :

*   BDD cibles
*   Tables
*   Champs

]

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

# Tests d'acceptance

[PO : comment contrôler que la réalisation est conforme]

Exemple avec SIREN [513913657](https://calfdata.s3.eu-west-3.amazonaws.com/INPI/TC_1/02_preparation_donnee/check_csv/513913657.xlsx):

* En tout, il y a 83 entrées. Dans la feuille `FROM_FTP`, chaque couleur représente un csv (regroupé par date de transmission). Comme indiqué par Mr Flament, il faut remplir les entrées d’un même csv par l’entrée n-1. La dernière entrée fait foi si différente avec n-1. Dans la feuille, c’est les ligne jaunes.
* La feuille FILLIN va faire cette étape de remplissage, et la feuille FILTER récupère uniquement la dernière ligne par date de transmission. L’enseigne est indiqué comme supprimée dans la donnée brute a différentes dates de transmission mais supprimé lors de la dernière transmission.


Raw data dans S3

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
