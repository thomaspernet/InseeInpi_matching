[

Usage : copier-coller le texte ci-dessous (en "_mode markdown_") à la création de nouvelles US, et retenir les paragraphe applicables

Grammaire des US :

```
En tant que {X} je souhaite {créer une nouvelle variable code postal néttoyée} afin de {faire correspo,dre les bonnes valeurs a l'INSEE}
```

*   Y est une fonctionnalité à valeur ajoutée <-- c'est **le TITRE de l'US**, afin de garder une cohérence, commencer par un **verbe à l'infinitif**
*   Z est le bénéfice attendu de la fonctionnalité <-- à placer dans le champ consacré en bas d'US
*   X est la personne (ou le rôle) qui va en bénéficier <-- à placer dans le champ consacré en bas d'US

]

# Contexte

[PO :

*   Situation avant mise en oeuvre de l'US
*   Explications fonctionnelles / vocabulaire si des termes métiers sont exploités

]

Toujours dans l'objectif de préparer la donnée pour le matching avec l'INSEE, il y a une nouvelle variable à néttoyer: `code_postal`.

Effectivement, le manque de formalisme du coté des greffes engendre beaucoup de fantaisie de la part du greffier. Ce dernier ne va pas se contenter d'indiquer le code postal avec 5 chiffres uniquement. Au contraire, certains greffiers vont mélanger lettres et digits, ou bien ne pas indiquer le code postal au complet. Les longueurs possibles constatées lors ne nos tests vont de 0 à 26 Ce problème n'est pas constaté à l'INSEE car cette dernière ne permet que deux possibilités. Un code postal vide ou un code postal à 5 chiffres.

Dans cette US, nous allons retravailler cette variable afin de la mettre au norme de l'INSEE, a savoir, 5 chiffres, ou null.

```
SELECT DISTINCT(LENGTH(code_postal)) as len_code_postal
FROM initial_partiel_evt_new_ets_status_final
-- WHERE code_postal != ''
ORDER BY len_code_postal
```

## Règles de gestion

*   Définition partiel

    *   si csv dans le dossier Stock, année > 2017, alors partiel, c'est a dire, modification complète du dossier due a une anomalie.
    *   la date d’ingestion est indiquée dans le path, ie comme les flux
*   Une séquence est un classement chronologique pour le quadruplet suivant:

    *   _siren_ + _code greffe_ + _numero gestion_ + _ID établissement_
*  Une création d'une séquence peut avoir plusieurs transmission a des intervalles plus ou moins long
    *   Si plusieurs transmissions avec le libellé “création établissement” ou “création" , alors il faut prendre la dernière date de transmission
    *   Il y a certains cas ou les lignes de créations doublons sont de faux événements (mauvais envoie de la part du greffier)
        *   Si le timestamp entre la première ligne et dernière ligne est supérieures a 31 jour (exclut), il faut:
            *   Récupération de la dernière ligne, et créer une variable flag, comme pour le statut
*   Evénement 1
    *   Les événements doivent impérativement suivre l'ordre d'apparition dans le csv du FTP
        *   Pour les événements, il est possible d'avoir plusieurs informations renseignées pour une même date de transmission pour une même séquence
    *   Le remplissage doit se faire de la manière suivante pour la donnée brute
        *   Pour une date de transmission donnée, c'est la dernière ligne de la séquence qui doit être utilisée, remplie des valeurs manquantes extraites des lignes précédentes. Si la dernière ligne de la séquence contient un champs non vide, il ne faut pas la remplacer par la ligne précédente.
- Partiel
  - En cas de corrections majeures, la séquence annule et remplace la création et événements antérieurs. Dans ce cas, toutes les données qui ont pu être transmises antérieurement via le stock initial ou le flux doivent donc être ignorées (prendre en compte la date de transmission indiquée dans le nom des sous-répertoires du stock et des fichiers
-  Siren sans Siège ou Principal
  - Il est possible qu'un SIREN n'ai pas de siege/principal. Normalement, cela doit être corrigé par un partiel
-  Etablissement sans création
  - Il arrive que des établissements soient supprimés (EVT) mais n'ont pas de ligne "création d'entreprise". Si cela, arrive, Infogreffe doit envoyer un partiel pour corriger. Il arrive que le greffe envoie seulement une ligne pour SEP, lorsque le Principal est fermé, le siège est toujours ouvert. Mais pas de nouvelle ligne dans la base. Le partiel devrait corriger cela.
- La variable `ville` de l'INPI n'est pas normalisée. C'est une variable libre de la créativité du greffier, qui doit être formalisée du mieux possible afin de permettre la jointure avec l'INSEE. Plusieurs règles regex ont été recensé comme la soustraction des numéros, caractères spéciaux, parenthèses, etc. Il est possible d'améliorer les règles si nécessaire
- [NEW] Le code postal doit être formalisé correctement, a savoir deux longueurs possibles: zero (Null) ou cinq. Dans certains cas, le code postal se trouve dans la variable de la ville. 

Workflow US  (via stock)

![workflow](https://www.lucidchart.com/publicSegments/view/d9e4494d-bfaf-4d0e-9e0f-53011cda7eb9/image.png)

# US / ISSUES liées

[PO & DEV : s'il existe des références, les inscrire]

# Besoin

[PO : Situation attendue]

Le besoin attendu dans cette US est le suivant:

- Créer une variable `code_postal_matching`  qui a été nettoyée et/ou enrichie pour etre conforme à l'INSEE

# Spécifications

## Input

[PO : dans le cas de transformation de données, préciser ,les sources :

*   Applications
*   Schémas
*   Tables: `inpi_etablissement_historique`
*   Champs: `code_postal` et `ville`

]

## Output

[PO : dans le cas de transformation de données, préciser les sorties :

*   BDD cibles
*   Tables: `inpi_etablissement_historique`
*   Champs: `code_postal_matching`

]

## Règles de gestion applicables

[PO : Formules applicables]

- [NEW] Le code postal doit être formalisé correctement, a savoir deux longueurs possibles: zero (Null) ou cinq. Dans certains cas, le code postal se trouve dans la variable de la ville.

# Charges de l'équipe

[

PO : Si des étapes particulières / des points d'attention sont attendus, être aussi explicite que possible

Spécifiquement pour l'intégration de nouvelles données dans DATUM :

*   Nombre de lignes chargées pour chaque nouvelle table
*   Poids de chaque nouvelle table
*   Durée du traitement ajouté (+ durée avant et après)

]

La préparation de la variable `code_postal_matching` est construite sur la règle suivante:

- Si la variable `code_postal` est vide alors extraction des digits dans la variable `ville` uniquement si la longueur est egale a 5.
- Si la longeur de la variable `code_postal` est différente de 5 alors NULL sinon `code_postal`

Le code SQL pour créer la variable `code_postal_matching` est disponible dans le Gitlab: [snippet](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/snippets/52)

Nous avons mis un fichier Excel dans le GitLab avec les valeurs distinctes du code postal de l'INPI: [choix_possible_code_postal.xlsx](US_Datum/Data_example/US_Code_postal/choix_possible_code_postal.xlsx)

# Tests d'acceptance

[PO : comment contrôler que la réalisation est conforme]

- Compter la logneur de la variable `code_postal_matching`.
- Compter le nombre de valeur NULL dans la variable `code_postal_matching`. Lors de nos tests, 29364 observations avaient une valeur NULL.
- Compter le nombre d'observations ayant un code postal NULL mais une valeur positive pour `code_postal_matching`. Lors de nos tests, 3243 observations ont été retrouvé via la ville

```
SELECT COUNT(*)
FROM
(SELECT *, CASE
WHEN code_postal = '' THEN REGEXP_EXTRACT(ville, '\d{5}')
WHEN LENGTH(code_postal) = 5 THEN code_postal
ELSE NULL END AS code_postal_matching
FROM  initial_partiel_evt_new_ets_status_final
 )
-- WHERE code_postal = '' AND code_postal_matching IS NOT NULL
-- WHERE code_postal_matching
-- WHERE code_postal_matching IS NOT NULL
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
