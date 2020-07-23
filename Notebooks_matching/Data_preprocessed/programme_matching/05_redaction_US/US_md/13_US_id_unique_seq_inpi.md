# Création ID séquence INPI 

```
Entant que {X} je souhaite {créer un identification unique par séquence} afin d'{éviter a avoir a recréer cette sequence lors du compte d'établissement ou lors de la siretisation}
```

**Metadatab**

- Taiga:
    - Numero US: [2976](https://tree.taiga.io/project/olivierlubet-air/us/2976)
- Gitlab
    - Notebook: []()
    - Markdown: []()
    - Data:
        - []()
        - 

# Contexte

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
- Le code postal doit être formalisé correctement, a savoir deux longueurs possibles: zero (Null) ou cinq. Dans certains cas, le code postal se trouve dans la variable de la ville.
- La variable pays doit être formalisée, a savoir correspondre au code pays de l'INSEE. Bien que la majeure partie des valeurs soit FRANCE ou France, il convient de normaliser la variable pour récuperer les informations des pays hors France.
- Les variables de l'adresse de l'INPI ne sont pas normalisées, et ne peuvent être utilisées en l'état. Il est donc indispensable de retravailler les variables adresse pour pouvoir les comparer avec l'INSEE. Nous utilisons une règle (pattern) regex pour vérifier si les mots contenus dans l'adresse de l'INPI sont aussi contenus à l'INSEE.
- L'INSEE codifie le type de voie de la manière suivante:
    - Si le type de voie est d'une longueur inférieure ou égale à 4 caractères, le type de voie n'est pas abrégé. Ainsi, RUE ou QUAI sont écrits tels quels, alors que AVENUE est abrégée en AV.
    - La codification de l'INSEE va donc être utilisé ppur créer la variable `voie_matching`
- Pour ne pas complexifié le processus de siretisation, seule le premier numéro de voie contenu dans l'adresse nettoyée est extraite pour ensuite etre matché avec l'INSEE.

Workflow US (via stock)

![workflow](https://www.lucidchart.com/publicSegments/view/d9e4494d-bfaf-4d0e-9e0f-53011cda7eb9/image.png)

# US / ISSUES liées

[PO & DEV : s'il existe des références, les inscrire]

# Besoin

Dans cette US, le besoin est le suivant:

- Créer un ID unique pour la séquence suivante:  siren + code greffe + nom greffe + numero gestion +ID établissement 


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
    - `siren`
    - `code greffe`
    - `nom greffe`
    - `numero gestion`
    - `ID établissement` 

### Exemple Input 1

Exemple d'input avec plusieurs séquences ayant un (SIREN 000199968) ou plusieurs événements (SIREN 000325175)

| index_id | siren     | code_greffe | nom_greffe | numero_gestion | id_etablissement |
|----------|-----------|-------------|------------|----------------|------------------|
| 2197841  | 000199968 | 7501        | Paris      | 1983B08179     | 2                |
| 2197843  | 000325175 | 1303        | Marseille  | 2011A00564     | 2                |
| 2197842  | 000325175 | 1303        | Marseille  | 2011A00564     | 2                |
| 2197844  | 000499053 | 9301        | Bobigny    | 1987B06881     | 10               |
| 2197845  | 000585794 | 5602        | Vannes     | 2018B00176     | 1                |
| 2197846  | 005410428 | 8002        | Amiens     | 1954A70042     | 1                |
| 2197847  | 005410428 | 8002        | Amiens     | 1954A70042     | 2                |
| 2197848  | 005411483 | 8002        | Amiens     | 1954A70148     | 1                |
| 2197849  | 005411954 | 8002        | Amiens     | 1954A70195     | 1                |
| 2197850  | 005420021 | 8002        | Amiens     | 1954B70002     | 1                |

## Output

[PO : dans le cas de transformation de données, préciser les sorties :

*   BDD cibles
*   Tables: `inpi_etablissement_historique`
*   Champs: 
    - `sequence_id` 

]

La table ci-dessous montre le résultat attendu après avoir créer l'ID unique. Cet ID va impacté l'ensemble des evenements. Par exemple, le SIREN `000325175` a deux événments, donc l'ID est rattaché a ses deux évenements. 

| index_id | siren     | code_greffe | nom_greffe | numero_gestion | id_etablissement | sequence_id |
|----------|-----------|-------------|------------|----------------|------------------|-------------|
| 164177   | 000199968 | 7501        | Paris      | 1983B08179     | 2                | 1           |
| 164179   | 000325175 | 1303        | Marseille  | 2011A00564     | 2                | 2           |
| 164178   | 000325175 | 1303        | Marseille  | 2011A00564     | 2                | 2           |
| 164180   | 000499053 | 9301        | Bobigny    | 1987B06881     | 10               | 3           |
| 164181   | 000585794 | 5602        | Vannes     | 2018B00176     | 1                | 4           |
| 164182   | 005410428 | 8002        | Amiens     | 1954A70042     | 1                | 5           |
| 164183   | 005410428 | 8002        | Amiens     | 1954A70042     | 2                | 6           |
| 164184   | 005411483 | 8002        | Amiens     | 1954A70148     | 1                | 7           |
| 164185   | 005411954 | 8002        | Amiens     | 1954A70195     | 1                | 8           |
| 164186   | 005420021 | 8002        | Amiens     | 1954B70002     | 1                | 9           |

## Règles de gestion applicables

[PO : Formules applicables]

Si nouvelle règle, ajouter ici.

Il faut bien respecter la séquence siren + code greffe + nom greffe + numero gestion +ID établissement  pour la création de l'ID

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

- Compter le nombre d'observations après traitement, vérifier qu'il y a le même nombre d'observations qu'avant traitement
- Prendre 2 siren avec deux établissements différents avec le meme siren + code greffe + nom greffe + numero gestion
    - Normalement, cela va montrer les types PRI et SIE, qui sont a la meme adresse, mais que l'INPI considère comme deux etablissements différents. 
- Imprimer 10 siren pour montrer que l'ID est correctement crée
- Compter le nombre d'établissements dans la base en utilisant:
    1. La séquence siren + code greffe + nom greffe + numero gestion +ID établissement
    2. La variable `sequence_id`
    3. Vérifier que les résutlats sont identiques

- Compter le nombre d'établissements par SIREN
    1. La séquence siren + code greffe + nom greffe + numero gestion +ID établissement
    2. La variable `sequence_id`
    3. Vérifier que les résutlats sont identiques

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

# Creation markdown
