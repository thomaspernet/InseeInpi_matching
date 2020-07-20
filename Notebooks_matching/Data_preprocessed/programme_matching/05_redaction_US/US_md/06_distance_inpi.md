# Créer adresse distance 

```
Entant que {X} je souhaite {créer la variable distance a partir des variables de l'adresse} afin de {pouvoir calculer la distance de Levhenstein et Jaccard}
```

**Metadatab**

- Taiga:
    - Numero US: [2949](https://tree.taiga.io/project/olivierlubet-air/us/2949)
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

- Création de la variable `adress_distance_inpi`: 
    - Concatenation des champs de l'adresse, suppression des espaces et des articles

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
*   Champs: `adresse_ligne1`, `adresse_ligne2`, `adresse_ligne3`


### Exemple Input 1

Exemple dans la table `inpi_etablissement_historique`, nous avons les variables suivantes en entrée:

| siren     | adresse_ligne1          | adresse_ligne2           | adresse_ligne3 |
|-----------|-------------------------|--------------------------|----------------|
| 797405776 |                         | 44 Rue du Vercors        |                |
| 797405776 |                         | 44 Rue du Vercors        |                |
| 797405784 |                         | 3 Place André Audinot    |                |
| 797405784 |                         | 3 Place André Audinot    |                |
| 797405792 | 12 Rue HONORE DE BALZAC |                          |                |
| 797405792 | 12 Rue HONORE DE BALZAC |                          |                |
| 797405826 | 8 Rue Général Foy       |                          |                |
| 797405826 | 8 Rue Général Foy       |                          |                |
| 797405834 |                         | 54 route de Tré Drano    |                |
| 797405842 |                         | 52 Lotissement le Verger |                |

## Output

[PO : dans le cas de transformation de données, préciser les sorties :

*   BDD cibles
*   Tables: `inpi_etablissement_historique`
*   Champs: `adress_distance_inpi`

]

La concatenation, le nettoyage et la mise en majuscule donne lieu a un la nouvelle variable ci dessous:

| siren     | adresse_ligne1          | adresse_ligne2           | adresse_ligne3 | adress_distance_inpi  |
|-----------|-------------------------|--------------------------|----------------|-----------------------|
| 797405776 |                         | 44 Rue du Vercors        |                | 44 RUE VERCORS        |
| 797405776 |                         | 44 Rue du Vercors        |                | 44 RUE VERCORS        |
| 797405784 |                         | 3 Place André Audinot    |                | 3 PLACE ANDRE AUDINOT |
| 797405784 |                         | 3 Place André Audinot    |                | 3 PLACE ANDRE AUDINOT |
| 797405792 | 12 Rue HONORE DE BALZAC |                          |                | 12 RUE HONORE BALZAC  |
| 797405792 | 12 Rue HONORE DE BALZAC |                          |                | 12 RUE HONORE BALZAC  |
| 797405826 | 8 Rue Général Foy       |                          |                | 8 RUE GENERAL FOY     |
| 797405826 | 8 Rue Général Foy       |                          |                | 8 RUE GENERAL FOY     |
| 797405834 |                         | 54 route de Tré Drano    |                | 54 ROUTE TRE DRANO    |
| 797405842 |                         | 52 Lotissement le Verger |                | 52 LOTISSEMENT VERGER |

## Règles de gestion applicables

[PO : Formules applicables]

Voici un tableau récapitulatif des règles appliquer sur les variables de l'adresse:

| Table | Variable | article | digit | debut/fin espace | espace | accent | Upper |
| --- | --- | --- | --- | --- | --- | --- | --- |
| INPI | adresse_regex_inpi | X | X | X | X | X | X |
|  | adress_distance_inpi | X |  | X | X | X | X |
|  | adresse_reconstituee_inpi |  |  | X | X | X | X |
| INSEE | adress_reconstituee_insee | X |  |  |  | |  |

# Charges de l'équipe

[

PO : Si des étapes particulières / des points d'attention sont attendus, être aussi explicite que possible

Spécifiquement pour l'intégration de nouvelles données dans DATUM :

*   Nombre de lignes chargées pour chaque nouvelle table
*   Poids de chaque nouvelle table
*   Durée du traitement ajouté (+ durée avant et après)

]

Le pattern regex pour supprimer les articles est le suivant:

`(?:^|(?<= ))(AU|AUX|AVEC|CE|CES|DANS|DE|DES|DU|ELLE|EN|ET|EUX|IL|ILS|LA|LE|LES(?:(?= )|$)`.

Il doit correspondre au pattern utilisé dans les US [2690](https://tree.taiga.io/project/olivierlubet-air/us/2690)  et [2954](https://tree.taiga.io/project/olivierlubet-air/us/2954).

Le code SQL utilisé lors de nos tests:

```
query = """
REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
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
              ), 
              '(?:^|(?<= ))(AU|AUX|AVEC|CE|CES|DANS|DE|DES|DU|ELLE|EN|ET|EUX|IL|ILS|LA|LE|LES(?:(?= )|$)', 
              ''
            ), 
            '\s\s+', 
            ' '
          ), 
          '^\s+|\s+$', 
          ''
      ) AS adress_distance_inpi
"""
```

# Tests d'acceptance

[PO : comment contrôler que la réalisation est conforme]

- Imprimer aléatoirement 10 adresses
- Imprimer des patterns ou il y a 1 champs adresses non vide
- Imprimer des patterns ou il y a 2 champs adresses non vides
- Imprimer des patterns ou il y a 3 champs adresses non vides
- Vérifier que la variable `adress_regex_inpi` n'a pas de " " (espace) en début ou fin de string.

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
