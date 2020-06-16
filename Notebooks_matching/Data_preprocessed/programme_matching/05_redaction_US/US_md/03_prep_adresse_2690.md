# Preparation regex adresse 

```
Entant que {X} je souhaite {normaliser la variable pays} afin de {pouvoir la faire correspondre à l'INSEE}
```

**Metadatab**

- Taiga:
    - Numero US: [2690](https://tree.taiga.io/project/olivierlubet-air/us/2690)
- Gitlab
    - Notebook: [03_prep_adresse_2690](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/05_redaction_US/03_prep_adresse_2690.ipynb)
    - Markdown: [03_prep_adresse_2690](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/05_redaction_US/03_prep_adresse_2690.md)
    - Data:
        - [inpi_ets_exemple_1](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/data/RawData/INPI/Stock/inpi_ets_exemple_1.csv)
        - [upper_stop](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/data/input/Parameters/upper_stop.csv)

# Contexte

En plus des variables de matching, l'étape de siretisation va utiliser le regex pattern pour être sur de fiabilité, mais aussi aider au dédoublonnage. Pour cela, nous avons mis en place une règle de gestion pour s'assurer de la fiabilité du matching et du dédoublonnage via une variable contenant un pattern regex. Des lors, nous pouvons comparer le pattern regex recréé via les variables de l'adresses et le comparer à la variable adresse de l'INSEE qui elle affiche plus de normalisme.


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
- [NEW] Les variables de l'adresse de l'INPI ne sont pas normalisées, et ne peuvent être utilisées en l'état. Il est donc indispensable de retravailler les variables adresse pour pouvoir les comparer avec l'INSEE. Nous utilisons une règle (pattern) regex pour vérifier si les mots contenus dans l'adresse de l'INPI sont aussi contenus à l'INSEE.

Workflow US (via stock)

![workflow](https://www.lucidchart.com/publicSegments/view/d9e4494d-bfaf-4d0e-9e0f-53011cda7eb9/image.png)

# US / ISSUES liées

[PO & DEV : s'il existe des références, les inscrire]

# Besoin

Dans cette US, le besoin est le suivant:

- Création d'une variable combinant les 3 variables adresse de l'INPI, néttoyée des accents, espaces et mise en majuscule.
- Création d'une variable combinant les 3 variables adresse de l'INPI contenant un pattern regex, qui va servir à la comparaison avec la variable adresse de l'INSEE



# Spécifications

### Origine information (si applicable) 

- Metadata:
    - Type: [CSV]
    - Source: [Gitlab](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/data/RawData/INPI/Stock/inpi_ets_exemple_1.csv)
    - Summary: Echantillon aléatoire de 3000 observations récupérées de notre table ETS
    - Type: [CSV]
    - Source: [Gitlab](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/data/input/Parameters/upper_stop.csv)
    - Summary: Liste de stop word a retirer dans l'adresse
        
## Input

[PO : dans le cas de transformation de données, préciser ,les sources :

*   Applications
*   Schémas
*   Tables: `inpi_etablissement_historique`
*   CSV: [upper_stop](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/data/input/Parameters/upper_stop.csv)
*   Champs: `adresse_ligne1`, `adresse_ligne2` et `adresse_ligne3`



### Exemple Input 1

L'exemple ci-dessous indique uniquement la donnée brute de la table `inpi_etablissement_historique` avec les 3 champs dont nous avons besoin: `adresse_ligne1`, `adresse_ligne2` et `adresse_ligne3`


    |    |     siren | adresse_ligne1                | adresse_ligne2                  | adresse_ligne3   |
    |---:|----------:|:------------------------------|:--------------------------------|:-----------------|
    |  0 | 487622797 | nan                           | 7 Rue Caraman                   | BP 67131         |
    |  1 | 841846488 | 43 Rue du Professeur Bergonié | nan                             | nan              |
    |  2 | 324958198 | nan                           | 6 Zone Industrielle les Gabares | nan              |
    |  3 | 812461218 | 7 Rue de l'Ancienne Eglise    | nan                             | nan              |
    |  4 | 850414509 | 2 Rue des Maronniers          | nan                             | nan              |
    

### Exemple Input 2

L'exemple ci-dessous indique une liste de candidat au stop word. Tous les mots contenus dans cette liste vont être enlever de l'adresse

    |    | stop   |
    |---:|:-------|
    |  0 | 0      |
    |  1 | AU     |
    |  2 | AUX    |
    |  3 | AVEC   |
    |  4 | CE     |
    

## Output

[PO : dans le cas de transformation de données, préciser les sorties :

*   BDD cibles
*   Tables: `inpi_etablissement_historique`
*   Champs: `adress_nettoyee`, `adresse_regex`

]

Le tableau ci dessous explicite les deux variables attendues, a savoir `adress_nettoyee` et `adresse_regex`. Les deux variables vont utiliser les variables de l'adresse pour reconstituer une adresse nettoyée puis en faire un pattern regex.

- La variable `adress_nettoyee` est la recombinaision des trois variables de l'adresse, nettoyée des accents, espace en debut de texte et mise en majuscule.
- La variable `adresse_regex` est la création du pattern regex, ayant le signe `$` en fin de mot et séparer avec `|`.

    |    |     siren | adresse_ligne1                | adresse_ligne2                  | adresse_ligne3   | adress_nettoyee                 | adresse_regex                |
    |---:|----------:|:------------------------------|:--------------------------------|:-----------------|:--------------------------------|:-----------------------------|
    |  0 | 487622797 | nan                           | 7 Rue Caraman                   | BP 67131         | 7 RUE CARAMAN BP 67131          | CARAMAN$                     |
    |  1 | 841846488 | 43 Rue du Professeur Bergonié | nan                             | nan              | 43 RUE DU PROFESSEUR BERGONIE   | PROFESSEUR$|BERGONIE$        |
    |  2 | 324958198 | nan                           | 6 Zone Industrielle les Gabares | nan              | 6 ZONE INDUSTRIELLE LES GABARES | ZONE$|INDUSTRIELLE$|GABARES$ |
    |  3 | 812461218 | 7 Rue de l'Ancienne Eglise    | nan                             | nan              | 7 RUE DE L ANCIENNE EGLISE      | ANCIENNE$|EGLISE$            |
    |  4 | 850414509 | 2 Rue des Maronniers          | nan                             | nan              | 2 RUE DES MARONNIERS            | MARONNIERS$                  |
    

## Règles de gestion applicables

[PO : Formules applicables]

- [NEW] Les variables de l'adresse de l'INPI ne sont pas normalisées, et ne peuvent être utilisées en l'état. Il est donc indispensable de retravailler les variables adresse pour pouvoir les comparer avec l'INSEE. Nous utilisons une règle (pattern) regex pour vérifier si les mots contenus dans l'adresse de l'INPI sont aussi contenus à l'INSEE.

# Charges de l'équipe

[

PO : Si des étapes particulières / des points d'attention sont attendus, être aussi explicite que possible

Spécifiquement pour l'intégration de nouvelles données dans DATUM :

*   Nombre de lignes chargées pour chaque nouvelle table
*   Poids de chaque nouvelle table
*   Durée du traitement ajouté (+ durée avant et après)

]

Pour construire la variable `adresse_regex`, nous avons créé 3 fonctions Python, que nous avons utilisé de manière séquentielle:

- `create_split_adress`: Découpe l'adresse en une liste de mots. [Snipet](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/snippets/60)
- `create_regex_adress`: Regroupe les mots de l'adresse ensemble avec comme séparateur "|" et le signe `$` [Snipet](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/snippets/61)
- `prepare_adress`: Créer deux colonnes nétoyées de l'adresse a partir d'un dataframe INPI [Snipet](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/snippets/62)

Les étapes sont les suivantes:

1. Combiner les variables `adresse_ligne1`, `adresse_ligne2` et `adresse_ligne3` ensemble. Si la variable ne contient pas de valeur, simplement remplir avec un espace vide. Il ne faut pas que l'adresse contienne `nan` ou `NULL` entre deux textes. Exemple "13 rue de NAN la liberté". Cette variable est appelée `adress_nettoyee`
2. Extraction des accents, des espaces en début de texte, les digits, et remplacer avec ` `.
3. Mettre en majuscule 
4. Extraction des stops word. Bien penser a extraire les stops words en prenant en compte les étapes précédentes, a savoir reconstruction de l'adresse, premier nettoyage et mise en majuscule
5. Creation d'une liste avec les mots de l'adrese.  Exemple: Adresse : "JONQUILLES JAUNES BORD MER" -> [JONQUILLES,JAUNES,BORD,MER]. 
6. Creation du pattern regex avec le `$` en fin de mot, séparé par `|`. Exemple `[PROFESSEUR, BERGONIE]` devient `PROFESSEUR$|BERGONIE$`. Cette variable est appelée `adresse_regex`
7. Nettoyer `adress_nettoyee` des accents, espaces en début de texte et mise en majuscule

# Tests d'acceptance

[PO : comment contrôler que la réalisation est conforme]

**Code reproduction**


- Prendre le csv suivant [inpi_ets_exemple_1](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/data/RawData/INPI/Stock/inpi_ets_exemple_1.csv) et vérifier d'avoir les memes valeurs pour la variable `adresse_regex`. Si les valeurs diffèrent pour certaines lignes, les indiquer dans un fichier Excel

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

    jupyter nbconvert --no-input --to md 03_prep_adresse_2690.ipynb
    Report Available at this adress:
     C:\Users\PERNETTH\Documents\Projects\InseeInpi_matching\Notebooks_matching\Data_preprocessed\programme_matching\05_redaction_US\US_md\03_prep_adresse_2690.md
    
