# Preparation voie et numéro de voie 

```
Entant que {X} je souhaite {preparer les variables voies et numero de voie} afin de {pouvoir les faire correspondre à l'INSEE}
```

**Metadatab**

- Taiga:
    - Numero US: [2697](https://tree.taiga.io/project/olivierlubet-air/us/2697)
- Gitlab
    - Notebook: [04_prep_voie_num_2697](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/05_redaction_US/04_prep_voie_num_2697.ipynb)
    - Markdown: [04_prep_voie_num_2697](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/05_redaction_US/04_prep_voie_num_2697.md)
    - Data:
        - [inpi_ets_exemple_1_2697](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/data/RawData/INPI/Stock/inpi_ets_exemple_1_2697.csv)
        - [typeVoieEtablissement](https://www.sirene.fr/sirene/public/variable/typeVoieEtablissement)

# Contexte

La dernière étape de la création des variables de matching consiste à utiliser la variable `adresse_nettoyée` (US 2690) pour extraire le premier numéro de voie et extraire le type de voie. Il faut noter que la règle de gestion concernant le numéro de voie ne concerne que le premier numéro. Le regex ne va pas extraire tous les numéros de voie présent dans l'adresse, uniquement le premier. Lors de nos tests, nous avons essayé de créer une règle spéciale qui extrait, tous les numéros de voie, puis match avec l'INSEE. La règle améliore le matching, mais rend plus complexe le processus. 

L'extraction du type de voie se fait grâce aux informations présentes dans le site de l'[INSEE](https://www.sirene.fr/sirene/public/variable/typeVoieEtablissement). En effet, le type de voie est codifié à l'INSEE, et nous utilisons cette codification pour extraire l'information présente dans la variable `adresse_nettoyée`.

Les deux variables crééent dans cette US vont être utilisée lors du matching

```
{'ville_matching', 'code_postal_matching', 'Code_Commune', 'voie_matching', 'numero_voie_matching'},
 {'ville_matching', 'code_postal_matching', 'Code_Commune', 'voie_matching'},
 {'ville_matching', 'code_postal_matching', 'Code_Commune', 'numero_voie_matching'},
 {'ville_matching', 'code_postal_matching', 'Code_Commune'},   
 {'ville_matching', 'code_postal_matching'},
 {'ville_matching'},
 {'code_postal_matching'},
 {'Code_Commune'}
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
- Le code postal doit être formalisé correctement, a savoir deux longueurs possibles: zero (Null) ou cinq. Dans certains cas, le code postal se trouve dans la variable de la ville.
- La variable pays doit être formalisée, a savoir correspondre au code pays de l'INSEE. Bien que la majeure partie des valeurs soit FRANCE ou France, il convient de normaliser la variable pour récuperer les informations des pays hors France.
- Les variables de l'adresse de l'INPI ne sont pas normalisées, et ne peuvent être utilisées en l'état. Il est donc indispensable de retravailler les variables adresse pour pouvoir les comparer avec l'INSEE. Nous utilisons une règle (pattern) regex pour vérifier si les mots contenus dans l'adresse de l'INPI sont aussi contenus à l'INSEE.
- [NEW] L'INSEE codifie le type de voie de la manière suivante:
    - Si le type de voie est d'une longueur inférieure ou égale à 4 caractères, le type de voie n'est pas abrégé. Ainsi, RUE ou QUAI sont écrits tels quels, alors que AVENUE est abrégée en AV.
    - La codification de l'INSEE va donc être utilisé ppur créer la variable `voie_matching`
- [NEW] Pour ne pas complexifié le processus de siretisation, seule le premier numéro de voie contenu dans l'adresse nettoyée est extraite pour ensuite etre matché avec l'INSEE.

Workflow US (via stock)

![workflow](https://www.lucidchart.com/publicSegments/view/d9e4494d-bfaf-4d0e-9e0f-53011cda7eb9/image.png)

# US / ISSUES liées

[PO & DEV : s'il existe des références, les inscrire]

# Besoin

Dans cette US, le besoin est le suivant:

- La création d'une variable `numero_voie_matching` contenant le premier numéro de voie contenu dans l'adresse nettoyée créé dans l'US 2690
- La création d'une variable `voie_matching` contenant le type de voie correspondant à la codification de l'INSEE



# Spécifications

### Origine information (si applicable) 

- Metadata:
    - Type: [Documentation] -> [CSV]
    - Source: [INSEE](https://www.sirene.fr/sirene/public/variable/typeVoieEtablissement) & [Gitlab](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/data/input/RawParameters/typeVoieEtablissement.csv)
    - Summary: Type de voie.C'est un élément constitutif de l'adresse à l'INSEE. Si le type de voie est d'une longueur inférieure ou égale à 4 caractères, le type de voie n'est pas abrégé. Ainsi, RUE ou QUAI sont écrits tels quels, alors que AVENUE est abrégée en AV.
    - Type: [CSV]
    - Source: [Gitlab](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/data/RawData/INPI/Stock/inpi_ets_exemple_1_2697.csv)
    - Summary: Echantillon aléatoire de 3000 observations récupérées de notre table ETS et qu'y a été enrichie de deux variables créént lors de l'US sur la preparation du pattern regex
    
## Input

[PO : dans le cas de transformation de données, préciser ,les sources :

*   Applications
*   Schémas
*   Tables: `inpi_etablissement_historique`
*   CSV: [typeVoieEtablissement](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/data/input/RawParameters/typeVoieEtablissement.csv)
*   Champs: [TABLE] `adress_nettoyee`, [CSV] `voie_clean`



### Exemple Input 1

L'exemple ci-dessous provient du csv que nous avons crée via les informations disponibles sur le site de l'[INSEE](https://www.sirene.fr/sirene/public/variable/typeVoieEtablissement). Ensuite, nous avons appliqué un code regex qui est indique à celui employé dans l'US 2690 pour nettoyé le champs `possibilité`. Ce champs contient le libellé du type de voie à l'INSEE. C'est un champs généraliste et permet de récuperer un bon nombre d'information dans l'adresse nettoyée de l'INPI

**Snippet**

- [Snippet 1](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/snippets/64)

    |    | voie_matching   | possibilite   | voie_clean   |
    |---:|:----------------|:--------------|:-------------|
    |  0 | ALL             | Allée         | ALLEE        |
    |  1 | AV              | Avenue        | AVENUE       |
    |  2 | BD              | Boulevard     | BOULEVARD    |
    |  3 | CAR             | Carrefour     | CARREFOUR    |
    |  4 | CHE             | Chemin        | CHEMIN       |
    

### Exemple Input 2

Dans l'exemple d'input ci dessous, nous avons pris une image de ce que la table `inpi_etablissement_historique` doit ressemblée après avoir réalisé l'US 2690. 


    |    |     siren | adresse_ligne1                | adresse_ligne2                  | adresse_ligne3   | adress_nettoyee                 | adresse_regex                |
    |---:|----------:|:------------------------------|:--------------------------------|:-----------------|:--------------------------------|:-----------------------------|
    |  0 | 487622797 | nan                           | 7 Rue Caraman                   | BP 67131         | 7 RUE CARAMAN BP 67131          | CARAMAN$                     |
    |  1 | 841846488 | 43 Rue du Professeur Bergonié | nan                             | nan              | 43 RUE DU PROFESSEUR BERGONIE   | PROFESSEUR$|BERGONIE$        |
    |  2 | 324958198 | nan                           | 6 Zone Industrielle les Gabares | nan              | 6 ZONE INDUSTRIELLE LES GABARES | ZONE$|INDUSTRIELLE$|GABARES$ |
    |  3 | 812461218 | 7 Rue de l'Ancienne Eglise    | nan                             | nan              | 7 RUE DE L ANCIENNE EGLISE      | ANCIENNE$|EGLISE$            |
    |  4 | 850414509 | 2 Rue des Maronniers          | nan                             | nan              | 2 RUE DES MARONNIERS            | MARONNIERS$                  |
    

## Output

[PO : dans le cas de transformation de données, préciser les sorties :

*   BDD cibles
*   Tables: `inpi_etablissement_historique`
*   Champs: `numero_voie_matching` et `voie_matching`

]

Le tableau ci dessous explicite les deux variables attendues, a savoir `numero_voie_matching` et `voie_matching`. Les deux variables vont utiliser la variable de l'adresse reconstituée (`adress_nettoyee`) pour extraire le premier numéro de voie, et le type de voie comme celui de l'INSEE.

- La variable `numero_voie_matching` est le premier digit présent dans la variable `adress_nettoyee`.
- La variable `voie_matching` est l'extraction du type de voie présent dans la variable `adress_nettoyee`, puis normalisé des valeurs possibles à l'INSEE.

    |    |     siren | adresse_ligne1                | adresse_ligne2                  | adresse_ligne3   | adress_nettoyee                 | adresse_regex                |   numero_voie_matching | voie_matching   |
    |---:|----------:|:------------------------------|:--------------------------------|:-----------------|:--------------------------------|:-----------------------------|-----------------------:|:----------------|
    |  0 | 487622797 | nan                           | 7 Rue Caraman                   | BP 67131         | 7 RUE CARAMAN BP 67131          | CARAMAN$                     |                      7 | RUE             |
    |  1 | 841846488 | 43 Rue du Professeur Bergonié | nan                             | nan              | 43 RUE DU PROFESSEUR BERGONIE   | PROFESSEUR$|BERGONIE$        |                     43 | RUE             |
    |  2 | 324958198 | nan                           | 6 Zone Industrielle les Gabares | nan              | 6 ZONE INDUSTRIELLE LES GABARES | ZONE$|INDUSTRIELLE$|GABARES$ |                      6 | nan             |
    |  3 | 812461218 | 7 Rue de l'Ancienne Eglise    | nan                             | nan              | 7 RUE DE L ANCIENNE EGLISE      | ANCIENNE$|EGLISE$            |                      7 | RUE             |
    |  4 | 850414509 | 2 Rue des Maronniers          | nan                             | nan              | 2 RUE DES MARONNIERS            | MARONNIERS$                  |                      2 | RUE             |
    

## Règles de gestion applicables

[PO : Formules applicables]

- [NEW] L'INSEE codifie le type de voie de la manière suivante:
    - Si le type de voie est d'une longueur inférieure ou égale à 4 caractères, le type de voie n'est pas abrégé. Ainsi, RUE ou QUAI sont écrits tels quels, alors que AVENUE est abrégée en AV.
    - La codification de l'INSEE va donc être utilisé ppur créer la variable `voie_matching`
- [NEW] Pour ne pas complexifié le processus de siretisation, seule le premier numéro de voie contenu dans l'adresse nettoyée est extraite pour ensuite etre matché avec l'INSEE.

# Charges de l'équipe

[

PO : Si des étapes particulières / des points d'attention sont attendus, être aussi explicite que possible

Spécifiquement pour l'intégration de nouvelles données dans DATUM :

*   Nombre de lignes chargées pour chaque nouvelle table
*   Poids de chaque nouvelle table
*   Durée du traitement ajouté (+ durée avant et après)

]

Pour construire les deux variables `numero_voie_matching` et `voie_matching`, nous avons utilisé un script Python qui réalise les étapes séquentielles suivantes:

1. Création de la variable `voie_clean` dans le csv provenant de l'INSEE. C'est la même règle regex que l'US 2690
    - Extraction des accents, des espaces en début de texte, les digits, et remplacer avec ` `.
    - Mettre en majuscule 
2. Création de la variable `numero_voie_matching` en utilisant une fonction regex qui récupère le premier digit de la variable `adress_nettoyee`
3. Création de la variable `voie_clean` qui utilise le csv provenant de l'INSEE afin d'extraire les possibilités de type de voie. Les possibilité sont listées dans le CSV provenant de l'INSEE, variable `voie_clean`
4. Création de la variable `voie_matching` qui est crée en réalisant un `left_join` avec le csv provenant de l'INSEE, sur la variable `voie_clean`
5. Drop des variables `voie_clean` et `possibilite`

Le code python est disponible dans le snippet [suivant](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/snippets/63), et code regex pour créer la variable `voie_clean` dans le csv provenant de l'INSEE est disponible dans le snippet [suivant](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/snippets/64)

# Tests d'acceptance

[PO : comment contrôler que la réalisation est conforme]

**Code reproduction**

- Prendre le csv suivant [inpi_ets_exemple_1](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/data/RawData/INPI/Stock/inpi_ets_exemple_1_2697.csv) et vérifier d'avoir les memes valeurs pour les variables `numero_voie_matching` et `voie_matching`. Si les valeurs diffèrent pour certaines lignes, les indiquer dans un fichier Excel.
- Il faut être sur d'avoir le même nombre d'observation en entrée et en sortie


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
