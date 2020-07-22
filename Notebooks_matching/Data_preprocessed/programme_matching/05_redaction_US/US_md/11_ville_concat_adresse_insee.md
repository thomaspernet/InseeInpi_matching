# Nettoyer la ville et adresse [INSEE]  

```
Entant que {X} je souhaite {nettoyer la variable ville et concatener adresse de la même manière que l’inpi} afin de {pouvoir la faire correspondre à l'INPI}
```

**Metadatab**

- Taiga:
    - Numero US: [2954](https://tree.taiga.io/project/olivierlubet-air/us/2954)
- Gitlab
    - Notebook: []()
    - Markdown: []()
    - Data:
        - []()
        - 

# Contexte

Le champs ville de l'INSEE est plutot bien normalisé, toutefois celui de l'INPI ne l'est pas. De fait, nous avons appliqué différentes règles sur la ville coté INPI pour faire correspondre au mieux avec l'INSEE. Dès lors, nous devons appliquer les même règles à l'INSEE pour faire matcher les deux champs (cf US [2613](https://tree.taiga.io/project/olivierlubet-air/us/2613))

L'INSEE a décomposé les informations de l'adresse en différent champs quand l'INPI n'a qu'un seul champs adresse déstructuré. Il faut donc reconstitué l'adresse à l'INSEE en concatenant les champs suivants:

- `numeroVoieEtablissement`: Numéro de voie établissement
- `indiceRepetitionEtablissement`: un indicateur de [répétition](https://www.sirene.fr/sirene/public/variable/indrep#:~:text=Indice%20de%20r%C3%A9p%C3%A9tition-,Indice%20de%20r%C3%A9p%C3%A9tition,les%20valeurs%20alphab%C3%A9tiques%20sont%20accept%C3%A9es.) du numéro dans la voie (Bis, Ter...)
- `voie_clean`: Type de voie non abrégé (US [2953](https://tree.taiga.io/project/olivierlubet-air/us/2953)
- `libelleVoieEtablissement`: information sur la localisation de l'adresse
- `complementAdresseEtablissement`: Information sur une potentielle adresse complémentaire

En plus de la concatenation des champs de l'adresse, nous appliquons les mêmes règles de nettoyages sur l'adresse de l'INPI, cf US [2690](https://tree.taiga.io/project/olivierlubet-air/us/2690).

Workflow US (via stock)

![workflow](https://www.lucidchart.com/publicSegments/view/d9e4494d-bfaf-4d0e-9e0f-53011cda7eb9/image.png)

# US / ISSUES liées

[PO & DEV : s'il existe des références, les inscrire]

# Besoin

Dans cette US, le besoin est le suivant:

- ville_matching
    - Nettoyage de la ville de l'INSEE (libelleCommuneEtablissement) de la même manière que l'INPI
- adress_reconstituee_insee:
    - Reconstitution de l'adresse à l'INSEE en utilisant le numéro de voie `numeroVoieEtablissement`, indicateur de répétition `indiceRepetitionEtablissement`,  le type de voie non abbrégé, `voie_clean`, l'adresse `libelleVoieEtablissement` et le `complementAdresseEtablissement` et suppression des article



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
    - `libelleCommuneEtablissement`
    - `numeroVoieEtablissement`
    - `indiceRepetitionEtablissement`
    - `voie_clean`
    - `libelleVoieEtablissement`
    - `complementAdresseEtablissement`

### Exemple Input 1

Exemple de normalisation d'origine de la ville à l'INSEE

| siren     | libellecommuneetablissement |
|-----------|-----------------------------|
| 827537077 | BRAX                        |
| 827537101 | SAINT-MAUR-DES-FOSSES       |
| 827537291 | IVRY-SUR-SEINE              |
| 827537358 | LE LUC                      |
| 827537895 | NIVOLAS-VERMELLE            |
| 827538125 | VALENCE                     |
| 827538257 | VENCE                       |
| 827538257 | CANNES                      |
| 827538406 | BANDOL                      |
| 827538489 | MANDELIEU-LA-NAPOULE        |

### Exemple Input 2

- Exemple 1: du découpage de l'adresse à l'INSEE. `indiceRepetitionEtablissement` vide

| siren     | numerovoieetablissement | voie_clean | libelleVoieEtablissement | complementAdresseEtablissement   |
|-----------|-------------------------|------------|--------------------------|----------------------------------|
| 827537077 | 1                       | PLACE      | DE L'EGLISE              |                                  |
| 827537101 | 111                     | AVENUE     | DU CENTENAIRE            |                                  |
| 827537291 | 11                      | PROMENADE  | DU LIEGAT                |                                  |
| 827537358 | 10                      | AVENUE     | GABRIEL BARBAROUX        |                                  |
| 827537895 | 17                      | AVENUE     | DU SQUARE                | LES LOGGIAS DU SQUARE            |
| 827538125 | 82                      | ALLEE      | BERNARD PALISSY          |                                  |
| 827538257 | 822                     | AVENUE     | RHIN ET DANUBE           |                                  |
| 827538257 | 27                      | AVENUE     | DU CAMP LONG             | RES LE PLAISANCE ENTREE D APT 47 |
| 827538406 | 187                     | AVENUE     | MAL FOCH                 |                                  |
| 827538489 | 410                     | AVENUE     | JANVIER PASSERO          | RESIDENCE LES 3 RIVIERES D10 21  |


- Exemple 2: du découpage de l'adresse à l'INSEE. `indiceRepetitionEtablissement` non vide

| siren     | numerovoieetablissement | indiceRepetitionEtablissement | voie_clean | libelleVoieEtablissement | complementAdresseEtablissement |
|-----------|-------------------------|-------------------------------|------------|--------------------------|--------------------------------|
| 827542374 | 7                       | B                             | AVENUE     | DE L ABBAYE BLANCHE      |                                |
| 827558255 | 11                      | B                             | AVENUE     | PHILIPPE LE BOUCHER      |                                |
| 827567496 | 101                     | B                             | ALLEE      | GEORGES BRASSENS         |                                |
| 827589367 | 158                     | B                             | AVENUE     | DE LA BOETIE             |                                |
| 827598939 | 1                       | A                             | AVENUE     | DE MONTRAPON             | 1 A                            |
| 827604802 | 72                      | B                             | AVENUE     | JULES VALLES             |                                |
| 827609363 | 18                      | B                             | AVENUE     | EMILE BOISSIER           |                                |
| 827618901 | 89                      | B                             | AVENUE     | DU SERS                  |                                |
| 827639345 | 1                       | B                             | ALLEE      | DES TOURMOTTES           |                                |
| 827668989 | 36                      | B                             | AVENUE     | PIERRE SEMARD            |                                |

## Output

[PO : dans le cas de transformation de données, préciser les sorties :

*   BDD cibles
*   Tables: `inpi_etablissement_historique`
*   Champs: 

]

### Exemple output 1: Nettoyage de la ville 

| siren     | libellecommuneetablissement | ville_matching |
|-----------|-----------------------------|----------------|
| 425166808 | SOORTS-HOSSEGOR             | SOORTSHOSSEGOR |
| 425166972 | LABENNE                     | LABENNE        |
| 425167350 | CAPBRETON                   | CAPBRETON      |
| 425167350 | CAPBRETON                   | CAPBRETON      |
| 425167723 | CAPBRETON                   | CAPBRETON      |
| 425167970 | CAPBRETON                   | CAPBRETON      |
| 425168069 | CAPBRETON                   | CAPBRETON      |
| 425168325 | GELOUX                      | GELOUX         |
| 425168499 | CLERMONT                    | CLERMONT       |
| 425168523 | GAUJACQ                     | GAUJACQ        |

### Exemple output 2: Reconstitution ville + nettoyage `indiceRepetitionEtablissement` vide

| siren     | numerovoieetablissement | indiceRepetitionEtablissement | voie_clean | libelleVoieEtablissement | complementAdresseEtablissement   | adress_reconstituee_insee                              |
|-----------|-------------------------|-------------------------------|------------|--------------------------|----------------------------------|--------------------------------------------------------|
| 827537077 | 1                       |                               | PLACE      | DE L'EGLISE              |                                  | 1 PLACE L EGLISE                                       |
| 827537101 | 111                     |                               | AVENUE     | DU CENTENAIRE            |                                  | 111 AVENUE CENTENAIRE                                  |
| 827537291 | 11                      |                               | PROMENADE  | DU LIEGAT                |                                  | 11 PROMENADE LIEGAT                                    |
| 827537358 | 10                      |                               | AVENUE     | GABRIEL BARBAROUX        |                                  | 10 AVENUE GABRIEL BARBAROUX                            |
| 827537895 | 17                      |                               | AVENUE     | DU SQUARE                | LES LOGGIAS DU SQUARE            | 17 AVENUE SQUARE LOGGIAS SQUARE                        |
| 827538125 | 82                      |                               | ALLEE      | BERNARD PALISSY          |                                  | 82 ALLEE BERNARD PALISSY                               |
| 827538257 | 822                     |                               | AVENUE     | RHIN ET DANUBE           |                                  | 822 AVENUE RHIN DANUBE                                 |
| 827538257 | 27                      |                               | AVENUE     | DU CAMP LONG             | RES LE PLAISANCE ENTREE D APT 47 | 27 AVENUE CAMP LONG RES PLAISANCE ENTREE D APT 47      |
| 827538406 | 187                     |                               | AVENUE     | MAL FOCH                 |                                  | 187 AVENUE MAL FOCH                                    |
| 827538489 | 410                     |                               | AVENUE     | JANVIER PASSERO          | RESIDENCE LES 3 RIVIERES D10 21  | 410 AVENUE JANVIER PASSERO RESIDENCE 3 RIVIERES D10 21 |

### Exemple output 3: Reconstitution ville + nettoyage `indiceRepetitionEtablissement` non vide

| siren     | numerovoieetablissement | indiceRepetitionEtablissement | voie_clean | libelleVoieEtablissement | complementAdresseEtablissement | adress_reconstituee_insee    |
|-----------|-------------------------|-------------------------------|------------|--------------------------|--------------------------------|------------------------------|
| 412182867 | 1                       | B                             | BOULEVARD  | COTTE                    |                                | 1 BIS BOULEVARD COTTE           |
| 412221111 | 4                       | T                             |            | BOURG SUD                |                                | 4 TER BOURG SUD                 |
| 412225179 | 142                     | T                             | BOULEVARD  | DE STRASBOURG            |                                | 142 TER BOULEVARD STRASBOURG    |
| 844302679 | 19                      | T                             | BOULEVARD  | DE L OUEST               |                                | 19 TER BOULEVARD L OUEST        |
| 844330514 | 250                     | B                             | BOULEVARD  | SAINT GERMAIN            |                                | 2501 BIS BOULEVARD SAINT GERMAIN |
| 844351742 | 2                       | B                             |            | VENELLE DU TERTRE        |                                | 21 BIS VENELLE TERTRE            |
| 844402586 | 6                       | A                             |            | MAIL DE BOURGCHEVREUIL   |                                | 6A MAIL BOURGCHEVREUIL       |
| 844422337 | 5012                    | B                             |            | CONDAMINES               |                                | 50121 BIS CONDAMINES             |
| 844426759 | 250                     | B                             | BOULEVARD  | SAINT GERMAIN            |                                | 2501 BIS BOULEVARD SAINT GERMAIN |
| 844434704 | 37                      | B                             | BOULEVARD  | SUCHET                   |                                | 371 BIS BOULEVARD SUCHET         |

## Règles de gestion applicables

[PO : Formules applicables]

Si nouvelle règle, ajouter ici.

Ensemble des règles de nettoyages pour les variables adresse

| Table | Variable | article | digit | debut/fin espace | espace | accent | Upper |
| --- | --- | --- | --- | --- | --- | --- | --- |
| INPI | adresse_regex_inpi | X | X | X | X | X | X |
|  | adress_distance_inpi | X |  | X | X | X | X |
|  | adresse_reconstituee_inpi |  |  | X | X | X | X |
| INSEE | adress_reconstituee_insee | X |  |  |  |  |  |

- Les règles de nettoyage de la ville de l'INSEE doivent être identiques à l'US [2613](https://tree.taiga.io/project/olivierlubet-air/us/2613)
- Les règles de nettoyage de l'adresse de l'INSEE doivent être identiques à l'US [2613](https://tree.taiga.io/project/olivierlubet-air/us/2690)
- Pour la variable `indiceRepetitionEtablissement`, voici la liste des modalités:

    - B	bis
    - T	ter
    - Q	quater
    - C	quinquies
    - On peut avoir aussi exceptionnellement des répétitions séquentielles A, B, C, D,.....


# Charges de l'équipe

[

PO : Si des étapes particulières / des points d'attention sont attendus, être aussi explicite que possible

Spécifiquement pour l'intégration de nouvelles données dans DATUM :

*   Nombre de lignes chargées pour chaque nouvelle table
*   Poids de chaque nouvelle table
*   Durée du traitement ajouté (+ durée avant et après)

]

* Il faut bien penser a prendre la variable indiceRepetitionEtablissement  juste après la variable numeroVoieEtablissement  pour indiquer si il y a un numéro de porte, batiment, etc et l'indice de répétition. Mettre un espace entre les variables.
* Concatenation adresse INSEE.
  * Besoin du coalerse, sinon le concat engendre un blanc si voie_clean est empty
  * [How to exclude NULL values inside CONCAT MySQL?](https://dba.stackexchange.com/questions/110949/how-to-exclude-null-values-inside-concat-mysql)
  
```  
CONCAT(
                         COALESCE(numeroVoieEtablissement,''),
                        COALESCE(indiceRepetitionEtablissement,''),
                        ' ',
                        COALESCE(voie_clean,''), ' ',  -- besoin sinon exclu
                        COALESCE(libelleVoieEtablissement,''), ' ',
                        COALESCE(complementAdresseEtablissement,''
                      )
``` 

# Tests d'acceptance

[PO : comment contrôler que la réalisation est conforme]

- Imprimer des exemples de reconstruction lorsque:
    - `indiceRepetitionEtablissement` est vide
    - `indiceRepetitionEtablissement` n'est pas vide
    - `voie_clean` est vide
    - `voie_clean` n'est pas vide
    - `complementAdresseEtablissement` est vide
    - `complementAdresseEtablissement` n'est pas vide
    
Pour l'ensemble des cas, vérifier si la concatenation a été faite correctement. 

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
