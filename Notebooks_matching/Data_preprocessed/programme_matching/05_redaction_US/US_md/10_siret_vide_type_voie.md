# Enlever les SIRET vide et ajouter le nom complet du type de voie [INSEE] 

```
Entant que {X} je souhaite {enlever les siret vides de la table INSEE et récupérer le type de voie non abrégé} afin de {pouvoir faire la correspondance avec la table INPI}
```

**Metadatab**

- Taiga:
    - Numero US: [2953](https://tree.taiga.io/project/olivierlubet-air/us/2953)
- Gitlab
    - Notebook: []()
    - Markdown: []()
    - Data:
        - []()
        - 

# Contexte

Maintenant que nous avons fini la préparation de la table de l'INPI, nous sommes en mesure de créer les variables nécéssaires à la siretisation dans la table de l'INSEE. En règle générale, la table de l'INSEE est davantage normalisée que celle de l'INPI, surtout concernant l'adresse.

Comme nous le savons déjà, l'adresse a l'INPI peut contenir des types de voies, telle que RUE, AVENUE, BOULEVARD, etc or l'INSEE ne donne que l'abréviation. Veuillez faire référence à l'US [2697](https://tree.taiga.io/project/olivierlubet-air/us/2697) pour connaitre les différents types à l'INSEE, et leur contrepartie possible à l'INPI. 

Pour faire rapprochement entre les deux tables avec le plus de précision possible, nous devons recréer une variable adresse qui contient les mêmes, ou quasiment les mêmes informations. Ainsi, dans ce premier US sur la modification de la table de l'INSEE, nous allons ajouter le type de voie non abrégé. 

De plus, pour éviter de loader toute la table de l'INSEE, qui fait plus de 6GB, nous enlevons l'ensemble des SIREN n'ayant pas de SIRET. Il n'y a aucune utilité a  garder des SIREN dont on est sur qu'ils n'ont pas de SIRET. 

Workflow US (via stock)

![workflow](https://www.lucidchart.com/publicSegments/view/d9e4494d-bfaf-4d0e-9e0f-53011cda7eb9/image.png)

# US / ISSUES liées

[PO & DEV : s'il existe des références, les inscrire]

# Besoin

Dans cette US, le besoin est le suivant:

- Enlever les SIREN n'ayant pas de siret
- voie_clean
    - Ajout de la variable non abbrégée du type de voie. Exemple, l'INSEE indique CH, pour chemin, il faut donc indiquer CHEMIN



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
    - `siret``
    - `typevoieetablissement`



### Exemple Input 1

La table ci-dessous est un extrait de la table de l'INSEE, ou nous avons filtré les siren n'ayant pas de siret

| siren     | nic | siret |
|-----------|-----|-------|
| 828907956 | O   |       |
| 828907964 | O   |       |
| 828907972 | O   |       |
| 828907980 | O   |       |
| 828907998 | O   |       |
| 828908004 | O   |       |
| 828908020 | O   |       |
| 828908038 | O   |       |
| 828908046 | O   |       |
| 828908053 | O   |       |

### Exemple Input 2

La table ci-dessous contient des exemples du type de voie abrégé à l'INSEE. Par exemple, `AV` est l'abréviation de `AVENUE`.

| siren     | nic   | siret          | typevoieetablissement |
|-----------|-------|----------------|-----------------------|
| 315497446 | 00015 | 31549744600015 |                       |
| 315497453 | 00037 | 31549745300037 | IMP                   |
| 315497453 | 00045 | 31549745300045 | RUE                   |
| 315497461 | 00030 | 31549746100030 | AV                    |
| 315497461 | 00048 | 31549746100048 | AV                    |
| 315497487 | 00035 | 31549748700035 |                       |
| 315497495 | 00012 | 31549749500012 | RTE                   |
| 315497495 | 00020 | 31549749500020 | RUE                   |
| 315497503 | 00013 | 31549750300013 | RUE                   |
| 315497511 | 00016 | 31549751100016 | RUE                   |

## Output

[PO : dans le cas de transformation de données, préciser les sorties :

*   BDD cibles
*   Tables: `inpi_etablissement_historique`
*   Champs: 

]

Ci dessous, un apperçu de la table INSEE après avoir supprimé les siret non vides et matché la table avec le type de voie

| siren     | siret          | typevoieetablissement | voie_clean |
|-----------|----------------|-----------------------|------------|
| 390423671 | 39042367100010 | AV                    | AVENUE     |
| 390423705 | 39042370500016 | PL                    | PLACE      |
| 390424745 | 39042474500011 | PL                    | PLACE      |
| 390425668 | 39042566800014 | PL                    | PLACE      |
| 390425775 | 39042577500017 | PL                    | PLACE      |
| 390425775 | 39042577500025 | PL                    | PLACE      |
| 390425908 | 39042590800014 | PL                    | PLACE      |
| 390426328 | 39042632800030 | PL                    | PLACE      |
| 390426575 | 39042657500010 | PL                    | PLACE      |
| 390428779 | 39042877900024 | AV                    | AVENUE     |

## Règles de gestion applicables

[PO : Formules applicables]

Si nouvelle règle, ajouter ici.

Lors de nos tests sur l'INSEE, nous avons utilisé uniquement les variables suivantes présents dans la donnée brute de l'INSEE:

- `siren`
- `siret` 
- `dateCreationEtablissement`
- `etablissementSiege`
- `etatAdministratifEtablissement`
- `complementAdresseEtablissement`
- `numeroVoieEtablissement`
- `indiceRepetitionEtablissement`
- `typeVoieEtablissement`
- `libelleVoieEtablissement`
- `codePostalEtablissement`
- `libelleCommuneEtablissement`
- `libelleCommuneEtrangerEtablissement`
- `distributionSpecialeEtablissement`
- `codeCommuneEtablissement`
- `codeCedexEtablissement`
- `libelleCedexEtablissement`
- `codePaysEtrangerEtablissement`
- `libellePaysEtrangerEtablissement`
- `enseigne1Etablissement`
- `enseigne2Etablissement`
- `enseigne3Etablissement`

# Charges de l'équipe

[

PO : Si des étapes particulières / des points d'attention sont attendus, être aussi explicite que possible

Spécifiquement pour l'intégration de nouvelles données dans DATUM :

*   Nombre de lignes chargées pour chaque nouvelle table
*   Poids de chaque nouvelle table
*   Durée du traitement ajouté (+ durée avant et après)

]

Code SQL que nous avons utilisé

```
query = """
WITH remove_empty_siret AS (
SELECT
siren,
siret, 
regexp_like(siret, '\d+') as test_siret,
dateCreationEtablissement,
         etablissementSiege,
         etatAdministratifEtablissement,
         complementAdresseEtablissement,
         numeroVoieEtablissement,
         indiceRepetitionEtablissement,
         typeVoieEtablissement,
         -- type_voie.voie_clean,
         libelleVoieEtablissement,
         codePostalEtablissement,
         libelleCommuneEtablissement,
         libelleCommuneEtrangerEtablissement,
         distributionSpecialeEtablissement,
         codeCommuneEtablissement,
         codeCedexEtablissement,
         libelleCedexEtablissement,
         codePaysEtrangerEtablissement,
         libellePaysEtrangerEtablissement,
         enseigne1Etablissement,
         enseigne2Etablissement,
         enseigne3Etablissement
FROM insee_rawdata 
 )
SELECT  
siren,
siret, 
dateCreationEtablissement,
         etablissementSiege,
         etatAdministratifEtablissement,
         codePostalEtablissement,
         codeCommuneEtablissement,
         libelleCommuneEtablissement,
         libelleVoieEtablissement,
         complementAdresseEtablissement,
         numeroVoieEtablissement,
         indiceRepetitionEtablissement,
         typeVoieEtablissement,
         type_voie.voie_clean,
         enseigne1Etablissement,
         enseigne2Etablissement,
         enseigne3Etablissement
FROM remove_empty_siret
LEFT JOIN type_voie 
ON remove_empty_siret.typevoieetablissement = type_voie.voie_matching
WHERE test_siret = true
LIMIT 10
"""
``` 

# Tests d'acceptance

[PO : comment contrôler que la réalisation est conforme]

- Comptez le nombre d'obs à l'INSEE
- Compter le nombre de valeurs sans siret. Lors de nos tests, nous avons trouvé:

| test_siret | count    |
|------------|----------|
| false      | 21144057 |
| true       | 29384505 |

- Vérifier que la nouvelle table fait le même nombre que de ligne `false`
- Compter le nomdre de `typeVoieEtablissement` et de  `voie_clean`. Verrifez que le compte est identique dans les deux cas

| typeVoieEtablissement | count_typeVoieEtablissement | voie_clean      | count_voie_clean |
|-----------------------|-----------------------------|-----------------|------------------|
| PRO                   | 16577                       | PROMENADE       | 16577            |
|                       | 4847467                     |                 | 0                |
| CHEM                  | 4                           |                 | 0                |
| PL                    | 1106487                     | PLACE           | 1106487          |
| ALL                   | 597161                      | ALLEE           | 597161           |
| COR                   | 5752                        | CORNICHE        | 5752             |
| AV                    | 3384218                     | AVENUE          | 3384218          |
| RUE                   | 13685668                    | RUE             | 13685668         |
| IMP                   | 378337                      | IMPASSE         | 378337           |
| CITE                  | 53354                       | CITE            | 53354            |
| SQ                    | 91176                       | SQUARE          | 91176            |
| ROC                   | 1018                        | ROCADE          | 1018             |
| PLN                   | 330                         | PLAINE          | 330              |
| TPL                   | 14                          | TERRE PLEIN     | 14               |
| VGE                   | 1                           |                 | 0                |
| QUA                   | 56275                       | QUARTIER        | 56275            |
| VLGE                  | 427                         | VILLAGE         | 427              |
| PLT                   | 332                         | PLATEAU         | 332              |
| CRS                   | 177691                      | COURS           | 177691           |
| LD                    | 486410                      | LIEU DIT        | 486410           |
| QUAI                  | 216787                      | QUAI            | 216787           |
| ESP                   | 12479                       | ESPLANADE       | 12479            |
| GR                    | 26688                       | GRANDE RUE      | 26688            |
| PAS                   | 58993                       | PASSAGE         | 58993            |
| DSC                   | 368                         | DESCENTE        | 368              |
| BD                    | 1295896                     | BOULEVARD       | 1295896          |
| LOT                   | 103128                      | LOTISSEMENT     | 103128           |
| SEN                   | 7941                        | SENTE   SENTIER | 7941             |
| CHS                   | 11846                       | CHAUSSEE        | 11846            |
| TRA                   | 29784                       | TRAVERSE        | 29784            |
| CAR                   | 2080                        | CARREFOUR       | 2080             |
| VLA                   | 19627                       | VILLA           | 19627            |
| RUE                   | 2                           |                 | 0                |
| CHE                   | 1090458                     | CHEMIN          | 1090458          |
| RES                   | 117332                      | RESIDENCE       | 117332           |
| RTE                   | 1344864                     | ROUTE           | 1344864          |
| RPT                   | 12124                       | ROND POINT      | 12124            |
| HLE                   | 656                         | HALLE           | 656              |
| MAR                   | 867                         | MARCHE          | 867              |
| DOM                   | 24380                       | DOMAINE         | 24380            |
| QUAI                  | 16                          |                 | 0                |
| CI                    | 1                           |                 | 0                |
| HAM                   | 57197                       | HAMEAU          | 57197            |
| FG                    | 24247                       | FAUBOURG        | 24247            |
| R                     | 15                          |                 | 0                |
| CAMI                  | 9                           |                 | 0                |
| LDT                   | 9                           |                 | 0                |
| RLE                   | 14515                       | RUELLE          | 14515            |
| ECA                   | 412                         | ECART           | 412              |
| PRV                   | 1184                        | PARVIS          | 1184             |
| MTE                   | 21901                       | MONTEE          | 21901            |


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
