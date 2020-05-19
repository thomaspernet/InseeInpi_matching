En tant que {X} je souhaite {créer le statut partiel et remplir des événements} afin de {d'ignorer les observations précédentes pour une séquence et obtenir une séquence avec les informations a jour}

# Contexte

Nous savons déjà que tous les csv incluent dans le dossier stock pour une année strictement supérieure a 2017 sont considérés comme des "événements" partiels. Un événement partiel va avoir la particularité de remettre a plat l'ensemble des fichiers précédemment intégrés dans la base en corrigeant les erreurs du greffe. Autrement dit, la ligne partiel va devenir la nouvelle ligne de référence pour la séquence. Création et événements antérieurs seront a ignorer. C’est le cas en particulier lorsque il y a incohérence entre des identifiants qui auraient été livrés dans le stock initial et ceux livrés dans le flux (ex. fichiers des établissements, représentants, observations) pour un même dossier (siren/numéro de gestion/code greffe). C’est également le cas de dossiers qui auraient été absents du stock initial et qui seraient retransmis après un délai.

Dès lors, nous pouvons dégager une nouvelle règle de gestion.

## Règles de gestion

 - Définition partiel
   - si csv dans le dossier Stock, année > 2017, alors partiel, c'est a dire, modification complète du dossier due a une anomalie.
   - la date d’ingestion est indiquée dans le path, ie comme les flux

 - Une séquence est un classement chronologique pour le quadruplet suivant:

   - *siren* + *code greffe* + *numero gestion* + *ID établissement*

- Evénement 1
  - Les événements doivent impérativement suivre l'ordre d'apparition dans le csv du FTP
    - Pour les événements, il est possible d'avoir plusieurs informations renseignées pour une même date de transmission pour une même séquence
  - Le remplissage doit se faire de la manière suivante pour la donnée brute
    - Pour une date de transmission donnée, c'est la dernière ligne de la séquence qui doit être utilisée, remplie des valeurs manquantes extraites des lignes précédentes. Si la dernière ligne de la séquence contient un champs non vide, il ne faut pas la remplacer par la ligne précédente.
- [NEW] Partiel
  - En cas de corrections majeures, la séquence annule et remplace la création et événements antérieurs. Dans ce cas, toutes les données qui ont pu être transmises antérieurement via le stock initial ou le flux doivent donc être ignorées (prendre en compte la date de transmission indiquée dans le nom des sous-répertoires du stock et des fichiers


Workflow US  (via stock)

![workflow](https://www.lucidchart.com/publicSegments/view/d9e4494d-bfaf-4d0e-9e0f-53011cda7eb9/image.png)

Dans cet US, en plus de devoir gérer le statut partiel, il faut remplir les informations manquantes dans la table `inpi_etablissement_evenement` en utilisant les informations de la table `inpi_etablissement_consolide`. Lors de l'ingestion de la data quotidienne, cette recréation peut se faire avec la table `inpi_etablissement_historique`  puisque les informations auront été recréé au préalable.

Workflow Global (via delta)

![](https://app.lucidchart.com/publicSegments/view/9e73b3ff-1648-4cda-ab7c-204290721629/image.png)

]

# US / ISSUES liées

[PO & DEV : s'il existe des références, les inscrire]

# Besoin

[PO : Situation attendue]

# Spécifications

## Input

[PO : dans le cas de transformation de données, préciser ,les sources :

*   Applications
*   Schémas
*   Tables: `inpi_etablissement_consolide` + `inpi_etablissement_evenement`
*   Champs: Schema potentiel: cf [json Gitlab](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/US_Datum/Schema_fields/US_2234/fields_2234.json) +  `statut`

]

### Exemple input partiel

Siren **005520242**

[Gitlab](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/US_Datum/US_5_partiel.md#exemple-input)

| origin  | siren     | code greffe | nom_greffe | numero_gestion | id_etablissement | file_timestamp | libelle_evt          | date_greffe | siège_pm | activité                                                                                                                                                                                                                                                                        | date_début_activité | origine_fonds | origine_fonds_info | type_exploitation    | csv_source                 |
|---------|-----------|-------------|------------|----------------|------------------|----------------|----------------------|-------------|----------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------|---------------|--------------------|----------------------|----------------------------|
| Initial | 005520242 | 8002        | Amiens     | 1955B70024     | 1                | 2016-06-23     | Etablissement ouvert | 2016-06-23  |          |                                                                                                                                                                                                                                                                                 |                     |               |                    |                      | 8002_S1_20170504_8_ets.csv |
| Partiel | 005520242 | 8002        | Amiens     | 1955B70024     | 1                | 2019-05-06     | Etablissement ouvert | 2018-07-09  |          |                                                                                                                                                                                                                                                                                 |                     |               |                    |                      | 8002_S2_20190506_8_ets.csv |
| Initial | 005520242 | 8002        | Amiens     | 1955B70024     | 2                | 2016-06-23     | Etablissement ouvert | 2016-06-23  |          | fabrication et la vente de tous produits chimiques, plus particulièrement de peintures industrielles, ménagères ou pour le bâtiment, de tous produits ou matières premières utilisées dans la fabrication des peintures et vernis négoce de peintures, revetements et matériaux | 1926-01-20          | Création      |                    | Exploitation directe | 8002_S1_20170504_8_ets.csv |
| Partiel | 005520242 | 8002        | Amiens     | 1955B70024     | 2                | 2019-05-06     | Etablissement ouvert | 2018-07-09  |          | fabrication et la vente de tous produits chimiques, plus particulièrement de peintures industrielles, ménagères ou pour le bâtiment, de tous produits ou matières premières utilisées dans la fabrication des peintures et vernis négoce de peintures, revetements et matériaux | 1926-01-20          | Création      |                    | Exploitation directe | 8002_S2_20190506_8_ets.csv |

### Exemple input remplissage évenement avec historique

Siren **439497280**

[Gitlab](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/US_Datum/US_5_partiel.md#exemple-input-remplissage-%C3%A9venement-avec-historique)

| siren     | code greffe | nom_greffe   | numero_gestion | id_etablissement | file_timestamp      | libelle_evt                                | date_greffe | siège_pm | activité                                                             | date_début_activité | origine_fonds | origine_fonds_info                                    | type_exploitation    | csv_source                                             | origin  |
|-----------|-------------|--------------|----------------|------------------|---------------------|--------------------------------------------|-------------|----------|----------------------------------------------------------------------|---------------------|---------------|-------------------------------------------------------|----------------------|--------------------------------------------------------|---------|
| 439497280 | 2202        | Saint-Brieuc | 2001B50181     | 1                | 2016-09-14          | Etablissement ouvert                       | 2016-09-14  |          | VENTE, LOCATION DE MATERIEL INFORMATIQUE, ELECTRONIQUE, BUREAUTIQUE. | 2001-09-01          | Achat         | - récédent propriétaire exploitan - TEC MULTIMEDI     | Exploitation directe | 2202_S1_20170504_8_ets.csv                             | Initial |
| 439497280 | 2202        | Saint-Brieuc | 2001B50181     | 1                | 2019-08-20 09:00:57 | Modifications relatives à un établissement | 2019-08-19  |          | Vente, location de matériel informatique, électronique, bureautique. | 2001-09-01          | Achat         | - Précédent propriétaire exploitant - JTEC MULTIMEDIA | Exploitation directe | 2202_612_20190820_090057_9_ets_nouveau_modifie_EVT.csv | EVT     |
| 439497280 | 2202        | Saint-Brieuc | 2001B50181     | 1                | 2019-11-16 09:10:58 | Etablissement supprimé                     | 2019-11-14  |          |                                                                      |                     |               |                                                       |                      | 2202_677_20191116_091058_10_ets_supprime_EVT.csv       | EVT     |

## Output

[PO : dans le cas de transformation de données, préciser les sorties :

*   BDD cibles
*   Tables:  inpi_etablissement_historique
*   Champs: Nouveau champs: `statut`

]

### Exemple output partiel

Siren 005520242

[Gitlab](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/US_Datum/US_5_partiel.md#exemple-output)

| origin  | statut | siren     | code_greffe | nom_greffe | numero_gestion | id_etablissement | file_timestamp          | libelle_evt          | date_greffe             | siege_pm | activite                                                                                                                                                                                                                                                                        | date_debut_activite | origine_fonds | origine_fonds_info | type_exploitation    | csv_source                 |
|---------|--------|-----------|-------------|------------|----------------|------------------|-------------------------|----------------------|-------------------------|----------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------|---------------|--------------------|----------------------|----------------------------|
| Initial | IGNORE | 005520242 | 8002        | Amiens     | 1955B70024     | 1                | 2016-06-23 00:00:00.000 | Etablissement ouvert | 2016-06-23 00:00:00.000 |          |                                                                                                                                                                                                                                                                                 |                     |               |                    |                      | 8002_S1_20170504_8_ets.csv |
| Partiel |        | 005520242 | 8002        | Amiens     | 1955B70024     | 1                | 2019-05-06 00:00:00.000 | Etablissement ouvert | 2018-07-09 00:00:00.000 |          |                                                                                                                                                                                                                                                                                 |                     |               |                    |                      | 8002_S2_20190506_8_ets.csv |
| Initial | IGNORE | 005520242 | 8002        | Amiens     | 1955B70024     | 2                | 2016-06-23 00:00:00.000 | Etablissement ouvert | 2016-06-23 00:00:00.000 |          | fabrication et la vente de tous produits chimiques, plus particulièrement de peintures industrielles, ménagères ou pour le bâtiment, de tous produits ou matières premières utilisées dans la fabrication des peintures et vernis négoce de peintures, revetements et matériaux | 1926-01-20          | Création      |                    | Exploitation directe | 8002_S1_20170504_8_ets.csv |
| Partiel |        | 005520242 | 8002        | Amiens     | 1955B70024     | 2                | 2019-05-06 00:00:00.000 | Etablissement ouvert | 2018-07-09 00:00:00.000 |          | fabrication et la vente de tous produits chimiques, plus particulièrement de peintures industrielles, ménagères ou pour le bâtiment, de tous produits ou matières premières utilisées dans la fabrication des peintures et vernis négoce de peintures, revetements et matériaux | 1926-01-20          | Création      |                    | Exploitation directe | 8002_S2_20190506_8_ets.csv |

### Exemple output remplissage évenement avec historique

Siren **439497280**

[Gitlab](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/US_Datum/US_5_partiel.md#exemple-input-remplissage-%C3%A9venement-avec-historique-1)

| siren     | code_greffe | nom_greffe   | numero_gestion | id_etablissement | file_timestamp          | libelle_evt                                | date_greffe             | siege_pm | activite                                                             | date_debut_activite | origine_fonds | origine_fonds_info                                    | type_exploitation    | csv_source                                             | origin  |
|-----------|-------------|--------------|----------------|------------------|-------------------------|--------------------------------------------|-------------------------|----------|----------------------------------------------------------------------|---------------------|---------------|-------------------------------------------------------|----------------------|--------------------------------------------------------|---------|
| 439497280 | 2202        | Saint-Brieuc | 2001B50181     | 1                | 2016-09-14 00:00:00.000 | Etablissement ouvert                       | 2016-09-14 00:00:00.000 |          | VENTE, LOCATION DE MATERIEL INFORMATIQUE, ELECTRONIQUE, BUREAUTIQUE. | 2001-09-01          | Achat         | - récédent propriétaire exploitan - TEC MULTIMEDI     | Exploitation directe | 2202_S1_20170504_8_ets.csv                             | Initial |
| 439497280 | 2202        | Saint-Brieuc | 2001B50181     | 1                | 2019-08-19 00:00:00.000 | Modifications relatives à un établissement | 2019-08-20 09:00:57.000 |          | Vente, location de matériel informatique, électronique, bureautique. | 2001-09-01          | Achat         | - Précédent propriétaire exploitant - JTEC MULTIMEDIA | Exploitation directe | 2202_612_20190820_090057_9_ets_nouveau_modifie_EVT.csv | EVT     |
| 439497280 | 2202        | Saint-Brieuc | 2001B50181     | 1                | 2019-11-14 00:00:00.000 | Etablissement supprimé                     | 2019-11-16 09:10:58.000 |          | Vente, location de matériel informatique, électronique, bureautique. | 2001-09-01          | Achat         | - Précédent propriétaire exploitant - JTEC MULTIMEDIA | Exploitation directe | 2202_677_20191116_091058_10_ets_supprime_EVT.csv       | EVT     |

## Règles de gestion applicables

[PO : Formules applicables]

1/ Est-ce que les csv dans le dossier Stock pour une date supérieure à 2017 peuvent être labélisés comme « partiel » rendant ainsi caduque toutes les valeurs précédentes d’un établissement ?
* OUI (Reponse Flament Lionel <lflament@inpi.fr>)

- [NEW] Partiel
  - En cas de corrections majeures, la séquence annule et remplace la création et événements antérieurs. Dans ce cas, toutes les données qui ont pu être transmises antérieurement via le stock initial ou le flux doivent donc être ignorées (prendre en compte la date de transmission indiquée dans le nom des sous-répertoires du stock et des fichiers

# Charges de l'équipe

[

PO : Si des étapes particulières / des points d'attention sont attendus, être aussi explicite que possible

Spécifiquement pour l'intégration de nouvelles données dans DATUM :

*   Nombre de lignes chargées pour chaque nouvelle table
*   Poids de chaque nouvelle table
*   Durée du traitement ajouté (+ durée avant et après)

- Dans cette étape, on crée une colonne `statut`, qui indique si les lignes sont a ignorer (IGNORE) ou non (Vide). La logique c'est de prendre la date maximum des stocks partiels par quadruplet, si la date de transfert est inférieure a la date max, alors on ignore.

La query est disponible [ici](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/snippets/40) pour la gestion des partiels et la query pour le remplissage des événements est disponible [ici](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/01_Athena_concatenate_ETS.md#step-5-remplissage-observations-manquantes)

## Exemple avec SIREN: 513913657

On utilise dans un Excel un exemple avec les valeurs du siren 428689392 ayant des ID établissements identiques pour des adresses différentes. Est souligné en bleu les valeurs qui potentiellement amendent la ligne n-1 (ex ligne 10 amende la ligne 9) -> fait référence au point 1/
Attention concernant une règle de gestion déjà formulée, il y a par exemple, l’ID établissement 10 qui appartient à la fois a Rennes, mais aussi Nanterre. De fait, il faut bien distinguer le greffe, car ce sont 2 établissements différents.

* Exemple: [428689392](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/US_Datum/Data_example/%5CUS_2464/428689392.xlsx)


# Tests d'acceptance

[PO : comment contrôler que la réalisation est conforme]

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
