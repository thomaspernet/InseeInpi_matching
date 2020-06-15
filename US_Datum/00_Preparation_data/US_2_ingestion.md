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

L'INPI met à disposition de ses abonnés un FTP contenant l'ensemble de ses données, historisés depuis 2017. Chaque jour de la semaine, sauf le lundi, de nouvelles données sont déposées sous la forme de plusieurs dizaines de petit fichiers.

## Comprendre les données de l'iNPI

Le contenu mis à disposition par l’INPI comprend :

*  Les dossiers des données relatives aux personnes actives (morales et physiques) :
  * Stock
    * Un stock initial constitué à la date du 4 mai 2017 pour les données issues des Tribunaux de commerce (TC)
    * Des stocks partiels constitués des dossiers complets relivrés à la demande de l’INPI après détection d’anomalies
  * Flux
    * Constitution du dossier d’une personne morale ou personne physique (identifiée par son siren et son n° de gestion), dans le cas d’une 1ère immatriculation,
    * Mise à jour des informations disponibles sur une personne morale ou personne physique en cas de modification ou de radiation.

Pour chacune des deux branches, il y a 6 catégories:

- [Actes](https://scm.saas.cagip.group.gca/PERNETTH/InseeInpi_matching/tree/master/Documentation/IMR#actes): ACTES -> Pages 39-40
- [Comptes Annuels](https:/PERNETTH/scm.saas.cagip.group.gca//InseeInpi_matching/tree/master/Documentation/IMR#comptes-annuels):COMPTESANN -> Pages 40-41
- [Observations](https://scm.saas.cagip.group.gca/PERNETTH/InseeInpi_matching/tree/master/Documentation/IMR#observations):OBS -> Pages 38-39
- [Etablissements](https://scm.saas.cagip.group.gca/PERNETTH/InseeInpi_matching/tree/master/Documentation/IMR#etablissements):ETS -> Pages 25-29
- [Personnes Morales](https://scm.saas.cagip.group.gca/PERNETTH/InseeInpi_matching/tree/master/Documentation/IMR#personnes-morales):PM -> Pages 15-18
- [Personnes Physiques](https://scm.saas.cagip.group.gca/PERNETTH/InseeInpi_matching/tree/master/Documentation/IMR#personnes-physiques):PP -> Pages 19-24
- [Représentants](https://scm.saas.cagip.group.gca/PERNETTH/InseeInpi_matching/tree/master/Documentation/IMR#repr%C3%A9sentants): REP -> Pages 30-37

Le détail se trouve dans le PDF suivant: [Doc_Tech_IMR_Mai_2019_v1.5.1.pdf](https://scm.saas.cagip.group.gca/PERNETTH/InseeInpi_matching/blob/master/Documentation/IMR/Doc_Tech_IMR_Mai_2019_v1.5.1.pdf)

Dans le cas d’une immatriculation (Personne morale ou Personne physique), le dossier est composé :
* A minima, d’informations sur l’identité de la personne (ex. date d’immatriculation, inscription principale ou secondaire, dénomination, nom, prénom, forme juridique, capital, activité principale etc.)
* Complété éventuellement par des informations relatives aux :
  * Représentants
  * Etablissements (établissement principal et/ou établissements secondaires)
  * Observations (incluant les procédures collectives, mentions de radiation etc.)
  * Dépôt des comptes annuels
  * Dépôt des actes
Les fichiers sont de 2 types :
* Fichiers créés à l’immatriculation d’un dossier
* Fichiers créés suite à un événement sur un dossier (modification, radiation)

La définition d'un stock partiel n'est pas clairement écrite dans la documentation de l'INPI. Toutefois,nous savons que la branche stock représente l'ensemble des fichiers depuis l'origine des greffes jusqu'à mai 2017. Les années suivantes ne concernent que les modifications de dossiers. Ainsi, nous pouvons dégager notre première règle de gestion:


## Règles de gestion

- [NEW] Definition partiel
  - si csv dans le dossier Partiel, année > 2017, alors partiel, c'est a dire, modification complète du dossier due a une anomalie.
  - la date d’ingestion est indiquée dans le path, ie comme les flux


![](https://www.lucidchart.com/publicSegments/view/d9e4494d-bfaf-4d0e-9e0f-53011cda7eb9/image.png)

# US / ISSUES liées

Prérequis : [#2237-Récupérer l'historique de l'ensemble des données INPI](/project/olivierlubet-air/us/2237)

# Besoin

L'objectif de cette US est de mettre à disposition des utilisateurs finaux les données présentes dans les fichiers sous la forme de tables.

Cette première partie s'intéresse **uniquement aux établissements (4 fichiers -> 4 tables).**

# Spécifications

## Input

*   Documentation jointe
*   Fichiers ets.csv, ets_nouveau_modifie_EVT.csv et ets_supprime_EVT.csv (description page 25)

### FICHIERS DE STOCK

*   Dans les répertoires "stock" : <code_greffe>_<num_transmission>_<AA><MM><JJ>_8_ets.csv
*   Dans les répertoires "partiels" : idem

### FICHIERS DE FLUX

FICHIERS TRANSMIS A L’IMMATRICULATION D’UN DOSSIER:

*   <code_greffe>_<num_transmission>_<AA><MM><JJ>_<HH><MM><SS>_8_ets.csv

FICHIERS TRANSMIS EN CAS DE MISE A JOUR D’UN DOSSIER:

*   <code_greffe>_<num_transmission>_<AA><MM><JJ>_<HH><MM><SS>_9_ets_nouveau_modifie_EVT.csv
*   <code_greffe>_<num_transmission>_<AA><MM><JJ>_<HH><MM><SS>_10_ets_supprime_EVT.csv

## Output

BDD : SRC_REFERENTIEL

Tables :

*   inpi_stock_etablissement
*   inpi_partiel_etablissement ( à définir selon une règle de gestion des dates )
*   inpi_flux_etablissement
*   inpi_flux_etablissement_nouveau_modifie
*   inpi_flux_etablissement_supprime

Schema potentiel: cd [json Gitlab](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/US_Datum/Schema_fields/US_2234/fields_2234.json)

## Règles de gestion applicables

L'ensemble des fichiers collectés doivent être intégrés dans les tables finales.

<!> ATTENTION <!> il n'est pas ici demandé de faire un "annule et remplace" sur des fichiers, mais bien d'exposer l'ensemble des fichiers reçus.

**Avant intégration des fichiers dans une table, ceux-ci doivent être enrichis (par des colonnes en fin de fichier) avec :**

*   Le nom du fichier (et confirmer : le chemin dans lequel se situe le fichier dans l'arborescence INPI)
*   Le rang de chacune des lignes

# Charges de l'équipe

*   Concevoir, puis réaliser, l'approche la plus pertinente pour exploiter les données et les stocker (créer des partitions par années / mois / jour ? recopier les fichiers, ou les lier en tables externes ?)
*   Ajouter les métadonnées (date, index, origine, type ...) sur chaque csv

# Tests d'acceptance

Pour chaque test, le nombre de lignes doit être égale ou supérieure (d'une faible marge).

Le tableau ci dessous compte le nombre d'obs pour la colonne `total`, le nombre de valeurs uniques pour `siren`, `id_ets`, `greffe`, `ville` et `enseigne`. Les deux dernières colonnes retournent la date minimum/maximum du timestamp.

| origin       |   total |   siren |   id_ets |   greffe |   ville |   enseigne | date_min                | date_max                |
|:-------------|--------:|--------:|---------:|---------:|--------:|-----------:|:------------------------|:------------------------|
| Initial      | 7575462 | 4824158 |     1207 |      209 |   70072 |     611613 | 0008-07-22 00:00:00.000 | 2017-05-04 00:00:00.000 |
| New 2018     | 1710178 |  462498 |       67 |      135 |   30276 |      52384 | 2018-01-01 20:15:23.000 | 2018-12-31 20:29:30.000 |
| New 2019     | 1645466 |  670505 |       95 |      135 |   34167 |      75332 | 2019-01-01 08:45:03.000 | 2019-12-31 20:21:17.000 |
| New 2017     |  633793 |  248891 |       61 |      134 |   24091 |      26751 | 2017-05-12 09:38:42.000 | 2017-12-30 10:09:01.000 |
| Partiel 2019 |  550229 |  269829 |      586 |      135 |   22978 |      40947 | 2019-05-06 00:00:00.000 | 2019-11-25 00:00:00.000 |
| Partiel 2018 |     315 |     192 |       11 |       69 |     176 |         13 | 2018-08-24 00:00:00.000 | 2018-08-24 00:00:00.000 |

A noter que le nombre d'ID etb ne correspond pas au nombre d’établissements présents dans la base. C'est le nombre de valeurs distinctes

La query utilisée pour générer les valeurs du tableau est dispo dans le snippet suivant :[snippet US 2:Test acceptance 1](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/snippets/35)

Verifier que les champs suivants ont bien le bon nombres de charactères ou le bon pattern:

| Variables           | Nb characteres | possibilité           |
|---------------------|----------------|-----------------------|
| code greffe         | 4              |                       |
| numero_gestion      | 10             |                       |
| siren               | 9              |                       |
| code_postal         | 0 ou 5         |                       |
| code_commune        | 0 ou 5         |                       |
| type                | 3              | SIE/PRI/SEC/SEP       |
| date_début_activité |                | "2019-05-03"          |
| id_etablissement    |                | 0/1/2/3/...           |
| date_greffe         |                | "2019-12-31"          |
| file_timestamp      |                | "2019-01-30 09:23:48" |

*   Les 4 tables sont requêtables
*   Les données sont observables depuis 2017

Cibler un SIREN ayant eu des évènements, et contrôler à partir des sites suivants :

*   Insee [avis-situation-sirene.insee.fr/IdentificationListeSiret.action](http://avis-situation-sirene.insee.fr/IdentificationListeSiret.action)
*   INPI/TC [data.inpi.fr](https://data.inpi.fr/)
*   Infogreffe [infogreffe.fr](https://www.infogreffe.fr/)

# CONCEPTION

Conception réalisée par Mohamed et Olivier et Hocine et Mouna et Jonathan

Point de conception du 29/04/2020 :

0- Fichiers dans l arborescence source :

Stock : public/IMR_Donnees_Saisies/tc/stock/AAAA/MM/JJ/<code_greffe><num_transmission><AA><MM><JJ>.zip

--> <code_greffe><num_transmission><AA><MM><JJ>_8_ets.csv

Flux : public/IMR_Donnees_Saisies/tc/flux/AAAA/MM/ <code_greffe><num_transmission><AA><MM><JJ>_<HH><MM><SS>_8_ets.csv

<code_greffe><num_transmission><AA><MM><JJ>_<HH><MM><SS>_9_ets_nouveau_modifie_EVT.csv

<code_greffe><num_transmission><AA><MM><JJ>_<HH><MM><SS>_10_ets_supprime_EVT.csv

1- Tibco dépose les fichiers dans le répertoire SAS et ensuite envoi de ces fichiers compressés par via OPS :

/app/list/datum/data_collecte/${BIF_ENV}/ingestion/inpi/data/stock/<code_greffe><num_transmission><AA><MM><JJ>.zip /app/list/datum/data_collecte/${BIF_ENV}/ingestion/inpi/data/flux/<code_greffe><num_transmission><AA><MM><JJ>.zip

2- Ensuite, dans un script shell dédié, dézipper et ajouter en fin de fichier avant renommage :

*   le nom du fichier source <code_greffe><num_transmission><AA><MM><JJ>_8_ets.csv

ou <code_greffe><num_transmission><AA><MM><JJ>_<HH><MM><SS>_8_ets.csv

ou <code_greffe><num_transmission><AA><MM><JJ>_<HH><MM><SS>_9_ets_nouveau_modifie_EVT.csv

ou <code_greffe><num_transmission><AA><MM><JJ>_<HH><MM><SS>_10_ets_supprime_EVT.csv

*   le rang de la ligne dans le fichier

*   horodate : file_timestamp (format date)

*   origin Initial ou Partiel pour Stock, NEW ou EVT pour Flux

*   origin_type (stock ou flux)

3- Dans ce même stript, envoyer dans le répertoire de réception les fichiers modifiés et renommés tels que : /app/list/datum/data_collecte/${BIF_ENV}/ingestion/inpi/reception/inpi_stock_etablissement_<AA><MM><JJ><HH><MM><SS>.csv (date fonctionnelle, récupérée du nom fichier précédent) /app/list/datum/data_collecte/${BIF_ENV}/ingestion/inpi/reception/inpi_flux_etablissement_<AA><MM><JJ><HH><MM><SS>.csv /app/list/datum/data_collecte/${BIF_ENV}/ingestion/inpi/reception/inpi_flux_etablissement_nouveau_modifie_<AA><MM><JJ><HH><MM><SS>.csv /app/list/datum/data_collecte/${BIF_ENV}/ingestion/inpi/reception/inpi_flux_etablissement_supprime_<AA><MM><JJ><HH><MM><SS>.csv

NB : pour gérer les fichiers déjà intégrés, on se basera sur le dossier /app/list/datum/data_collecte/${BIF_ENV}/ingestion/inpi/archive et on utilisera la date fonctionnelle comme précisé ci-dessus

NB2 : voir pour la purge (md5?) et l archivage

NB3 : voir avec Nicolas Fruit s il est possible d avoir les fichiers source en delta

[DEV:

*   Déplacement des fichiers renommés (src_mask_date) dans le repo /reception avec ajout des colonnes (date, index, origine, type)

*   créer le fichier src_inpi.cfg pour la nouvelle source

*   créer les fichiers .sql (ext, trim,create) et le cfg associé

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
