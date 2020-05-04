# Introduction

Le dossier [01_preparation](https://github.com/thomaspernet/InseeInpi_matching/tree/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation) a deux objectifs principaux:

- Préparer la donnée de l'INPI provenant du FTP
  * Un [notebook](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/01_Athena_concatenate_ETS.ipynb) va gérer la concatenation de la data et la mettre en forme
    * Stock/Flux de l'INPI
      * concatenation
      * Reconstruction valeurs manquantes pour les partiels et événements
- Normaliser la donnée INPI/INSEE
  * Un second [notebook](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/02_Prepare_ETS.ipynb) va normaliser la donnée INPI/INSEE 
    * Mettre a l'identique les variables de matching entre l'INPI et l'INSEE
    

Il faut noter qu'il y a un notebook par catégorie:

* ACTES
* COMPTES_ANNUELS
* [ETS](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/01_Athena_concatenate_ETS.ipynb)
* OBS
* [PM](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/01_Athena_concatenate_PM.ipynb)
* [PP](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/01_Athena_concatenate_PP.ipynb)
* [REP](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/01_Athena_concatenate_REP.ipynb)
# 01  Préparation de la donnée 

L'idée est de récupérer les stocks initiaux, partiels, créations et les événements pour créer un csv avec tous les événements qui se sont passés depuis l'origine du siren.

La préparation de la donnée se fait en 5 étapes:

1. Mise en place des [paramètres et queries](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/01_Athena_concatenate_ETS.md#step-1-parametre-et-queries)
2. [Concatenation](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/01_Athena_concatenate_ETS.md#step-2-concatenation-data) de la data
3. [Creation](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/01_Athena_concatenate_ETS.md#step-3-creation-table-initialpartielevtnew) de chaque table Initial/Partiel/EVT/NEW
4. [Creation](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/01_Athena_concatenate_ETS.md#step-4-creation-statut-partiel) du Statut partiel
5. [Remplissage](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/01_Athena_concatenate_ETS.md#step-5-remplissage-observations-manquantes) des observations manquantes



Chacune de ses étapes est réalisé par un notebook appelé [01_Athena_concatenate_ETS](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/01_Athena_concatenate_ETS.md#query-préparation-événement)  avec les spécificités suivantes:

1. **[Step 1 Préparation json parameter](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/01_Athena_concatenate_stock.md#préparation-json-parameters) et des query SQL**

2. **[Step 2 Concatenation data](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/01_Athena_concatenate_stock.md#step-2-concatenation-data)**

  - Stock
    - [Initial](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/TC_1/01_donnee_source/Stock/Stock_Initial/)
    - [Partiel](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/TC_1/01_donnee_source/Stock/Stock_Partiel/)

  - Flux

    - [NEW](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/TC_1/01_donnee_source/Flux/2017/ETS/NEW/)	
    - [EVT](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/TC_1/01_donnee_source/Flux/2017/ETS/EVT/)
      - Remplissage valeurs manquantes

3. **[Step 3: Creation table Initial/Partiel/EVT/NEW](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/01_Athena_concatenate_stock.md#step-3-creation-table-initialpartielevtnew)**

  - Combine les tables *ets_evt_, ets_partiel_, ets_new_*  et *ets_inital* dans une table appelée *initial_partiel_evt_new_etb*

4. **[Step 4: Creation statut partiel](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/01_Athena_concatenate_stock.md#step-4-creation-statut-partiel)**

  - Création de la colonne *status* qui indique si les lignes sont à ignorer ou non

    - Exemple [donnée d'entrée](https://eu-west-3.console.aws.amazon.com/athena/home?region=eu-west-3#query/history/5aee64e6-288d-4d50-be56-baef6d4fd6ba)
    - Exemple [donnée de sortie](https://eu-west-3.console.aws.amazon.com/athena/home?region=eu-west-3#query/history/0caba433-1515-4a2a-9433-73e8952f3f05)

5. **[Step 5: Remplissage observations manquantes](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/01_Athena_concatenate_stock.md#step-5-remplissage-observations-manquantes)**

  - Récupération information selon Origin  (Stock  ou NEW  ) pour compléter les valeurs manquantes des EVT  
    - Exemple [donnée d'entrée](https://eu-west-3.console.aws.amazon.com/athena/home?region=eu-west-3#query/history/549fd473-eb88-45fc-bec2-a26853a4577b)

    - Exemple [donnée de sortie](https://eu-west-3.console.aws.amazon.com/athena/home?region=eu-west-3#query/history/9bdde308-ea66-4f95-9b31-10b03391d5e8)

A noter que le détail se trouve dans le notebook [01_Athena_concatenate_ETS](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/01_Athena_concatenate_ETS.md#query-préparation-événement). La méthode est réplicable sur l'ensemble des autres catégories. 

### Règles de gestion considérées

- Une séquence est un classement chronologique pour le quadruplet suivant:

- - *siren* + *code greffe* + *numero gestion* + *ID établissement*
  - Dans la table finale, le classement a cette forme *siren, Nom_Greffe, code greffe, numero_gestion, id_etablissement, file_timestamp*

- Une ligne **partiel** va rendre caduque l'ensemble des séquences précédentes.

- Une ligne **événement** ne modifie que le champ comportant la modification. Les champs non modifiés vont être remplis par la ligne t-1

- Le remplissage doit se faire de deux façons:
  - une première fois avec la date de transmission (plusieurs informations renseignées pour une même date de transmission pour un même séquence). La dernière ligne remplit des valeurs précédentes de la séquence.
  - une seconde fois en ajoutant les valeurs non renseignées avec les événements qui correspondent aux lignes précédentes (date de transmission différente et/ou initial, partiel et création).

### Sources de données

Il y a 3 dossiers source:

- [Stock initial](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/TC_1/01_donnee_source/Stock/Stock_Initial/)

- [Stock partiel](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/TC_1/01_donnee_source/Stock/Stock_Partiel/)

- [Flux](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/TC_1/01_donnee_source/Flux/)

Exemple pour les ETB

- Stock:

  - [Stock initial](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/TC_1/01_donnee_source/Stock/Stock_initial/ETS/)
  - [Stock partiel 2018](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/TC_1/01_donnee_source/Stock/Stock_partiel/2018/ETS)

- Flux 
  - [NEW 2017](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/TC_1/01_donnee_source/Flux/2017/ETS/NEW/)
  - [EVT 2017](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/TC_1/01_donnee_source/Flux/2017/ETS/EVT/)

# 02 Normalisation

Dès que la donnée est prête, il faut utiliser le deuxième [notebook](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/02_siretisation/00_Siretisation_etb.ipynb) pour normaliser la donnée INPI/NSEE. Principalement, le programme normalise certaines variables telles que l'adresse ou la ville.

## Préparation préalable

Le dossier `data` comporte trois sous dossiers:

- `logs`: contient les logs découlant de la siretisation
- `output`: Contient les fichiers siretisés
- `input`: Contient quatre sous-dossiers
  - `INPI`: Dossier pivot permettant le stockage des fichiers temporaires a siretiser
  - `INSEE`: Dossier pivot permettant le stockage des fichiers de l'INSEE
  - `Parameters`: Contient les csv permettant de préparer les fichiers
  - `SIREN_INPI`: Fichiers avec le nombre de SIREN a siretiser



L'architecture est la suivante:

```
├───data
│   ├───input
│   │   ├───INPI
│   │   │   └───special_treatment
│   │   ├───INSEE
│   │   ├───Parameters
│   │   ├───RawParameters
│   │   └───SIREN_INPI
│   ├───logs
│   ├───output
│   └───RawData
│       ├───INPI
│       │   └───Stock
│       └───INSEE
│           ├───Flux
│           │   ├───ETS
│           │   └───UL
│           └───Stock
│               ├───ETS
│               └───UL
```



# Class preparation Data

Le code source est disponible [ici](https://github.com/thomaspernet/InseeInpi_matching/blob/dev_thomas/Notebooks_matching/programme_matching/inpi_insee/preparation_data.py) et le notebook pour lancer le programme est disponible [ici](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/02_normalise_data.ipynb)

- Exemple de préparation

  - Concatenation des variables Adresse_Ligne1 + Adresse_Ligne2 + Adresse_Ligne3
  - Normalisation de la variable concatenée -> Extraction des caractères spéciaux, espace, digit puis capitalisation
  - Extraction de tous les stop words du fichier upper_word
  - [Exemple](https://eu-west-3.console.aws.amazon.com/athena/home?region=eu-west-3#query/history/64b8b55e-57fb-4651-9080-e827a9fddf85)

| siren     | adresse_ligne1 | adresse_ligne2    | adresse_ligne3             | adress_new                                   | adresse_new_clean_reg |
| --------- | -------------- | ----------------- | -------------------------- | -------------------------------------------- | --------------------- |
| 752609594 |                | 67 rue Joe Dassin | Parc 2000 C/z Aya Services | 67 RUE JOE DASSIN PARC 2000 C Z AYA SERVICES | JOE$|DASSIN$          |

## Detail

### INSEE

Les données sources de l'INSEE proviennent de [Data Gouv](https://www.data.gouv.fr/fr/datasets/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret/)

- communes_insee:
    - Le fichier source pour les communes se trouvent à cette [URL](https://www.insee.fr/fr/information/3720946)
    - Le notebook pour reconstituer le csv est disponible a cette [URL](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/data/Source_intermediates.ipynb). ⚠️ Repo privé + branche
- voie:
    - Le fichier source pour les communes se trouve à cette [URL](https://www.sirene.fr/sirene/public/variable/libelleVoieEtablissement)
    - Le notebook pour reconstituer le csv est disponible a cette [URL](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/data/Source_intermediates.ipynb). ⚠️ Repo privé + branche
- upper_word:
    - La liste des upper word (stop word capitalisé) provient de la librarie [NLTK](https://www.nltk.org/) avec un ajout manuel.

### INPI

Les données de l'INPI proviennent de ces différents Notebooks:

- [inpi_etb](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/data/Source_intermediates.ipynb)

### Normalisation du fichier INPI.

Le fichier INPI doit contenir un seul fichier `.csv` avant d'être ingéré par le programme. Le fichier va être importé dans un format Dask, ce qui permet de paralléliser les calculs et bien sûr d'éviter les problèmes de mémoire.

La normalisation du fichier de l'INPI se fait en plusieurs étapes:

1) Exclusion des observations avec des NaN pour chacune des variables candidates, à savoir:

    - Adresse_Ligne1
    - Adresse_Ligne2
    - Adresse_Ligne3
    - Code_Postal
    - Ville
    - Code_Commune

2) Extraction des SIREN a SIRETISER -> cela évite d'utiliser toute la base INSEE pour la sirétisation et donc d'accélérer le process.

3) Calcul du nombre de SIRET par SIREN via la fonction `nombre_siret_siren`

4) Normalisation de la variable commune via la fonction `clean_commune`
  -	 Extraction des digits dans la ville. En effet, certaines communes incluent l'arrondissement dans la variable.
  - Extraction des caractères spéciaux et espaces
  - Capitalisation du nom de la commune
  - Matching avec le fichier Commune pour avoir le nom de la commune de l'INSEE.

5) Préparation de l'adresse via la fonction `prepare_adress`
  - Concaténation des variables `Adresse_Ligne1` + `Adresse_Ligne2` + `Adresse_Ligne3`
  - Normalisation de la variable concatenée -> Extraction des caractères speciaux, espace, digit puis capitalisation
  - Extraction de tous les stop words du fichier `upper_word`
  - Split de chaque mot restant de l'adresse
  - Creation du regex de la forme suivante:  `MOT1$|MOT2$`
  - Extraction des digits:
    - Première variable avec le premier digit
    - Seconde variable avec une liste de digit et jointure -> DIGIT1|DIGIT2
  - Merge avec le fichier `voie` pour obtenir le type de voie de l'INSEE
  - Calcul du nombre de digits dans l'adresse
      - Si le nombre de digits dans l'adresse est inférieur à 2, alors NaN. C'est une variable utlisée pendant le matching des règles spéciales
  - Creation d'une variable `index` correspondant à l'index du dataframe. Indispensable

Le fichier est sauvegardé en format gz dans *inpi_etb_stock_0.gz*.

### Normalisation du fichier INSEE

Pour l'étape de siretisation, les variables candidates sont les suivantes:

- "siren",
- "siret",
- "etablissementSiege",
- "etatAdministratifEtablissement",
- "numeroVoieEtablissement",
- "indiceRepetitionEtablissement",
- "typeVoieEtablissement",
- "libelleVoieEtablissement",
- "complementAdresseEtablissement",
- "codeCommuneEtablissement",
- "libelleCommuneEtablissement",
- "codePostalEtablissement",
- "codeCedexEtablissement",
- "libelleCedexEtablissement",
- "distributionSpecialeEtablissement",
- "libelleCommuneEtrangerEtablissement",
- "codePaysEtrangerEtablissement",
- "libellePaysEtrangerEtablissement",
- "dateCreationEtablissement"

Comme pour le fichier de l'INPI, le fichier csv est importé en Dask Dataframe. Les étapes sont les suivantes:

1) Filtrer les SIREN à sirétiser uniquement

2) Filtre la date limite à l'INSEE. Cette étape sert essentiellement pour siretiser les bases de stocks. Cela évite d'utiliser des valeurs "dans le futur" donc inconnues à l'INPI

3) Remplacement des "-" par des " " dans la variable `libelleCommuneEtablissement`

4) Extraction des digits en format "liste" de la variable `libelleVoieEtablissement`

5) Calcul du nombre de SIRET par SIREN

6) Calcul du nombre de digit dans la variable `libelleCommuneEtablissement`

- Si le nombre de digits dans  la variable `libelleCommuneEtablissement` est inférieur à 2 inférieure a 2, alors NaN. C'est une variable utlisée pendant le matching des règles spéciales.

# Verification programme

Les vérifications utilisent les moteurs de recherche suivants:

-  Insee

  -  http://avis-situation-sirene.insee.fr/IdentificationListeSiret.action

-  INPI/TC

  - https://data.inpi.fr/

-  Infogreffe

  - https://www.infogreffe.fr/

## Exemple de séquence

1. Prendre un SIREN avec une séquence

- initial	
  - EVT (une seule date greffe)
  - Vérifier que tous les champs EVT sont bien remplis → modification + remplissage t-1

2. Prendre un SIREN avec une séquence

  - initial 
    - EVT 1 (une seule date greffe)
    - EVT 2 (une seule date greffe, différent de EVT 1 )

3. Prendre un SIREN avec une séquence

  - EVT  
    - EVT 1 (une seule date greffe)
    - EVT 2 (une seule date greffe, différent de EVT 1 )
    - EVT 3 (une seule date greffe, différent de EVT 1 )

4. Prendre un SIREN avec une séquence

  - EVT NEW

    - EVT (une seule date greffe)

    - Vérifier que tous les champs EVT sont bien remplis → modification + remplissage t-1

5. Prendre un SIREN avec une séquence

  - EVT NEW 
    - EVT 1 (une seule date greffe)
    - EVT 2 (une seule date greffe, différent de EVT 1 )

6. Prendre un SIREN avec une séquence

  - EVT NEW 
    - EVT 1 (une seule date greffe)
    - EVT 2 (une seule date greffe, différent de EVT 1 )
    - EVT 3 (une seule date greffe, différent de EVT 1 )

7.  Autre

  - Autres cas/séquences que ceux déjà évoqués

8. Ignore d'un event dans le cas d'un partiel

9. Ignore d'un initial dans le cas d'un partiel

## Reponse SIREN

| Source | Sequence | SIREN     | Status   |
|--------|----------|-----------|----------|
| ETS    | 1        | 500040795 | Ok       |
| ETS    | 1        | 35821321  | Ok       |
| ETS    | 2        | 439497280 | Ok       |
| ETS    | 2        | 97180863  | Ok       |
| ETS    | 3        | 823259650 | problème |
| ETS    | 3        | 95480281  | problème |
| ETS    | 4        | 301347787 | Ok       |
| ETS    | 4        | 318067923 | Ok       |
| ETS    | 4        | 312479017 | Ok       |
| ETS    | 5        | 311469894 | Ok       |
| ETS    | 5        | 378035174 | Ok       |
| ETS    | 6        | 323398057 | problème |
| ETS    | 6        | 487429979 | Ok       |
| ETS    | 7        | 319903381 | problème |
| ETS    | 7        | 0         | problème |
| ETS    | 8        | 5580121   | Ok       |
| ETS    | 9        | 5520242   | Ok       |

## Problèmes/vérifications

Problèmes
* SIREN: 319903381
    * Query ATHENA:
      * https://eu-west-3.console.aws.amazon.com/athena/home?region=eu-west-3#query/history/cb510254-b473-4c7d-bda6-3678715016ba
* Description problème
  * Décalage des colonnes 
  * Probablement du au ";" dans l'adresse
  * URL Donnée brute
    * INPI/TC_1/01_donnee_source/Flux/2017/ETS/EVT/3303_46_20170707_081835_9_ets_nouveau_modifie_EVT.csv

* SIREN: 823259650
    * Query ATHENA: https://eu-west-3.console.aws.amazon.com/athena/home?force&region=eu-west-3#query/history/32b98cf0-71f7-473e-870d-d4a895b271e3
* Description problème
    * il manque l'evt de création de l'établissement 3 situé 13 rue du Moulin de la Groie
=> la ligne de suppression est donc incomplète (enseigne, activité =Mécanique industrielle...).


  * Investigation: 
    * l'établissement 3 (13 rue du Moulin de la Groie) n'a pas été reçu dans les flux (établissement 2 uniquement présent en initial)https://eu-west-3.console.aws.amazon.com/athena/home?region=eu-west-3#query/history/6a3d66fd-155b-429e-8fc4-2a0925b25aaa
    * Il n'est pas visible à l'inpi https://data.inpi.fr/entreprises/823259650 mais visible sur le registre siren insee
=> Question à poser directement à l'INPI? 
Confirmer avec l'INPI que c'est bien un problème dans les flux reçus.

* Lien vers CSV
  * https://calfdata.s3.eu-west-3.amazonaws.com/INPI/TC_1/02_preparation_donnee/check_csv/823259650.csv
    * La ligne 3 n'a pas de création d'entreprise
    * Manque l'établissement ALL ROGER GUILLEMET : SEP
      * Présent à l'INSEE
* Questions
  * Il y a des Siren dont l établissement est supprimé (EVT) mais qui n ont pas de ligne "création d entreprise". Est ce normal?
    * Reponse:
      * Normalement, infogreffe doit envoyer le partiel pour corriger
      * Il arrive que le greffe envoie seulement une ligne pour SEP, lorsque le Principal est fermé, comme dans ce cas, le siège est toujours ouvert. Mais pas de nouvelle ligne dans la base. Le partiel devrait corriger cela
      * Exemple ou le greffe indique 2 lignes dans l'initial pour Siege et principale 448416636 
        * https://eu-west-3.console.aws.amazon.com/athena/home?region=eu-west-3#query/history/9722780b-b039-44de-a74c-53019db5bcae
      * Test siren compliqué 312560584 → 31256058402620  
      * Les partiels sont mis a jour tous les lundis (dernière demande a Infogreffe → 57.000 siren)
      * En interne, l'INPI a des règles pour faire les controles
  * Est il possible d'avoir des établissements figurants a l'INSEE mais qui ne sont pas référencés à l'INPI (hors établissement ayant formulé un droit à l'oubli)
    * Dans le cas de figure ci dessus, le siège ou principal n'est pas dans la base. Donc oui, il peut y avoir des différences INPI/INSEE → généralement corrigées par les partiels.
      * Infogreffe n'envoie pas assez rapidement les infos a l'INPI
* SIREN: 095480281
    * Query ATHENA:https://eu-west-3.console.aws.amazon.com/athena/home?force&region=eu-west-3#query/history/d41284dd-13f6-40cc-a5f5-11c4063b7a6f
  * Description problème
    * il manque les 2 autres etablissements qui sont fermés.
Me semble plus un problème dans les flux reçus.

* SIREN: 323398057
    * Query ATHENA: https://eu-west-3.console.aws.amazon.com/athena/home?force&region=eu-west-3#query/history/e06d8485-f8a7-44bf-a9e9-38730d973cf9
    * Description problème
    * il manque la création de l'etablissement 18 Rue des Rosiers => on a seulement sa suppression => ligne incomplète.
  Me semble plus un problème dans les flux reçus.

* SIREN : 000000000
    * Query ATHENA : https://eu-west-3.console.aws.amazon.com/athena/home?force&region=eu-west-3#query/history/a0fa8098-e7bb-4770-9049-45fc02d9ccab
  * Description problème
    * 94 lignes qui sont avec un siren 000000000
  * Investigation:
    * répartition sur plusieurs types et lots de fichiers : https://eu-west-3.console.aws.amazon.com/athena/home?force&region=eu-west-3#query/history/25e51561-77ae-4bc2-8989-966374b56483
    * est présent dans le fichier données source avec SIREN à 000000000 dans le RAW mais sous une autre numérotation : https://s3.console.aws.amazon.com/s3/object/calfdata/INPI/TC_1/00_RawData/public/IMR_Donnees_Saisies/tc/flux/2019/10/01/7202/643/7202_643_20190925_091748_9_ets_nouveau_modifie_EVT.csv?region=eu-west-3&tab=overview&prefixSearch=7202_643_20190925_091748_9_ets_nouveau_modifie_EVT
    * idem valeurs de SIREN à 00000000 dans le FTP INPI directement /public/IMR_Donnees_Saisies/tc/flux/2019/10/01/7202/643/7202_643_20190925_091748_9_ets_nouveau_modifie_EVT.csv
=> Question à poser directement à l'INPI?
