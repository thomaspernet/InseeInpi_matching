---
jupyter:
  jupytext:
    formats: ipynb,md
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.2'
      jupytext_version: 1.4.0
  kernelspec:
    display_name: Python 3
    language: python
    name: python3
---

<!-- #region -->
# filtrer date greffe plusieurs transmissions  

```
Entant que {X} je souhaite {filtrer les dates de greffe ayant plusieurs transmissions} afin de {pouvoir écarter les lignes outdated}
```

**Metadatab**

- Taiga:
    - Numero US: [2746](https://tree.taiga.io/project/olivierlubet-air/us/2746)
- Gitlab
    - Notebook: [05_filre_date_greffe](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/05_redaction_US/05_filre_date_greffe.ipynb)
    - Markdown: [05_filre_date_greffe](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/05_redaction_US/05_filre_date_greffe.md)
    - Data:
        - [inpi_etablissement_historique]()
        - 

# Contexte

On sait qu’il faut enrichir les séquences (siren/code greffe/ numéro gestion/id établissement) de manière incrémentale pour les transmissions, mais aussi en prenant les valeurs t-1 pour remplir les champs manquants.

Maintenant, il y a de nombreux cas ou la date de greffe reste la même, mais contenant plusieurs dates de transmission. Nous pouvons dès lors formuler une nouvelle règle de gestion:

Pour une même date de greffe a plusieurs transmissions, nous devons prendre la dernière ligne que nous avons enrichi

Par ailleurs, Lesransmissions pour une même date de greffe ont plusieurs mois de décalage, L'INPI ne peut pas expliquer la raison car l'INPI diffuse ce qu’infogreffe envoit




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
- Une date de greffe peut contenir plusieurs dates de transmission avec des décalage en terme de mois plus ou moins élevé. Il n'y a pas de règles quant aux envoies des dossiers de transmission. Ainsi, nous pouvons filtrer les lignes par séquence (_siren_ + _code greffe_ + _numero gestion_ + _ID établissement) + date_greffe en ne gardant que la dernière date de transmission qui a été enrichie 

Workflow US (via stock)

![workflow](https://www.lucidchart.com/publicSegments/view/d9e4494d-bfaf-4d0e-9e0f-53011cda7eb9/image.png)

# US / ISSUES liées

[PO & DEV : s'il existe des références, les inscrire]

# Besoin

Dans cette US, le besoin est le suivant:

- Filtrer les lignes ayant des dates inférieures au timestamp maximum pour une séquence et date de greffe donnée.


<!-- #endregion -->

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
*   Champs: Tous



```python
import pandas as pd
import numpy as np
```

## Output

[PO : dans le cas de transformation de données, préciser les sorties :

*   BDD cibles
*   Tables: `inpi_etablissement_historique`
*   Champs: Tous

]

Une table avec les valeurs filtrées des dates de transmission inférieures au maximum timestamp. Exemple de query

```
SELECT * 
FROM (
SELECT siren, code_greffe, numero_gestion, id_etablissement, date_greffe, COUNT(*) as count_seq_date
FROM initial_partiel_evt_new_ets_status_final 
-- WHERE siren = '818153637'
GROUP BY siren, code_greffe, numero_gestion, id_etablissement, date_greffe
   ) as temp
   
LEFT JOIN initial_partiel_evt_new_ets_status_final
ON 
temp.siren = initial_partiel_evt_new_ets_status_final.siren AND
temp.code_greffe = initial_partiel_evt_new_ets_status_final.code_greffe AND
temp.numero_gestion = initial_partiel_evt_new_ets_status_final.numero_gestion AND
temp.id_etablissement = initial_partiel_evt_new_ets_status_final.id_etablissement AND 
temp.date_greffe = initial_partiel_evt_new_ets_status_final.date_greffe
WHERE count_seq_date > 1
LIMIT 15
```

| siren     | code_greffe | numero_gestion | id_etablissement | date_greffe             | count_seq_date | siren     | code_greffe | nom_greffe      | numero_gestion | id_etablissement | status | origin | file_timestamp          | date_greffe             | libelle_evt          | type | siege_pm | rcs_registre | adresse_ligne1            | adresse_ligne2                 | adresse_ligne3 | code_postal | ville                | code_commune | pays   | domiciliataire_nom | domiciliataire_siren | domiciliataire_greffe | domiciliataire_complement | siege_domicile_representant | nom_commercial | enseigne | activite_ambulante | activite_saisonniere | activite_non_sedentaire | date_debut_activite | activite                                                                                                                                                                                                                                                                                                                                                                          | origine_fonds                                     | origine_fonds_info | type_exploitation        | csv_source                         |
|-----------|-------------|----------------|------------------|-------------------------|----------------|-----------|-------------|-----------------|----------------|------------------|--------|--------|-------------------------|-------------------------|----------------------|------|----------|--------------|---------------------------|--------------------------------|----------------|-------------|----------------------|--------------|--------|--------------------|----------------------|-----------------------|---------------------------|-----------------------------|----------------|----------|--------------------|----------------------|-------------------------|---------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------|--------------------|--------------------------|------------------------------------|
| 833609597 | 7501        | 2017B28308     | 2                | 2017-12-07 00:00:00.000 | 2              | 833609597 | 7501        | Paris           | 2017B28308     | 2                |        | NEW    | 2017-12-08 20:45:41.000 | 2017-12-07 00:00:00.000 | Etablissement ouvert | PRI  | France   | Paris        | 28 rue de Mogador         |                                |                | 75009       | Paris                | 75109        | France |                    |                      |                       |                           |                             |                |          | Non                | Non                  | Non                     | 06/11/2017          | DÃ©veloppement, construction, promotion, gestion et exploitation de tous   biens mobiliers ou immobiliers ayant pour objet la production d'Ã©nergie                                                                                                                                                                                                                               | CrÃ©ation                                         |                    | Exploitation directe     | 7501_106_20171208_204541_8_ets.csv |
| 833609597 | 7501        | 2017B28308     | 2                | 2017-12-07 00:00:00.000 | 2              | 833609597 | 7501        | Paris           | 2017B28308     | 2                |        | NEW    | 2017-12-07 20:58:48.000 | 2017-12-07 00:00:00.000 | Etablissement ouvert | PRI  | France   | Paris        | 28 rue de Mogador         |                                |                | 75009       | Paris                | 75109        | France |                    |                      |                       |                           |                             |                |          | Non                | Non                  | Non                     | 06/11/2017          | DÃ©veloppement, construction, promotion, gestion et exploitation de tous   biens mobiliers ou immobiliers ayant pour objet la production d'Ã©nergie                                                                                                                                                                                                                               | CrÃ©ation                                         |                    | Exploitation directe     | 7501_105_20171207_205848_8_ets.csv |
| 802418970 | 3302        | 2017B04677     | 2                | 2017-09-18 00:00:00.000 | 2              | 802418970 | 3302        | Bordeaux        | 2017B04677     | 2                |        | NEW    | 2017-09-21 08:32:39.000 | 2017-09-18 00:00:00.000 | Etablissement ouvert | SEC  | France   | Bobigny      | 8 Rue Professeur AndrÃ©   |  Lavignolle (immeuble 2)       |                | 33000       | Bordeaux             | 33063        | FRANCE |                    |                      |                       |                           |                             |                |          | non                | non                  | non                     | 20/09/2017          | L'INSTALLATION, LA MAINTENANCE ET LA REPARATION DE TOUS EQUIPEMENTS   D'EMISSION ET/OU DE RECEPTION, ET/OU TRANSPORT DE SIGNAUX                                                                                                                                                                                                                                                   | CrÃ©ation                                         |                    | Exploitation directe     | 3302_194_20170921_083239_8_ets.csv |
| 802418970 | 3302        | 2017B04677     | 2                | 2017-09-18 00:00:00.000 | 2              | 802418970 | 3302        | Bordeaux        | 2017B04677     | 2                |        | NEW    | 2017-09-19 08:42:12.000 | 2017-09-18 00:00:00.000 | Etablissement ouvert | SEC  | France   | Bobigny      | 8 Rue Professeur AndrÃ©   |  Lavignolle (immeuble 2)       |                | 33000       | Bordeaux             | 33063        | FRANCE |                    |                      |                       |                           |                             |                |          | non                | non                  | non                     | 20/09/2017          | L'INSTALLATION, LA MAINTENANCE ET LA REPARATION DE TOUS EQUIPEMENTS   D'EMISSION ET/OU DE RECEPTION, ET/OU TRANSPORT DE SIGNAUX                                                                                                                                                                                                                                                   | CrÃ©ation                                         |                    | Exploitation directe     | 3302_190_20170919_084212_8_ets.csv |
| 842301202 | 2402        | 2018A00479     | 1                | 2018-09-14 00:00:00.000 | 3              | 842301202 | 2402        | PÃ©rigueux      | 2018A00479     | 1                |        | NEW    | 2018-09-22 08:50:33.000 | 2018-09-14 00:00:00.000 | Etablissement ouvert | PRI  |          |              |                           | 15 avenue de la Croix Herbouze |                | 24650       | Chancelade           | 24102        | FRANCE |                    |                      |                       |                           |                             | MCE COMPANY    |          | non                | non                  | non                     | 12/09/2018          | E-Commerce articles de bien-Ãªtre hygiÃ¨ne articles textiles tous   produits non-alimentaires non-rÃ¨glementÃ©s conseil en dÃ©veloppement                                                                                                                                                                                                                                         | CrÃ©ation                                         |                    | Exploitation personnelle | 2402_359_20180922_085033_8_ets.csv |
| 842301202 | 2402        | 2018A00479     | 1                | 2018-09-14 00:00:00.000 | 3              | 842301202 | 2402        | PÃ©rigueux      | 2018A00479     | 1                |        | NEW    | 2018-09-21 09:24:25.000 | 2018-09-14 00:00:00.000 | Etablissement ouvert | PRI  |          |              |                           | 15 avenue de la Croix Herbouze |                | 24650       | Chancelade           | 24102        | FRANCE |                    |                      |                       |                           |                             | MCE COMPANY    |          | non                | non                  | non                     | 12/09/2018          | E-Commerce articles de bien-Ãªtre hygiÃ¨ne articles textiles tous   produits non-alimentaires non-rÃ¨glementÃ©s conseil en dÃ©veloppement                                                                                                                                                                                                                                         | CrÃ©ation                                         |                    | Exploitation personnelle | 2402_358_20180921_092425_8_ets.csv |
| 842301202 | 2402        | 2018A00479     | 1                | 2018-09-14 00:00:00.000 | 3              | 842301202 | 2402        | PÃ©rigueux      | 2018A00479     | 1                |        | NEW    | 2018-09-15 06:35:41.000 | 2018-09-14 00:00:00.000 | Etablissement ouvert | PRI  |          |              |                           | 15 avenue de la Croix Herbouze |                | 24650       | Chancelade           | 24102        | FRANCE |                    |                      |                       |                           |                             | MCE COMPANY    |          | non                | non                  | non                     | 12/09/2018          | E-Commerce articles de bien-Ãªtre hygiÃ¨ne articles textiles tous   produits non-alimentaires non-rÃ¨glementÃ©s conseil en dÃ©veloppement                                                                                                                                                                                                                                         | CrÃ©ation                                         |                    | Exploitation personnelle | 2402_354_20180915_063541_8_ets.csv |
| 842301541 | 3302        | 2018A03721     | 2                | 2018-09-18 00:00:00.000 | 2              | 842301541 | 3302        | Bordeaux        | 2018A03721     | 2                |        | NEW    | 2018-09-20 06:24:05.000 | 2018-09-18 00:00:00.000 | Etablissement ouvert | PRI  |          |              | 9 Rue Blaise Pascal       |  le Castelou BÃ¢t A Apt 5      |                | 33400       | Talence              | 33522        | FRANCE |                    |                      |                       |                           |                             |                |          | non                | non                  | non                     | 13/09/2018          | Coursier Ã  vÃ©lo                                                                                                                                                                                                                                                                                                                                                                 | CrÃ©ation                                         |                    | Exploitation directe     | 3302_818_20180920_062405_8_ets.csv |
| 842301541 | 3302        | 2018A03721     | 2                | 2018-09-18 00:00:00.000 | 2              | 842301541 | 3302        | Bordeaux        | 2018A03721     | 2                |        | NEW    | 2018-09-19 06:34:46.000 | 2018-09-18 00:00:00.000 | Etablissement ouvert | PRI  |          |              | 9 Rue Blaise Pascal       |  le Castelou BÃ¢t A Apt 5      |                | 33400       | Talence              | 33522        | FRANCE |                    |                      |                       |                           |                             |                |          | non                | non                  | non                     | 13/09/2018          | Coursier Ã  vÃ©lo                                                                                                                                                                                                                                                                                                                                                                 | CrÃ©ation                                         |                    | Exploitation directe     | 3302_816_20180919_063446_8_ets.csv |
| 842301962 | 1303        | 2018B03964     | 2                | 2018-09-14 00:00:00.000 | 2              | 842301962 | 1303        | Marseille       | 2018B03964     | 2                |        | NEW    | 2018-10-04 09:43:46.000 | 2018-09-14 00:00:00.000 | Etablissement ouvert | PRI  |          |              | 141 Avenue de Saint Menet |  RÃ©sidence La Reynarde Bat E1 |                | 13011       | Marseille            | 13211        | FRANCE |                    |                      |                       |                           |                             |                |          | non                | non                  | non                     | 01/07/2018          | La sociÃ©tÃ© a pour objet, les activitÃ©s de nettoyage de tous types de   locaux et chantiers, l'entretien et le traitement des sols, des murs et des   vitres, les petits travaux de rafraichissement, de remise en Ã©tat de   l'amÃ©nagement d'espaces intÃ©rieurs et extÃ©rieur, ainsi que la vente de   produits et services associÃ©s aux activitÃ©s Ã©numÃ©rÃ©es ci-dessus. | CrÃ©ation                                         |                    | Exploitation directe     | 1303_750_20181004_094346_8_ets.csv |
| 842301962 | 1303        | 2018B03964     | 2                | 2018-09-14 00:00:00.000 | 2              | 842301962 | 1303        | Marseille       | 2018B03964     | 2                |        | NEW    | 2018-09-19 06:24:32.000 | 2018-09-14 00:00:00.000 | Etablissement ouvert | PRI  |          |              | 141 Avenue de Saint Menet |  RÃ©sidence La Reynarde Bat E1 |                | 13011       | Marseille            | 13211        | FRANCE |                    |                      |                       |                           |                             |                |          | non                | non                  | non                     | 01/07/2018          | La sociÃ©tÃ© a pour objet, les activitÃ©s de nettoyage de tous types de   locaux et chantiers, l'entretien et le traitement des sols, des murs et des   vitres, les petits travaux de rafraichissement, de remise en Ã©tat de   l'amÃ©nagement d'espaces intÃ©rieurs et extÃ©rieur, ainsi que la vente de   produits et services associÃ©s aux activitÃ©s Ã©numÃ©rÃ©es ci-dessus. | CrÃ©ation                                         |                    | Exploitation directe     | 1303_724_20180919_062432_8_ets.csv |
| 847550225 | 1301        | 2019B00611     | 1                | 2019-03-05 00:00:00.000 | 2              | 847550225 | 1301        | Aix-en-Provence | 2019B00611     | 1                |        | NEW    | 2019-04-02 09:02:07.000 | 2019-03-05 00:00:00.000 | Etablissement ouvert | PRI  |          |              |                           | 3 avenue des Belges            |                | 13100       | Aix-en-Provence      | 13001        | FRANCE |                    |                      |                       |                           |                             |                |          | non                | non                  | non                     | 02/02/2019          | Agence de travail temporaire                                                                                                                                                                                                                                                                                                                                                      | Transfert d'Ã©tablissement (origine hors ressort) |                    | Exploitation directe     | 1301_522_20190402_090207_8_ets.csv |
| 847550225 | 1301        | 2019B00611     | 1                | 2019-03-05 00:00:00.000 | 2              | 847550225 | 1301        | Aix-en-Provence | 2019B00611     | 1                |        | NEW    | 2019-03-06 08:52:24.000 | 2019-03-05 00:00:00.000 | Etablissement ouvert | PRI  |          |              |                           | 3 avenue des Belges            |                | 13100       | Aix-en-Provence      | 13001        | FRANCE |                    |                      |                       |                           |                             |                |          | non                | non                  | non                     | 02/02/2019          | Agence de travail temporaire                                                                                                                                                                                                                                                                                                                                                      | Transfert d'Ã©tablissement (origine hors ressort) |                    | Exploitation directe     | 1301_502_20190306_085224_8_ets.csv |
| 847550324 | 8701        | 2019D00018     | 1                | 2019-01-21 00:00:00.000 | 3              | 847550324 | 8701        | Limoges         | 2019D00018     | 1                |        | NEW    | 2019-01-29 09:11:50.000 | 2019-01-21 00:00:00.000 | Etablissement ouvert | SEP  |          |              |                           | 208 route de la Chassagne      |                | 87480       | Saint-Priest-Taurion | 87178        | FRANCE |                    |                      |                       |                           |                             |                |          | non                | non                  | non                     | 18/01/2019          | Acquisition et gestion d'immeubles edification refection construction et   amenagement d'immeubles                                                                                                                                                                                                                                                                                | CrÃ©ation                                         |                    | Exploitation directe     | 8701_439_20190129_091150_8_ets.csv |
| 847550324 | 8701        | 2019D00018     | 1                | 2019-01-21 00:00:00.000 | 3              | 847550324 | 8701        | Limoges         | 2019D00018     | 1                |        | NEW    | 2019-01-24 09:21:45.000 | 2019-01-21 00:00:00.000 | Etablissement ouvert | SEP  |          |              |                           | 208 route de la Chassagne      |                | 87480       | Saint-Priest-Taurion | 87178        | FRANCE |                    |                      |                       |                           |                             |                |          | non                | non                  | non                     | 18/01/2019          | Acquisition et gestion d'immeubles edification refection construction et   amenagement d'immeubles                                                                                                                                                                                                                                                                                | CrÃ©ation                                         |                    | Exploitation directe     | 8701_436_20190124_092145_8_ets.csv |

<!-- #region -->
## Règles de gestion applicables

[PO : Formules applicables]

Si nouvelle règle, ajouter ici.

Pour une séquence donnée (siren, code_greffe, numero_gestion, id_etablissement) et date_greffe, il faut supprimer les lignes qui ne sont pas égales au dernier timestamp. L'ensemble des lignes a du normalement être enrichi lors des étapes précédentes. La dernière doit doit ainsi contenir les dernières informations disponible pour cette date de greffe.

# Charges de l'équipe

[

PO : Si des étapes particulières / des points d'attention sont attendus, être aussi explicite que possible

Spécifiquement pour l'intégration de nouvelles données dans DATUM :

*   Nombre de lignes chargées pour chaque nouvelle table
*   Poids de chaque nouvelle table
*   Durée du traitement ajouté (+ durée avant et après)

]

- Il faut faire attention si plusieurs partiels pour la même date de greffe a ce que le champs status du dernier timestamp connu de soit pas egal a IGNORE

# Tests d'acceptance

[PO : comment contrôler que la réalisation est conforme]

1.  Compter le nombre de cas impacter par la règle

```
SELECT distinct(COUNT(count_seq_date)) as count_possibility, count_seq_date
FROM (
SELECT COUNT(*) as count_seq_date
FROM initial_partiel_evt_new_ets_status_final 
-- WHERE siren = '818153637'
GROUP BY siren, code_greffe, numero_gestion, id_etablissement, date_greffe
  )
  GROUP BY count_seq_date
  ORDER BY count_seq_date
```

| count_possibility | count_seq_date |
|-------------------|----------------|
| 10479562          | 1              |
| 509037            | 2              |
| 248738            | 3              |
| 113889            | 4              |
| 53384             | 5              |
| 27002             | 6              |
| 14506             | 7              |
| 8375              | 8              |
| 4928              | 9              |
| 2916              | 10             |
| 1926              | 11             |
| 1198              | 12             |
| 839               | 13             |
| 511               | 14             |
| 385               | 15             |
| 265               | 16             |
| 191               | 17             |
| 125               | 18             |
| 74                | 19             |
| 80                | 20             |
| 36                | 21             |
| 31                | 22             |
| 42                | 23             |
| 21                | 24             |
| 13                | 25             |
| 17                | 26             |
| 8                 | 27             |
| 4                 | 28             |
| 6                 | 29             |
| 4                 | 30             |
| 5                 | 31             |
| 2                 | 32             |
| 2                 | 33             |
| 3                 | 34             |
| 1                 | 35             |
| 2                 | 36             |
| 3                 | 37             |
| 2                 | 38             |
| 1                 | 39             |
| 2                 | 43             |
| 1                 | 45             |

Lors de nos tests, nous avons eu les résultats suivants:

- Verifier si le champs `status` n'est pas égal a `IGNORE` pour le timestamp maximum de la séquence _siren_ + _code greffe_ + _numero gestion_ + _ID établissement_  + Date de greffe
    - Exemple de siren: 005580683
    - Trouver un siren avec un enchainement de partiel pour une meme date de greffe, et vérifier que les avant dernieres lignes pour le champs status sont égales a `IGNORE` et que la dernière ligne, a savoir celle qui va etre utilisée, n'est pas égale a `IGNORE`
- Verifier que la taille de l'output soit supérieure à la taille de l'input
       - Compter le nombre de lignes a filtrer, compter le nombre de lignes filtré et vérifier que la taille de la table filtrée a bien le bon nombre 
       - C'est a dire, que le nombre de lignes de l'output doit être égal a la somme de la colonne `count_possibility ` dans notre exemple ci dessus

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
<!-- #endregion -->

# Creation markdown

```python
import os, time, shutil, urllib, ipykernel, json
from pathlib import Path
from notebook import notebookapp
```

```python
def create_report(extension = "html"):
    """
    Create a report from the current notebook and save it in the 
    Report folder (Parent-> child directory)
    
    1. Exctract the current notbook name
    2. Convert the Notebook 
    3. Move the newly created report
    
    Args:
    extension: string. Can be "html", "pdf", "markdown"
    
    
    """
    
    ### Get notebook name
    connection_file = os.path.basename(ipykernel.get_connection_file())
    kernel_id = connection_file.split('-', 1)[1].split('.')[0]

    for srv in notebookapp.list_running_servers():
        try:
            if srv['token']=='' and not srv['password']:  
                req = urllib.request.urlopen(srv['url']+'api/sessions')
            else:
                req = urllib.request.urlopen(srv['url']+ \
                                             'api/sessions?token=' + \
                                             srv['token'])
            sessions = json.load(req)
            notebookname = sessions[0]['name']
        except:
            pass  
    
    sep = '.'
    path = os.getcwd()
    parent_path = str(Path(path).parent)
    
    ### Path report
    #path_report = "{}/Reports".format(parent_path)
    #path_report = "{}/Reports".format(path)
    
    ### Path destination
    name_no_extension = notebookname.split(sep, 1)[0]
    if extension == 'markdown':
        #extension = 'md'
        os.remove(name_no_extension +'.{}'.format('md'))
        source_to_move = name_no_extension +'.{}'.format('md')
    else:
        source_to_move = name_no_extension +'.{}'.format(extension)
    dest = os.path.join(path,'US_md', source_to_move)
    
    print('jupyter nbconvert --no-input --to {} {}'.format(
    extension,notebookname))
    
    ### Generate notebook
    os.system('jupyter nbconvert --no-input --to {} {}'.format(
    extension,notebookname))
    
    ### Move notebook to report folder
    #time.sleep(5)
    shutil.move(source_to_move, dest)
    print("Report Available at this adress:\n {}".format(dest))
```

```python
create_report(extension = "markdown")
```
