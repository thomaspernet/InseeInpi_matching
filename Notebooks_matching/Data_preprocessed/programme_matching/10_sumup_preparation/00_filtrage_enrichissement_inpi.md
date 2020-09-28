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
# Filtrage et enrichissement de la donnée de l’INPI

Copy paste from Coda to fill the information

## Objective(s)

Filtrage et enrichissement de la donnée de l’INPI
Select the US you just created →Filtrage et enrichissement de la donnée de l’INPI

* The ID is ued26xzfy75910v
* Add notebook Epic Epic 6 US US 2 Filtrage et enrichissement de la donnée de l’INPI

# Objective(s)

*  La préparation de la donnée de l’INPI requière plusieurs étapes de filtrate et d’enrichissement de la donnée. Dans cette US, nous allons détailler comment procéder pour préparer la donnée de l’INPI mais aussi mettre en avant les “problèmes” et points d’attention rencontrées.
* Le schéma se résume au diagramme ci-dessous

![](https://app.lucidchart.com/publicSegments/view/9e73b3ff-1648-4cda-ab7c-204290721629/image.png)


# Metadata

* Epic: Epic 6
* US: US 2
* Date Begin: 9/21/2020
* Duration Task: 0
* Description: Création d’un notebook pour expliquer comment préparer la donnée de l’INPI 
* Status: Active
  * Change Status task: Active
  * Update table: Modify rows
* Source URL: US 02 Filtrage et enrichissement data INPI
* Task type: Jupyter Notebook
* Users: Thomas Pernet
* Watchers: Thomas Pernet
* User Account: https://937882855452.signin.aws.amazon.com/console
* Estimated Log points: 10
* Task tag: #data-preparation,#documentation,#inpi
* Toggl Tag: #documentation


# Destination Output/Delivery

## Table/file

* GitHub:
  * https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/10_sumup_preparation/00_filtrage_enrichissement_inpi.md

 
<!-- #endregion -->

## Connexion serveur

```python
from awsPy.aws_authorization import aws_connector
from awsPy.aws_glue import service_glue
from awsPy.aws_s3 import service_s3
from pathlib import Path
import pandas as pd
import numpy as np
import seaborn as sns
import os, shutil, json
from itertools import chain

path = os.getcwd()
parent_path = str(Path(path).parent)
path_cred = r"{}/credential_AWS.json".format(parent_path)
con = aws_connector.aws_instantiate(credential = path_cred,
                                       region = 'eu-west-3')

region = 'eu-west-3'
bucket = 'calfdata'
```

```python
con = aws_connector.aws_instantiate(credential = path_cred,
                                       region = region)
client= con.client_boto()
s3 = service_s3.connect_S3(client = client,
                      bucket = bucket, verbose = False) 
glue = service_glue.connect_glue(client = client) 
```

```python
pandas_setting = True
if pandas_setting:
    cm = sns.light_palette("green", as_cmap=True)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', None)
```

# La donnée de l'INPI 

Dans l'introduction, nous avons mentionné qu'une transmission de dossier peut être étaller sur plusieurs mois voir année. Il est donc impératif d'intégrer les dossiers de manière séquencielle, et la dernière transmission est prioritaire sur les précédentes. 

## Plusieurs transmission par date de greffe

Il y a deux autres points d'attention qu'il faut prendre en compte. Le premier est en lien étroit avec la manière dont sont transmis les dossiers. Les greffiers peuvent transmettre les informations aux comptes sur plusieurs mois, années, comme indiqué précédement, mais plus bizarement par jour. Effectivement, le même numéro de dossier peut avoir plusieurs transmissions le même jour, ce qui signifie que le CSV peut possséder plusieurs lignes pour un dossier et date donnée. 

Avoir plusieurs dates de transmission pour un même dossier ne serait pas problématique si chaque ligne contenait l'ensemble des informations contenu dans le schéma de donnée. Les données d'identification sont toujours présentes, mais pour le reste l'INPI ne trasnmet que les variations d'une ligne à l'autre. Le tableau ci dessous est un exemple de ce cas de figure:

Le quadruplet Code greffe, 1303, numéro de gestion,	2003A01166, siren, 450687512, ID Etablissement 3 possède 4 transmission datant du 20170802. 

Dans l'exemple affiché, nous devons récupérer la dernière ligne (4) car c'est celle qui a été transmise en dernier. Toutefois, il manque l'information sur l'enseigne qui a été communiqué en ligne 2. Ainsi, il est indispensable d'enrichir les informations d'une ligne a l'autre. Il faut garder en tête que la ligne la plus récente prévaut sur la précédente en cas de différence. Finalement, nous ne devons avoir qu'une seule ligne par quadruplet pour une date de greffe donnée.

```python
key ='INPI/TC_1/00_RawData/public/IMR_Donnees_Saisies/tc/flux/2017/08/02/1303/112/1303_112_20170802_090910_9_ets_nouveau_modifie_EVT.csv'
(
    s3.read_df_from_s3(
    key = key,
                   sep = ';'
)
    #.sort_values(by = 'Siren')
    .loc[lambda  x : 
         (x['Siren'].isin(['450687512']))
        & (x['ID_Etablissement'].isin([3]))
        ]
    .reset_index()
    .head(4)
    .T
)
```

Le cas de figure que nous venons d'évoquer reste le même lorsque la trasnmission c'est faite a des dates différentes. Autrement dit, le filtrage et l'enrichissement se fait intra jour et intra quadruplet par date de greffe


## Dossier transmis en tant que partiel

La transmission de dossier de la part du greffe vers l'INPI peut dans certains cas contenir des erreurs ou anomalies. L'INPI indique alors la procédure a suivre. 
Les greffes des Tribunaux de commerce peuvent être amenés à effectuer des corrections sur des dossiers selon deux modes :

* Soit sous forme de fichier de flux à traiter selon les règles habituelles d’intégration des mises à jour (corrections mineures),
* Soit sous forme de dossier complet retransmis dans le répertoire de stock (ie stocks partiels), à retraiter en annule et remplace (corrections majeures).
  * C’est le cas en particulier lorsque il y a incohérence entre des identifiants qui auraient été livrés dans le stock initial et ceux livrés dans le flux (ex. fichiers des établissements, représentants, observations) pour un même dossier (siren/numéro de gestion/code greffe). C’est également le cas de dossiers qui auraient été absents du stock initial et qui seraient retransmis après un délai.
  * Dans ce cas, toutes les données qui ont pu être transmises antérieurement via le stock initial ou le flux doivent donc être ignorées (prendre en compte la date de transmission indiquée dans le nom des sous-répertoires du stock et des fichiers cf. description des répertoires de stock TC ci-dessus).
  
Autrement dit, si la modification est mineure, elle sera disponible dans les événements, sinon, il faudra prendre le CSV le plus récent de la branche stock du FTP, et annuler toutes les lignes précédentes, même si il y a des événements. Un partiel vient corriger et faire une remise a zéro du dossier.

Le tableau ci dessous est un exemple de correction de dossier. Le quadruplet Code greffe, 9301, numéro de gestion,	2019B10958, siren, 878606615, ID Etablissement 1 a connu un transmission de partiel le 20191125 venant corriger les deux précédentes transmissions. La correction corrige l'adresse qui a été mal transmise lors de la création de l'établissement.

```python
key1 ='INPI/TC_1/00_RawData/public/IMR_Donnees_Saisies/tc/flux/2019/11/08/9301/1637/9301_1637_20191108_091055_8_ets.csv'
key2 ='INPI/TC_1/00_RawData/public/IMR_Donnees_Saisies/tc/flux/2019/11/15/9301/1653/9301_1653_20191115_084921_9_ets_nouveau_modifie_EVT.csv'
key3 = 'INPI/TC_1/01_donnee_source/Stock/Stock_Partiel/2019/ETS/9301_S7_20191125_8_ets.csv'
(
     pd.concat(map(
         lambda x: 
         (s3.read_df_from_s3(x, sep = ";")
          .loc[lambda  x : 
         (x['Siren'].isin(['878606615']))
        & (x['ID_Etablissement'].isin([1]))
              ]
         )
         , [key1, key2, key3]
              )
)
    .T
)
```

## Filtrage et enrichissement 

Dans le point sur **Plusieurs transmission par date de greffe**, nous avons mentionné la nécéssité de filtrer et d'enrichir les lignes les plus récentes avec leur antécédent le plus proche pour ne contenir qu'une seule ligne pour le quadruplet et une date de greffe donnée. La logique d'enrichissment de la donnée doit aussi être éffectuée entre les dates de greffes. Le greffe ne va transmettre que les changements d'information d'une date de greffe a une autre. Les variables d'identification vont bien sur être indiquées. A partir du moment ou un champs a été rempli, et non modifié, nous allons devoir le remplir pour chacun des événements transmit. La seule possibilité ou le remplissage n'a pas lieu d'être est lorsque l'INPI transmet un partiel. Le partiel va corriger et annuler toutes les lignes précédentes.

En résumé, chaque transmission pour une date de greffe ne doit posséder qu'une seule ligne. C'est le cas de figure indiqué dans le point **Plusieurs transmission par date de greffe**. Ensuite, un enrichissement de la donnée doit se faire entre les dates de greffes. Par exemple, si un quadruplet possède le schéma suivant: Création, événement 1 et suppression, alors la table finale aura 3 lignes, avec un découlement de l'information entre la création et la suppression. L'une des différences entre la ligne 1 et la ligne 3 est la modification des informations induite par la ligne 2.

Les tables ci dessous illuste ce cas de figure.

Le tableau 1 regroupe toutes les informations brutes. Le quadruplet code_greffe, 1101, numero_gestion, 2000D00074, siren, 331319582, id_etablissement, 1 a fait l'objet de 5 transmissions de la part du greffe, et contient en tout 8 ligne. En regardant de plus prêt, on peut constater que les dates de greffes 2017-12-18 et 2018-09-12 ont plusieurs transmissions, avec des informations non renseignés. La dernière ligne étant un partiel, elle va annuler tout ce qui s'est passé précédement.

```python
key1 ='INPI/TC_1/01_donnee_source/Stock/Stock_Initial/2017/ETS/1101_S1_20170504_8_ets.csv'
key2 ='INPI/TC_1/00_RawData/public/IMR_Donnees_Saisies/tc/flux/2017/12/29/1101/162/1101_162_20171229_085906_9_ets_nouveau_modifie_EVT.csv'
key3 ='INPI/TC_1/00_RawData/public/IMR_Donnees_Saisies/tc/flux/2017/12/19/1101/155/1101_155_20171219_085917_9_ets_nouveau_modifie_EVT.csv'
key4 ='INPI/TC_1/01_donnee_source/Flux/2018/ETS/EVT/1101_167_20180106_201232_9_ets_nouveau_modifie_EVT.csv'
key5 ='INPI/TC_1/00_RawData/public/IMR_Donnees_Saisies/tc/flux/2018/08/02/1101/310/1101_310_20180802_070250_9_ets_nouveau_modifie_EVT.csv'
key6 ='INPI/TC_1/00_RawData/public/IMR_Donnees_Saisies/tc/flux/2018/09/13/1101/340/1101_340_20180913_055556_9_ets_nouveau_modifie_EVT.csv'
key7 ='INPI/TC_1/01_donnee_source/Stock/Stock_Partiel/2019/ETS/1101_S2_20190506_8_ets.csv'

(
     pd.concat(map(
         lambda x: 
         (s3.read_df_from_s3(x, sep = ";")
          .loc[lambda  x : 
         (x['Siren'].isin(['331319582']))
        & (x['ID_Etablissement'].isin([1]))
              ]
         )
         , [key1, key2, key3, key4, key5, key6, key7
           ]
              )
)
    .T
)
```

La préparation de la data de l'INPI va consister a filtrer les lignes avec plusieurs dates de transmission et enrichir les champs manquants. 


## Creation artificielle d'établissement

Lors de l'introduction sur les entreprises et établissements, nous avons détaillé le type de statut qu'un établissement peut avoir. Une entreprise qui est enregistrée au registre des entreprises doit avoir un siège. Le siège est l'adresse "juridique" de l'entreprise. L'entreprise peut posséder un établissement, domicilié à l'adresse ou elle réalise la plupart de son business. Il est très probable que le principal partage la même adresse que le siège. Finalement, tous les établissements en plus du siège et principal sont appelés "secondaire". 

Selon l'INSEE, le siret est l'identifiant permettant de distinguer un établissement d'un autre. L'INPI n'inclut pas le siret dans ses bases de données, mais identifie l'établissement via le quadruplet: siren, numéro de gestion, numéro de dossier et id établissement.

Selon l'INSEE, le siret est attribué par le biais de l'adresse. Un établissement ayant pour siège et principal la même adresse va partager le même siren. D'un point de vue data, si le siret est à la fois siège et principal, alors il n'y aura qu'une seule ligne. 

L'INPI n'a pas la même rigueur que l'INSEE car elle à plus de trois labels possibles pour caractériser un établissement. Il y a "SIE", "PRI", "SEP" et "SEC". Le status "SEP" indique que l'établissement est à la fois siège et principal. Certains greffes vont utiliser ce label pour caractériser les établissements siège et principal, alors que d'autres vont utiliser "SIE" et "PRI". Dans les deux cas, l'adresse est identique mais l'identifiant va différer pour le deuxième groupe de greffe. A partir du moment ou le statut est différent, cela va engendrer à la création d'un nouvel ID. 

Cela pose un problème statistique lorsque nous parlons d'établissement au sens de l'INPI. L'INPI va gonfler artificiellement le nombre d'établissements à cause de la création d'un nouvel identifiant lorsque le greffe va créer deux lignes sur la même adresse.

Dans l'exemple ci-dessous, le quadruplet  code_greffe, 7301, numero_gestion, 2001D00111, siren, 437864820, id_etablissement, 1/10 possède deux établissements au sens de l'INPI a cause du double label "SIE" et "PRI".

```python
key ='INPI/TC_1/01_donnee_source/Stock/Stock_Initial/2017/ETS/7301_S1_20170504_8_ets.csv'
(
    s3.read_df_from_s3(
    key = key,
                   sep = ';'
)
    #.sort_values(by = 'Siren')
    .loc[lambda  x : 
         (x['Siren'].isin(['437864820']))
        ]
    .reset_index()
    .T
)
```

Le greffe ne va cependant pas renseigner tous les champs lors de la création d'un établissement a double label. C'est souvent le siège ou l'information sur la date de début d'activité et l'activité sont manquantes. La raison est que le siège n'est pas corrélé à l'activité. Une entreprise peut ête légalement active sans forcément avoir un établissement en activité. 

Dans certains cas, cela peut poser problème si le greffe ne modifie pas les deux "établissements". Prenons l'exemple ou le greffe crée un établissement a double label, puis ne modifie que le principal. Nous ne pourrons pas changer les informations du siège, car la clé n'est pas la même (ID établissement différent). Maintenant, l'entrepreneur décide de fermer le principal, le greffe va transmettre la fermeture du principal à l'INPI. Toutefois, selon la définition d'établissement au sens de l'INPI, le siège est encore ouvert, mais le principal est fermé.


## Conclusion

Pour récapituler, la donnée de l'INPI est dispachée entre deux branches dans le FTP. La branche dite de stock, peut être subdivisée en deux groupes. Le groupe des stocks initiaux, qui regroupe toutes les informations sur les entreprises précédents la date du 04/05/2017. Le groupe des stocks partiel qui inclut toutes les corrections majeures des dossiers. La branche des flux contient toutes les informations relatives aux entreprises passée la date du 04/05/2017. Les informations vont être catégorisées selon si le dossier est une création, modification ou supression d'établissement. Chaque dossier est transmit par l'intermédiaire d'un CSV. Dès lors, une transmission fait référence à un CSV transmit par un greffe, a une date donnée. La date de transmission ne correspond pas à la date de greffe, qui est indiquée dans le CSV. Un dossier est identifié via le quadruplet siren, code greffe, numéro de gestion et id établissement.

Le greffe va constituer un dossier a une date de greffe donnée, mais peut transmettre l'information au compte goute. Plus précisément, la transmission a l'INPI peut être faite sur un, deux, trois ou plus de jours, étalé sur plusieurs semaines, mois ou années. La transmission a deux particularités. Premièrement, un dossier peut avoir plusieurs lignes au sein d'un CSV. Le greffe ne va indiquer que les différences entre les lignes, mise a part les champs d'identification. Lorsque cela est le cas, il faut enrichir l'ensemble des champs en prenant la première valeur précédente non vide. En cas de divergence, il faut toujours prioriser la ligne la plus récente. Après avoir enrichi le dossier, il faut garder uniquement la dernière valeur pour n'avoir qu'une seule ligne par date de greffe.

La vie d'une entreprise fait qu'un établissement fasse l'objet de plusieurs événements. Par exemple, une modification d'enseigne ou une suppression d'établissement. Le greffe va la encore appliquer la même logique de ne transmettre que les champs qui ont été modifié, avec les informations permettant d'identifier l'établissement. Il faut donc appliquer une deuxième fois l'enrichissement des lignes. Au final, un événement est relayé par une date de greffe via une ou plusieurs transmission. Pour éviter la redondance de l'information, un filtrage est effectué après avoir enrichi la donnée.

Finalement, la transmission d'un partiel vient corriger les erreurs de dossiers, rendant caduque toutes les transmissions précédants la date du partiel.

L'INPI peut gonfler le nombre d'établissements d'une entreprise lorsque le greffe labelise la même adresse en tant que "SIE" et "PRI".

Pour conclure, la table finale va avoir une seule ligne par quadruplet et date de greffe. 


# Préparation table ETS INPI filtrée et enrichie

La préparation de la table des ETS se fait en 3 étapes:

1. Création des tables
2. Filtrage et enrichissement des flux intra day et intra date de greffe
3. Enrichissements des lignes d'un événement a l'autre et filtrage des événements partiels

![](https://app.lucidchart.com/publicSegments/view/5c24129a-f50a-4977-97b3-9a62eaa936b7/image.png)

La première étape est relativement simple car elle consiste a créer les tables des stocks et des flux. L'arborescence du S3 est la suivante:

```
01_donnees_source
    ├───Flux
    │   ├───2017
    │   │   ├───ETS
    │   │   │   ├───EVT
    │   │   │   └───NEW
    │   ├───2018
    │   │   ├───ETS
    │   │   │   ├───EVT
    │   │   │   └───NEW
    │   └───2019
    │   │   ├───ETS
    │   │   │   ├───EVT
    │   │   │   └───NEW
    └───Stock
        ├───Stock_initial
            ├───2017
            │   ├───ETS
        └───Stock_partiel
            ├───2018
            │   ├───ETS
            ├───2019
            │   ├───ETS
            └───2020
                ├───ETS
```

Dans la seconde étape, nous allons concatener les tables des partiels et des flux. De plus nous allons filtrer et enrichir la donnée des flux. L'enrichissement va se faire greffe par greffe car la donnée est trop volumineuse pour etre traité en un seul bloc. La technique que nous avons utilisé n'a pas été optimisé ce qui pousse a faire un traitement brique par brique. Dès que l'enrichissement au niveau du timestamp est fait, il faut répliquer l'opération au niveau de la date de greffe.

La troisième est dernière étape est divisée en trois partie. Dans un premier temps, il faut concatener les tables de stock et des flux filtrés et enrichis, ensite il est nécéssaire de filtrer les événements précédents un partiel. Finalement, il faut enrichir la donnée d'un événement a un autre.

Un point de rappel sur les règles de gestion appliquées

- Une séquence est un classement chronologique pour le quadruplet suivant:
    - siren + code greffe + numero gestion + ID établissement pour les Etablissements
- Une ligne événement ne modifie que le champs comportant la modification. Les champs non modifiés vont être remplis par la ligne t-1
- Une ligne partiel va rendre caduque l'ensemble des séquences précédentes.
- Le remplissage doit se faire de deux façons
    - une première fois avec la date de transmission (plusieurs informations renseignées pour une meme date de transmission pour une même séquence). La dernière ligne remplie des valeurs précédentes de la séquence -> 
2. Filtrage et enrichissement des 
    - une seconde fois en ajoutant les valeurs non renseignées pour cet évènement, en se basant sur les informations des lignes précédentes du triplet (quadruplet pour les Etablissements). Les lignes précédentes ont une date de transmission différente et/ou initial, partiel et création. -> Flux entre les événements 


## Préparation json parameters

Pour faciliter l'ingestion de données en batch, on prépare un json `parameters` avec les paths où récupérer la data, le nom des tables, les origines, mais aussi un champ pour récupérer l'ID de l'execution dans Athena

```python
### If chinese characters, set  ensure_ascii=False
s3.download_file(key = 'DATA/ETL/parameters_ETL_TEMPLATE.json')
with open('parameters_ETL_TEMPLATE.json', 'r') as fp:
    parameters = json.load(fp)
```

```python
parameters['GLOBAL']['DATABASE'] = 'ets_inpi'
```

```python
parameters
```

## 2. Prepare `TABLES.CREATION`

This part usually starts with raw/transformed data in S3. The typical architecture in the S3 is:

- `DATA/RAW_DATA` or `DATA/UNZIP_DATA_APPEND_ALL` or `DATA/TRANSFORMED`. One of our rule is, if the user needs to create a table from a CSV/JSON (raw or transformed), then the query should be written in the key `TABLES.CREATION` and the notebook in the folder `01_prepare_tables`

One or more notebooks in the folder `01_prepare_tables` are used to create the raw tables. Please, use the notebook named `XX_template_table_creation_AWS` to create table using the key `TABLES.CREATION`

```python
new_table = [
    {
    "database": "ets_inpi",
    "name": "ets_initial",
    "output_id": "",
    "separator": ";",
    "s3URI": "s3://calfdata/INPI/TC_1/01_donnee_source/Stock/Stock_Initial/2017/ETS",
    "schema": [
{'Name': 'code_greffe', 'Type': 'string', 'Comment': ''},
 {'Name': 'nom_greffe', 'Type': 'string', 'Comment': ''},
 {'Name': 'numero_gestion', 'Type': 'string', 'Comment': ''},
 {'Name': 'siren', 'Type': 'string', 'Comment': ''},
 {'Name': 'type', 'Type': 'string', 'Comment': ''},
 {'Name': 'siege_pm', 'Type': 'string', 'Comment': ''},
 {'Name': 'rcs_registre', 'Type': 'string', 'Comment': ''},
 {'Name': 'adresse_ligne1', 'Type': 'string', 'Comment': ''},
 {'Name': 'adresse_ligne2', 'Type': 'string', 'Comment': ''},
 {'Name': 'adresse_ligne3', 'Type': 'string', 'Comment': ''},
 {'Name': 'code_postal', 'Type': 'string', 'Comment': ''},
 {'Name': 'ville', 'Type': 'string', 'Comment': ''},
 {'Name': 'code_commune', 'Type': 'string', 'Comment': ''},
 {'Name': 'pays', 'Type': 'string', 'Comment': ''},
 {'Name': 'domiciliataire_nom', 'Type': 'string', 'Comment': ''},
 {'Name': 'domiciliataire_siren', 'Type': 'string', 'Comment': ''},
 {'Name': 'domiciliataire_greffe', 'Type': 'string', 'Comment': ''},
 {'Name': 'domiciliataire_complement', 'Type': 'string', 'Comment': ''},
 {'Name': 'siege_domicile_representant', 'Type': 'string', 'Comment': ''},
 {'Name': 'nom_commercial', 'Type': 'string', 'Comment': ''},
 {'Name': 'enseigne', 'Type': 'string', 'Comment': ''},
 {'Name': 'activite_ambulante', 'Type': 'string', 'Comment': ''},
 {'Name': 'activite_saisonniere', 'Type': 'string', 'Comment': ''},
 {'Name': 'activite_non_sedentaire', 'Type': 'string', 'Comment': ''},
 {'Name': 'date_debut_activite', 'Type': 'string', 'Comment': ''},
 {'Name': 'activite', 'Type': 'string', 'Comment': ''},
 {'Name': 'origine_fonds', 'Type': 'string', 'Comment': ''},
 {'Name': 'origine_fonds_info', 'Type': 'string', 'Comment': ''},
 {'Name': 'type_exploitation', 'Type': 'string', 'Comment': ''},
 {'Name': 'id_etablissement', 'Type': 'string', 'Comment': ''},
 {'Name': 'date_greffe', 'Type': 'string', 'Comment': ''},
 {'Name': 'libelle_evt', 'Type': 'string', 'Comment': ''},
 {'Name': 'csv_source', 'Type': 'string', 'Comment': ''},
 {'Name': 'nature', 'Type': 'string', 'Comment': ''},
 {'Name': 'type_data', 'Type': 'string', 'Comment': ''},
 {'Name': 'origin', 'Type': 'string', 'Comment': ''},
 {'Name': 'file_timestamp', 'Type': 'string', 'Comment': ''}
    ],
    
    },
     {
    "database": "ets_inpi",
    "name": "ets_partiel_2018",
    "output_id": "",
    "separator": ";",
    "s3URI": "s3://calfdata/INPI/TC_1/01_donnee_source/Stock/Stock_Partiel/2018/ETS",
    "schema": [
{'Name': 'code_greffe', 'Type': 'string', 'Comment': ''},
 {'Name': 'nom_greffe', 'Type': 'string', 'Comment': ''},
 {'Name': 'numero_gestion', 'Type': 'string', 'Comment': ''},
 {'Name': 'siren', 'Type': 'string', 'Comment': ''},
 {'Name': 'type', 'Type': 'string', 'Comment': ''},
 {'Name': 'siege_pm', 'Type': 'string', 'Comment': ''},
 {'Name': 'rcs_registre', 'Type': 'string', 'Comment': ''},
 {'Name': 'adresse_ligne1', 'Type': 'string', 'Comment': ''},
 {'Name': 'adresse_ligne2', 'Type': 'string', 'Comment': ''},
 {'Name': 'adresse_ligne3', 'Type': 'string', 'Comment': ''},
 {'Name': 'code_postal', 'Type': 'string', 'Comment': ''},
 {'Name': 'ville', 'Type': 'string', 'Comment': ''},
 {'Name': 'code_commune', 'Type': 'string', 'Comment': ''},
 {'Name': 'pays', 'Type': 'string', 'Comment': ''},
 {'Name': 'domiciliataire_nom', 'Type': 'string', 'Comment': ''},
 {'Name': 'domiciliataire_siren', 'Type': 'string', 'Comment': ''},
 {'Name': 'domiciliataire_greffe', 'Type': 'string', 'Comment': ''},
 {'Name': 'domiciliataire_complement', 'Type': 'string', 'Comment': ''},
 {'Name': 'siege_domicile_representant', 'Type': 'string', 'Comment': ''},
 {'Name': 'nom_commercial', 'Type': 'string', 'Comment': ''},
 {'Name': 'enseigne', 'Type': 'string', 'Comment': ''},
 {'Name': 'activite_ambulante', 'Type': 'string', 'Comment': ''},
 {'Name': 'activite_saisonniere', 'Type': 'string', 'Comment': ''},
 {'Name': 'activite_non_sedentaire', 'Type': 'string', 'Comment': ''},
 {'Name': 'date_debut_activite', 'Type': 'string', 'Comment': ''},
 {'Name': 'activite', 'Type': 'string', 'Comment': ''},
 {'Name': 'origine_fonds', 'Type': 'string', 'Comment': ''},
 {'Name': 'origine_fonds_info', 'Type': 'string', 'Comment': ''},
 {'Name': 'type_exploitation', 'Type': 'string', 'Comment': ''},
 {'Name': 'id_etablissement', 'Type': 'string', 'Comment': ''},
 {'Name': 'date_greffe', 'Type': 'string', 'Comment': ''},
 {'Name': 'libelle_evt', 'Type': 'string', 'Comment': ''},
 {'Name': 'csv_source', 'Type': 'string', 'Comment': ''},
 {'Name': 'nature', 'Type': 'string', 'Comment': ''},
 {'Name': 'type_data', 'Type': 'string', 'Comment': ''},
 {'Name': 'origin', 'Type': 'string', 'Comment': ''},
 {'Name': 'file_timestamp', 'Type': 'string', 'Comment': ''}
    ],
    
    },
     {
    "database": "ets_inpi",
    "name": "ets_partiel_2019",
    "output_id": "",
    "separator": ";",
    "s3URI": "s3://calfdata/INPI/TC_1/01_donnee_source/Stock/Stock_Partiel/2019/ETS",
    "schema": [
{'Name': 'code_greffe', 'Type': 'string', 'Comment': ''},
 {'Name': 'nom_greffe', 'Type': 'string', 'Comment': ''},
 {'Name': 'numero_gestion', 'Type': 'string', 'Comment': ''},
 {'Name': 'siren', 'Type': 'string', 'Comment': ''},
 {'Name': 'type', 'Type': 'string', 'Comment': ''},
 {'Name': 'siege_pm', 'Type': 'string', 'Comment': ''},
 {'Name': 'rcs_registre', 'Type': 'string', 'Comment': ''},
 {'Name': 'adresse_ligne1', 'Type': 'string', 'Comment': ''},
 {'Name': 'adresse_ligne2', 'Type': 'string', 'Comment': ''},
 {'Name': 'adresse_ligne3', 'Type': 'string', 'Comment': ''},
 {'Name': 'code_postal', 'Type': 'string', 'Comment': ''},
 {'Name': 'ville', 'Type': 'string', 'Comment': ''},
 {'Name': 'code_commune', 'Type': 'string', 'Comment': ''},
 {'Name': 'pays', 'Type': 'string', 'Comment': ''},
 {'Name': 'domiciliataire_nom', 'Type': 'string', 'Comment': ''},
 {'Name': 'domiciliataire_siren', 'Type': 'string', 'Comment': ''},
 {'Name': 'domiciliataire_greffe', 'Type': 'string', 'Comment': ''},
 {'Name': 'domiciliataire_complement', 'Type': 'string', 'Comment': ''},
 {'Name': 'siege_domicile_representant', 'Type': 'string', 'Comment': ''},
 {'Name': 'nom_commercial', 'Type': 'string', 'Comment': ''},
 {'Name': 'enseigne', 'Type': 'string', 'Comment': ''},
 {'Name': 'activite_ambulante', 'Type': 'string', 'Comment': ''},
 {'Name': 'activite_saisonniere', 'Type': 'string', 'Comment': ''},
 {'Name': 'activite_non_sedentaire', 'Type': 'string', 'Comment': ''},
 {'Name': 'date_debut_activite', 'Type': 'string', 'Comment': ''},
 {'Name': 'activite', 'Type': 'string', 'Comment': ''},
 {'Name': 'origine_fonds', 'Type': 'string', 'Comment': ''},
 {'Name': 'origine_fonds_info', 'Type': 'string', 'Comment': ''},
 {'Name': 'type_exploitation', 'Type': 'string', 'Comment': ''},
 {'Name': 'id_etablissement', 'Type': 'string', 'Comment': ''},
 {'Name': 'date_greffe', 'Type': 'string', 'Comment': ''},
 {'Name': 'libelle_evt', 'Type': 'string', 'Comment': ''},
 {'Name': 'csv_source', 'Type': 'string', 'Comment': ''},
 {'Name': 'nature', 'Type': 'string', 'Comment': ''},
 {'Name': 'type_data', 'Type': 'string', 'Comment': ''},
 {'Name': 'origin', 'Type': 'string', 'Comment': ''},
 {'Name': 'file_timestamp', 'Type': 'string', 'Comment': ''}
    ],
    
    },
     {
    "database": "ets_inpi",
    "name": "ets_new_2017",
    "output_id": "",
    "separator": ";",
    "s3URI": "s3://calfdata/INPI/TC_1/01_donnee_source/Flux/2017/ETS/NEW",
    "schema": [
{'Name': 'code_greffe', 'Type': 'string', 'Comment': ''},
 {'Name': 'nom_greffe', 'Type': 'string', 'Comment': ''},
 {'Name': 'numero_gestion', 'Type': 'string', 'Comment': ''},
 {'Name': 'siren', 'Type': 'string', 'Comment': ''},
 {'Name': 'type', 'Type': 'string', 'Comment': ''},
 {'Name': 'siege_pm', 'Type': 'string', 'Comment': ''},
 {'Name': 'rcs_registre', 'Type': 'string', 'Comment': ''},
 {'Name': 'adresse_ligne1', 'Type': 'string', 'Comment': ''},
 {'Name': 'adresse_ligne2', 'Type': 'string', 'Comment': ''},
 {'Name': 'adresse_ligne3', 'Type': 'string', 'Comment': ''},
 {'Name': 'code_postal', 'Type': 'string', 'Comment': ''},
 {'Name': 'ville', 'Type': 'string', 'Comment': ''},
 {'Name': 'code_commune', 'Type': 'string', 'Comment': ''},
 {'Name': 'pays', 'Type': 'string', 'Comment': ''},
 {'Name': 'domiciliataire_nom', 'Type': 'string', 'Comment': ''},
 {'Name': 'domiciliataire_siren', 'Type': 'string', 'Comment': ''},
 {'Name': 'domiciliataire_greffe', 'Type': 'string', 'Comment': ''},
 {'Name': 'domiciliataire_complement', 'Type': 'string', 'Comment': ''},
 {'Name': 'siege_domicile_representant', 'Type': 'string', 'Comment': ''},
 {'Name': 'nom_commercial', 'Type': 'string', 'Comment': ''},
 {'Name': 'enseigne', 'Type': 'string', 'Comment': ''},
 {'Name': 'activite_ambulante', 'Type': 'string', 'Comment': ''},
 {'Name': 'activite_saisonniere', 'Type': 'string', 'Comment': ''},
 {'Name': 'activite_non_sedentaire', 'Type': 'string', 'Comment': ''},
 {'Name': 'date_debut_activite', 'Type': 'string', 'Comment': ''},
 {'Name': 'activite', 'Type': 'string', 'Comment': ''},
 {'Name': 'origine_fonds', 'Type': 'string', 'Comment': ''},
 {'Name': 'origine_fonds_info', 'Type': 'string', 'Comment': ''},
 {'Name': 'type_exploitation', 'Type': 'string', 'Comment': ''},
 {'Name': 'id_etablissement', 'Type': 'string', 'Comment': ''},
 {'Name': 'date_greffe', 'Type': 'string', 'Comment': ''},
 {'Name': 'libelle_evt', 'Type': 'string', 'Comment': ''},
 {'Name': 'csv_source', 'Type': 'string', 'Comment': ''},
 {'Name': 'nature', 'Type': 'string', 'Comment': ''},
 {'Name': 'type_data', 'Type': 'string', 'Comment': ''},
 {'Name': 'origin', 'Type': 'string', 'Comment': ''},
 {'Name': 'file_timestamp', 'Type': 'string', 'Comment': ''}
    ],
    
    },
     {
    "database": "ets_inpi",
    "name": "ets_new_2018",
    "output_id": "",
    "separator": ";",
    "s3URI": "s3://calfdata/INPI/TC_1/01_donnee_source/Flux/2018/ETS/NEW",
    "schema": [
{'Name': 'code_greffe', 'Type': 'string', 'Comment': ''},
 {'Name': 'nom_greffe', 'Type': 'string', 'Comment': ''},
 {'Name': 'numero_gestion', 'Type': 'string', 'Comment': ''},
 {'Name': 'siren', 'Type': 'string', 'Comment': ''},
 {'Name': 'type', 'Type': 'string', 'Comment': ''},
 {'Name': 'siege_pm', 'Type': 'string', 'Comment': ''},
 {'Name': 'rcs_registre', 'Type': 'string', 'Comment': ''},
 {'Name': 'adresse_ligne1', 'Type': 'string', 'Comment': ''},
 {'Name': 'adresse_ligne2', 'Type': 'string', 'Comment': ''},
 {'Name': 'adresse_ligne3', 'Type': 'string', 'Comment': ''},
 {'Name': 'code_postal', 'Type': 'string', 'Comment': ''},
 {'Name': 'ville', 'Type': 'string', 'Comment': ''},
 {'Name': 'code_commune', 'Type': 'string', 'Comment': ''},
 {'Name': 'pays', 'Type': 'string', 'Comment': ''},
 {'Name': 'domiciliataire_nom', 'Type': 'string', 'Comment': ''},
 {'Name': 'domiciliataire_siren', 'Type': 'string', 'Comment': ''},
 {'Name': 'domiciliataire_greffe', 'Type': 'string', 'Comment': ''},
 {'Name': 'domiciliataire_complement', 'Type': 'string', 'Comment': ''},
 {'Name': 'siege_domicile_representant', 'Type': 'string', 'Comment': ''},
 {'Name': 'nom_commercial', 'Type': 'string', 'Comment': ''},
 {'Name': 'enseigne', 'Type': 'string', 'Comment': ''},
 {'Name': 'activite_ambulante', 'Type': 'string', 'Comment': ''},
 {'Name': 'activite_saisonniere', 'Type': 'string', 'Comment': ''},
 {'Name': 'activite_non_sedentaire', 'Type': 'string', 'Comment': ''},
 {'Name': 'date_debut_activite', 'Type': 'string', 'Comment': ''},
 {'Name': 'activite', 'Type': 'string', 'Comment': ''},
 {'Name': 'origine_fonds', 'Type': 'string', 'Comment': ''},
 {'Name': 'origine_fonds_info', 'Type': 'string', 'Comment': ''},
 {'Name': 'type_exploitation', 'Type': 'string', 'Comment': ''},
 {'Name': 'id_etablissement', 'Type': 'string', 'Comment': ''},
 {'Name': 'date_greffe', 'Type': 'string', 'Comment': ''},
 {'Name': 'libelle_evt', 'Type': 'string', 'Comment': ''},
 {'Name': 'csv_source', 'Type': 'string', 'Comment': ''},
 {'Name': 'nature', 'Type': 'string', 'Comment': ''},
 {'Name': 'type_data', 'Type': 'string', 'Comment': ''},
 {'Name': 'origin', 'Type': 'string', 'Comment': ''},
 {'Name': 'file_timestamp', 'Type': 'string', 'Comment': ''}
    ],
    
    },
     {
    "database": "ets_inpi",
    "name": "ets_new_2019",
    "output_id": "",
    "separator": ";",
    "s3URI": "s3://calfdata/INPI/TC_1/01_donnee_source/Flux/2019/ETS/NEW",
    "schema": [
{'Name': 'code_greffe', 'Type': 'string', 'Comment': ''},
 {'Name': 'nom_greffe', 'Type': 'string', 'Comment': ''},
 {'Name': 'numero_gestion', 'Type': 'string', 'Comment': ''},
 {'Name': 'siren', 'Type': 'string', 'Comment': ''},
 {'Name': 'type', 'Type': 'string', 'Comment': ''},
 {'Name': 'siege_pm', 'Type': 'string', 'Comment': ''},
 {'Name': 'rcs_registre', 'Type': 'string', 'Comment': ''},
 {'Name': 'adresse_ligne1', 'Type': 'string', 'Comment': ''},
 {'Name': 'adresse_ligne2', 'Type': 'string', 'Comment': ''},
 {'Name': 'adresse_ligne3', 'Type': 'string', 'Comment': ''},
 {'Name': 'code_postal', 'Type': 'string', 'Comment': ''},
 {'Name': 'ville', 'Type': 'string', 'Comment': ''},
 {'Name': 'code_commune', 'Type': 'string', 'Comment': ''},
 {'Name': 'pays', 'Type': 'string', 'Comment': ''},
 {'Name': 'domiciliataire_nom', 'Type': 'string', 'Comment': ''},
 {'Name': 'domiciliataire_siren', 'Type': 'string', 'Comment': ''},
 {'Name': 'domiciliataire_greffe', 'Type': 'string', 'Comment': ''},
 {'Name': 'domiciliataire_complement', 'Type': 'string', 'Comment': ''},
 {'Name': 'siege_domicile_representant', 'Type': 'string', 'Comment': ''},
 {'Name': 'nom_commercial', 'Type': 'string', 'Comment': ''},
 {'Name': 'enseigne', 'Type': 'string', 'Comment': ''},
 {'Name': 'activite_ambulante', 'Type': 'string', 'Comment': ''},
 {'Name': 'activite_saisonniere', 'Type': 'string', 'Comment': ''},
 {'Name': 'activite_non_sedentaire', 'Type': 'string', 'Comment': ''},
 {'Name': 'date_debut_activite', 'Type': 'string', 'Comment': ''},
 {'Name': 'activite', 'Type': 'string', 'Comment': ''},
 {'Name': 'origine_fonds', 'Type': 'string', 'Comment': ''},
 {'Name': 'origine_fonds_info', 'Type': 'string', 'Comment': ''},
 {'Name': 'type_exploitation', 'Type': 'string', 'Comment': ''},
 {'Name': 'id_etablissement', 'Type': 'string', 'Comment': ''},
 {'Name': 'date_greffe', 'Type': 'string', 'Comment': ''},
 {'Name': 'libelle_evt', 'Type': 'string', 'Comment': ''},
 {'Name': 'csv_source', 'Type': 'string', 'Comment': ''},
 {'Name': 'nature', 'Type': 'string', 'Comment': ''},
 {'Name': 'type_data', 'Type': 'string', 'Comment': ''},
 {'Name': 'origin', 'Type': 'string', 'Comment': ''},
 {'Name': 'file_timestamp', 'Type': 'string', 'Comment': ''}
    ],
    
    },
     {
    "database": "ets_inpi",
    "name": "ets_evt_2017",
    "output_id": "",
    "separator": ";",
    "s3URI": "s3://calfdata/INPI/TC_1/01_donnee_source/Flux/2017/ETS/EVT",
    "schema": [
{'Name': 'code_greffe', 'Type': 'string', 'Comment': ''},
 {'Name': 'nom_greffe', 'Type': 'string', 'Comment': ''},
 {'Name': 'numero_gestion', 'Type': 'string', 'Comment': ''},
 {'Name': 'siren', 'Type': 'string', 'Comment': ''},
 {'Name': 'type', 'Type': 'string', 'Comment': ''},
 {'Name': 'siege_pm', 'Type': 'string', 'Comment': ''},
 {'Name': 'rcs_registre', 'Type': 'string', 'Comment': ''},
 {'Name': 'adresse_ligne1', 'Type': 'string', 'Comment': ''},
 {'Name': 'adresse_ligne2', 'Type': 'string', 'Comment': ''},
 {'Name': 'adresse_ligne3', 'Type': 'string', 'Comment': ''},
 {'Name': 'code_postal', 'Type': 'string', 'Comment': ''},
 {'Name': 'ville', 'Type': 'string', 'Comment': ''},
 {'Name': 'code_commune', 'Type': 'string', 'Comment': ''},
 {'Name': 'pays', 'Type': 'string', 'Comment': ''},
 {'Name': 'domiciliataire_nom', 'Type': 'string', 'Comment': ''},
 {'Name': 'domiciliataire_siren', 'Type': 'string', 'Comment': ''},
 {'Name': 'domiciliataire_greffe', 'Type': 'string', 'Comment': ''},
 {'Name': 'domiciliataire_complement', 'Type': 'string', 'Comment': ''},
 {'Name': 'siege_domicile_representant', 'Type': 'string', 'Comment': ''},
 {'Name': 'nom_commercial', 'Type': 'string', 'Comment': ''},
 {'Name': 'enseigne', 'Type': 'string', 'Comment': ''},
 {'Name': 'activite_ambulante', 'Type': 'string', 'Comment': ''},
 {'Name': 'activite_saisonniere', 'Type': 'string', 'Comment': ''},
 {'Name': 'activite_non_sedentaire', 'Type': 'string', 'Comment': ''},
 {'Name': 'date_debut_activite', 'Type': 'string', 'Comment': ''},
 {'Name': 'activite', 'Type': 'string', 'Comment': ''},
 {'Name': 'origine_fonds', 'Type': 'string', 'Comment': ''},
 {'Name': 'origine_fonds_info', 'Type': 'string', 'Comment': ''},
 {'Name': 'type_exploitation', 'Type': 'string', 'Comment': ''},
 {'Name': 'id_etablissement', 'Type': 'string', 'Comment': ''},
 {'Name': 'date_greffe', 'Type': 'string', 'Comment': ''},
 {'Name': 'libelle_evt', 'Type': 'string', 'Comment': ''},
 {'Name': 'csv_source', 'Type': 'string', 'Comment': ''},
 {'Name': 'nature', 'Type': 'string', 'Comment': ''},
 {'Name': 'type_data', 'Type': 'string', 'Comment': ''},
 {'Name': 'origin', 'Type': 'string', 'Comment': ''},
 {'Name': 'file_timestamp', 'Type': 'string', 'Comment': ''}
    ],
    
    },
     {
    "database": "ets_inpi",
    "name": "ets_evt_2018",
    "output_id": "",
    "separator": ";",
    "s3URI": "s3://calfdata/INPI/TC_1/01_donnee_source/Flux/2018/ETS/EVT",
    "schema": [
{'Name': 'code_greffe', 'Type': 'string', 'Comment': ''},
 {'Name': 'nom_greffe', 'Type': 'string', 'Comment': ''},
 {'Name': 'numero_gestion', 'Type': 'string', 'Comment': ''},
 {'Name': 'siren', 'Type': 'string', 'Comment': ''},
 {'Name': 'type', 'Type': 'string', 'Comment': ''},
 {'Name': 'siege_pm', 'Type': 'string', 'Comment': ''},
 {'Name': 'rcs_registre', 'Type': 'string', 'Comment': ''},
 {'Name': 'adresse_ligne1', 'Type': 'string', 'Comment': ''},
 {'Name': 'adresse_ligne2', 'Type': 'string', 'Comment': ''},
 {'Name': 'adresse_ligne3', 'Type': 'string', 'Comment': ''},
 {'Name': 'code_postal', 'Type': 'string', 'Comment': ''},
 {'Name': 'ville', 'Type': 'string', 'Comment': ''},
 {'Name': 'code_commune', 'Type': 'string', 'Comment': ''},
 {'Name': 'pays', 'Type': 'string', 'Comment': ''},
 {'Name': 'domiciliataire_nom', 'Type': 'string', 'Comment': ''},
 {'Name': 'domiciliataire_siren', 'Type': 'string', 'Comment': ''},
 {'Name': 'domiciliataire_greffe', 'Type': 'string', 'Comment': ''},
 {'Name': 'domiciliataire_complement', 'Type': 'string', 'Comment': ''},
 {'Name': 'siege_domicile_representant', 'Type': 'string', 'Comment': ''},
 {'Name': 'nom_commercial', 'Type': 'string', 'Comment': ''},
 {'Name': 'enseigne', 'Type': 'string', 'Comment': ''},
 {'Name': 'activite_ambulante', 'Type': 'string', 'Comment': ''},
 {'Name': 'activite_saisonniere', 'Type': 'string', 'Comment': ''},
 {'Name': 'activite_non_sedentaire', 'Type': 'string', 'Comment': ''},
 {'Name': 'date_debut_activite', 'Type': 'string', 'Comment': ''},
 {'Name': 'activite', 'Type': 'string', 'Comment': ''},
 {'Name': 'origine_fonds', 'Type': 'string', 'Comment': ''},
 {'Name': 'origine_fonds_info', 'Type': 'string', 'Comment': ''},
 {'Name': 'type_exploitation', 'Type': 'string', 'Comment': ''},
 {'Name': 'id_etablissement', 'Type': 'string', 'Comment': ''},
 {'Name': 'date_greffe', 'Type': 'string', 'Comment': ''},
 {'Name': 'libelle_evt', 'Type': 'string', 'Comment': ''},
 {'Name': 'csv_source', 'Type': 'string', 'Comment': ''},
 {'Name': 'nature', 'Type': 'string', 'Comment': ''},
 {'Name': 'type_data', 'Type': 'string', 'Comment': ''},
 {'Name': 'origin', 'Type': 'string', 'Comment': ''},
 {'Name': 'file_timestamp', 'Type': 'string', 'Comment': ''}
    ],
    
    },
     {
    "database": "ets_inpi",
    "name": "ets_evt_2019",
    "output_id": "",
    "separator": ";",
    "s3URI": "s3://calfdata/INPI/TC_1/01_donnee_source/Flux/2019/ETS/EVT",
    "schema": [
{'Name': 'code_greffe', 'Type': 'string', 'Comment': ''},
 {'Name': 'nom_greffe', 'Type': 'string', 'Comment': ''},
 {'Name': 'numero_gestion', 'Type': 'string', 'Comment': ''},
 {'Name': 'siren', 'Type': 'string', 'Comment': ''},
 {'Name': 'type', 'Type': 'string', 'Comment': ''},
 {'Name': 'siege_pm', 'Type': 'string', 'Comment': ''},
 {'Name': 'rcs_registre', 'Type': 'string', 'Comment': ''},
 {'Name': 'adresse_ligne1', 'Type': 'string', 'Comment': ''},
 {'Name': 'adresse_ligne2', 'Type': 'string', 'Comment': ''},
 {'Name': 'adresse_ligne3', 'Type': 'string', 'Comment': ''},
 {'Name': 'code_postal', 'Type': 'string', 'Comment': ''},
 {'Name': 'ville', 'Type': 'string', 'Comment': ''},
 {'Name': 'code_commune', 'Type': 'string', 'Comment': ''},
 {'Name': 'pays', 'Type': 'string', 'Comment': ''},
 {'Name': 'domiciliataire_nom', 'Type': 'string', 'Comment': ''},
 {'Name': 'domiciliataire_siren', 'Type': 'string', 'Comment': ''},
 {'Name': 'domiciliataire_greffe', 'Type': 'string', 'Comment': ''},
 {'Name': 'domiciliataire_complement', 'Type': 'string', 'Comment': ''},
 {'Name': 'siege_domicile_representant', 'Type': 'string', 'Comment': ''},
 {'Name': 'nom_commercial', 'Type': 'string', 'Comment': ''},
 {'Name': 'enseigne', 'Type': 'string', 'Comment': ''},
 {'Name': 'activite_ambulante', 'Type': 'string', 'Comment': ''},
 {'Name': 'activite_saisonniere', 'Type': 'string', 'Comment': ''},
 {'Name': 'activite_non_sedentaire', 'Type': 'string', 'Comment': ''},
 {'Name': 'date_debut_activite', 'Type': 'string', 'Comment': ''},
 {'Name': 'activite', 'Type': 'string', 'Comment': ''},
 {'Name': 'origine_fonds', 'Type': 'string', 'Comment': ''},
 {'Name': 'origine_fonds_info', 'Type': 'string', 'Comment': ''},
 {'Name': 'type_exploitation', 'Type': 'string', 'Comment': ''},
 {'Name': 'id_etablissement', 'Type': 'string', 'Comment': ''},
 {'Name': 'date_greffe', 'Type': 'string', 'Comment': ''},
 {'Name': 'libelle_evt', 'Type': 'string', 'Comment': ''},
 {'Name': 'csv_source', 'Type': 'string', 'Comment': ''},
 {'Name': 'nature', 'Type': 'string', 'Comment': ''},
 {'Name': 'type_data', 'Type': 'string', 'Comment': ''},
 {'Name': 'origin', 'Type': 'string', 'Comment': ''},
 {'Name': 'file_timestamp', 'Type': 'string', 'Comment': ''}
    ],
    
    }
]
```

To remove an item from the list, use `pop` with the index to remove. Exemple `parameters['TABLES']['CREATION']['ALL_SCHEMA'].pop(6)` will remove the 5th item

```python
to_remove = False
if to_remove:
    parameters['TABLES']['CREATION']['ALL_SCHEMA'].pop(0)
```

```python
parameters['TABLES']['CREATION']['ALL_SCHEMA'].extend(new_table)
```

```python
#print(json.dumps(parameters['TABLES']['CREATION']['ALL_SCHEMA'], indent=4, sort_keys=False, ensure_ascii=False))
```

```python
json_filename ='parameters_ETL.json'
json_file = json.dumps(parameters)
f = open(json_filename,"w")
f.write(json_file)
f.close()
s3.upload_file(json_filename, 'DATA/ETL')
```

```python
s3.download_file(key = 'DATA/ETL/parameters_ETL.json')
with open('parameters_ETL.json', 'r') as fp:
    parameters = json.load(fp)
```

## Step 1: Creation tables

Afin de ne pas mélanger l'ensemble des tables, nous allons créer 3 tables distinctes:

- 1 table pour les stocks initiaux: `ets_stock`
- 1 table pour les événements: `ets_flux`
- 1 table pour les partiels: `ets_partiel`

Etant donné que nous avons compartimenté les données par origine et année, nous devons créer une étape intermédiaire qui contient les tables par année

```python
"CREATE DATABASE ets_inpi"
```

On drop les tables si elles existent déjà.

```python
s3_output = parameters['GLOBAL']['QUERIES_OUTPUT']
db = parameters['GLOBAL']['DATABASE']
```

```python
for key, value in parameters["TABLES"]["CREATION"].items():
    if key == "ALL_SCHEMA":
        for table_info in value:
            # CREATE QUERY

            ### Create top/bottom query
            table_top = parameters["TABLES"]["CREATION"]["template"]["top"].format(
                        table_info["database"], table_info["name"]
                    )
            table_bottom = parameters["TABLES"]["CREATION"]["template"][
                        "bottom_OpenCSVSerde"
                    ].format(table_info["separator"], table_info["s3URI"])

            ### Create middle
            table_middle = ""
            nb_var = len(table_info["schema"])
            for i, val in enumerate(table_info["schema"]):
                if i == nb_var - 1:
                    table_middle += parameters["TABLES"]["CREATION"]["template"][
                                "middle"
                            ].format(val['Name'], val['Type'], ")")
                else:
                    table_middle += parameters["TABLES"]["CREATION"]["template"][
                                "middle"
                            ].format(val['Name'], val['Type'], ",")

            query = table_top + table_middle + table_bottom

            ## DROP IF EXIST

            s3.run_query(
                            query="DROP TABLE {}".format(table_info["name"]),
                            database=db,
                            s3_output=s3_output
                    )

            ## RUN QUERY
            output = s3.run_query(
                        query=query,
                        database=table_info["database"],
                        s3_output=s3_output,
                        filename=None,  ## Add filename to print dataframe
                        destination_key=None,  ### Add destination key if need to copy output
                    )

                ## SAVE QUERY ID
            table_info['output_id'] = output['QueryID']

                     ### UPDATE CATALOG -> ISSUE WITH WINDOWS :) 
                #https://github.com/boto/boto3/issues/1238
            #glue.update_schema_table(
            #            database=table_info["database"],
            #            table=table_info["name"],
            #            schema=table_info["schema"],
            #        )

            print(output)
```

## 3. Prepare `TABLES.PREPARATION`

In this stage of the ETL, we are processing the data from existing tables in Athena. This stage is meant to use one or more table to create temporary, intermediate or final tables to use in the analysis. The notebook template is named `XX_template_table_preprocessing_AWS` and should be saved in the child folder `02_prepare_tables_model`


Comme indiqué précédemment, il faut concatener les tables avant de faire le filtrage et enrichissement.

```python
step_0 = [
    {
   "STEPS_0":{
      "name":"Concatenation table stock",
      "execution":[
         {
            "database":"ets_inpi",
            "name":"ets_partiel",
            "output_id":"",
            "query":{
               "top":"WITH append AS ( SELECT * FROM ets_inpi.ets_partiel_2018",
               "middle":" UNION SELECT * FROM ets_inpi.ets_partiel_2019 ",
               "bottom":" ) SELECT * FROM append ORDER BY siren, code_greffe,nom_greffe,numero_gestion, id_etablissement, file_timestamp"
            }
         }
      ],
       "schema":[
               {
                  "Name":"",
                  "Type":"",
                  "Comment":""
               }
            ]
   }
},
    {
   "STEPS_1":{
      "name":"Concatenation table flux",
      "execution":[
         {
            "database":"ets_inpi",
            "name":"ets_flux",
            "output_id":"",
            "query":{
               "top":"WITH append AS ( SELECT * FROM ets_inpi.ets_new_2017",
               "middle":"  UNION SELECT * FROM ets_inpi.ets_new_2018 UNION SELECT * FROM ets_inpi.ets_new_2019 UNION SELECT * FROM ets_inpi.ets_evt_2017 UNION SELECT * FROM ets_inpi.ets_evt_2018 UNION SELECT * FROM ets_inpi.ets_evt_2019",
               "bottom":" ) SELECT * FROM append ORDER BY siren, code_greffe,nom_greffe,numero_gestion, id_etablissement, file_timestamp"
            }
         }
      ],
       "schema":[
               {
                  "Name":"",
                  "Type":"",
                  "Comment":""
               }
            ]
   }
}
]
```

```python
to_remove = False
if to_remove:
    parameters['TABLES']['PREPARATION']['ALL_SCHEMA'].pop(0)
```

```python
parameters['TABLES']['PREPARATION']['ALL_SCHEMA'].extend(step_0)
```

```python
parameters['TABLES']['PREPARATION']['ALL_SCHEMA']
```

```python
json_filename ='parameters_ETL.json'
json_file = json.dumps(parameters)
f = open(json_filename,"w")
f.write(json_file)
f.close()
s3.upload_file(json_filename, 'DATA/ETL')
```

```python
for key, value in parameters["TABLES"]["PREPARATION"].items():
    if key == "ALL_SCHEMA":
        ### LOOP STEPS
        for i, steps in enumerate(value):
            step_name = "STEPS_{}".format(i)

            ### LOOP EXECUTION WITHIN STEP
            for j, step_n in enumerate(steps[step_name]["execution"]):

                ### DROP IF EXIST
                s3.run_query(
                    query="DROP TABLE {}.{}".format(step_n["database"], step_n["name"]),
                    database=db,
                    s3_output=s3_output,
                )

                ### CREATE TOP
                table_top = parameters["TABLES"]["PREPARATION"]["template"][
                    "top"
                ].format(step_n["database"], step_n["name"],)

                ### COMPILE QUERY
                query = (
                    table_top
                    + step_n["query"]["top"]
                    + step_n["query"]["middle"]
                    + step_n["query"]["bottom"]
                )
                output = s3.run_query(
                    query=query,
                    database=db,
                    s3_output=s3_output,
                    filename=None,  ## Add filename to print dataframe
                    destination_key=None,  ### Add destination key if need to copy output
                )

                ## SAVE QUERY ID
                step_n["output_id"] = output["QueryID"]

                ### UPDATE CATALOG
                #glue.update_schema_table(
                #    database=step_n["database"],
                #    table=step_n["name"],
                #    schema=steps[step_name]["schema"],
                #)
                #print(query)

                print(output)
```

## Step 2: filtrage intra day et intra date de greffe

Dans cette étape, nous devons enrichir les lignes selon la partition suivante:

- siren, 
- code_greffe,
- nom_greffe,
- numero_gestion, 
- id_etablissement, 
    - file_timestamp
    - date_greffe

puis il faut récupérer la dernière ligne du `file_timestamp` pour une date de greffe (`date_greffe`) donnée.

Nous allons procéder en deux étapes totalement identiques. La première consiste a filtrer et enrichir la data en utilisant le time_stamp (date de transmission) et dans un second temps en filtrant et enrichissant via la date de greffe (événement). Au final, nous devons avoir qu'une seule ligne enrichie pour une entreprise et un événement donnée.

Etant donnée la taille de la data, nous allons préparer les flux selon les greffes. Les fichiers sont stockés dans le S3, [calfdata/INPI/TC_1/02_preparation_donnee/TEMP_ETS_FLUX](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/TC_1/02_preparation_donnee/TEMP_ETS_FLUX/?region=eu-west-3&tab=overview) pour le timestamp et vont etre récupéré dans la query suivante pour créer une table reconstruite. Chacun des csv portera le nom du greffe.

Les greffes "7202", "7501" et "9201" doivent etre traités séparement car ils sont trop volumineux.



```python
greffes = [

["1801",
"7803",
"0301",
"1104",
"8101",
"4801",
"6601",
"2801",
"2104",
"1402",
"1601",
"0605",
"8302",
"7601",
"5601",
"2702",
"7608",
"2401",
"6201",
"8201"],
["5103",
"1407",
"5401",
"7001",
"1704",
"5802",
"6901",
"6403",
"4202",
"0901",
"7301",
"3502",
"4401",
"5501",
"9401",
"1101",
"7802",
"4502",
"8801",
"1708",
"3402"],
["7901",
"2602",
"2001",
"6401",
"0602",
"3902",
"5602",
"3405",
"6502",
"4901",
"8002",
"5906",
"8102",
"0603",
"0202",
"7801",
"4302",
"7701",
"2402",
"5002",
"6101",
"5910"],
["1303",
"3303",
"0702",
"8602",
"9301",
"4601",
"5902",
"1301",
"6303",
"4201",
"6202",
"3801",
"6002",
"7102",
"2301",
"5001",
"5402",
"9001",
"7106",
"2002",
"4001",
"3201"],
["4101",
"0501",
"3003",
"1501",
"4402",
"8903",
"0203",
"5952",
"2202",
"1304",
"3701",
"8701",
"3302",
"3501",
"0802",
"6903",
"5101",
"4701",
"3102",
"1203",
"3802"],
["2501",
"8901",
"2903",
"2701",
"6001",
"5201",
"8305",
"1901",
"7702",
"8401"],
["7202"],
["7501"],
["9201"],
["8501",
"7401",
"7606",
"0101",
"7402",
"0601",
"8303",
"0303",
"5301",
"3601",
"1305",
"4002",
"2901",
"1001",
"0401"]
]

```

```python
s3.download_file(key = 'DATA/ETL/parameters_ETL.json')
with open('parameters_ETL.json', 'r') as fp:
    parameters = json.load(fp)
```

```python
step_1 = {
   "STEPS_2":{
      "name":"filtrage intra day et intra date de greffe",
      "execution":[
         {
            "database":"ets_inpi",
            "name":"",
            "output_id":"",
            "query":{
               "top":"WITH createID AS ( SELECT *, ROW_NUMBER() OVER ( PARTITION BY siren,code_greffe,nom_greffe,numero_gestion,id_etablissement,file_timestamp ) As row_ID, DENSE_RANK () OVER ( ORDER BY siren,code_greffe,nom_greffe,numero_gestion,id_etablissement,file_timestamp ) As ID FROM ets_inpi.ets_flux WHERE  ( code_greffe = '{}') ) ",
               "middle":""" SELECT * FROM ( WITH filled AS ( SELECT ID, row_ID, siren,code_greffe,nom_greffe,numero_gestion,id_etablissement,file_timestamp, first_value(\"libelle_evt\") over (partition by ID, \"libelle_evt_partition\" order by ID, row_ID ) as \"libelle_evt\" ,first_value(\"date_greffe\") over (partition by ID, \"date_greffe_partition\" order by ID, row_ID ) as \"date_greffe\" ,first_value(\"type\") over (partition by ID, \"type_partition\" order by ID, row_ID ) as \"type\" ,first_value(\"siege_pm\") over (partition by ID, \"siege_pm_partition\" order by ID, row_ID ) as \"siege_pm\" ,first_value(\"rcs_registre\") over (partition by ID, \"rcs_registre_partition\" order by ID, row_ID ) as \"rcs_registre\" ,first_value(\"adresse_ligne1\") over (partition by ID, \"adresse_ligne1_partition\" order by ID, row_ID ) as \"adresse_ligne1\" ,first_value(\"adresse_ligne2\") over (partition by ID, \"adresse_ligne2_partition\" order by ID, row_ID ) as \"adresse_ligne2\" ,first_value(\"adresse_ligne3\") over (partition by ID, \"adresse_ligne3_partition\" order by ID, row_ID ) as \"adresse_ligne3\" ,first_value(\"code_postal\") over (partition by ID, \"code_postal_partition\" order by ID, row_ID ) as \"code_postal\" ,first_value(\"ville\") over (partition by ID, \"ville_partition\" order by ID, row_ID ) as \"ville\" ,first_value(\"code_commune\") over (partition by ID, \"code_commune_partition\" order by ID, row_ID ) as \"code_commune\" ,first_value(\"pays\") over (partition by ID, \"pays_partition\" order by ID, row_ID ) as \"pays\" ,first_value(\"domiciliataire_nom\") over (partition by ID, \"domiciliataire_nom_partition\" order by ID, row_ID ) as \"domiciliataire_nom\" ,first_value(\"domiciliataire_siren\") over (partition by ID, \"domiciliataire_siren_partition\" order by ID, row_ID ) as \"domiciliataire_siren\" ,first_value(\"domiciliataire_greffe\") over (partition by ID, \"domiciliataire_greffe_partition\" order by ID, row_ID ) as \"domiciliataire_greffe\" ,first_value(\"domiciliataire_complement\") over (partition by ID, \"domiciliataire_complement_partition\" order by ID, row_ID ) as \"domiciliataire_complement\" ,first_value(\"siege_domicile_representant\") over (partition by ID, \"siege_domicile_representant_partition\" order by ID, row_ID ) as \"siege_domicile_representant\" ,first_value(\"nom_commercial\") over (partition by ID, \"nom_commercial_partition\" order by ID, row_ID ) as \"nom_commercial\" ,first_value(\"enseigne\") over (partition by ID, \"enseigne_partition\" order by ID, row_ID ) as \"enseigne\" ,first_value(\"activite_ambulante\") over (partition by ID, \"activite_ambulante_partition\" order by ID, row_ID ) as \"activite_ambulante\" ,first_value(\"activite_saisonniere\") over (partition by ID, \"activite_saisonniere_partition\" order by ID, row_ID ) as \"activite_saisonniere\" ,first_value(\"activite_non_sedentaire\") over (partition by ID, \"activite_non_sedentaire_partition\" order by ID, row_ID ) as \"activite_non_sedentaire\" ,first_value(\"date_debut_activite\") over (partition by ID, \"date_debut_activite_partition\" order by ID, row_ID ) as \"date_debut_activite\" ,first_value(\"activite\") over (partition by ID, \"activite_partition\" order by ID, row_ID ) as \"activite\" ,first_value(\"origine_fonds\") over (partition by ID, \"origine_fonds_partition\" order by ID, row_ID ) as \"origine_fonds\" ,first_value(\"origine_fonds_info\") over (partition by ID, \"origine_fonds_info_partition\" order by ID, row_ID ) as \"origine_fonds_info\" ,first_value(\"type_exploitation\") over (partition by ID, \"type_exploitation_partition\" order by ID, row_ID ) as \"type_exploitation\" ,first_value(\"csv_source\") over (partition by ID, \"csv_source_partition\" order by ID, row_ID ) as \"csv_source\" FROM ( SELECT *, sum(case when \"libelle_evt\" = '' then 0 else 1 end) over (partition by ID order by row_ID) as \"libelle_evt_partition\" ,sum(case when \"date_greffe\" = '' then 0 else 1 end) over (partition by ID order by row_ID) as \"date_greffe_partition\" ,sum(case when \"type\" = '' then 0 else 1 end) over (partition by ID order by row_ID) as \"type_partition\" ,sum(case when \"siege_pm\" = '' then 0 else 1 end) over (partition by ID order by row_ID) as \"siege_pm_partition\" ,sum(case when \"rcs_registre\" = '' then 0 else 1 end) over (partition by ID order by row_ID) as \"rcs_registre_partition\" ,sum(case when \"adresse_ligne1\" = '' then 0 else 1 end) over (partition by ID order by row_ID) as \"adresse_ligne1_partition\" ,sum(case when \"adresse_ligne2\" = '' then 0 else 1 end) over (partition by ID order by row_ID) as \"adresse_ligne2_partition\" ,sum(case when \"adresse_ligne3\" = '' then 0 else 1 end) over (partition by ID order by row_ID) as \"adresse_ligne3_partition\" ,sum(case when \"code_postal\" = '' then 0 else 1 end) over (partition by ID order by row_ID) as \"code_postal_partition\" ,sum(case when \"ville\" = '' then 0 else 1 end) over (partition by ID order by row_ID) as \"ville_partition\" ,sum(case when \"code_commune\" = '' then 0 else 1 end) over (partition by ID order by row_ID) as \"code_commune_partition\" ,sum(case when \"pays\" = '' then 0 else 1 end) over (partition by ID order by row_ID) as \"pays_partition\" ,sum(case when \"domiciliataire_nom\" = '' then 0 else 1 end) over (partition by ID order by row_ID) as \"domiciliataire_nom_partition\" ,sum(case when \"domiciliataire_siren\" = '' then 0 else 1 end) over (partition by ID order by row_ID) as \"domiciliataire_siren_partition\" ,sum(case when \"domiciliataire_greffe\" = '' then 0 else 1 end) over (partition by ID order by row_ID) as \"domiciliataire_greffe_partition\" ,sum(case when \"domiciliataire_complement\" = '' then 0 else 1 end) over (partition by ID order by row_ID) as \"domiciliataire_complement_partition\" ,sum(case when \"siege_domicile_representant\" = '' then 0 else 1 end) over (partition by ID order by row_ID) as \"siege_domicile_representant_partition\" ,sum(case when \"nom_commercial\" = '' then 0 else 1 end) over (partition by ID order by row_ID) as \"nom_commercial_partition\" ,sum(case when \"enseigne\" = '' then 0 else 1 end) over (partition by ID order by row_ID) as \"enseigne_partition\" ,sum(case when \"activite_ambulante\" = '' then 0 else 1 end) over (partition by ID order by row_ID) as \"activite_ambulante_partition\" ,sum(case when \"activite_saisonniere\" = '' then 0 else 1 end) over (partition by ID order by row_ID) as \"activite_saisonniere_partition\" ,sum(case when \"activite_non_sedentaire\" = '' then 0 else 1 end) over (partition by ID order by row_ID) as \"activite_non_sedentaire_partition\" ,sum(case when \"date_debut_activite\" = '' then 0 else 1 end) over (partition by ID order by row_ID) as \"date_debut_activite_partition\" ,sum(case when \"activite\" = '' then 0 else 1 end) over (partition by ID order by row_ID) as \"activite_partition\" ,sum(case when \"origine_fonds\" = '' then 0 else 1 end) over (partition by ID order by row_ID) as \"origine_fonds_partition\" ,sum(case when \"origine_fonds_info\" = '' then 0 else 1 end) over (partition by ID order by row_ID) as \"origine_fonds_info_partition\" ,sum(case when \"type_exploitation\" = '' then 0 else 1 end) over (partition by ID order by row_ID) as \"type_exploitation_partition\" ,sum(case when \"csv_source\" = '' then 0 else 1 end) over (partition by ID order by row_ID) as \"csv_source_partition\" FROM createID ORDER BY ID, row_ID ASC ) ORDER BY ID, row_ID ) """,
                "bottom":" SELECT siren,code_greffe,nom_greffe,numero_gestion,id_etablissement,file_timestamp, libelle_evt,date_greffe,type,siege_pm,rcs_registre,adresse_ligne1,adresse_ligne2,adresse_ligne3,code_postal,ville,code_commune,pays,domiciliataire_nom,domiciliataire_siren,domiciliataire_greffe,domiciliataire_complement,siege_domicile_representant,nom_commercial,enseigne,activite_ambulante,activite_saisonniere,activite_non_sedentaire,date_debut_activite,activite,origine_fonds,origine_fonds_info,type_exploitation,csv_source, CASE WHEN siren IS NOT NULL THEN 'EVT' ELSE NULL END as origin FROM ( SELECT *, ROW_NUMBER() OVER( PARTITION BY ID ORDER BY ID, row_ID DESC ) AS max_value FROM filled ) AS T WHERE max_value = 1 )ORDER BY siren,code_greffe,nom_greffe,numero_gestion,id_etablissement,file_timestamp "
            }
         }
      ],
       "schema":[
               {
                  "Name":"",
                  "Type":"",
                  "Comment":""
               }
            ]
   }
}
```

```python
parameters['TABLES']['PREPARATION']['ALL_SCHEMA'].append(step_1)
```

```python
parameters['TABLES']['PREPARATION']['ALL_SCHEMA'][-1]
```

```python
json_filename ='parameters_ETL.json'
json_file = json.dumps(parameters)
f = open(json_filename,"w")
f.write(json_file)
f.close()
s3.upload_file(json_filename, 'DATA/ETL')
```

```python
for greffe in greffes[-2:]:
    ### Preparer le filtre
    filter_greffe = "' OR code_greffe = '".join(greffe)
    namefile = '_'.join(greffe)
```

```python
for greffe in greffes[-4:]:
    ### Preparer le filtre
    filter_greffe = "' OR code_greffe = '".join(greffe)
    namefile = '_'.join(greffe)
    for key, value in parameters["TABLES"]["PREPARATION"].items():
        if key == "ALL_SCHEMA":
            ### LOOP STEPS
            for i, steps in enumerate(value):
                step_name = "STEPS_{}".format(i)
                if step_name in ['STEPS_2']:

                    ### LOOP EXECUTION WITHIN STEP
                    for j, step_n in enumerate(steps[step_name]["execution"]):
                        ### CREATE TOP
                        ### COMPILE QUERY
                        query = (
                            step_n["query"]["top"].format(filter_greffe)
                            + step_n["query"]["middle"]
                            + step_n["query"]["bottom"]
                        )
                        output = s3.run_query(
                            query=query,
                            database=db,
                            s3_output=s3_output,
                            filename=None,  ## Add filename to print dataframe
                            destination_key=None,  ### Add destination key if need to copy output
                        )

                        ## SAVE QUERY ID
                        step_n["output_id"] = output["QueryID"]

                        ### UPDATE CATALOG
                        #glue.update_schema_table(
                        #    database=step_n["database"],
                        #    table=step_n["name"],
                        #    schema=steps[step_name]["schema"],
                        #)
                        print(output)
                        source_key = '{}/{}.csv'.format(s3_output, output['QueryID'])
                        destination_key = '{0}/{1}.csv'.format("INPI/TC_1/02_preparation_donnee/TEMP_ETS_FLUX",namefile)
                        s3.move_object_s3(source_key, destination_key, remove = True)
```

### Table filtree et enrichie intermediaire timestamps


Nous venons de filtrer les transmissions intra day, mais pas par date de greffe. L'ensemble des CSV sont dans le S3. Il nous suffit de créer une table intermédiaire, puis de réitéter l'opération non pas sur le timestamp, mais sur la date de greffe. Il est possible qu'une transmission possède plusieurs lignes pour la même transmission. C'est une erreur de notre part lors de la création de la table initiale, nous aurions du créer un numéro de ligne au sein du groupe et ne récupérer que la ligne maximum. Temporairement, nous filtrons la dernière ligne, même si elle n'est indiquée comme la dernière dans les CSV (entre date de greffe)

```python
s3.download_file(key = 'DATA/ETL/parameters_ETL.json')
with open('parameters_ETL.json', 'r') as fp:
    parameters = json.load(fp)
```

```python
table_filre_timestamp = [{
    "database": "ets_inpi",
    "name": "ets_flux_filtre_enrichie_timestamp",
    "output_id": "",
    "separator": ",",
    "s3URI": "s3://calfdata/INPI/TC_1/02_preparation_donnee/TEMP_ETS_FLUX",
    "schema": [
        {'Name': 'siren', 'Type': 'string', 'Comment': ''},
 {'Name': 'code_greffe', 'Type': 'string', 'Comment': ''},
 {'Name': 'nom_greffe', 'Type': 'string', 'Comment': ''},
 {'Name': 'numero_gestion', 'Type': 'string', 'Comment': ''},
 {'Name': 'id_etablissement', 'Type': 'string', 'Comment': ''},
 {'Name': 'file_timestamp', 'Type': 'string', 'Comment': ''},
 {'Name': 'libelle_evt', 'Type': 'string', 'Comment': ''},
 {'Name': 'date_greffe', 'Type': 'string', 'Comment': ''},
 {'Name': 'type', 'Type': 'string', 'Comment': ''},
 {'Name': 'siege_pm', 'Type': 'string', 'Comment': ''},
 {'Name': 'rcs_registre', 'Type': 'string', 'Comment': ''},
 {'Name': 'adresse_ligne1', 'Type': 'string', 'Comment': ''},
 {'Name': 'adresse_ligne2', 'Type': 'string', 'Comment': ''},
 {'Name': 'adresse_ligne3', 'Type': 'string', 'Comment': ''},
 {'Name': 'code_postal', 'Type': 'string', 'Comment': ''},
 {'Name': 'ville', 'Type': 'string', 'Comment': ''},
 {'Name': 'code_commune', 'Type': 'string', 'Comment': ''},
 {'Name': 'pays', 'Type': 'string', 'Comment': ''},
 {'Name': 'domiciliataire_nom', 'Type': 'string', 'Comment': ''},
 {'Name': 'domiciliataire_siren', 'Type': 'string', 'Comment': ''},
 {'Name': 'domiciliataire_greffe', 'Type': 'string', 'Comment': ''},
 {'Name': 'domiciliataire_complement', 'Type': 'string', 'Comment': ''},
 {'Name': 'siege_domicile_representant', 'Type': 'string', 'Comment': ''},
 {'Name': 'nom_commercial', 'Type': 'string', 'Comment': ''},
 {'Name': 'enseigne', 'Type': 'string', 'Comment': ''},
 {'Name': 'activite_ambulante', 'Type': 'string', 'Comment': ''},
 {'Name': 'activite_saisonniere', 'Type': 'string', 'Comment': ''},
 {'Name': 'activite_non_sedentaire', 'Type': 'string', 'Comment': ''},
 {'Name': 'date_debut_activite', 'Type': 'string', 'Comment': ''},
 {'Name': 'activite', 'Type': 'string', 'Comment': ''},
 {'Name': 'origine_fonds', 'Type': 'string', 'Comment': ''},
 {'Name': 'origine_fonds_info', 'Type': 'string', 'Comment': ''},
 {'Name': 'type_exploitation', 'Type': 'string', 'Comment': ''},
 {'Name': 'csv_source', 'Type': 'string', 'Comment': ''}
    ]
}
]
```

```python
parameters['TABLES']['CREATION']['ALL_SCHEMA'].extend(table_filre_timestamp)
```

```python
parameters['TABLES']['CREATION']['ALL_SCHEMA'][-1]
```

```python
#parameters['TABLES']['CREATION']['ALL_SCHEMA'].pop(-1)
```

```python
json_filename ='parameters_ETL.json'
json_file = json.dumps(parameters)
f = open(json_filename,"w")
f.write(json_file)
f.close()
s3.upload_file(json_filename, 'DATA/ETL')
```

```python
for key, value in parameters["TABLES"]["CREATION"].items():
    if key == "ALL_SCHEMA":
        for table_info in value:
            if table_info['name'] in ['ets_flux_filtre_enrichie_timestamp']:
                # CREATE QUERY

                ### Create top/bottom query
                table_top = parameters["TABLES"]["CREATION"]["template"]["top"].format(
                            table_info["database"], table_info["name"]
                        )
                table_bottom = parameters["TABLES"]["CREATION"]["template"][
                            "bottom_OpenCSVSerde"
                        ].format(table_info["separator"], table_info["s3URI"])

                ### Create middle
                table_middle = ""
                nb_var = len(table_info["schema"])
                for i, val in enumerate(table_info["schema"]):
                    if i == nb_var - 1:
                        table_middle += parameters["TABLES"]["CREATION"]["template"][
                                    "middle"
                                ].format(val['Name'], val['Type'], ")")
                    else:
                        table_middle += parameters["TABLES"]["CREATION"]["template"][
                                    "middle"
                                ].format(val['Name'], val['Type'], ",")

                query = table_top + table_middle + table_bottom
                ## DROP IF EXIST

                s3.run_query(
                                query="DROP TABLE {}".format(table_info["name"]),
                                database=db,
                                s3_output=s3_output
                        )

                ## RUN QUERY
                output = s3.run_query(
                            query=query,
                            database=table_info["database"],
                            s3_output=s3_output,
                            filename=None,  ## Add filename to print dataframe
                            destination_key=None,  ### Add destination key if need to copy output
                        )

                    ## SAVE QUERY ID
                table_info['output_id'] = output['QueryID']

                         ### UPDATE CATALOG
                #glue.update_schema_table(
                #            database=table_info["database"],
                #            table=table_info["name"],
                #            schema=table_info["schema"],
                #        )

                print(output)
```

### Table filtree et enrichie intermediaire date greffe

Etant donnée la taille de la data, nous allons préparer les flux selon les greffes. Les fichiers sont stockés dans le S3, [calfdata/INPI/TC_1/02_preparation_donnee/TEMP_ETS_FLUX_FILTRE](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/TC_1/02_preparation_donnee/TEMP_ETS_FLUX_FILTRE/?region=eu-west-3&tab=overview) et vont etre récupéré dans la query suivante pour créer une table reconstruite. Chacun des csv portera le nom du greffe.

```python
s3.download_file(key = 'DATA/ETL/parameters_ETL.json')
with open('parameters_ETL.json', 'r') as fp:
    parameters = json.load(fp)
```

```python
step_3 = {
   "STEPS_3":{
      "name":"Table filtree et enrichie intermediaire date greffe",
      "execution":[
         {
            "database":"ets_inpi",
            "name":"",
            "output_id":"",
            "query":{
               "top":"WITH createID AS ( SELECT *, ROW_NUMBER() OVER ( PARTITION BY siren,code_greffe,nom_greffe,numero_gestion,id_etablissement,date_greffe ) As row_ID, DENSE_RANK () OVER ( ORDER BY siren,code_greffe,nom_greffe,numero_gestion,id_etablissement,date_greffe ) As ID FROM ets_inpi.ets_flux_filtre_enrichie_timestamp WHERE  ( code_greffe = '{}') ) ",
               "middle":""" SELECT * FROM ( WITH filled AS ( SELECT ID, row_ID, siren,code_greffe,nom_greffe,numero_gestion,id_etablissement,date_greffe, first_value("libelle_evt") over (partition by ID, "libelle_evt_partition" order by ID, row_ID ) as "libelle_evt" ,first_value("type") over (partition by ID, "type_partition" order by ID, row_ID ) as "type" ,first_value("siege_pm") over (partition by ID, "siege_pm_partition" order by ID, row_ID ) as "siege_pm" ,first_value("rcs_registre") over (partition by ID, "rcs_registre_partition" order by ID, row_ID ) as "rcs_registre" ,first_value("adresse_ligne1") over (partition by ID, "adresse_ligne1_partition" order by ID, row_ID ) as "adresse_ligne1" ,first_value("adresse_ligne2") over (partition by ID, "adresse_ligne2_partition" order by ID, row_ID ) as "adresse_ligne2" ,first_value("adresse_ligne3") over (partition by ID, "adresse_ligne3_partition" order by ID, row_ID ) as "adresse_ligne3" ,first_value("code_postal") over (partition by ID, "code_postal_partition" order by ID, row_ID ) as "code_postal" ,first_value("ville") over (partition by ID, "ville_partition" order by ID, row_ID ) as "ville" ,first_value("code_commune") over (partition by ID, "code_commune_partition" order by ID, row_ID ) as "code_commune" ,first_value("pays") over (partition by ID, "pays_partition" order by ID, row_ID ) as "pays" ,first_value("domiciliataire_nom") over (partition by ID, "domiciliataire_nom_partition" order by ID, row_ID ) as "domiciliataire_nom" ,first_value("domiciliataire_siren") over (partition by ID, "domiciliataire_siren_partition" order by ID, row_ID ) as "domiciliataire_siren" ,first_value("domiciliataire_greffe") over (partition by ID, "domiciliataire_greffe_partition" order by ID, row_ID ) as "domiciliataire_greffe" ,first_value("domiciliataire_complement") over (partition by ID, "domiciliataire_complement_partition" order by ID, row_ID ) as "domiciliataire_complement" ,first_value("siege_domicile_representant") over (partition by ID, "siege_domicile_representant_partition" order by ID, row_ID ) as "siege_domicile_representant" ,first_value("nom_commercial") over (partition by ID, "nom_commercial_partition" order by ID, row_ID ) as "nom_commercial" ,first_value("enseigne") over (partition by ID, "enseigne_partition" order by ID, row_ID ) as "enseigne" ,first_value("activite_ambulante") over (partition by ID, "activite_ambulante_partition" order by ID, row_ID ) as "activite_ambulante" ,first_value("activite_saisonniere") over (partition by ID, "activite_saisonniere_partition" order by ID, row_ID ) as "activite_saisonniere" ,first_value("activite_non_sedentaire") over (partition by ID, "activite_non_sedentaire_partition" order by ID, row_ID ) as "activite_non_sedentaire" ,first_value("date_debut_activite") over (partition by ID, "date_debut_activite_partition" order by ID, row_ID ) as "date_debut_activite" ,first_value("activite") over (partition by ID, "activite_partition" order by ID, row_ID ) as "activite" ,first_value("origine_fonds") over (partition by ID, "origine_fonds_partition" order by ID, row_ID ) as "origine_fonds" ,first_value("origine_fonds_info") over (partition by ID, "origine_fonds_info_partition" order by ID, row_ID ) as "origine_fonds_info" ,first_value("type_exploitation") over (partition by ID, "type_exploitation_partition" order by ID, row_ID ) as "type_exploitation" ,first_value("csv_source") over (partition by ID, "csv_source_partition" order by ID, row_ID ) as "csv_source" FROM ( SELECT *, sum(case when "libelle_evt" = '' then 0 else 1 end) over (partition by ID order by row_ID) as "libelle_evt_partition" ,sum(case when "type" = '' then 0 else 1 end) over (partition by ID order by row_ID) as "type_partition" ,sum(case when "siege_pm" = '' then 0 else 1 end) over (partition by ID order by row_ID) as "siege_pm_partition" ,sum(case when "rcs_registre" = '' then 0 else 1 end) over (partition by ID order by row_ID) as "rcs_registre_partition" ,sum(case when "adresse_ligne1" = '' then 0 else 1 end) over (partition by ID order by row_ID) as "adresse_ligne1_partition" ,sum(case when "adresse_ligne2" = '' then 0 else 1 end) over (partition by ID order by row_ID) as "adresse_ligne2_partition" ,sum(case when "adresse_ligne3" = '' then 0 else 1 end) over (partition by ID order by row_ID) as "adresse_ligne3_partition" ,sum(case when "code_postal" = '' then 0 else 1 end) over (partition by ID order by row_ID) as "code_postal_partition" ,sum(case when "ville" = '' then 0 else 1 end) over (partition by ID order by row_ID) as "ville_partition" ,sum(case when "code_commune" = '' then 0 else 1 end) over (partition by ID order by row_ID) as "code_commune_partition" ,sum(case when "pays" = '' then 0 else 1 end) over (partition by ID order by row_ID) as "pays_partition" ,sum(case when "domiciliataire_nom" = '' then 0 else 1 end) over (partition by ID order by row_ID) as "domiciliataire_nom_partition" ,sum(case when "domiciliataire_siren" = '' then 0 else 1 end) over (partition by ID order by row_ID) as "domiciliataire_siren_partition" ,sum(case when "domiciliataire_greffe" = '' then 0 else 1 end) over (partition by ID order by row_ID) as "domiciliataire_greffe_partition" ,sum(case when "domiciliataire_complement" = '' then 0 else 1 end) over (partition by ID order by row_ID) as "domiciliataire_complement_partition" ,sum(case when "siege_domicile_representant" = '' then 0 else 1 end) over (partition by ID order by row_ID) as "siege_domicile_representant_partition" ,sum(case when "nom_commercial" = '' then 0 else 1 end) over (partition by ID order by row_ID) as "nom_commercial_partition" ,sum(case when "enseigne" = '' then 0 else 1 end) over (partition by ID order by row_ID) as "enseigne_partition" ,sum(case when "activite_ambulante" = '' then 0 else 1 end) over (partition by ID order by row_ID) as "activite_ambulante_partition" ,sum(case when "activite_saisonniere" = '' then 0 else 1 end) over (partition by ID order by row_ID) as "activite_saisonniere_partition" ,sum(case when "activite_non_sedentaire" = '' then 0 else 1 end) over (partition by ID order by row_ID) as "activite_non_sedentaire_partition" ,sum(case when "date_debut_activite" = '' then 0 else 1 end) over (partition by ID order by row_ID) as "date_debut_activite_partition" ,sum(case when "activite" = '' then 0 else 1 end) over (partition by ID order by row_ID) as "activite_partition" ,sum(case when "origine_fonds" = '' then 0 else 1 end) over (partition by ID order by row_ID) as "origine_fonds_partition" ,sum(case when "origine_fonds_info" = '' then 0 else 1 end) over (partition by ID order by row_ID) as "origine_fonds_info_partition" ,sum(case when "type_exploitation" = '' then 0 else 1 end) over (partition by ID order by row_ID) as "type_exploitation_partition" ,sum(case when "csv_source" = '' then 0 else 1 end) over (partition by ID order by row_ID) as "csv_source_partition" FROM createID ORDER BY ID, row_ID ASC ) ORDER BY ID, row_ID ) """,
               "bottom":" SELECT siren,code_greffe,nom_greffe,numero_gestion,id_etablissement,date_greffe, libelle_evt,type,siege_pm,rcs_registre,adresse_ligne1,adresse_ligne2,adresse_ligne3,code_postal,ville,code_commune,pays,domiciliataire_nom,domiciliataire_siren,domiciliataire_greffe,domiciliataire_complement,siege_domicile_representant,nom_commercial,enseigne,activite_ambulante,activite_saisonniere,activite_non_sedentaire,date_debut_activite,activite,origine_fonds,origine_fonds_info,type_exploitation,csv_source, CASE WHEN siren IS NOT NULL THEN 'EVT' ELSE NULL END as origin FROM ( SELECT *, ROW_NUMBER() OVER( PARTITION BY ID ORDER BY ID, row_ID DESC ) AS max_value FROM filled ) AS T WHERE max_value = 1 )ORDER BY siren,code_greffe,nom_greffe,numero_gestion,id_etablissement,date_greffe"
            }
         }
      ],
       "schema":[
               {
                  "Name":"",
                  "Type":"",
                  "Comment":""
               }
            ]
   }
}
```

```python
parameters['TABLES']['PREPARATION']['ALL_SCHEMA'].append(step_3)
```

```python
parameters['TABLES']['PREPARATION']['ALL_SCHEMA'][-1]
```

```python
json_filename ='parameters_ETL.json'
json_file = json.dumps(parameters)
f = open(json_filename,"w")
f.write(json_file)
f.close()
s3.upload_file(json_filename, 'DATA/ETL')
```

```python
for greffe in greffes:
    ### Preparer le filtre
    filter_greffe = "' OR code_greffe = '".join(greffe)
    namefile = '_'.join(greffe)
    for key, value in parameters["TABLES"]["PREPARATION"].items():
        if key == "ALL_SCHEMA":
            ### LOOP STEPS
            for i, steps in enumerate(value):
                step_name = "STEPS_{}".format(i)
                if step_name in ['STEPS_3']:

                    ### LOOP EXECUTION WITHIN STEP
                    for j, step_n in enumerate(steps[step_name]["execution"]):
                        ### CREATE TOP
                        ### COMPILE QUERY
                        query = (
                            step_n["query"]["top"].format(filter_greffe)
                            + step_n["query"]["middle"]
                            + step_n["query"]["bottom"]
                        )
                        output = s3.run_query(
                            query=query,
                            database=db,
                            s3_output=s3_output,
                            filename=None,  ## Add filename to print dataframe
                            destination_key=None,  ### Add destination key if need to copy output
                        )

                        ## SAVE QUERY ID
                        step_n["output_id"] = output["QueryID"]

                        ### UPDATE CATALOG
                        #glue.update_schema_table(
                        #    database=step_n["database"],
                        #    table=step_n["name"],
                        #    schema=steps[step_name]["schema"],
                        #)
                        print(output)
                        source_key = '{}/{}.csv'.format(s3_output, output['QueryID'])
                        destination_key = '{0}/{1}.csv'.format("INPI/TC_1/02_preparation_donnee/TEMP_ETS_FLUX_FILTRE",namefile)
                        s3.move_object_s3(source_key, destination_key, remove = True)
```

On peut créer la table filtrée et enrichie avec une seule ligne par date de greffe

```python
s3.download_file(key = 'DATA/ETL/parameters_ETL.json')
with open('parameters_ETL.json', 'r') as fp:
    parameters = json.load(fp)
```

```python
table_filre_greffe = [{
    "database": "ets_inpi",
    "name": "ets_flux_filtre_enrichie_date_greffe",
    "output_id": "",
    "separator": ",",
    "s3URI": "s3://calfdata/INPI/TC_1/02_preparation_donnee/TEMP_ETS_FLUX_FILTRE",
    "schema": [
       {'Name': 'siren', 'Type': 'string', 'Comment': ''},
 {'Name': 'code_greffe', 'Type': 'string', 'Comment': ''},
 {'Name': 'nom_greffe', 'Type': 'string', 'Comment': ''},
 {'Name': 'numero_gestion', 'Type': 'string', 'Comment': ''},
 {'Name': 'id_etablissement', 'Type': 'string', 'Comment': ''},
 {'Name': 'date_greffe', 'Type': 'string', 'Comment': ''},
 {'Name': 'libelle_evt', 'Type': 'string', 'Comment': ''},
 {'Name': 'type', 'Type': 'string', 'Comment': ''},
 {'Name': 'siege_pm', 'Type': 'string', 'Comment': ''},
 {'Name': 'rcs_registre', 'Type': 'string', 'Comment': ''},
 {'Name': 'adresse_ligne1', 'Type': 'string', 'Comment': ''},
 {'Name': 'adresse_ligne2', 'Type': 'string', 'Comment': ''},
 {'Name': 'adresse_ligne3', 'Type': 'string', 'Comment': ''},
 {'Name': 'code_postal', 'Type': 'string', 'Comment': ''},
 {'Name': 'ville', 'Type': 'string', 'Comment': ''},
 {'Name': 'code_commune', 'Type': 'string', 'Comment': ''},
 {'Name': 'pays', 'Type': 'string', 'Comment': ''},
 {'Name': 'domiciliataire_nom', 'Type': 'string', 'Comment': ''},
 {'Name': 'domiciliataire_siren', 'Type': 'string', 'Comment': ''},
 {'Name': 'domiciliataire_greffe', 'Type': 'string', 'Comment': ''},
 {'Name': 'domiciliataire_complement', 'Type': 'string', 'Comment': ''},
 {'Name': 'siege_domicile_representant', 'Type': 'string', 'Comment': ''},
 {'Name': 'nom_commercial', 'Type': 'string', 'Comment': ''},
 {'Name': 'enseigne', 'Type': 'string', 'Comment': ''},
 {'Name': 'activite_ambulante', 'Type': 'string', 'Comment': ''},
 {'Name': 'activite_saisonniere', 'Type': 'string', 'Comment': ''},
 {'Name': 'activite_non_sedentaire', 'Type': 'string', 'Comment': ''},
 {'Name': 'date_debut_activite', 'Type': 'string', 'Comment': ''},
 {'Name': 'activite', 'Type': 'string', 'Comment': ''},
 {'Name': 'origine_fonds', 'Type': 'string', 'Comment': ''},
 {'Name': 'origine_fonds_info', 'Type': 'string', 'Comment': ''},
 {'Name': 'type_exploitation', 'Type': 'string', 'Comment': ''},
 {'Name': 'csv_source', 'Type': 'string', 'Comment': ''},
 {'Name': 'origin', 'Type': 'string', 'Comment': ''}
    ]
}
]
```

```python
#parameters['TABLES']['CREATION']['ALL_SCHEMA']
```

```python
to_remove = False
if to_remove:
    parameters['TABLES']['CREATION']['ALL_SCHEMA'].pop(-1)
```

```python
parameters['TABLES']['CREATION']['ALL_SCHEMA'].extend(table_filre_greffe)
```

```python
parameters['TABLES']['CREATION']['ALL_SCHEMA'][-1]
```

```python
json_filename ='parameters_ETL.json'
json_file = json.dumps(parameters)
f = open(json_filename,"w")
f.write(json_file)
f.close()
s3.upload_file(json_filename, 'DATA/ETL')
```

```python
for key, value in parameters["TABLES"]["CREATION"].items():
    if key == "ALL_SCHEMA":
        for table_info in value:
            if table_info['name'] in ['ets_flux_filtre_enrichie_date_greffe']:
                # CREATE QUERY

                ### Create top/bottom query
                table_top = parameters["TABLES"]["CREATION"]["template"]["top"].format(
                            table_info["database"], table_info["name"]
                        )
                table_bottom = parameters["TABLES"]["CREATION"]["template"][
                            "bottom_OpenCSVSerde"
                        ].format(table_info["separator"], table_info["s3URI"])

                ### Create middle
                table_middle = ""
                nb_var = len(table_info["schema"])
                for i, val in enumerate(table_info["schema"]):
                    if i == nb_var - 1:
                        table_middle += parameters["TABLES"]["CREATION"]["template"][
                                    "middle"
                                ].format(val['Name'], val['Type'], ")")
                    else:
                        table_middle += parameters["TABLES"]["CREATION"]["template"][
                                    "middle"
                                ].format(val['Name'], val['Type'], ",")

                query = table_top + table_middle + table_bottom
                ## DROP IF EXIST

                s3.run_query(
                                query="DROP TABLE {}".format(table_info["name"]),
                                database=db,
                                s3_output=s3_output
                        )

                ## RUN QUERY
                output = s3.run_query(
                            query=query,
                            database=table_info["database"],
                            s3_output=s3_output,
                            filename=None,  ## Add filename to print dataframe
                            destination_key=None,  ### Add destination key if need to copy output
                        )

                    ## SAVE QUERY ID
                table_info['output_id'] = output['QueryID']

                         ### UPDATE CATALOG
                #glue.update_schema_table(
                #            database=table_info["database"],
                #            table=table_info["name"],
                #            schema=table_info["schema"],
                #        )

                print(output)
```

## Step 3: Enrichissements des lignes d'un événement a l'autre et filtrage des événements partiels

Nous avons dès à présent 3 tables contenant l'ensemble des événements d'un établissement. La table initial, la table des flux filtrés et enrichis et la table des partiels. Il faut reconstituer la table finale en concatenant ses trois tables puis en enrichissant les lignes selon l'événement précédents et en indiquant les lignes a ignorer en cas de partiel. 



Tout d'abord, nous allons créer une table intermédiaire dans lequel la concaténation et la création du status 'IGNORE' va ête réalisé. 

```python
s3.download_file(key = 'DATA/ETL/parameters_ETL.json')
with open('parameters_ETL.json', 'r') as fp:
    parameters = json.load(fp)
```

```python
step_4 = {
   "STEPS_4":{
      "name":"ignore les status IGNORE",
      "execution":[
         {
            "database":"ets_inpi",
            "name":"ets_filtre_enrichie_historique_tmp",
            "output_id":"",
            "query":{
               "top":" WITH concat_ AS ( SELECT siren, code_greffe, nom_greffe, numero_gestion, id_etablissement, Coalesce( try(date_parse(date_greffe, '%Y-%m-%d')), try(date_parse(date_greffe, '%Y/%m/%d')), try(date_parse(date_greffe, '%d %M %Y')), try(date_parse(date_greffe, '%d/%m/%Y')), try(date_parse(date_greffe, '%d-%m-%Y')) ) as date_greffe, libelle_evt, type, siege_pm, rcs_registre, adresse_ligne1, adresse_ligne2, adresse_ligne3, code_postal, code_commune, pays, domiciliataire_nom, domiciliataire_siren, domiciliataire_greffe, domiciliataire_complement, siege_domicile_representant, enseigne, activite_ambulante, activite_saisonniere, activite_non_sedentaire, date_debut_activite, activite, origine_fonds, origine_fonds_info, ville, nom_commercial, type_exploitation, csv_source, 'FLUX' AS origin FROM ets_flux_filtre_enrichie_date_greffe ",
               "middle":" UNION ( SELECT siren, code_greffe, nom_greffe, numero_gestion, id_etablissement, Coalesce( try(date_parse(date_greffe, '%Y-%m-%d')), try(date_parse(date_greffe, '%Y/%m/%d')), try(date_parse(date_greffe, '%d %M %Y')), try(date_parse(date_greffe, '%d/%m/%Y')), try(date_parse(date_greffe, '%d-%m-%Y')) ) as date_greffe, libelle_evt, type, siege_pm, rcs_registre, adresse_ligne1, adresse_ligne2, adresse_ligne3, code_postal, code_commune, pays, domiciliataire_nom, domiciliataire_siren, domiciliataire_greffe, domiciliataire_complement, siege_domicile_representant, enseigne, activite_ambulante, activite_saisonniere, activite_non_sedentaire, date_debut_activite, activite, origine_fonds, origine_fonds_info, ville, nom_commercial, type_exploitation, csv_source, 'INITIAL' AS origin FROM ets_initial ) UNION ( SELECT date_.siren, date_.code_greffe, date_.nom_greffe, date_.numero_gestion, date_.id_etablissement, Coalesce( try(date_parse(date_greffe, '%Y-%m-%d')), try(date_parse(date_greffe, '%Y/%m/%d')), try(date_parse(date_greffe, '%d %M %Y')), try(date_parse(date_greffe, '%d/%m/%Y')), try(date_parse(date_greffe, '%d-%m-%Y')) ) as date_greffe, libelle_evt, type, siege_pm, rcs_registre, adresse_ligne1, adresse_ligne2, adresse_ligne3, code_postal, code_commune, pays, domiciliataire_nom, domiciliataire_siren, domiciliataire_greffe, domiciliataire_complement, siege_domicile_representant, enseigne, activite_ambulante, activite_saisonniere, activite_non_sedentaire, date_debut_activite, activite, origine_fonds, origine_fonds_info, ville, nom_commercial, type_exploitation, csv_source, 'PARTIEL' AS origin FROM ( SELECT siren, code_greffe, nom_greffe, numero_gestion, id_etablissement, date_greffe, type, libelle_evt, siege_pm, rcs_registre, adresse_ligne1, adresse_ligne2, adresse_ligne3, code_postal, code_commune, pays, domiciliataire_nom, domiciliataire_siren, domiciliataire_greffe, domiciliataire_complement, siege_domicile_representant, enseigne, activite_ambulante, activite_saisonniere, activite_non_sedentaire, date_debut_activite, activite, origine_fonds, origine_fonds_info, ville, nom_commercial, type_exploitation, csv_source, Coalesce( try( cast(file_timestamp as timestamp) ) ) as file_timestamp FROM ets_partiel ) as date_ INNER JOIN ( SELECT siren, code_greffe, nom_greffe, numero_gestion, id_etablissement, MAX( Coalesce( try( cast(file_timestamp as timestamp) ) ) ) as file_timestamp FROM ets_partiel GROUP BY siren, code_greffe, nom_greffe, numero_gestion, id_etablissement ) as max_ ON date_.siren = max_.siren AND date_.code_greffe = max_.code_greffe AND date_.nom_greffe = max_.nom_greffe AND date_.numero_gestion = max_.numero_gestion AND date_.id_etablissement = max_.id_etablissement AND date_.file_timestamp = max_.file_timestamp ) ORDER BY siren, code_greffe, nom_greffe, numero_gestion, id_etablissement, date_greffe ) ",
               "bottom":" SELECT concat_.siren, concat_.code_greffe, concat_.nom_greffe, concat_.numero_gestion, concat_.id_etablissement, libelle_evt, date_greffe, type, siege_pm, rcs_registre, adresse_ligne1, adresse_ligne2, adresse_ligne3, code_postal, code_commune, pays, domiciliataire_nom, domiciliataire_siren, domiciliataire_greffe, domiciliataire_complement, siege_domicile_representant, enseigne, activite_ambulante, activite_saisonniere, activite_non_sedentaire, date_debut_activite, activite, origine_fonds, origine_fonds_info, ville, nom_commercial, type_exploitation, csv_source, origin, CASE WHEN date_greffe <= date_greffe_max AND origin != 'PARTIEL' THEN 'IGNORE' ELSE NULL END as status FROM concat_ LEFT JOIN ( SELECT siren, code_greffe, nom_greffe, numero_gestion, id_etablissement, date_greffe as date_greffe_max FROM concat_ WHERE origin = 'PARTIEL' ) as partiel ON concat_.siren = partiel.siren AND concat_.code_greffe = partiel.code_greffe AND concat_.nom_greffe = partiel.nom_greffe AND concat_.numero_gestion = partiel.numero_gestion AND concat_.id_etablissement = partiel.id_etablissement"
            }
         }
      ],
       "schema":[
               {
                  "Name":"",
                  "Type":"",
                  "Comment":""
               }
            ]
   }
}
```

```python
to_remove = False
if to_remove:
    parameters['TABLES']['PREPARATION']['ALL_SCHEMA'].pop(-1)
```

```python
parameters['TABLES']['PREPARATION']['ALL_SCHEMA'].append(step_4)
```

```python
parameters['TABLES']['PREPARATION']['ALL_SCHEMA'][-1]
```

```python
json_filename ='parameters_ETL.json'
json_file = json.dumps(parameters)
f = open(json_filename,"w")
f.write(json_file)
f.close()
s3.upload_file(json_filename, 'DATA/ETL')
```

```python
for key, value in parameters["TABLES"]["PREPARATION"].items():
    if key == "ALL_SCHEMA":
        ### LOOP STEPS
        for i, steps in enumerate(value):
            step_name = "STEPS_{}".format(i)
            if step_name in ['STEPS_4']:

                ### LOOP EXECUTION WITHIN STEP
                for j, step_n in enumerate(steps[step_name]["execution"]):
                    ### DROP IF EXIST
                    s3.run_query(
                        query="DROP TABLE {}.{}".format(step_n["database"], step_n["name"]),
                        database=db,
                        s3_output=s3_output,
                    )

                    ### CREATE TOP
                    table_top = parameters["TABLES"]["PREPARATION"]["template"][
                    "top"
                ].format(step_n["database"], step_n["name"],)

                    ### CREATE TOP
                    ### COMPILE QUERY
                    query = (
                        table_top
                            + step_n["query"]["top"]
                            + step_n["query"]["middle"]
                            + step_n["query"]["bottom"]
                        )
                    output = s3.run_query(
                            query=query,
                            database=db,
                            s3_output=s3_output,
                            filename=None,  ## Add filename to print dataframe
                            destination_key=None,  ### Add destination key if need to copy output
                        )

                        ## SAVE QUERY ID
                    step_n["output_id"] = output["QueryID"]

                        ### UPDATE CATALOG
                        #glue.update_schema_table(
                        #    database=step_n["database"],
                        #    table=step_n["name"],
                        #    schema=steps[step_name]["schema"],
                        #)
                    print(output)
                        #parameters['steps']['step_2']['output_id'].append(output['QueryID'])
                        #source_key = '{}/{}.csv'.format(s3_output, output['QueryID'])
                        #destination_key = '{0}/{1}.csv'.format(INPI/TC_1/02_preparation_donnee/TEMP_ETS_FLUX_FILTRE,greffe)
                        #s3.move_object_s3(source_key, destination_key, remove = True)
```

La seconde partie de l'étape va procéder a l'enrichissement des valeurs sur les flux à partir du moment ou la ligne n'est pas à ignore

```python
s3.download_file(key = 'DATA/ETL/parameters_ETL.json')
with open('parameters_ETL.json', 'r') as fp:
    parameters = json.load(fp)
```

```python
step_5 = {
   "STEPS_5":{
      "name":"Enrichissement des flux sur variable n et n-1",
      "execution":[
         {
            "database":"ets_inpi",
            "name":"ets_filtre_enrichie_historique",
            "output_id":"",
            "query":{
               "top":"SELECT siren, code_greffe, nom_greffe, numero_gestion, id_etablissement, origin, status, date_greffe, libelle_evt,",
               "middle":""" CASE WHEN origin = 'FLUX' AND status != 'IGNORE' AND "type" = '' THEN LAG ("type", 1) OVER ( PARTITION BY siren,"code_greffe", numero_gestion, id_etablissement ORDER BY siren,'code_greffe', numero_gestion, id_etablissement,date_greffe ) ELSE "type" END AS "type" , CASE WHEN origin = 'FLUX' AND status != 'IGNORE' AND "siege_pm" = '' THEN LAG ("siege_pm", 1) OVER ( PARTITION BY siren,"code_greffe", numero_gestion, id_etablissement ORDER BY siren,'code_greffe', numero_gestion, id_etablissement,date_greffe ) ELSE "siege_pm" END AS "siege_pm" , CASE WHEN origin = 'FLUX' AND status != 'IGNORE' AND "rcs_registre" = '' THEN LAG ("rcs_registre", 1) OVER ( PARTITION BY siren,"code_greffe", numero_gestion, id_etablissement ORDER BY siren,'code_greffe', numero_gestion, id_etablissement,date_greffe ) ELSE "rcs_registre" END AS "rcs_registre" , CASE WHEN origin = 'FLUX' AND status != 'IGNORE' AND "adresse_ligne1" = '' THEN LAG ("adresse_ligne1", 1) OVER ( PARTITION BY siren,"code_greffe", numero_gestion, id_etablissement ORDER BY siren,'code_greffe', numero_gestion, id_etablissement,date_greffe ) ELSE "adresse_ligne1" END AS "adresse_ligne1" , CASE WHEN origin = 'FLUX' AND status != 'IGNORE' AND "adresse_ligne2" = '' THEN LAG ("adresse_ligne2", 1) OVER ( PARTITION BY siren,"code_greffe", numero_gestion, id_etablissement ORDER BY siren,'code_greffe', numero_gestion, id_etablissement,date_greffe ) ELSE "adresse_ligne2" END AS "adresse_ligne2" , CASE WHEN origin = 'FLUX' AND status != 'IGNORE' AND "adresse_ligne3" = '' THEN LAG ("adresse_ligne3", 1) OVER ( PARTITION BY siren,"code_greffe", numero_gestion, id_etablissement ORDER BY siren,'code_greffe', numero_gestion, id_etablissement,date_greffe ) ELSE "adresse_ligne3" END AS "adresse_ligne3" , CASE WHEN origin = 'FLUX' AND status != 'IGNORE' AND "code_postal" = '' THEN LAG ("code_postal", 1) OVER ( PARTITION BY siren,"code_greffe", numero_gestion, id_etablissement ORDER BY siren,'code_greffe', numero_gestion, id_etablissement,date_greffe ) ELSE "code_postal" END AS "code_postal" , CASE WHEN origin = 'FLUX' AND status != 'IGNORE' AND "ville" = '' THEN LAG ("ville", 1) OVER ( PARTITION BY siren,"code_greffe", numero_gestion, id_etablissement ORDER BY siren,'code_greffe', numero_gestion, id_etablissement,date_greffe ) ELSE "ville" END AS "ville" , CASE WHEN origin = 'FLUX' AND status != 'IGNORE' AND "code_commune" = '' THEN LAG ("code_commune", 1) OVER ( PARTITION BY siren,"code_greffe", numero_gestion, id_etablissement ORDER BY siren,'code_greffe', numero_gestion, id_etablissement,date_greffe ) ELSE "code_commune" END AS "code_commune" , CASE WHEN origin = 'FLUX' AND status != 'IGNORE' AND "pays" = '' THEN LAG ("pays", 1) OVER ( PARTITION BY siren,"code_greffe", numero_gestion, id_etablissement ORDER BY siren,'code_greffe', numero_gestion, id_etablissement,date_greffe ) ELSE "pays" END AS "pays" , CASE WHEN origin = 'FLUX' AND status != 'IGNORE' AND "domiciliataire_nom" = '' THEN LAG ("domiciliataire_nom", 1) OVER ( PARTITION BY siren,"code_greffe", numero_gestion, id_etablissement ORDER BY siren,'code_greffe', numero_gestion, id_etablissement,date_greffe ) ELSE "domiciliataire_nom" END AS "domiciliataire_nom" , CASE WHEN origin = 'FLUX' AND status != 'IGNORE' AND "domiciliataire_siren" = '' THEN LAG ("domiciliataire_siren", 1) OVER ( PARTITION BY siren,"code_greffe", numero_gestion, id_etablissement ORDER BY siren,'code_greffe', numero_gestion, id_etablissement,date_greffe ) ELSE "domiciliataire_siren" END AS "domiciliataire_siren" , CASE WHEN origin = 'FLUX' AND status != 'IGNORE' AND "domiciliataire_greffe" = '' THEN LAG ("domiciliataire_greffe", 1) OVER ( PARTITION BY siren,"code_greffe", numero_gestion, id_etablissement ORDER BY siren,'code_greffe', numero_gestion, id_etablissement,date_greffe ) ELSE "domiciliataire_greffe" END AS "domiciliataire_greffe" , CASE WHEN origin = 'FLUX' AND status != 'IGNORE' AND "domiciliataire_complement" = '' THEN LAG ("domiciliataire_complement", 1) OVER ( PARTITION BY siren,"code_greffe", numero_gestion, id_etablissement ORDER BY siren,'code_greffe', numero_gestion, id_etablissement,date_greffe ) ELSE "domiciliataire_complement" END AS "domiciliataire_complement" , CASE WHEN origin = 'FLUX' AND status != 'IGNORE' AND "siege_domicile_representant" = '' THEN LAG ("siege_domicile_representant", 1) OVER ( PARTITION BY siren,"code_greffe", numero_gestion, id_etablissement ORDER BY siren,'code_greffe', numero_gestion, id_etablissement,date_greffe ) ELSE "siege_domicile_representant" END AS "siege_domicile_representant" , CASE WHEN origin = 'FLUX' AND status != 'IGNORE' AND "nom_commercial" = '' THEN LAG ("nom_commercial", 1) OVER ( PARTITION BY siren,"code_greffe", numero_gestion, id_etablissement ORDER BY siren,'code_greffe', numero_gestion, id_etablissement,date_greffe ) ELSE "nom_commercial" END AS "nom_commercial" , CASE WHEN origin = 'FLUX' AND status != 'IGNORE' AND "enseigne" = '' THEN LAG ("enseigne", 1) OVER ( PARTITION BY siren,"code_greffe", numero_gestion, id_etablissement ORDER BY siren,'code_greffe', numero_gestion, id_etablissement,date_greffe ) ELSE "enseigne" END AS "enseigne" , CASE WHEN origin = 'FLUX' AND status != 'IGNORE' AND "activite_ambulante" = '' THEN LAG ("activite_ambulante", 1) OVER ( PARTITION BY siren,"code_greffe", numero_gestion, id_etablissement ORDER BY siren,'code_greffe', numero_gestion, id_etablissement,date_greffe ) ELSE "activite_ambulante" END AS "activite_ambulante" , CASE WHEN origin = 'FLUX' AND status != 'IGNORE' AND "activite_saisonniere" = '' THEN LAG ("activite_saisonniere", 1) OVER ( PARTITION BY siren,"code_greffe", numero_gestion, id_etablissement ORDER BY siren,'code_greffe', numero_gestion, id_etablissement,date_greffe ) ELSE "activite_saisonniere" END AS "activite_saisonniere" , CASE WHEN origin = 'FLUX' AND status != 'IGNORE' AND "activite_non_sedentaire" = '' THEN LAG ("activite_non_sedentaire", 1) OVER ( PARTITION BY siren,"code_greffe", numero_gestion, id_etablissement ORDER BY siren,'code_greffe', numero_gestion, id_etablissement,date_greffe ) ELSE "activite_non_sedentaire" END AS "activite_non_sedentaire" , CASE WHEN origin = 'FLUX' AND status != 'IGNORE' AND "date_debut_activite" = '' THEN LAG ("date_debut_activite", 1) OVER ( PARTITION BY siren,"code_greffe", numero_gestion, id_etablissement ORDER BY siren,'code_greffe', numero_gestion, id_etablissement,date_greffe ) ELSE "date_debut_activite" END AS "date_debut_activite" , CASE WHEN origin = 'FLUX' AND status != 'IGNORE' AND "activite" = '' THEN LAG ("activite", 1) OVER ( PARTITION BY siren,"code_greffe", numero_gestion, id_etablissement ORDER BY siren,'code_greffe', numero_gestion, id_etablissement,date_greffe ) ELSE "activite" END AS "activite" , CASE WHEN origin = 'FLUX' AND status != 'IGNORE' AND "origine_fonds" = '' THEN LAG ("origine_fonds", 1) OVER ( PARTITION BY siren,"code_greffe", numero_gestion, id_etablissement ORDER BY siren,'code_greffe', numero_gestion, id_etablissement,date_greffe ) ELSE "origine_fonds" END AS "origine_fonds" , CASE WHEN origin = 'FLUX' AND status != 'IGNORE' AND "origine_fonds_info" = '' THEN LAG ("origine_fonds_info", 1) OVER ( PARTITION BY siren,"code_greffe", numero_gestion, id_etablissement ORDER BY siren,'code_greffe', numero_gestion, id_etablissement,date_greffe ) ELSE "origine_fonds_info" END AS "origine_fonds_info" , CASE WHEN origin = 'FLUX' AND status != 'IGNORE' AND "type_exploitation" = '' THEN LAG ("type_exploitation", 1) OVER ( PARTITION BY siren,"code_greffe", numero_gestion, id_etablissement ORDER BY siren,'code_greffe', numero_gestion, id_etablissement,date_greffe ) ELSE "type_exploitation" END AS "type_exploitation" , CASE WHEN origin = 'FLUX' AND status != 'IGNORE' AND "csv_source" = '' THEN LAG ("csv_source", 1) OVER ( PARTITION BY siren,"code_greffe", numero_gestion, id_etablissement ORDER BY siren,'code_greffe', numero_gestion, id_etablissement,date_greffe ) ELSE "csv_source" END AS "csv_source" """,
               "bottom":" FROM ets_filtre_enrichie_historique_tmp ORDER BY siren,code_greffe, numero_gestion, id_etablissement,date_greffe "
            }
         }
      ],
       "schema":[
               {
                  "Name":"",
                  "Type":"",
                  "Comment":""
               }
            ]
   }
}
```

```python
to_remove = False
if to_remove:
    parameters['TABLES']['PREPARATION']['ALL_SCHEMA'].pop(-1)
```

```python
parameters['TABLES']['PREPARATION']['ALL_SCHEMA'].append(step_5)
```

```python
parameters['TABLES']['PREPARATION']['ALL_SCHEMA'][-1]
```

```python
json_filename ='parameters_ETL.json'
json_file = json.dumps(parameters)
f = open(json_filename,"w")
f.write(json_file)
f.close()
s3.upload_file(json_filename, 'DATA/ETL')
```

```python
for key, value in parameters["TABLES"]["PREPARATION"].items():
    if key == "ALL_SCHEMA":
        ### LOOP STEPS
        for i, steps in enumerate(value):
            step_name = "STEPS_{}".format(i)
            if step_name in ['STEPS_5']:

                ### LOOP EXECUTION WITHIN STEP
                for j, step_n in enumerate(steps[step_name]["execution"]):
                    ### DROP IF EXIST
                    s3.run_query(
                        query="DROP TABLE {}.{}".format(step_n["database"], step_n["name"]),
                        database=db,
                        s3_output=s3_output,
                    )

                    ### CREATE TOP
                    table_top = parameters["TABLES"]["PREPARATION"]["template"][
                    "top"
                ].format(step_n["database"], step_n["name"],)

                    ### CREATE TOP
                    ### COMPILE QUERY
                    query = (
                        table_top
                            + step_n["query"]["top"]
                            + step_n["query"]["middle"]
                            + step_n["query"]["bottom"]
                        )
                    output = s3.run_query(
                            query=query,
                            database=db,
                            s3_output=s3_output,
                            filename=None,  ## Add filename to print dataframe
                            destination_key=None,  ### Add destination key if need to copy output
                        )

                        ## SAVE QUERY ID
                    step_n["output_id"] = output["QueryID"]

                        ### UPDATE CATALOG
                        #glue.update_schema_table(
                        #    database=step_n["database"],
                        #    table=step_n["name"],
                        #    schema=steps[step_name]["schema"],
                        #)
                    print(output)
```

Finalisation ETL avec les ID des queries executées.

```python
json_filename ='parameters_ETL.json'
json_file = json.dumps(parameters)
f = open(json_filename,"w")
f.write(json_file)
f.close()
s3.upload_file(json_filename, 'DATA/ETL')
```

# Analytics

The cells below execute the job in the key `ANALYSIS`. You need to change the `primary_key` and `secondary_key` 


## Count missing values

```python
from datetime import date
today = date.today().strftime('%Y%M%d')
today
```

Il y un bug avec boto3 et Glue. Il faut donc récupérer maunellement le schema de donnée .. Nous n'avons pas pu modifier le catalogue de données via le fichier JSON.

```python
schema = {
	"StorageDescriptor": {
		"Columns": 
			 [
				{
					"Name": "siren",
					"Type": "string",
					"comment": ""
				},
				{
					"Name": "code_greffe",
					"Type": "string",
					"comment": ""
				},
				{
					"Name": "nom_greffe",
					"Type": "string",
					"comment": ""
				},
				{
					"Name": "numero_gestion",
					"Type": "string",
					"comment": ""
				},
				{
					"Name": "id_etablissement",
					"Type": "string",
					"comment": ""
				},
				{
					"Name": "origin",
					"Type": "varchar(7)",
					"comment": ""
				},
				{
					"Name": "status",
					"Type": "varchar(6)",
					"comment": ""
				},
				{
					"Name": "date_greffe",
					"Type": "timestamp",
					"comment": ""
				},
				{
					"Name": "libelle_evt",
					"Type": "string",
					"comment": ""
				},
				{
					"Name": "type",
					"Type": "string",
					"comment": ""
				},
				{
					"Name": "siege_pm",
					"Type": "string",
					"comment": ""
				},
				{
					"Name": "rcs_registre",
					"Type": "string",
					"comment": ""
				},
				{
					"Name": "adresse_ligne1",
					"Type": "string",
					"comment": ""
				},
				{
					"Name": "adresse_ligne2",
					"Type": "string",
					"comment": ""
				},
				{
					"Name": "adresse_ligne3",
					"Type": "string",
					"comment": ""
				},
				{
					"Name": "code_postal",
					"Type": "string",
					"comment": ""
				},
				{
					"Name": "ville",
					"Type": "string",
					"comment": ""
				},
				{
					"Name": "code_commune",
					"Type": "string",
					"comment": ""
				},
				{
					"Name": "pays",
					"Type": "string",
					"comment": ""
				},
				{
					"Name": "domiciliataire_nom",
					"Type": "string",
					"comment": ""
				},
				{
					"Name": "domiciliataire_siren",
					"Type": "string",
					"comment": ""
				},
				{
					"Name": "domiciliataire_greffe",
					"Type": "string",
					"comment": ""
				},
				{
					"Name": "domiciliataire_complement",
					"Type": "string",
					"comment": ""
				},
				{
					"Name": "siege_domicile_representant",
					"Type": "string",
					"comment": ""
				},
				{
					"Name": "nom_commercial",
					"Type": "string",
					"comment": ""
				},
				{
					"Name": "enseigne",
					"Type": "string",
					"comment": ""
				},
				{
					"Name": "activite_ambulante",
					"Type": "string",
					"comment": ""
				},
				{
					"Name": "activite_saisonniere",
					"Type": "string",
					"comment": ""
				},
				{
					"Name": "activite_non_sedentaire",
					"Type": "string",
					"comment": ""
				},
				{
					"Name": "date_debut_activite",
					"Type": "string",
					"comment": ""
				},
				{
					"Name": "activite",
					"Type": "string",
					"comment": ""
				},
				{
					"Name": "origine_fonds",
					"Type": "string",
					"comment": ""
				},
				{
					"Name": "origine_fonds_info",
					"Type": "string",
					"comment": ""
				},
				{
					"Name": "type_exploitation",
					"Type": "string",
					"comment": ""
				},
				{
					"Name": "csv_source",
					"Type": "string",
					"comment": ""
				}
			],
		"inputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
		"outputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
		"compressed": "false",
		"numBuckets": "0",
		"SerDeInfo": {
			"name": "ets_filtre_enrichie_historique",
			"serializationLib": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
			"parameters": {}
		},
		"bucketCols": [],
		"sortCols": [],
		"parameters": {},
		"SkewedInfo": {},
		"storedAsSubDirectories": "false"
	},
	"parameters": {
		"EXTERNAL": "TRUE",
		"has_encrypted_data": "false"
	}
}
```

```python
table_top = parameters["ANALYSIS"]["COUNT_MISSING"]["top"]
table_middle = ""
table_bottom = parameters["ANALYSIS"]["COUNT_MISSING"]["bottom"].format(
    step_n["database"], step_n["name"]
)

for key, value in enumerate(schema["StorageDescriptor"]["Columns"]):
    if key == len(schema["StorageDescriptor"]["Columns"]) - 1:

        table_middle += "{} ".format(
            parameters["ANALYSIS"]["COUNT_MISSING"]["middle"].format(value["Name"])
        )
    else:
        table_middle += "{} ,".format(
            parameters["ANALYSIS"]["COUNT_MISSING"]["middle"].format(value["Name"])
        )
query = table_top + table_middle + table_bottom
output = s3.run_query(
    query=query,
    database=db,
    s3_output=s3_output,
    filename="count_missing",  ## Add filename to print dataframe
    destination_key=None,  ### Add destination key if need to copy output
)
display(
    output.T.rename(columns={0: "total_missing"})
    .assign(total_missing_pct=lambda x: x["total_missing"] / x.iloc[0, 0])
    .sort_values(by=["total_missing"], ascending=False)
    .style.format("{0:,.2%}", subset=["total_missing_pct"])
    .bar(subset="total_missing_pct", color=["#d65f5f"])
)
```

# Brief description table

In this part, we provide a brief summary statistic from the lattest jobs. For the continuous analysis with a primary/secondary key, please add the relevant variables you want to know the count and distribution


## Categorical Description

During the categorical analysis, we wil count the number of observations for a given group and for a pair.

### Count obs by group

- Index: primary group
- nb_obs: Number of observations per primary group value
- percentage: Percentage of observation per primary group value over the total number of observations

Returns the top 10 only

```python
for field in schema["StorageDescriptor"]["Columns"]:
    if field["Type"] in ["string", "object","varchar(6)","varchar(7)" "varchar(12)"]:

        print("Nb of obs for {}".format(field["Name"]))

        query = parameters["ANALYSIS"]["CATEGORICAL"]["PAIR"].format(
            step_n["database"], step_n["name"], field["Name"]
        )
        output = s3.run_query(
            query=query,
            database=db,
            s3_output=s3_output,
            filename="count_categorical_{}".format(
                field["Name"]
            ),  ## Add filename to print dataframe
            destination_key=None,  ### Add destination key if need to copy output
        )

        ### Print top 10

        display(
            (
                output.set_index([field["Name"]])
                .assign(percentage=lambda x: x["nb_obs"] / x["nb_obs"].sum())
                .sort_values("percentage", ascending=False)
                .head(10)
                .style.format("{0:.2%}", subset=["percentage"])
                .bar(subset=["percentage"], color="#d65f5f")
            )
        )
```

### Count obs by two pair

You need to pass the primary group in the cell below

- Index: primary group
- Columns: Secondary key -> All the categorical variables in the dataset
- nb_obs: Number of observations per primary group value
- Total: Total number of observations per primary group value (sum by row)
- percentage: Percentage of observations per primary group value over the total number of observations per primary group value (sum by row)

Returns the top 10 only

```python
primary_key = "libelle_evt"
```

```python
for field in schema["StorageDescriptor"]["Columns"]:
    if field["Type"] in ["string", "object", "varchar(6)","varchar(7)" "varchar(12)"]:
        if field["Name"] != primary_key:
            print(
                "Nb of obs for the primary group {} and {}".format(
                    primary_key, field["Name"]
                )
            )
            query = parameters["ANALYSIS"]["CATEGORICAL"]["MULTI_PAIR"].format(
                step_n["database"], step_n["name"], primary_key, field["Name"]
            )

            output = s3.run_query(
                query=query,
                database=db,
                s3_output=s3_output,
                filename="count_categorical_{}_{}".format(
                    primary_key, field["Name"]
                ),  # Add filename to print dataframe
                destination_key=None,  # Add destination key if need to copy output
            )

            display(
                (
                    pd.concat(
                        [
                            (
                                output.loc[
                                    lambda x: x[field["Name"]].isin(
                                        (
                                            output.assign(
                                                total_secondary=lambda x: x["nb_obs"]
                                                .groupby([x[field["Name"]]])
                                                .transform("sum")
                                            )
                                            .drop_duplicates(
                                                subset="total_secondary", keep="last"
                                            )
                                            .sort_values(
                                                by=["total_secondary"], ascending=False
                                            )
                                            .iloc[:10, 1]
                                            .to_list()
                                        )
                                    )
                                ]
                                .set_index([primary_key, field["Name"]])
                                .unstack([0])
                                .fillna(0)
                                .assign(total=lambda x: x.sum(axis=1))
                                .sort_values(by=["total"])
                            ),
                            (
                                output.loc[
                                    lambda x: x[field["Name"]].isin(
                                        (
                                            output.assign(
                                                total_secondary=lambda x: x["nb_obs"]
                                                .groupby([x[field["Name"]]])
                                                .transform("sum")
                                            )
                                            .drop_duplicates(
                                                subset="total_secondary", keep="last"
                                            )
                                            .sort_values(
                                                by=["total_secondary"], ascending=False
                                            )
                                            .iloc[:10, 1]
                                            .to_list()
                                        )
                                    )
                                ]
                                .rename(columns={"nb_obs": "percentage"})
                                .set_index([primary_key, field["Name"]])
                                .unstack([0])
                                .fillna(0)
                                .apply(lambda x: x / x.sum(), axis=1)
                            ),
                        ],
                        axis=1,
                    )
                    .fillna(0)
                    # .sort_index(axis=1, level=1)
                    .style.format("{0:,.2f}", subset=["nb_obs", "total"])
                    .bar(subset=["total"], color="#d65f5f")
                    .format("{0:,.2%}", subset=("percentage"))
                    .background_gradient(
                        cmap=sns.light_palette("green", as_cmap=True), subset=("nb_obs")
                    )
                )
            )
```

# Generation report

```python
import os, time, shutil, urllib, ipykernel, json
from pathlib import Path
from notebook import notebookapp
```

```python
def create_report(extension = "html", keep_code = False):
    """
    Create a report from the current notebook and save it in the 
    Report folder (Parent-> child directory)
    
    1. Exctract the current notbook name
    2. Convert the Notebook 
    3. Move the newly created report
    
    Args:
    extension: string. Can be "html", "pdf", "md"
    
    
    """
    
    ### Get notebook name
    connection_file = os.path.basename(ipykernel.get_connection_file())
    kernel_id = connection_file.split('-', 1)[0].split('.')[0]

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
    #parent_path = str(Path(path).parent)
    
    ### Path report
    #path_report = "{}/Reports".format(parent_path)
    #path_report = "{}/Reports".format(path)
    
    ### Path destination
    name_no_extension = notebookname.split(sep, 1)[0]
    source_to_move = name_no_extension +'.{}'.format(extension)
    dest = os.path.join(path,'Reports', source_to_move)
    
    ### Generate notebook
    if keep_code:
        os.system('jupyter nbconvert --to {} {}'.format(
    extension,notebookname))
    else:
        os.system('jupyter nbconvert --no-input --to {} {}'.format(
    extension,notebookname))
    
    ### Move notebook to report folder
    #time.sleep(5)
    shutil.move(source_to_move, dest)
    print("Report Available at this adress:\n {}".format(dest))
```

```python
create_report(extension = "html", keep_code = True)
```
