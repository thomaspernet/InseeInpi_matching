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
from awsPy.aws_athena import service_athena
from awsPy.aws_s3 import service_s3
from pathlib import Path
import pandas as pd
import numpy as np
import seaborn as sns
import os, shutil

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
