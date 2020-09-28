---
jupyter:
  jupytext:
    formats: ipynb,md
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.2'
      jupytext_version: 1.4.0
  kernel_info:
    name: python3
  kernelspec:
    display_name: Python 3
    language: python
    name: python3
---

<!-- #region id="mKJUtoJnDIUZ" -->
# Création table INSEE transformée contenant les nouvelles variables permettant la siretisation

# Objective(s)

*  Creation de la table INSEE avec les data de juillet
* Création des variables pour faire les tests de siretisation
* Please, update the Source URL by clicking on the button after the information have been pasted
  * US 03 Creation Variables data INPI et INSEE Modify rows
  * Delete tables and Github related to the US: Delete rows

# Metadata

* Epic: Epic 6
* US: US 4
* Date Begin: 9/28/2020
* Duration Task: 1
* Description: Creation des variables qui vont servir a réaliser les tests pour la siretisation
* Step type: Transform table
* Status: Active
  * Change Status task: Active
  * Update table: Modify rows
* Source URL: US 03 Creation Variables data INPI et INSEE
* Task type: Jupyter Notebook
* Users: Thomas Pernet
* Watchers: Thomas Pernet
* User Account: https://937882855452.signin.aws.amazon.com/console
* Estimated Log points: 10
* Task tag: #athena,#lookup-table,#sql,#data-preparation,#insee
* Toggl Tag: #documentation

# Input Cloud Storage [AWS/GCP]

## Table/file

* Origin: 
* Athena
* Name: 
* ets_insee_raw_juillet
* Github: 
  * 

# Destination Output/Delivery

## Table/file

* Origin: 
* Athena
* Name:
* ets_insee_transformed
* GitHub:

<!-- #endregion -->
```python inputHidden=false jupyter={"outputs_hidden": false} outputHidden=false id="h4Umnv0nDIUa"
from awsPy.aws_authorization import aws_connector
from awsPy.aws_s3 import service_s3
from awsPy.aws_glue import service_glue
from pathlib import Path
import pandas as pd
import numpy as np
import seaborn as sns
import os, shutil, json

path = os.getcwd()
parent_path = str(Path(path).parent)
path_cred = r"{}/credential_AWS.json".format(parent_path)
con = aws_connector.aws_instantiate(credential = path_cred,
                                       region = 'eu-west-3')

region = 'eu-west-3'
bucket = 'calfdata'

```

```python inputHidden=false jupyter={"outputs_hidden": false} outputHidden=false id="tTZePIXWDIUf"
con = aws_connector.aws_instantiate(credential = path_cred,
                                       region = region)
client= con.client_boto()
s3 = service_s3.connect_S3(client = client,
                      bucket = bucket, verbose = True) 
glue = service_glue.connect_glue(client = client) 
```

```python id="er-S8VWyDIUi"
pandas_setting = True
if pandas_setting:
    cm = sns.light_palette("green", as_cmap=True)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', None)
```

<!-- #region id="bea5ESvKDIUm" -->
# Etape création table tansformée INSEE

La préparation de la table transformée de l'INSEE se fait en deux étapes. La première étape consiste bien sur à intégrer dans la base de donnée, la table brute de l'INSEE. Nous utiliserons la table datant de juillet 2020, pour correspondre avec celle de l'équipe Datum.

Dans un second temps, nous allons 6 variables, qui sont résumés dans le tableau ci dessous

| Tables | Variables                          | Commentaire                                                                                                                                                                                                        | Bullet_inputs                                                                                                                 | Bullet_point_regex                                     | Inputs                                                                                                                        | US_md                                                          | query_md_gitlab                                                                                                                                                                                                                                                              | Pattern_regex                                          |
|--------|------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------|
| INSEE  | voie_clean                         | Extraction du type de voie contenu dans l’adresse. Variable type voie nom complet. Exemple, l'INSEE indique CH, pour chemin, il faut donc indiquer CHEMIN. Besoin table externe (type_voie) pour créer la variable |                                                                                                                               |                                                        |                                                                                                                               | [2953](https://tree.taiga.io/project/olivierlubet-air/us/2953) | [etape-1-pr%C3%A9paration-voie_clean](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/04_ETS_add_variables_insee.md#etape-1-pr%C3%A9paration-voie_clean)                     |                                                        |
| INSEE  | indiceRepetitionEtablissement_full | Récupération du nom complet des indices de répétion; par exemple B devient BIS, T devient TER                                                                                                                      | indiceRepetitionEtablissement                                                                                                 | Regles_speciales                                       | indiceRepetitionEtablissement                                                                                                 | [2953](https://tree.taiga.io/project/olivierlubet-air/us/2953) | []()                                                                                                                                                                                                                                                                         | Regles_speciales                                       |
| INSEE  | adresse_reconstituee_insee         | Concatenation des champs de l'adresse et suppression des espace                                                                                                                                                    | numeroVoieEtablissement indiceRepetitionEtablissement_full voie_clean libelleVoieEtablissement complementAdresseEtablissement | debut/fin espace espace Upper                          | numeroVoieEtablissement,indiceRepetitionEtablissement_full,voie_clean,libelleVoieEtablissement,complementAdresseEtablissement | [2954](https://tree.taiga.io/project/olivierlubet-air/us/2954) | [etape-2-preparation-adress_reconstituee_insee](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/04_ETS_add_variables_insee.md#etape-2-preparation-adress_reconstituee_insee) | debut/fin espace,espace,Upper                          |
| INSEE  | adresse_distance_insee             | Concatenation des champs de l'adresse, suppression des espaces et des articles. Utilisé pour calculer le score permettant de distinguer la similarité/dissimilarité entre deux adresses (INPI vs INSEE)            | numeroVoieEtablissement indiceRepetitionEtablissement_full voie_clean libelleVoieEtablissement complementAdresseEtablissement | article digit debut/fin espace espace Upper            | numeroVoieEtablissement,indiceRepetitionEtablissement_full,voie_clean,libelleVoieEtablissement,complementAdresseEtablissement | [3004](https://tree.taiga.io/project/olivierlubet-air/us/3004) | [etape-3-adresse_distance_insee](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/04_ETS_add_variables_insee.md#etape-3-adresse_distance_insee)                               | article,digit,debut/fin espace,espace,Upper            |
| INSEE  | list_numero_voie_matching_insee    | Liste contenant tous les numéros de l'adresse dans l'INSEE                                                                                                                                                         | numeroVoieEtablissement indiceRepetitionEtablissement_full voie_clean libelleVoieEtablissement complementAdresseEtablissement | article digit debut/fin espace                         | numeroVoieEtablissement,indiceRepetitionEtablissement_full,voie_clean,libelleVoieEtablissement,complementAdresseEtablissement | [3004](https://tree.taiga.io/project/olivierlubet-air/us/3004) | [etape-4-creation-liste-num%C3%A9ro-de-voie](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/04_ETS_add_variables_insee.md#etape-4-creation-liste-num%C3%A9ro-de-voie)       | article,digit,debut/fin espace                         |
| INSEE  | ville_matching                     | Nettoyage regex de la ville et suppression des espaces. La même logique de nettoyage est appliquée coté INPI                                                                                                       | libelleCommuneEtablissement                                                                                                   | article digit debut/fin espace espace Regles_speciales | libelleCommuneEtablissement                                                                                                   | [2954](https://tree.taiga.io/project/olivierlubet-air/us/2954) | [etape-2-cr%C3%A9ation-ville_matching](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/04_ETS_add_variables_insee.md#etape-2-cr%C3%A9ation-ville_matching)                   | article,digit,debut/fin espace,espace,Regles_speciales |
| INSEE  | count_initial_insee                | Compte du nombre de siret (établissement) par siren (entreprise)                                                                                                                                                   | siren                                                                                                                         |                                                        | siren                                                                                                                         | [2955](https://tree.taiga.io/project/olivierlubet-air/us/2955) | [etape-5-count_initial_insee](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/04_ETS_add_variables_insee.md#etape-5-count_initial_insee)                                     |                                                        |
    
## Prepare `TABLE.CREATION` parameters
    
Le fichier config JSON contient déjà les étapes de préparation de l'INPI. Nous allons continuer d'ajouter les queries a éxécuter dans le JSON afin d'avoir un processus complet contenu dans un seul est même fichier. 
<!-- #endregion -->

```python id="U-PscJ-YDIUm"
### If chinese characters, set  ensure_ascii=False
s3.download_file(key = 'DATA/ETL/parameters_ETL.json')
with open('parameters_ETL.json', 'r') as fp:
    parameters = json.load(fp)
```

```python id="S482exQ8DIUp"
parameters['TABLES']['CREATION']['ALL_SCHEMA'][-1]
```

<!-- #region id="oxyFXf8iDIUs" -->
## 2. Prepare `TABLES.CREATION`

This part usually starts with raw/transformed data in S3. The typical architecture in the S3 is:

- `DATA/RAW_DATA` or `DATA/UNZIP_DATA_APPEND_ALL` or `DATA/TRANSFORMED`. One of our rule is, if the user needs to create a table from a CSV/JSON (raw or transformed), then the query should be written in the key `TABLES.CREATION` and the notebook in the folder `01_prepare_tables`

One or more notebooks in the folder `01_prepare_tables` are used to create the raw tables. Please, use the notebook named `XX_template_table_creation_AWS` to create table using the key `TABLES.CREATION`
<!-- #endregion -->

```python id="_0NnpDD-DIUt"
table_raw_insee = [{
    "database": "ets_insee",
    "name": "ets_insee_raw_juillet",
    "output_id": "",
    "separator": ",",
    "s3URI": "s3://calfdata/INSEE/00_rawData/ETS_01_07_2020",
    "schema": [
        {'Name': 'siren', 'Type': 'string', 'Comment': ''},
 {'Name': 'nic', 'Type': 'string', 'Comment': ''},
 {'Name': 'siret', 'Type': 'string', 'Comment': ''},
 {'Name': 'statutdiffusionetablissement', 'Type': 'string', 'Comment': ''},
 {'Name': 'datecreationetablissement', 'Type': 'string', 'Comment': ''},
 {'Name': 'trancheeffectifsetablissement', 'Type': 'string', 'Comment': ''},
 {'Name': 'anneeeffectifsetablissement', 'Type': 'string', 'Comment': ''},
 {'Name': 'activiteprincipaleregistremetiersetablissement',
  'Type': 'string',
  'Comment': ''},
 {'Name': 'datederniertraitementetablissement',
  'Type': 'string',
  'Comment': ''},
 {'Name': 'etablissementsiege', 'Type': 'string', 'Comment': ''},
 {'Name': 'nombreperiodesetablissement', 'Type': 'string', 'Comment': ''},
 {'Name': 'complementadresseetablissement', 'Type': 'string', 'Comment': ''},
 {'Name': 'numerovoieetablissement', 'Type': 'string', 'Comment': ''},
 {'Name': 'indicerepetitionetablissement', 'Type': 'string', 'Comment': ''},
 {'Name': 'typevoieetablissement', 'Type': 'string', 'Comment': ''},
 {'Name': 'libellevoieetablissement', 'Type': 'string', 'Comment': ''},
 {'Name': 'codepostaletablissement', 'Type': 'string', 'Comment': ''},
 {'Name': 'libellecommuneetablissement', 'Type': 'string', 'Comment': ''},
 {'Name': 'libellecommuneetrangeretablissement',
  'Type': 'string',
  'Comment': ''},
 {'Name': 'distributionspecialeetablissement',
  'Type': 'string',
  'Comment': ''},
 {'Name': 'codecommuneetablissement', 'Type': 'string', 'Comment': ''},
 {'Name': 'codecedexetablissement', 'Type': 'string', 'Comment': ''},
 {'Name': 'libellecedexetablissement', 'Type': 'string', 'Comment': ''},
 {'Name': 'codepaysetrangeretablissement', 'Type': 'string', 'Comment': ''},
 {'Name': 'libellepaysetrangeretablissement', 'Type': 'string', 'Comment': ''},
 {'Name': 'complementadresse2etablissement', 'Type': 'string', 'Comment': ''},
 {'Name': 'numerovoie2etablissement', 'Type': 'string', 'Comment': ''},
 {'Name': 'indicerepetition2etablissement', 'Type': 'string', 'Comment': ''},
 {'Name': 'typevoie2etablissement', 'Type': 'string', 'Comment': ''},
 {'Name': 'libellevoie2etablissement', 'Type': 'string', 'Comment': ''},
 {'Name': 'codepostal2etablissement', 'Type': 'string', 'Comment': ''},
 {'Name': 'libellecommune2etablissement', 'Type': 'string', 'Comment': ''},
 {'Name': 'libellecommuneetranger2etablissement',
  'Type': 'string',
  'Comment': ''},
 {'Name': 'distributionspeciale2etablissement',
  'Type': 'string',
  'Comment': ''},
 {'Name': 'codecommune2etablissement', 'Type': 'string', 'Comment': ''},
 {'Name': 'codecedex2etablissement', 'Type': 'string', 'Comment': ''},
 {'Name': 'libellecedex2etablissement', 'Type': 'string', 'Comment': ''},
 {'Name': 'codepaysetranger2etablissement', 'Type': 'string', 'Comment': ''},
 {'Name': 'libellepaysetranger2etablissement',
  'Type': 'string',
  'Comment': ''},
 {'Name': 'datedebut', 'Type': 'string', 'Comment': ''},
 {'Name': 'etatadministratifetablissement', 'Type': 'string', 'Comment': ''},
 {'Name': 'enseigne1etablissement', 'Type': 'string', 'Comment': ''},
 {'Name': 'enseigne2etablissement', 'Type': 'string', 'Comment': ''},
 {'Name': 'enseigne3etablissement', 'Type': 'string', 'Comment': ''},
 {'Name': 'denominationusuelleetablissement', 'Type': 'string', 'Comment': ''},
 {'Name': 'activiteprincipaleetablissement', 'Type': 'string', 'Comment': ''},
 {'Name': 'nomenclatureactiviteprincipaleetablissement',
  'Type': 'string',
  'Comment': ''},
 {'Name': 'caractereemployeuretablissement', 'Type': 'string', 'Comment': ''}
    ]
}
]
#len(parameters['TABLES']['CREATION']['ALL_SCHEMA'])
```

```python
#filte = "42dcc900-50a8-4ab5-83e2-9778ca6fea72.csv"
#list_ = []
#for i in list(pd.read_csv(filte).columns):
#    list_.append(
#    {'Name': i, 'Type': 'string', 'Comment': ''}
#    )
#list_
```

<!-- #region id="fwcsoOPCDIUx" -->
To remove an item from the list, use `pop` with the index to remove. Exemple `parameters['TABLES']['CREATION']['ALL_SCHEMA'].pop(6)` will remove the 5th item
<!-- #endregion -->

```python id="ZV3cHbKLDIUx"
to_remove = False
if to_remove:
    parameters['TABLES']['CREATION']['ALL_SCHEMA'].pop(0)
```

```python id="auvdEPNLDIU1"
parameters['TABLES']['CREATION']['ALL_SCHEMA'].extend(table_raw_insee)
```

```python id="-mmdbMCoDIU4"
parameters['TABLES']['CREATION']['ALL_SCHEMA'][-1]
```

```python id="IqqQ6sbPDIU6"
json_filename ='parameters_ETL.json'
json_file = json.dumps(parameters)
f = open(json_filename,"w")
f.write(json_file)
f.close()
s3.upload_file(json_filename, 'DATA/ETL')
```

```python id="XgTAunRjDIU9"
s3.download_file(key = 'DATA/ETL/parameters_ETL.json')
with open('parameters_ETL.json', 'r') as fp:
    parameters = json.load(fp)
```

<!-- #region id="h1QKN8xVDIVA" -->
Move `parameters_ETL.json` to the parent folder `01_prepare_tables`
<!-- #endregion -->

```python id="fCwL6Ou5DIVF"
s3_output = parameters['GLOBAL']['QUERIES_OUTPUT']
db = parameters['GLOBAL']['DATABASE']
```

```python id="SiOrUmG2DIVI"
for key, value in parameters["TABLES"]["CREATION"].items():
    if key == "ALL_SCHEMA":
        for table_info in value:
            if table_info['name'] in ['ets_insee_raw_juillet']:

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

## Creation table transformée

La tale tranformée contient 6 variables supplémentaires qui vont être utilisées pour la réalisation des tests. Les 6 variables sont les suivantes:

* `voie_clean` 
    - Ajout de la variable non abbrégée du type de voie. Exemple, l'INSEE indique CH, pour chemin, il faut donc indiquer CHEMIN
* `count_initial_insee`
    - Compte du nombre de siret (établissement) par siren (entreprise).
* ville_matching 
    - Nettoyage de la ville de l'INSEE (`libelleCommuneEtablissement`) de la même manière que l'INPI
* adress_reconstituee_insee:
    - Reconstitution de l'adresse à l'INSEE en utilisant le numéro de voie `numeroVoieEtablissement`, le type de voie non abbrégé, `voie_clean`, l'adresse `libelleVoieEtablissement`  et le `complementAdresseEtablissement` et suppression des articles
* adresse_distance_insee
* list_enseigne:
    - Concatenation de:
        - `enseigne1etablissement`
        - `enseigne2etablissement`
        - `enseigne3etablissement`

Pour créer le pattern regex, on utilise une liste de type de voie disponible dans le Gitlab et à l'INSEE, que nous avons ensuite modifié manuellement. 

- Input
    - CSV: [TypeVoie.csv](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/data/input/Parameters/typeVoieEtablissement.csv)
        - CSV dans S3: [Parameters/upper_stop.csv](https://s3.console.aws.amazon.com/s3/buckets/calfdata/Parameters/TYPE_VOIE/)
        - A créer en table
   - Athena: type_voie
       - CSV dans S3: [Parameters/type_voie.csv](https://s3.console.aws.amazon.com/s3/buckets/calfdata/Parameters/TYPE_VOIE_SQL/)
- Code Python: [Exemple Input 1](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/05_redaction_US/04_prep_voie_num_2697.md#exemple-input-1)

Pour rappel, nous sommes a l'étape 8 de la préparation des données

Le nettoyage des variables de l'adresse suive le schema suivant:

| Table | Variables                 | Article | Digit | Debut/fin espace | Espace | Accent | Upper |
|-------|---------------------------|---------|-------|------------------|--------|--------|-------|
| INSEE  | adresse_distance_insee     | X       | X     | X                | X      | X      | X     |
| INSEE  | adresse_reconstituee_insee |         |       | X                | X      | X      | X     |

```python
step_8 = {
   "STEPS_8":{
      "name":"Creation des variables pour la réalisation des tests pour la siretisation",
      "execution":[
         {
            "database":"ets_insee",
            "name":"ets_insee_transformed",
            "output_id":"",
            "query":{
               "top":" WITH remove_empty_siret AS ( SELECT siren, siret, dateCreationEtablissement, etablissementSiege, etatAdministratifEtablissement, complementAdresseEtablissement, numeroVoieEtablissement, indiceRepetitionEtablissement, CASE WHEN indiceRepetitionEtablissement = 'B' THEN 'BIS' WHEN indiceRepetitionEtablissement = 'T' THEN 'TER' WHEN indiceRepetitionEtablissement = 'Q' THEN 'QUATER' WHEN indiceRepetitionEtablissement = 'C' THEN 'QUINQUIES' ELSE indiceRepetitionEtablissement END as indiceRepetitionEtablissement_full, typeVoieEtablissement, libelleVoieEtablissement, codePostalEtablissement, libelleCommuneEtablissement, libelleCommuneEtrangerEtablissement, distributionSpecialeEtablissement, codeCommuneEtablissement, codeCedexEtablissement, libelleCedexEtablissement, codePaysEtrangerEtablissement, libellePaysEtrangerEtablissement, enseigne1Etablissement, enseigne2Etablissement, enseigne3Etablissement, array_remove( array_distinct( SPLIT( concat( enseigne1etablissement, ',', enseigne2etablissement, ',', enseigne3etablissement ), ',' ) ), '' ) as list_enseigne FROM ets_insee.ets_insee_raw_juillet ) ",
                "middle":" SELECT * FROM ( WITH concat_adress AS( SELECT siren, siret, dateCreationEtablissement, etablissementSiege, etatAdministratifEtablissement, codePostalEtablissement, codeCommuneEtablissement, libelleCommuneEtablissement, ville_matching, numeroVoieEtablissement, array_distinct( regexp_extract_all( REGEXP_REPLACE( REGEXP_REPLACE( REGEXP_REPLACE( CONCAT( COALESCE(numeroVoieEtablissement, ''), ' ', COALESCE( indiceRepetitionEtablissement_full, '' ), ' ', COALESCE(voie_clean, ''), ' ', COALESCE(libelleVoieEtablissement, ''), ' ', COALESCE( complementAdresseEtablissement, '' ) ), '[^\w\s]| +', ' ' ), '(?:^|(?<= ))(AU|AUX|AVEC|CE|CES|DANS|DE|DES|DU|ELLE|EN|ET|EUX|IL|ILS|LA|LE|LES)(?:(?= )|$)', '' ), '\s+\s+', ' ' ), '[0-9]+' ) ) AS list_numero_voie_matching_insee, typeVoieEtablissement, voie_clean, libelleVoieEtablissement, complementAdresseEtablissement, indiceRepetitionEtablissement_full, REGEXP_REPLACE( NORMALIZE( UPPER( trim( REGEXP_REPLACE( REGEXP_REPLACE( REGEXP_REPLACE( CONCAT( COALESCE(numeroVoieEtablissement, ''), ' ', COALESCE( indiceRepetitionEtablissement_full, '' ), ' ', COALESCE(voie_clean, ''), ' ', COALESCE(libelleVoieEtablissement, ''), ' ', COALESCE( complementAdresseEtablissement, '' ) ), '[^\w\s]| +', ' ' ), '\s\s+', ' ' ), '^\s+|\s+$', '' ) ) ), NFD ), '\pM', '' ) AS adresse_reconstituee_insee, REGEXP_REPLACE( NORMALIZE( UPPER( REGEXP_REPLACE( trim( REGEXP_REPLACE( REGEXP_REPLACE( CONCAT( COALESCE(numeroVoieEtablissement, ''), ' ', COALESCE( indiceRepetitionEtablissement_full, '' ), ' ', COALESCE(voie_clean, ''), ' ', COALESCE(libelleVoieEtablissement, ''), ' ', COALESCE( complementAdresseEtablissement, '' ) ), '[^\w\s]|\d+| +', ' ' ), '(?:^|(?<= ))(AU|AUX|AVEC|CE|CES|DANS|DE|DES|DU|ELLE|EN|ET|EUX|IL|ILS|LA|LE|LES)(?:(?= )|$)', '' ) ), '\s+\s+', ' ' ) ), NFD ), '\pM', '' ) AS adresse_distance_insee, enseigne1Etablissement, enseigne2Etablissement, enseigne3Etablissement, list_enseigne FROM ( SELECT siren, siret, dateCreationEtablissement, etablissementSiege, etatAdministratifEtablissement, codePostalEtablissement, codeCommuneEtablissement, libelleCommuneEtablissement, REGEXP_REPLACE( REGEXP_REPLACE( REGEXP_REPLACE( REGEXP_REPLACE( REGEXP_REPLACE( REGEXP_REPLACE( REGEXP_REPLACE( libelleCommuneEtablissement, '^\d+\s|\s\d+\s|\s\d+$', '' ), '^LA\s+|^LES\s+|^LE\s+|\\(.*\\)|^L(ES|A|E) | L(ES|A|E) | L(ES|A|E)$|CEDEX | CEDEX | CEDEX|^E[R*] | E[R*] | E[R*]$', '' ), '^STE | STE | STE$|^STES | STES | STES', 'SAINTE' ), '^ST | ST | ST$', 'SAINT' ), 'S/|^S | S | S$', 'SUR' ), '/S', 'SOUS' ), '[^\w\s]|\([^()]*\)|ER ARRONDISSEMENT|E ARRONDISSEMENT|" \
"|^SUR$|CEDEX|[0-9]+|\s+', '' ) as ville_matching, libelleVoieEtablissement, complementAdresseEtablissement, numeroVoieEtablissement, indiceRepetitionEtablissement_full, typeVoieEtablissement, enseigne1Etablissement, enseigne2Etablissement, enseigne3Etablissement, list_enseigne FROM remove_empty_siret ) LEFT JOIN inpi.type_voie ON typevoieetablissement = type_voie.voie_matching ) ",
                "bottom":" SELECT count_initial_insee, concat_adress.siren, siret, dateCreationEtablissement, etablissementSiege, etatAdministratifEtablissement, codePostalEtablissement, codeCommuneEtablissement, libelleCommuneEtablissement, ville_matching, libelleVoieEtablissement, complementAdresseEtablissement, numeroVoieEtablissement, CASE WHEN cardinality( list_numero_voie_matching_insee ) = 0 THEN NULL ELSE list_numero_voie_matching_insee END as list_numero_voie_matching_insee, indiceRepetitionEtablissement_full, typeVoieEtablissement, voie_clean, adresse_reconstituee_insee, adresse_distance_insee, enseigne1Etablissement, enseigne2Etablissement, enseigne3Etablissement, CASE WHEN cardinality(list_enseigne) = 0 THEN NULL ELSE list_enseigne END AS list_enseigne FROM concat_adress LEFT JOIN ( SELECT siren, COUNT(siren) as count_initial_insee FROM concat_adress GROUP BY siren ) as count_siren ON concat_adress.siren = count_siren.siren ) " }
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
to_remove = True
if to_remove:
    parameters['TABLES']['PREPARATION']['ALL_SCHEMA'].pop(-1)
```

```python
parameters['TABLES']['PREPARATION']['ALL_SCHEMA'].append(step_8)
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
            if step_name in ['STEPS_8']:

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

                    print(output)
```

<!-- #region id="8K6P4GUMDIVN" -->
# Analytics

The cells below execute the job in the key `ANALYSIS`. You need to change the `primary_key` and `secondary_key`.

Il n'est pas possible de récupérer le schema de Glue avec Boto3 sous windows. Nous devons récuperer le schéma manuellement
<!-- #endregion -->

```python
schema = {
	"StorageDescriptor": {
		"Columns":  [
				{
					"Name": "count_initial_insee",
					"Type": "bigint",
					"comment": ""
				},
				{
					"Name": "siren",
					"Type": "string",
					"comment": ""
				},
				{
					"Name": "siret",
					"Type": "string",
					"comment": ""
				},
				{
					"Name": "datecreationetablissement",
					"Type": "string",
					"comment": ""
				},
				{
					"Name": "etablissementsiege",
					"Type": "string",
					"comment": ""
				},
				{
					"Name": "etatadministratifetablissement",
					"Type": "string",
					"comment": ""
				},
				{
					"Name": "codepostaletablissement",
					"Type": "string",
					"comment": ""
				},
				{
					"Name": "codecommuneetablissement",
					"Type": "string",
					"comment": ""
				},
				{
					"Name": "libellecommuneetablissement",
					"Type": "string",
					"comment": ""
				},
				{
					"Name": "ville_matching",
					"Type": "string",
					"comment": ""
				},
				{
					"Name": "libellevoieetablissement",
					"Type": "string",
					"comment": ""
				},
				{
					"Name": "complementadresseetablissement",
					"Type": "string",
					"comment": ""
				},
				{
					"Name": "numerovoieetablissement",
					"Type": "string",
					"comment": ""
				},
				{
					"Name": "list_numero_voie_matching_insee",
					"Type": "array<string>",
					"comment": ""
				},
				{
					"Name": "indicerepetitionetablissement_full",
					"Type": "string",
					"comment": ""
				},
				{
					"Name": "typevoieetablissement",
					"Type": "string",
					"comment": ""
				},
				{
					"Name": "voie_clean",
					"Type": "string",
					"comment": ""
				},
				{
					"Name": "adresse_reconstituee_insee",
					"Type": "string",
					"comment": ""
				},
				{
					"Name": "adresse_distance_insee",
					"Type": "string",
					"comment": ""
				},
				{
					"Name": "enseigne1etablissement",
					"Type": "string",
					"comment": ""
				},
				{
					"Name": "enseigne2etablissement",
					"Type": "string",
					"comment": ""
				},
				{
					"Name": "enseigne3etablissement",
					"Type": "string",
					"comment": ""
				},
				{
					"Name": "list_enseigne",
					"Type": "array<string>",
					"comment": ""
				}
			],
		"location": "s3://calfdata/SQL_OUTPUT_ATHENA/tables/51d2765a-0852-4a0a-9333-943ee7e66f5d/",
		"inputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
		"outputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
		"compressed": "false",
		"numBuckets": "0",
		"SerDeInfo": {
			"name": "ets_insee_transformed",
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

<!-- #region id="xvOS6SftDIVO" -->
## Count missing values
<!-- #endregion -->

```python id="fV8oF3BxDIVP"
from datetime import date
today = date.today().strftime('%Y%M%d')
today
```

```python
#table_info["name"] = "ets_insee_transformed"
```

```python id="B4PV0taoDIVS"
table_top = parameters["ANALYSIS"]["COUNT_MISSING"]["top"]
table_middle = ""
table_bottom = parameters["ANALYSIS"]["COUNT_MISSING"]["bottom"].format(
    table_info["database"], table_info["name"]
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

<!-- #region id="ViqZ4ro1DIVU" -->
# Brief description table

In this part, we provide a brief summary statistic from the lattest jobs. For the continuous analysis with a primary/secondary key, please add the relevant variables you want to know the count and distribution
<!-- #endregion -->

<!-- #region id="5NRhygTxDIVV" -->
## Categorical Description

During the categorical analysis, we wil count the number of observations for a given group and for a pair.

### Count obs by group

- Index: primary group
- nb_obs: Number of observations per primary group value
- percentage: Percentage of observation per primary group value over the total number of observations

Returns the top 10 only
<!-- #endregion -->

```python id="14pMnRZPDIVV"
for field in schema["StorageDescriptor"]["Columns"]:
    if field["Type"] in ["string", "object", "varchar(12)"]:

        print("Nb of obs for {}".format(field["Name"]))

        query = parameters["ANALYSIS"]["CATEGORICAL"]["PAIR"].format(
            table_info["database"], table_info["name"], field["Name"]
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

<!-- #region id="HUrDFOV6DIVo" -->
# Generation report
<!-- #endregion -->

```python id="vkHkD2U8DIVp"
import os, time, shutil, urllib, ipykernel, json
from pathlib import Path
from notebook import notebookapp
```

```python id="Ab11xUncDIVr"
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

```python id="zjf7kUNQDIVu"
create_report(extension = "html", keep_code = True)
```
