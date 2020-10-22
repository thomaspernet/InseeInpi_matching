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

<!-- #region id="cI5JOtG4DIn2" -->
# Creation table ets_inpi contenant les nouvelles variables permettant la siretisation

# Objective(s)

*  Création des variables suivantes pour permettre la réalisation des tests avec l’INSEE

# Metadata

* Epic: Epic 6
* US: US 3
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
* Task tag: #athena,#lookup-table,#s3,#sql,#data-preparation,#documentation
* Toggl Tag: #documentation

# Input Cloud Storage [AWS/GCP]

## Table/file

* Origin: 
* Athena
* Name: 
* ets_filtre_enrichie_historique
* Github: 
  * https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/10_sumup_preparation/00_filtrage_enrichissement_inpi.md

# Destination Output/Delivery

## Table/file

* Origin: 
* Athena
* Name:
* ets_inpi_transformed
* GitHub:
* https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/10_sumup_preparation/02_creation_variables_siretisation_inpi.md
<!-- #endregion -->
```python inputHidden=false jupyter={"outputs_hidden": false} outputHidden=false id="30Og9fxWDIn2"
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

```python inputHidden=false jupyter={"outputs_hidden": false} outputHidden=false id="pBP8v2IBDIn6"
con = aws_connector.aws_instantiate(credential = path_cred,
                                       region = region)
client= con.client_boto()
s3 = service_s3.connect_S3(client = client,
                      bucket = bucket, verbose = True) 
glue = service_glue.connect_glue(client = client) 
```

```python id="ZsapT38zDIn9"
pandas_setting = True
if pandas_setting:
    cm = sns.light_palette("green", as_cmap=True)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', None)
```

# Introduction 

L’objectif de la sirétisation est d’attribuer un siret à un établissement appartenant à une entreprise. Le siret est un identifiant unique rattaché à un établissement, et donc à une adresse. Un siret, dès lors, ne peut posséder plusieurs adresses. En cas de création, déménagement ou fermeture d’établissement, un nouveau siret sera attribué. 

L’INSEE est en charge de la création et attribution du siret. Toutefois, cette information ne figure pas à l’INPI. La manière dont l’INPI distingue des établissements est légèrement différente de l’INSEE. A l’INPI, il faut regarder la séquence siren, code greffe, numéro de gestion et numéro d’établissement pour identifier un établissement d’un autre. 

La difficulté de la siretisation vient du manque de normalisme entre les deux organismes. L’INSEE affiche davantage de conformité dans la création de la variable qu’à l’INPI. La création de l’adresse du coté de l’INPI est laissée à la seule appréciation du greffe. Il est a noté que des fautes sont possibles à la fois à l’INPI mais aussi à l’INSEE. 

La seconde difficulté rencontrée est la différence d’état des fichiers entre l’INSEE et l’INPI. L’INSEE fournit chaque mois un stock de donnée, ou dit autrement, donne un état des lieux à l’instant t des entreprises en France. Un établissement peut être ouvert en t-1 mais fermée à date t . Ce changement d’état ne va pas figurer à l’INSEE. Nous allons recevoir uniquement le dernier état connu, a savoir la fermeture. D’un point de vue technique, un siret n’a qu’une seule ligne dans la table de l’INSEE. La table de l’INPI va contenir l’ensemble des données historiques, avant toutes les modifications effectuées sur les établissements. En prenant l’exemple ci dessus, nous connaissons le status de l’établissement lors des deux dates, un premier état ouvert et un second état fermé. Dès lors nous pouvons avoir plusieurs lignes possibles par siret. 

Lors de notre processus de siretisation, nous allons rapprocher les deux tables en utilisant un score de similarité et des règles de gestion. Le rapprochement  entre les deux tables va se faire via le siren, la ville et le code postal. Il est très probable qu’une entreprise possède plusieurs établissements dans la même ville, ce qui va aboutir à un doublonage des observations. Autrement dit, un même établissement à l’INPI va posséder plusieurs siret. Par exemple, si une entreprise possède 2 établissement dans la même ville, et que ses deux établissements sont aussi présents à l’INPI, alors le rapprochement va déboucher sur 4 lignes (deux lignes par établissements). Il faut multiplier le nombre de création de nouvelles lignes par le nombre d’événements à l’INPI. Si la table de l’INPI à 2 événements pour un établissement, alors cela va créer 4 lignes supplémentaires (deux par événement). 

L’utilisation du score de similarité (défini ci dessous) et les règles de gestion vont permettre de distinguer les siret aux bonnes adresses. Du fait de la compléxité de certaines adresses, il peut y avoir des lignes qui ne peuvent être siretisé. Lorsque ce cas se présente, nous avons deux méthodes. La méthode une consiste à regarder si la séquence n’a pas été trouvé via une autre ligne. En effet, si nous avons pu trouver le siret d’une séquence via une autre ligne, nous pouvons l’attribuer à l’ensemble des lignes de la même séquence. Nous savons qu’une séquence ne peut pas changer, et qu’une fois le siret trouvé, ce sera toujours le même. Attention, nous avons détecté dans certains cas des séquences avec plusieurs siret car l’INPI a modifié l’adresse d’une séquence sans fermer l’établissement, ce qui n’est pas possible dans les faits. La seconde méthode repose sur le NLP (Natural Language Processing), plus précisément le Word2Vec pour calculer la similarité entre les adresses plus détaillées à l’INPI qu’a l’INSEE. Si un des mots de l’INSEE n’est pas présent dans l’adresse de l’INPI alors que l’adresse est plus détaillée, nous allons calculer un indicateur de similarité entre les mot de l’INSEE  non présents dans l’INPI est ceux de l’INPI. Si une des valeurs est supérieure a un seuil, on peut dire que c’est la bonne adresse. Par exemple, l’INPI peut avoir écrit BD alors que l’INSEE a écrit BOULEVARD . Le modèle va comprendre que les deux mots ont la même signification. 

## Tableau recapitulatif variables

Pour créer les tests pour la siretisation, nous avons besoin de créer de nouvelle variables. Le tableau ci-dessous récapital les variables pour la table de l'INPI.

| Tables | Variables                      | Commentaire                                                                                                                                                                                             | Bullet_inputs                                                | US_md                                                          | query_md_gitlab                                                                                                                                                                                                                                                                                        | Pattern_regex                                                       |
|--------|--------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------|----------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------|
| INPI   | sequence_id                    | ID unique pour la séquence suivante: siren + code greffe + nom greffe + numero gestion +ID établissement                                                                                                | siren code_greffe nom_greffe numero_gestion id_etablissement | [2976](https://tree.taiga.io/project/olivierlubet-air/us/2976) | [create-id-and-id-sequence](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/01_Athena_concatenate_ETS.md#create-id-and-id-sequence)                                                                    |                                                                     |
| INPI   | adresse_reconstituee_inpi      | Concatenation des champs de l'adresse et suppression des espace                                                                                                                                         | adresse_ligne1 adresse_ligne2 adresse_ligne3                 | [2690](https://tree.taiga.io/project/olivierlubet-air/us/2690) | [adress_reconsitituee_inpi](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/03_ETS_add_variables.md#adress_reconsitituee_inpi)                                                                         | debut/fin espace,espace,accent,Upper                                |
| INPI   | adresse_distance_inpi          | Concatenation des champs de l'adresse, suppression des espaces et des articles. Utilisé pour calculer le score permettant de distinguer la similarité/dissimilarité entre deux adresses (INPI vs INSEE) | adresse_ligne1 adresse_ligne2 adresse_ligne3                 | [2949](https://tree.taiga.io/project/olivierlubet-air/us/2949) | [adresse_distance_inpi](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/03_ETS_add_variables.md#adresse_distance_inpi)                                                                                 | article,digit,debut/fin espace,espace,accent,Upper                  |
| INPI   | ville_matching                 | Nettoyage regex de la ville et suppression des espaces. La même logique de nettoyage est appliquée coté INSEE                                                                                           | ville                                                        | [2613](https://tree.taiga.io/project/olivierlubet-air/us/2613) | [etape-1-pr%C3%A9paration-ville_matching](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/03_ETS_add_variables.md#etape-1-pr%C3%A9paration-ville_matching)                                             | article,digit,debut/fin espace,espace,accent,Upper,Regles_speciales |
| INPI   | list_numero_voie_matching_inpi | Liste contenant tous les numéros de l'adresse dans l'INPI                                                                                                                                               | adresse_ligne1 adresse_ligne2 adresse_ligne3                 | [3000](https://tree.taiga.io/project/olivierlubet-air/us/3000) | [etape-5-creation-liste-num%C3%A9ro-de-voie](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/03_ETS_add_variables.md#etape-5-creation-liste-num%C3%A9ro-de-voie)                                       | digit,debut/fin espace,espace                                       |
| INPI   | last_libele_evt                | Extraction du dernier libellé de l'événement connu pour une séquence, et appliquer cette information à l'ensemble de la séquence                                                                        | libelle_evt                                                  | [2950](https://tree.taiga.io/project/olivierlubet-air/us/2950) | [etape-4-cr%C3%A9ation-last_libele_evt-status_admin-status_ets](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/03_ETS_add_variables.md#etape-4-cr%C3%A9ation-last_libele_evt-status_admin-status_ets) |                                                                     |
| INPI   | status_admin                   | Informe du status ouvert/fermé concernant une séquence                                                                                                                                                  | last_libele_evt                                              | [2951](https://tree.taiga.io/project/olivierlubet-air/us/2951) | [etape-4-cr%C3%A9ation-last_libele_evt-status_admin-status_ets](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/03_ETS_add_variables.md#etape-4-cr%C3%A9ation-last_libele_evt-status_admin-status_ets) | Regles_speciales                                                    |
| INPI   | status_ets                     | Informe du type d'établissement (SIE/PRI/SEC) concernant une séquence                                                                                                                                   | type                                                         | [2951](https://tree.taiga.io/project/olivierlubet-air/us/2951) | [etape-4-cr%C3%A9ation-last_libele_evt-status_admin-status_ets](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/03_ETS_add_variables.md#etape-4-cr%C3%A9ation-last_libele_evt-status_admin-status_ets) | Regles_speciales                                                    |


## Transformation data

Il y a 9 variables a construire pour finaliser la table de l'INPI. Parmis les 9 variables, 7 peuvent être créer assez simplement alors que les 3 autres demandent de faire de partionner la data sur une séquence. Une sequence fait référence a à la définition d'un établissement au sens de l'INPI, comme évoqué en introduction de la documentation.

**Aucun grouping**

* `index_id`: 
    - Création du numéro de ligne
* `enseigne`: 
    - Mise en majuscule
* `ville_matching`:
    - Nettoyage regex de la ville et suppression des espaces
* `adress_reconstituee_inpi`
    - Concatenation des champs de l'adresse et suppression des espaces
* `adress_distance_inpi`: 
    - Concatenation des champs de l'adresse, suppression des espaces et des articles
* `list_numero_voie_matching`:
    - Liste contenant tous les numéros de l'adresse dans l'INPI
* `status_ets`: 
    - Informe du type d'établissement (SIE/PRI.SEC) concernant une séquence

**Grouping**

* `sequence_id`:
    - Attribution d'un ID unique pour la sequence siren + code greffe + nom greffe + numero gestion +ID établissement. Cela fait référence a la définition d'établissement au sens de l'INPI.
* `status_admin`: 
    - Informe du status ouvert/fermé concernant une séquence
* `last_libelle_evt`: 
    - Extraction du dernier libellé de l'événement connu pour une séquence, et appliquer cette information à l'ensemble de la séquence    
    
Pour faciliter la construction de la table `ets_inpi_transformed`, nous allons procéder a une étape intermédiaire, a savoir la création de `ets_inpi_transformed_temp`. Cette table intermédiaire va calculer l'index et la séquence.

```python id="nOhbGckqDIoC"
### If chinese characters, set  ensure_ascii=False
s3.download_file(key = 'DATA/ETL/parameters_ETL.json')
with open('parameters_ETL.json', 'r') as fp:
    parameters = json.load(fp)
#print(json.dumps(parameters, indent=4, sort_keys=True, ensure_ascii=False))
```

### Etap 1: Creation séquence

Dans cette étape, nous allons créer un index, qui est simplement le numéro de ligne, ainsi qu'un ID établissement unique, qui groupe les variables `siren`, `code_greffe`, `nom_greffe`, `numero_gestion`, `id_etablissement`.

Pour rappel, nous en sommes a l'étape 6 dans la pipeline.

```python id="uVGYsgexDIoF"
step_6 = {
   "STEPS_6":{
      "name":"Creation sequence caracterisant un établissement au sens de l'INPI",
      "execution":[
         {
            "database":"ets_inpi",
            "name":"ets_inpi_transformed_temp",
            "output_id":"",
            "query":{
               "top":" WITH cte AS ( SELECT siren, code_greffe, nom_greffe, numero_gestion, id_etablissement, sequence_id, ROW_NUMBER() OVER ( PARTITION BY sequence_id ORDER BY sequence_id) as rownum FROM ( SELECT siren, code_greffe, nom_greffe, numero_gestion, id_etablissement, DENSE_RANK () OVER( ORDER BY siren, code_greffe, nom_greffe, numero_gestion, id_etablissement) AS sequence_id FROM ets_filtre_enrichie_historique ) ) ",
               "middle":" SELECT ROW_NUMBER() OVER () as index_id, sequence_id, ets_filtre_enrichie_historique.siren, ets_filtre_enrichie_historique.code_greffe, ets_filtre_enrichie_historique.nom_greffe, ets_filtre_enrichie_historique.numero_gestion, ets_filtre_enrichie_historique.id_etablissement, status, origin, date_greffe, libelle_evt, type, siege_pm, rcs_registre, adresse_ligne1, adresse_ligne2, adresse_ligne3, code_postal, ville, code_commune, pays, domiciliataire_nom, domiciliataire_siren, domiciliataire_greffe, domiciliataire_complement, siege_domicile_representant, nom_commercial, enseigne, activite_ambulante, activite_saisonniere, activite_non_sedentaire, date_debut_activite, activite, origine_fonds, origine_fonds_info, type_exploitation, csv_source FROM ets_filtre_enrichie_historique INNER JOIN ( ",
               "bottom":" SELECT * FROM cte WHERE rownum = 1 ) as no_dup_cte ON ets_filtre_enrichie_historique.siren = no_dup_cte.siren AND ets_filtre_enrichie_historique.code_greffe = no_dup_cte.code_greffe AND ets_filtre_enrichie_historique.nom_greffe = no_dup_cte.nom_greffe AND ets_filtre_enrichie_historique.numero_gestion = no_dup_cte.numero_gestion AND ets_filtre_enrichie_historique.id_etablissement = no_dup_cte.id_etablissement "
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

<!-- #region id="RHp1yZ4-DIoI" -->
To remove an item from the list, use `pop` with the index to remove. Exemple `parameters['TABLES']['PREPARATION']['ALL_SCHEMA'].pop(6)` will remove the 5th item
<!-- #endregion -->

```python id="qwfUq0__DIoJ"
to_remove = False
if to_remove:
    parameters['TABLES']['PREPARATION']['ALL_SCHEMA'].pop(-1)
```

```python id="b-M0cc5ADIoN"
parameters['TABLES']['PREPARATION']['ALL_SCHEMA'].append(step_6)
```

Query executée

```python
for key, value in parameters["TABLES"]["PREPARATION"].items():
    if key == "ALL_SCHEMA":
        ### LOOP STEPS
        for i, steps in enumerate(value):
            step_name = "STEPS_{}".format(i)
            if step_name in [ "STEPS_6"]:
                print('\n', steps[step_name]['name'], '\n')
                for j, step_n in enumerate(steps[step_name]["execution"]):
                    ### COMPILE QUERY
                    ### CREATE TOP
                    table_top = parameters["TABLES"]["PREPARATION"]["template"][
                    "top"
                ].format(step_n["database"], step_n["name"],)

                
                    query = (
                        table_top
                        + "\n"
                        + step_n["query"]["top"]
                        + "\n"
                        + step_n["query"]["middle"]
                        + "\n"
                        + step_n["query"]["bottom"]
                    )

                    print(query)

```

```python id="MEw20IRNDIoQ"
json_filename ='parameters_ETL.json'
json_file = json.dumps(parameters)
f = open(json_filename,"w")
f.write(json_file)
f.close()
s3.upload_file(json_filename, 'DATA/ETL')
```

```python id="JDdataIdDIoS"
s3.download_file(key = 'DATA/ETL/parameters_ETL.json')
with open('parameters_ETL.json', 'r') as fp:
    parameters = json.load(fp)
```

<!-- #region id="qWVE6NhlDIoV" -->
Move `parameters_ETL.json` to the parent folder `01_prepare_tables`
<!-- #endregion -->

```python id="EGtMXwPsDIoa"
s3_output = parameters['GLOBAL']['QUERIES_OUTPUT']
db = parameters['GLOBAL']['DATABASE']
```

Il faut être patient car l'éxécution prend environ 20 minutes.

```python id="IIfWNKz_DIod"
for key, value in parameters["TABLES"]["PREPARATION"].items():
    if key == "ALL_SCHEMA":
        ### LOOP STEPS
        for i, steps in enumerate(value):
            step_name = "STEPS_{}".format(i)
            if step_name in ['STEPS_6']:

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

Appercu tables créées

```python
pd.set_option('display.max_colwidth', 50)
```

```python
for key, value in parameters["TABLES"]["PREPARATION"].items():
    if key == "ALL_SCHEMA":
        ### LOOP STEPS
        for i, steps in enumerate(value):
            step_name = "STEPS_{}".format(i)
            if step_name in ['STEPS_6']:
                print('\n', steps[step_name]['name'], '\n')
                for j, step_n in enumerate(steps[step_name]["execution"]):
                    query = """
                    SELECT *
                    FROM {}
                    LIMIT 10
                    """.format(step_n['name'])
                    
                    output = s3.run_query(
                    query=query,
                    database=db,
                    s3_output=s3_output,
                    filename='show_{}'.format(step_n['name']),  ## Add filename to print dataframe
                    destination_key=None,  ### Add destination key if need to copy output
                )
                    
                    display(output)

```

### Etap 2: Creation variables simples et groupings

Dès lors que l'index et la sequence ont été créée, nous allons pouvoir calculer les variables de grouping sur ce dernier. De plus, nous allons créer les variables dites "simples" au préalable.

Le calcul de la variable `status_admin` nécéssite la variable `last_libelle_evt`. En effet, nous avons besoin de connaitre le status le plus récent d'un établissement pour indiquer si un établissement est en activité ou fermé administrativment. Lorsque l'établissement est fermé, il faut l'indiquer sur l'ensemble de la séquence car les valeurs passées doivent être mises a jour.

Le nettoyage des variables de l'adresse suive le schema suivant:

| Table | Variables                 | Article | Digit | Debut/fin espace | Espace | Accent | Upper |
|-------|---------------------------|---------|-------|------------------|--------|--------|-------|
| INPI  | adresse_distance_inpi     | X       | X     | X                | X      | X      | X     |
| INPI  | adresse_reconstituee_inpi |         |       | X                | X      | X      | X     |

```python
step_7 = {
   "STEPS_7":{
      "name":"Creation variables simple et le status administratif d'un etablissement",
      "execution":[
         {
            "database":"ets_inpi",
            "name":"ets_inpi_transformed",
            "output_id":"",
            "query":{
               "top":"WITH create_var AS ( SELECT index_id, ets_inpi_transformed_temp.sequence_id, ets_inpi_transformed_temp.siren, ets_inpi_transformed_temp.code_greffe, ets_inpi_transformed_temp.nom_greffe, ets_inpi_transformed_temp.numero_gestion, ets_inpi_transformed_temp.id_etablissement, status, origin, date_greffe, libelle_evt, last_libele_evt, CASE WHEN last_libele_evt = 'Etablissement supprimé' THEN 'F' ELSE 'A' END AS status_admin, type, CASE WHEN type = 'SIE' OR type = 'SEP' THEN 'true' ELSE 'false' END AS status_ets, siege_pm, rcs_registre, adresse_ligne1, adresse_ligne2, adresse_ligne3, REGEXP_REPLACE( trim( REGEXP_REPLACE( REGEXP_REPLACE( NORMALIZE( UPPER( CONCAT( adresse_ligne1, ' ', adresse_ligne2, ' ', adresse_ligne3 ) ), NFD ), '\pM', '' ), '[^\w\s]| +', ' ' ) ), '\s+\s+', ' ' ) AS adresse_reconstituee_inpi , regexp_replace( trim( REGEXP_REPLACE( REGEXP_REPLACE( REGEXP_REPLACE( NORMALIZE( UPPER( CONCAT( adresse_ligne1, ' ', adresse_ligne2, ' ', adresse_ligne3 ) ), NFD ), '\pM', '' ), '[^\w\s]|\d+| +', ' ' ), '(?:^|(?<= ))(AU|AUX|AVEC|CE|CES|DANS|DE|DES|DU|ELLE|EN|ET|EUX|IL|ILS|LA|LE|LES)(?:(?= )|$)', '' ) ), '\s+\s+', ' ' ) AS adresse_distance_inpi, array_distinct( regexp_extract_all( trim( REGEXP_REPLACE( REGEXP_REPLACE( NORMALIZE( UPPER( CONCAT( adresse_ligne1, ' ', adresse_ligne2, ' ', adresse_ligne3 ) ), NFD ), '\pM', '' ), '[^\w\s]| +', ' ' ) ), '[0-9]+' ) ) AS list_numero_voie_matching_inpi, code_postal, CASE WHEN code_postal = '' THEN REGEXP_EXTRACT(ville, '\d{5}') WHEN LENGTH(code_postal) = 5 THEN code_postal ELSE NULL END AS code_postal_matching, ville, REGEXP_REPLACE( REGEXP_REPLACE( REGEXP_REPLACE( REGEXP_REPLACE( REGEXP_REPLACE( REGEXP_REPLACE( REGEXP_REPLACE( REGEXP_REPLACE( REGEXP_REPLACE( NORMALIZE( UPPER(ville), NFD ), '\pM', '' ), '^\d+\s|\s\d+\s|\s\d+$', '' ), '^LA\s+|^LES\s+|^LE\s+|\\(.*\\)|^L(ES|A|E) | L(ES|A|E) | L(ES|A|E)$|CEDEX | CEDEX | CEDEX|^E[R*] | E[R*] | E[R*]$', '' ), '^STE | STE | STE$|^STES | STES | STES', 'SAINTE' ), '^ST | ST | ST$', 'SAINT' ), 'S/|^S | S | S$', 'SUR' ), '/S', 'SOUS' ), '[^\w\s]|\([^()]*\)|ER ARRONDISSEMENT|E ARRONDISSEMENT|" \
"|^SUR$|CEDEX|[0-9]+|\s+', '' ), 'MARSEILLEE', 'MARSEILLE' ) as ville_matching, code_commune, pays, domiciliataire_nom, domiciliataire_siren, domiciliataire_greffe, domiciliataire_complement, siege_domicile_representant, nom_commercial, UPPER(enseigne) as enseigne, activite_ambulante, activite_saisonniere, activite_non_sedentaire, date_debut_activite, activite, origine_fonds, origine_fonds_info, type_exploitation, csv_source FROM ets_inpi_transformed_temp",
               "middle":" INNER JOIN ( SELECT ets_inpi_transformed_temp.sequence_id, ets_inpi_transformed_temp.libelle_evt as last_libele_evt, max_date_greffe FROM ets_inpi_transformed_temp INNER JOIN ( SELECT sequence_id, MAX(date_greffe) as max_date_greffe FROM ets_inpi_transformed_temp GROUP BY sequence_id ) AS temp ON temp.sequence_id = ets_inpi_transformed_temp.sequence_id AND temp.max_date_greffe = ets_inpi_transformed_temp.date_greffe ) as max_date ON max_date.sequence_id = ets_inpi_transformed_temp.sequence_id ) ",
                "bottom":" SELECT index_id, sequence_id, siren, code_greffe, nom_greffe, numero_gestion, id_etablissement, status, origin, date_greffe, libelle_evt, last_libele_evt, status_admin, type, status_ets, siege_pm, rcs_registre, adresse_ligne1, adresse_ligne2, adresse_ligne3, adresse_reconstituee_inpi, adresse_distance_inpi, CASE WHEN cardinality(list_numero_voie_matching_inpi) = 0 THEN NULL ELSE list_numero_voie_matching_inpi END as list_numero_voie_matching_inpi, code_postal, code_postal_matching, ville_matching, code_commune, pays, domiciliataire_nom, domiciliataire_siren, domiciliataire_greffe, domiciliataire_complement, siege_domicile_representant, nom_commercial, enseigne, activite_ambulante, activite_saisonniere, activite_non_sedentaire, date_debut_activite, activite, origine_fonds, origine_fonds_info, type_exploitation, csv_source FROM create_var ",
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
parameters['TABLES']['PREPARATION']['ALL_SCHEMA'].append(step_7)
```

Query executée

```python
for key, value in parameters["TABLES"]["PREPARATION"].items():
    if key == "ALL_SCHEMA":
        ### LOOP STEPS
        for i, steps in enumerate(value):
            step_name = "STEPS_{}".format(i)
            if step_name in [ "STEPS_6"]:
                print('\n', steps[step_name]['name'], '\n')
                for j, step_n in enumerate(steps[step_name]["execution"]):
                    ### COMPILE QUERY
                    ### CREATE TOP
                    table_top = parameters["TABLES"]["PREPARATION"]["template"][
                    "top"
                ].format(step_n["database"], step_n["name"],)

                
                    query = (
                        table_top
                        + "\n"
                        + step_n["query"]["top"]
                        + "\n"
                        + step_n["query"]["middle"]
                        + "\n"
                        + step_n["query"]["bottom"]
                    )

                    print(query)
```

```python
for key, value in parameters["TABLES"]["PREPARATION"].items():
    if key == "ALL_SCHEMA":
        ### LOOP STEPS
        for i, steps in enumerate(value):
            step_name = "STEPS_{}".format(i)
            if step_name in ['STEPS_7']:

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

```python
json_filename ='parameters_ETL.json'
json_file = json.dumps(parameters)
f = open(json_filename,"w")
f.write(json_file)
f.close()
s3.upload_file(json_filename, 'DATA/ETL')
```

<!-- #region id="tkXIgOQcDIog" -->
Get the schema of the lattest job
<!-- #endregion -->

<!-- #region id="DvROvxftDIok" -->
# Analytics

The cells below execute the job in the key `ANALYSIS`. You need to change the `primary_key` and `secondary_key`.

Il n'est pas possible de récupérer le schema de Glue avec Boto3 sous windows. Nous devons récuperer le schéma manuellement
<!-- #endregion -->

```python
schema = {
	"StorageDescriptor": {
		"Columns": [
				{
					"Name": "index_id",
					"Type": "bigint",
					"comment": ""
				},
				{
					"Name": "sequence_id",
					"Type": "bigint",
					"comment": ""
				},
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
					"Name": "status",
					"Type": "varchar(6)",
					"comment": ""
				},
				{
					"Name": "origin",
					"Type": "varchar(7)",
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
					"Name": "last_libele_evt",
					"Type": "string",
					"comment": ""
				},
				{
					"Name": "status_admin",
					"Type": "varchar(1)",
					"comment": ""
				},
				{
					"Name": "type",
					"Type": "string",
					"comment": ""
				},
				{
					"Name": "status_ets",
					"Type": "varchar(5)",
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
					"Name": "adresse_reconstituee_inpi",
					"Type": "string",
					"comment": ""
				},
				{
					"Name": "adresse_distance_inpi",
					"Type": "string",
					"comment": ""
				},
				{
					"Name": "list_numero_voie_matching_inpi",
					"Type": "array<string>",
					"comment": ""
				},
				{
					"Name": "code_postal",
					"Type": "string",
					"comment": ""
				},
				{
					"Name": "code_postal_matching",
					"Type": "string",
					"comment": ""
				},
				{
					"Name": "ville_matching",
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
		"location": "s3://calfdata/SQL_OUTPUT_ATHENA/tables/7d26db88-7b1a-4084-9ee3-17f3a59c4f8d/",
		"inputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
		"outputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
		"compressed": "false",
		"numBuckets": "0",
		"SerDeInfo": {
			"name": "ets_inpi_transformed",
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

<!-- #region id="TtEjycmxDIol" -->
## Count missing values
<!-- #endregion -->

```python id="3xU6B60NDIom"
from datetime import date
today = date.today().strftime('%Y%M%d')
today
```

```python id="ShHZcC-YDIoo"
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

<!-- #region id="-W6g6SFhDIoq" -->
# Brief description table

In this part, we provide a brief summary statistic from the lattest jobs. For the continuous analysis with a primary/secondary key, please add the relevant variables you want to know the count and distribution
<!-- #endregion -->

<!-- #region id="QH4FZ3IkDIor" -->
## Categorical Description

During the categorical analysis, we wil count the number of observations for a given group and for a pair.

### Count obs by group

- Index: primary group
- nb_obs: Number of observations per primary group value
- percentage: Percentage of observation per primary group value over the total number of observations

Returns the top 10 only
<!-- #endregion -->

```python id="F8m9Ja9aDIos"
for field in schema["StorageDescriptor"]["Columns"]:
    if field["Type"] in ["string", "object", "varchar(12)"]:

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

<!-- #region id="kySMFnjVDIou" -->
### Count obs by two pair

You need to pass the primary group in the cell below

- Index: primary group
- Columns: Secondary key -> All the categorical variables in the dataset
- nb_obs: Number of observations per primary group value
- Total: Total number of observations per primary group value (sum by row)
- percentage: Percentage of observations per primary group value over the total number of observations per primary group value (sum by row)

Returns the top 10 only
<!-- #endregion -->

```python id="ySqwANvMDIov"
primary_key = "last_libele_evt"
```

```python id="zhgodxZRDIox"
for field in schema["StorageDescriptor"]["Columns"]:
    if field["Type"] in ["string", "object", "varchar(12)"]:
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

<!-- #region id="kJLL-NklDIpB" -->
# Generation report
<!-- #endregion -->

```python id="rkh77-3nDIpB"
import os, time, shutil, urllib, ipykernel, json
from pathlib import Path
from notebook import notebookapp
```

```python id="n8jkfXmdDIpE"
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

```python id="5qEsk80XDIpG"
#create_report(extension = "html", keep_code = True)
```
