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
# Creation de la table contenant les statut entre les adresses INSEE et INPI

# Objective(s)

*  Creation des cas de figures possible entre la comparaison de l’adresse INSEE et l’adresse de l’INPI
* Please, update the Source URL by clicking on the button after the information have been pasted
  * US 05 Creation variables de siretisation Modify rows
  * Delete tables and Github related to the US: Delete rows

# Metadata

* Epic: Epic 6
* US: US 5
* Date Begin: 9/29/2020
* Duration Task: 0
* Description: Creation de la documentation avec les types de statut et creation de la table statut
* Step type: Transform table
* Status: Active
  * Change Status task: Active
  * Update table: Modify rows
* Source URL: US 05 Creation variables de siretisation
* Task type: Jupyter Notebook
* Users: Thomas Pernet
* Watchers: Thomas Pernet
* User Account: https://937882855452.signin.aws.amazon.com/console
* Estimated Log points: 5
* Task tag: #athena,#sql,#data-preparation,#inpi,#insee
* Toggl Tag: #documentation

# Input Cloud Storage [AWS/GCP]


## Table/file

* Origin: 
* Athena
* Name: 
* ets_insee_inpi
* Github: 
  * https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/11_sumup_siretisation/00_merge_ets_insee_inpi.md

# Destination Output/Delivery

## Table/file

* Origin: 
* Athena
* Name:
* ets_insee_inpi_statut_cas
* GitHub:
* https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/11_sumup_siretisation/01_cas_de_figure.md

<!-- #endregion -->

# Introduction

Le second ensemble du processus de siretisation peut se diviser en 6 sous-ensembles, ou chacune des sous-parties fait référence à la création d'une ou plusieurs variables de test. 

Le tableau ci dessous récapitule les sous-ensembles 1 à 6, qui vont de la création des types de ressemble entre l'adresse de l'INSEE et de l'INPI jusqu'aux tests sur la date, l'état administratif et de l'ensemble


 | Rang | Nom_variable                              | Dependence                                    | Notebook                           | Difficulte | Table_generee                                                                                                                                                            | Variables_crees_US                                                                 | Possibilites                  |
|------|-------------------------------------------|-----------------------------------------------|------------------------------------|------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------|-------------------------------|
| 1    | status_cas                                |                                               | 01_cas_de_figure                   | Moyen      | ets_insee_inpi_status_cas                                                                                                                                              | status_cas,intersection,pct_intersection,union_,inpi_except,insee_except           | CAS_1,CAS_2,CAS_3,CAS_4,CAS_5 |
| 2    | test_list_num_voie                        | intersection_numero_voie,union_numero_voie    | 03_test_list_num_voie              | Moyen      | ets_insee_inpi_list_num_voie                                                                                                                                           | intersection_numero_voie,union_numero_voie                                         | FALSE,NULL,TRUE,PARTIAL       |
| 3    | test_enseigne                             | list_enseigne,enseigne                        | 04_test_enseigne                   | Moyen      | ets_insee_inpi_list_enseigne                                                                                                                                           | list_enseigne_contain                                                              | FALSE,NULL,TRUE               |
| 4    | test_pct_intersection                     | pct_intersection,index_id_max_intersection    | 06_creation_nb_siret_siren_max_pct | Facile     | ets_insee_inpi_var_group_max                                                                                                                                           | count_inpi_index_id_siret,count_inpi_siren_siret,index_id_max_intersection         | FALSE,TRUE                    |
| 4    | test_index_id_duplicate                   | count_inpi_index_id_siret                     | 06_creation_nb_siret_siren_max_pct | Facile     | ets_insee_inpi_var_group_max                                                                                                                                           | count_inpi_index_id_siret,count_inpi_siren_siret,index_id_max_intersection         | FALSE,TRUE                    |
| 4    | test_siren_insee_siren_inpi               | count_initial_insee,count_inpi_siren_siret    | 06_creation_nb_siret_siren_max_pct | Facile     | ets_insee_inpi_var_group_max                                                                                                                                           | count_inpi_index_id_siret,count_inpi_siren_siret,index_id_max_intersection         | FALSE,TRUE                    |
| 5    | test_similarite_exception_words           | max_cosine_distance                           | 08_calcul_cosine_levhenstein       | Difficile  | ets_insee_inpi_similarite_max_word2vec                                                                                                                                 | unzip_inpi,unzip_insee,max_cosine_distance,levenshtein_distance,key_except_to_test | FALSE,NULL,TRUE               |
| 5    | test_distance_levhenstein_exception_words | levenshtein_distance                          | 08_calcul_cosine_levhenstein       | Difficile  | ets_insee_inpi_similarite_max_word2vec                                                                                                                                 | unzip_inpi,unzip_insee,max_cosine_distance,levenshtein_distance,key_except_to_test | FALSE,NULL,TRUE               |
| 6    | test_date                                 | datecreationetablissement,date_debut_activite | 10_match_et_creation_regles.md     | Facile     | ets_insee_inpi_list_num_voie,ets_insee_inpi_list_enseigne,ets_insee_inpi_similarite_max_word2vec,ets_insee_inpi_status_cas,ets_insee_inpi_var_group_max,ets_insee_inpi |                                                                                    | FALSE,TRUE                    |
| 6    | test_siege                                | status_ets,etablissementsiege                 | 10_match_et_creation_regles.md     | Facile     | ets_insee_inpi_list_num_voie,ets_insee_inpi_list_enseigne,ets_insee_inpi_similarite_max_word2vec,ets_insee_inpi_status_cas,ets_insee_inpi_var_group_max,ets_insee_inpi |                                                                                    | FALSE,TRUE,NULL               |
| 6    | test_status_admin                         | etatadministratifetablissement,status_admin   | 10_match_et_creation_regles.md     | Facile     | ets_insee_inpi_list_num_voie,ets_insee_inpi_list_enseigne,ets_insee_inpi_similarite_max_word2vec,ets_insee_inpi_status_cas,ets_insee_inpi_var_group_max,ets_insee_inpi |                                                                                    | FALSE,NULL,TRUE               |



Pour facilité la comprehension de la création des variables de tests, nous allons créer des tables intermédiaires. Pour chaque table, nous allons créer un notebook avec le détail des queries et une analyse de la table. Le graphique ci dessous contient le nom des notebooks ainsi que les champs créées. 

Dans ce notebook, nous allons générer une table avec les caractéristiques qui relient l'adresse de l'INSEE et de l'INPI. La particularité de ce notebook est la creation de la variable `status_cas`. La variable `status_cas`  indique le cas de figure détecté entre l'adresse de l'INSEE et l'INPI. Il y a 5 possibilités au total:

*   CAS_1: Les mots dans l’adresse de l’INPI sont égales aux mots dans l’adresse de l’INSEE
*   CAS_2: Aucun des mots de l’adresse de l’INPI sont égales aux mots dans l’adresse de l’INSEE
*   CAS_3: Cardinalite exception parfaite mots INPI ou INSEE
*   CAS_4: Cardinalite Exception mots INPI diffférente Cardinalite Exception mots INSEE
*   CAS_5: Cadrinalite exception insee est égal de 0 ou cardinalite exception inpi est égal de 0
*   CAS_6: CAS_NO_ADRESSE
    
La création de cette variable repose sur des variables intermédiaire:

* `insee_except`: Liste de mots provenant de l'INSEE non contenue dans l'INPI
* `inpi_except`: Liste de mots provenant de l'INPI non contenue dans l'INSEE
* `intersection`: Nombre de mots en commun
* `union_`: Nombre de mots total entre les deux adresses
* `pct_intersection`: `intersection` / `union_`


![](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/-/raw/master/IMAGES/10_schema_siretisation.png)


## Similarité adresse 

Le rapprochement entre les deux tables, à savoir l’INSEE et l’INPI, va amener à la création de deux vecteurs d’adresse. Un vecteur avec des mots contenus spécifiquement à l’INSEE, et un second vecteur avec les mots de l’adresse de l’INPI. Notre objectif est de comparé ses deux vecteurs pour définir si ils sont identiques ou non. Nous avons distingué 7 cas de figures possibles entre les deux vecteurs (figure ci dessous).

![](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/-/raw/master/IMAGES/11_cas_de_figure.jpeg)

### Définition

La comparaison de l'adresse de l'INSEE et de l'INPI repose sur les concepts d'union et d'intersection (figure ci dessous)

![](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/-/raw/master/IMAGES/12_union_intersection.png)



- Union: $|INSEE \cup INPI| = |INSEE|+|INPI|-|INSEE \cap INPI|$
    * Exemple:
        - $INSEE = [A, B, C, D, E]$
        - $INPI = [A, B, F, G]$
        - $Union = [A, B, C, D, E, F, G]$
- Intersection: $|INSEE \cap INPI|$
    - Exemple:
        - $INSEE = [A, B, C, D, E]$
        - $INPI = [A, B, F, G]$
        - $Intersect = [A, B]$
* Cardinality: taille d’un array
  * Exemple:
      - $INSEE = [A, B, C, D, E] = 5$
      - $INPI = [A, B, F, G] = 4$
      
### Detail cas de figure

Dans cette partie, nous allons détailler l'ensemble des cas de figures. Toutefois, dans la partie finale nous allons regrouper les cas 5 et 6 ensemble.

#### Cas de figure 1: similarité parfaite

* Definition: Les mots dans l’adresse de l’INPI sont égales aux mots dans l’adresse de l’INSEE
- Math definition: $\frac{|INSEE \cap INPI|}{|INSEE|+|INPI|-|INSEE \cap INPI|} =1$
- Règle: $\text{intersection} = \text{union} \rightarrow \text{cas 1}$

| list_inpi              | list_insee             | insee_except | intersection | union_ |
|------------------------|------------------------|--------------|--------------|--------|
| [BOULEVARD, HAUSSMANN] | [BOULEVARD, HAUSSMANN] | []           | 2            | 2      |
| [QUAI, GABUT]          | [QUAI, GABUT]          | []           | 2            | 2      |
| [BOULEVARD, VOLTAIRE]  | [BOULEVARD, VOLTAIRE]  | []           | 2            | 2      |

#### Cas de figure 2: Dissimilarité parfaite

* Definition: Aucun des mots de l’adresse de l’INPI sont égales aux mots dans l’adresse de l’INSEE
- Math definition: $\frac{|INSEE \cap INPI|}{|INSEE|+|INPI|-|INSEE \cap INPI|}$
- Règle: $\text{intersection} = 0 \rightarrow \text{cas 2}$

| list_inpi                               | list_insee                              | insee_except                            | intersection | union_ |
|-----------------------------------------|-----------------------------------------|-----------------------------------------|--------------|--------|
| [CHEMIN, MOUCHE]                        | [AVENUE, CHARLES, GAULLE, SAINT, GENIS] | [AVENUE, CHARLES, GAULLE, SAINT, GENIS] | 0            | 7      |
| [AVENUE, CHARLES, GAULLE, SAINT, GENIS] | [CHEMIN, MOUCHE]                        | [CHEMIN, MOUCHE]                        | 0            | 7      |

#### Cas de figure 3: Intersection parfaite INPI

* Definition: Tous les mots dans l’adresse de l’INPI  sont contenus dans l’adresse de l’INSEE
- Math definition: $\frac{|INPI|}{|INSEE \cap INPI|}  \text{  = 1 and }|INSEE \cap INPI| <> |INSEE \cup INPI|$
- Règle: $|\text{list_inpi}|= \text{intersection}  \text{  = 1 and }\text{intersection} \neq  \text{union} \rightarrow \text{cas 3}$

| list_inpi                    | list_insee                                               | insee_except                            | intersection | union_ |
|------------------------------|----------------------------------------------------------|-----------------------------------------|--------------|--------|
| [ALLEE, BERLIOZ]             | [ALLEE, BERLIOZ, CHEZ, MME, IDALI]                       | [CHEZ, MME, IDALI]                      | 2            | 5      |
| [RUE, MAI, OLONNE, SUR, MER] | [RUE, HUIT, MAI, OLONNE, SUR, MER]                       | [HUIT]                                  | 5            | 6      |
| [RUE, CAMILLE, CLAUDEL]      | [RUE, CAMILLE, CLAUDEL, VITRE]                           | [VITRE]                                 | 3            | 4      |
| [ROUTE, D, ESLETTES]         | [ROUTE, D, ESLETTES, A]                                  | [A]                                     | 3            | 4      |
| [AVENUE, MAI]                | [AVENUE, HUIT, MAI]                                      | [HUIT]                                  | 2            | 3      |
| [RUE, SOUS, DINE]            | [RUE, SOUS, DINE, RES, SOCIALE, HENRIETTE, D, ANGEVILLE] | [RES, SOCIALE, HENRIETTE, D, ANGEVILLE] | 3            | 8      |

#### Cas de figure 4: Intersection parfaite INSEE

* Definition: Tous les mots dans l’adresse de l’INSEE  sont contenus dans l’adresse de l’INPI
- Math definition: $\frac{|INSEE|}{|INSEE \cap INPI|}  \text{  = 1 and }|INSEE \cap INPI| <> |INSEE \cup INPI|$
- Règle: $|\text{list_insee}|= \text{intersection}  \text{  = 1 and }\text{intersection} \neq  \text{union} \rightarrow \text{cas 4}$

| list_inpi                                                 | list_insee                                      | insee_except | intersection | union_ |
|-----------------------------------------------------------|-------------------------------------------------|--------------|--------------|--------|
| [ROUTE, D, ENGHIEN]                                       | [ROUTE, ENGHIEN]                                | []           | 2            | 3      |
| [ZAC, PARC, D, ACTIVITE, PARIS, EST, ALLEE, LECH, WALESA] | [ALLEE, LECH, WALESA, ZAC, PARC, ACTIVITE, EST] | []           | 7            | 9      |
| [LIEU, DIT, PADER, QUARTIER, RIBERE]                      | [LIEU, DIT, RIBERE]                             | []           | 3            | 5      |
| [A, BOULEVARD, CONSTANTIN, DESCAT]                        | [BOULEVARD, CONSTANTIN, DESCAT]                 | []           | 3            | 4      |
| [RUE, MENILMONTANT, BP]                                   | [RUE, MENILMONTANT]                             | []           | 2            | 3      |

#### Cas de figure 5: Cardinality exception parfaite INSEE INPI, intersection positive 

* Definition: L’adresse de l’INPI contient des mots de l’adresse de l’INPI et la cardinality des mots non présents dans les deux adresses est équivalente
- Math definition: $|INPI|-|INPI \cap INSEE| = |INSEE|-|INPI \cap INSEE|$
- Règle: $|\text{insee_except}| = |\text{inpi_except}| \text{ and } \text{intersection} > 0 \rightarrow \text{cas 5}$

| list_inpi                                                                                  | list_insee                                                                              | insee_except | inpi_except  | intersection | union_ |
|--------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------|--------------|--------------|--------------|--------|
| [AVENUE, GEORGES, VACHER, C, A, SAINTE, VICTOIRE, IMMEUBLE, CCE, CD, ZI, ROUSSET, PEYNIER] | [AVENUE, GEORGES, VACHER, C, A, STE, VICTOIRE, IMMEUBLE, CCE, CD, ZI, ROUSSET, PEYNIER] | [STE]        | [SAINTE]     | 12           | 14     |
| [BIS, AVENUE, PAUL, DOUMER, RES, SAINT, MARTIN, BAT, D, C, O, M, ROSSI]                    | [BIS, AVENUE, PAUL, DOUMER, RES, ST, MARTIN, BAT, D, C, O, M, ROSSI]                    | [ST]         | [SAINT]      | 12           | 14     |
| [ROUTE, DEPARTEMENTALE, CHEZ, SOREME, CENTRE, COMMERCIAL, L, OCCITAN, PLAN, OCCIDENTAL]    | [ROUTE, DEPARTEMENTALE, CHEZ, SOREME, CENTRE, COMMERCIAL, L, OCCITAN, PLAN, OC]         | [OC]         | [OCCIDENTAL] | 9            | 11     |
| [LIEU, DIT, FOND, CHAMP, MALTON, PARC, EOLIEN, SUD, MARNE, PDL]                            | [LIEU, DIT, FONDD, CHAMP, MALTON, PARC, EOLIEN, SUD, MARNE, PDL]                        | [FONDD]      | [FOND]       | 9            | 11     |
| [AVENUE, ROBERT, BRUN, ZI, CAMP, LAURENT, LOT, NUMERO, ST, BERNARD]                        | [AVENUE, ROBERT, BRUN, ZI, CAMP, LAURENT, LOT, ST, BERNARD, N]                          | [N]          | [NUMERO]     | 9            | 11     |
| [PLACE, MARCEL, DASSAULT, PARC, D, ACTIVITES, TY, NEHUE, BATIMENT, H]                      | [PLACE, MARCEL, DASSAULT, PARC, D, ACTIVITES, TY, NEHUE, BAT, H]                        | [BAT]        | [BATIMENT]   | 9            | 11     |

#### Cas de figure 6: Cardinality exception INSEE supérieure INPI, intersection positive

* Definition: L’adresse de l’INPI contient des mots de l’adresse de l’INPI et la cardinality des mots non présents dans l’adresse de l’INSEE est supérieure à la cardinality de l’adresse de l’INPI
- Math definition: $|INPI|-|INPI \cap INSEE| < |INSEE|-|INPI \cap INSEE|$
- Règle: $|\text{insee_except}| > |\text{inpi_except}| \text{ and } \text{intersection} > 0 \rightarrow \text{cas 6}$

| list_inpi                                                                         | list_insee                                                                               | insee_except          | inpi_except   | intersection | union_ |
|-----------------------------------------------------------------------------------|------------------------------------------------------------------------------------------|-----------------------|---------------|--------------|--------|
| [AVENUE, AUGUSTE, PICARD, POP, UP, TOURVILLE, CC, EMPLACEMENT, DIT, PRECAIRE, N]  | [AVENUE, AUGUSTE, PICARD, POP, UP, TOURVILL, CC, TOURVILLE, EMPLACEMT, DIT, PRECAIRE, N] | [TOURVILL, EMPLACEMT] | [EMPLACEMENT] | 10           | 13     |
| [ROUTE, COTE, D, AZUR, C, O, TENERGIE, ARTEPARC, MEYREUIL, BAT, A]                | [ROUTE, C, O, TENERGIE, ARTEPARC, MEYREUI, BAT, A, RTE, COTE, D, AZUR]                   | [MEYREUI, RTE]        | [MEYREUIL]    | 10           | 13     |
| [C, O, TENERGIE, ARTEPARC, MEYREUIL, BATIMENT, A, ROUTE, COTE, D, AZUR]           | [ROUTE, C, O, TENERGIE, ARTEPARC, MEYREUI, BATIMENT, A, RTE, COTE, D, AZUR]              | [MEYREUI, RTE]        | [MEYREUIL]    | 10           | 13     |
| [LOTISSEMENT, VANGA, DI, L, ORU, VILLA, FRANCK, TINA, CHEZ, COLOMBANI, CHRISTIAN] | [LIEU, DIT, VANGA, DI, L, ORU, VILLA, FRANCK, TINA, CHEZ, COLOMBANI, CHRISTIAN]          | [LIEU, DIT]           | [LOTISSEMENT] | 10           | 13     |
| [AVENUE, DECLARATION, DROITS, HOMME, RES, CLOS, ST, MAMET, BAT, C, APPT]          | [AVENUE, DECL, DROITS, L, HOMME, RES, CLOS, ST, MAMET, BAT, C, APPT]                     | [DECL, L]             | [DECLARATION] | 10           | 13     |

#### Cas de figure 7: Cardinality exception INPI supérieure INSEE, intersection positive

* Definition: L’adresse de l’INSEE contient des mots de l’adresse de l’INPI et la cardinality des mots non présents dans l’adresse de l’INPI est supérieure à la cardinality de l’adresse de l’INSEE
- Math definition: $|INPI|-|INPI \cap INSEE| > |INSEE|-|INPI \cap INSEE|$
- Règle: $|\text{insee_except}| < |\text{inpi_except} \text{ and } \text{intersection} > 0 \rightarrow \text{cas 7}$

| list_inpi                                                                                    | list_insee                                                                   | insee_except | inpi_except                 | intersection | union_ |
|----------------------------------------------------------------------------------------------|------------------------------------------------------------------------------|--------------|-----------------------------|--------------|--------|
| [RTE, CABRIERES, D, AIGUES, CHEZ, MR, DOL, JEAN, CLAUDE, LIEUDIT, PLAN, PLUS, LOIN]          | [ROUTE, CABRIERES, D, AIGUES, CHEZ, MR, DOL, JEAN, CLAUDE, PLAN, PLUS, LOIN] | [ROUTE]      | [RTE, LIEUDIT]              | 11           | 14     |
| [ROUTE, N, ZAC, PONT, RAYONS, CC, GRAND, VAL, ILOT, B, BAT, A, LOCAL]                        | [ZONE, ZAC, PONT, RAYONS, CC, GRAND, VAL, ILOT, B, BAT, A, LOCAL]            | [ZONE]       | [ROUTE, N]                  | 11           | 14     |
| [BOULEVARD, PAUL, VALERY, BAT, B, ESC, H, APPT, C, O, MADAME, BLANDINE, BOVE]                | [BOULEVARD, PAUL, VALERY, BAT, B, ESC, H, APT, C, O, BOVE, BLANDINE]         | [APT]        | [APPT, MADAME]              | 11           | 14     |
| [RUE, JEANNE, D, ARC, A, L, ANGLE, N, ROLLON, EME, ETAGE, POLE, PRO, AGRI]                   | [RUE, JEANNE, D, ARC, A, L, ANGLE, N, ROLLON, E, ETAGE]                      | [E]          | [EME, POLE, PRO, AGRI]      | 10           | 15     |
| [CHEZ, MR, MME, DANIEL, DEZEMPTE, AVENUE, BALCONS, FRONT, MER, L, OISEAU, BLEU, BATIMENT, B] | [AVENUE, BALCONS, FRONT, MER, CHEZ, MR, MME, DANIEL, DEZEMPTE, L, OISEA]     | [OISEA]      | [OISEAU, BLEU, BATIMENT, B] | 10           | 15     |


## Connexion serveur

```python
from awsPy.aws_authorization import aws_connector
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

# Input/output

```python
s3_output = 'SQL_OUTPUT_ATHENA'
database = 'ets_siretisation'
```

```python
query = """
DROP TABLE ets_siretisation.ets_insee_inpi_statut_cas;
"""
s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = None, ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

```python
create_table = """
CREATE TABLE ets_siretisation.ets_insee_inpi_statut_cas
WITH (
  format='PARQUET'
) AS
WITH create_var AS (
  SELECT 
    row_id,
    siret, 
    adresse_distance_insee, 
    adresse_distance_inpi, 
    array_distinct(
      array_except(
        split(adresse_distance_insee, ' '), 
        split(adresse_distance_inpi, ' ')
      )
    ) as insee_except, 
    array_distinct(
      array_except(
        split(adresse_distance_inpi, ' '), 
        split(adresse_distance_insee, ' ')
      )
    ) as inpi_except, 
    CAST(
      cardinality(
        array_distinct(
          array_intersect(
            split(adresse_distance_inpi, ' '), 
            split(adresse_distance_insee, ' ')
          )
        )
      ) AS DECIMAL(10, 2)
    ) as intersection, 
    CAST(
      cardinality(
        array_distinct(
          array_union(
            split(adresse_distance_inpi, ' '), 
            split(adresse_distance_insee, ' ')
          )
        )
      ) AS DECIMAL(10, 2)
    ) as union_, 
    CAST(
      cardinality(
        array_distinct(
          array_intersect(
            split(adresse_distance_inpi, ' '), 
            split(adresse_distance_insee, ' ')
          )
        )
      ) AS DECIMAL(10, 2)
    )/ CAST(
      cardinality(
        array_distinct(
          array_union(
            split(adresse_distance_inpi, ' '), 
            split(adresse_distance_insee, ' ')
          )
        )
      ) AS DECIMAL(10, 2)
    ) as pct_intersection 
  FROM 
    ets_siretisation.ets_insee_inpi
) 

SELECT 
  * 
FROM 
  (
    WITH test AS (
      SELECT 
      row_id,
        siret, 
        adresse_distance_insee, 
        adresse_distance_inpi, 
        CASE WHEN cardinality(insee_except) = 0 THEN NULL ELSE insee_except END as insee_except,
        CASE WHEN cardinality(inpi_except) = 0 THEN NULL ELSE inpi_except END as inpi_except,
        intersection, 
        union_, 
        intersection / union_ as pct_intersection, 
        CASE WHEN intersection = union_ THEN 'CAS_1' WHEN intersection = 0 THEN 'CAS_2' WHEN CARDINALITY(insee_except) = CARDINALITY(inpi_except) 
        AND intersection != union_ 
        AND intersection != union_ 
        AND intersection != 0 THEN 'CAS_3' WHEN (
          CARDINALITY(insee_except) = 0 
          OR CARDINALITY(inpi_except) = 0
        ) 
        AND intersection != union_ 
        AND intersection != 0 THEN 'CAS_5' WHEN CARDINALITY(insee_except) != CARDINALITY(inpi_except) 
        AND intersection != union_ 
        AND intersection != union_ 
        AND intersection != 0 THEN 'CAS_4' ELSE 'CAS_NO_ADRESSE' END AS status_cas 
      FROM 
        create_var 
    )
    SELECT *
    FROM test
    )
"""
s3.run_query(
            query=create_table,
            database=database,
            s3_output=s3_output,
  filename = None, ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

```python
query = """
SELECT *
       FROM (SELECT * 
             FROm ets_siretisation.ets_insee_inpi_statut_cas
       WHERE status_cas = 'CAS_1'
       LIMIT 1
             )
       UNION (SELECT *
       FROM siretisation.ets_insee_inpi_status_cas
       WHERE status_cas = 'CAS_2'
              LIMIT 1
              )
       UNION (SELECT *
       FROM siretisation.ets_insee_inpi_status_cas
       WHERE status_cas = 'CAS_3'
              LIMIT 1
              )
       UNION (SELECT *
       FROM siretisation.ets_insee_inpi_status_cas
       WHERE status_cas = 'CAS_4'
              LIMIT 1
              )
       UNION (SELECT *
       FROM siretisation.ets_insee_inpi_status_cas
       WHERE status_cas = 'CAS_5'
              LIMIT 1
              )
       ORDER BY status_cas
       

"""

tb = s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = 'tb_exemple', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )

tb = pd.concat([

pd.concat([
tb[['siret', 'adresse_distance_insee', 'adresse_distance_inpi']]
],keys=["Input"], axis = 1),
pd.concat([
tb[['insee_except', 'inpi_except', 'intersection', 'union_', 'pct_intersection','status_cas']]
],keys=["Output"], axis = 1)
], axis = 1
)

tb
```

# Analyse

1. Vérifier que le nombre de lignes est indentique avant et après la création des variables
2. Compter le nombre de lignes par cas
3. Compter le nombre d'index par cas
4. Créer un tableau avec une ligne par cas


## 1. Vérifier que le nombre de lignes est indentique avant et après la création des variables

```python
query = """
SELECT COUNT(*) as CNT
FROM ets_siretisation.ets_insee_inpi
"""
s3.run_query(
            query=query,
            database='ets_siretisation',
            s3_output=s3_output,
  filename = 'count_ets_insee_inpi', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

```python
query = """
SELECT count(*) as CNT
       FROM ets_insee_inpi_statut_cas
 """
s3.run_query(
            query=query,
            database='ets_siretisation',
            s3_output=s3_output,
  filename = 'count_ets_insee_inpi_status_cas', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

## 2. Compter le nombre de lignes par cas

```python
query = """
SELECT status_cas, count(*) as CNT
       FROM ets_insee_inpi_statut_cas
       GROUP BY status_cas
       ORDER BY status_cas
       """
s3.run_query(
            query=query,
            database='ets_siretisation',
            s3_output=s3_output,
  filename = 'count_group_ets_insee_inpi_status_cas', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

## 4. Créer un tableau avec une ligne par cas

```python
query = """
       SELECT *
       FROM (SELECT * 
             FROM ets_insee_inpi_statut_cas
       WHERE status_cas = 'CAS_1'
       LIMIT 1
             )
       UNION (SELECT *
       FROM ets_insee_inpi_statut_cas
       WHERE status_cas = 'CAS_2'
              LIMIT 1
              )
       UNION (SELECT *
       FROM ets_insee_inpi_statut_cas
       WHERE status_cas = 'CAS_3'
              LIMIT 1
              )
       UNION (SELECT *
       FROM ets_insee_inpi_statut_cas
       WHERE status_cas = 'CAS_4'
              LIMIT 1
              )
       UNION (SELECT *
       FROM ets_insee_inpi_statut_cas
       WHERE status_cas = 'CAS_5'
              LIMIT 1
              )
       ORDER BY status_cas

"""

tb = s3.run_query(
            query=query,
            database='ets_siretisation',
            s3_output=s3_output,
  filename = 'tb_exemple', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
pd.concat([

pd.concat([
tb[['siret', 'adresse_distance_insee', 'adresse_distance_inpi']]
],keys=["Input"], axis = 1),
pd.concat([
tb[['insee_except', 'inpi_except', 'intersection', 'union_', 'pct_intersection','status_cas']]
],keys=["Output"], axis = 1)
], axis = 1
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
