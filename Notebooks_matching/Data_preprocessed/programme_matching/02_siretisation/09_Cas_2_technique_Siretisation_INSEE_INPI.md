---
jupyter:
  jupytext:
    formats: ipynb,md
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.2'
      jupytext_version: 1.4.2
  kernelspec:
    display_name: Python 3
    language: python
    name: python3
---

# Cas 2 Test nombre lignes siretise

Copy paste from Coda to fill the information

## Objective(s)

- Lors de [l’US 7: Test nombre lignes siretise avec nouvelles regles de gestion](https://coda.io/d/CreditAgricole_dCtnoqIftTn/US-07-ETS-version-3_su0VF), une batterie d’analyse a été réalisé ce qui a permit de mettre en évidence des règles de gestion pour discriminer les lignes qui ont des doublons, mais aussi ce qui n’en n’ont pas. Effectivement, certaines lignes sans doublons n’ ont pas nécessairement une relation (i.e. l’adresse n’est pas celle du siret). 

  - Nous avons pu distinguer 4 grandes catégories pour séparer les lignes. Le tableau ci dessous récapitule les possibilités

![00_ensemble_cas_siretisation.jpeg](https://codahosted.io/docs/CtnoqIftTn/blobs/bl-vmJJOeJ__6/455e18296e4076d4b2ec1dbcb5f1364af4b7824c8e96262f78095b67a3db4fd10422ee1adc6e06779452eeb0a47003a6f4a1e29995c559aa9531a8e6499db04a0e089b7e173ed187259fc4aa4a799935f8ead28ed9dfe555a307bdac2eb840e4991b963c)

  - Dans cette US, nous allons traiter du cas ou il y a des doublon, et la relation entre l’adresse INSEE et INPI appartient au cas de figure 1,3 ou 4. Pour filtrer la table avec les lignes qui satisfont ses deux critères, il faut utiliser les variables:

   - index_id_duplicate
   - test_adresse_cas_1_3_4

   * Les variables à utiliser pour les deux sont:
    * test_siege
    * test_enseigne
    * test_sequence_siret
  * Des que les lignes sont écartées, il faut réaliser un document json avec les informations sur la siretisation, qui indique les informations suivantes:
    *  Nom csv : LOGS_CAS_2.json
    * IN S3: RESULTATS_SIRETISATION/LOGS_ANALYSE
    
```    
dic_ = {
    
    'CAS': 'CAS_2',
    'count_all_index':total_index.values[0][0],
    'count_filtered_in': total_index_filtre.values[0][0],
    'count_filtered_out':total_index_found.values[0][0],
    'percentage_found': total_index_found.values[0][0]/total_index.values[0][0]
}
```

  * Un csv avec le siret par index sera créé. Le CSV doit contenir les informations suivantes:
    *  Nom csv : RESULTAT_CAS_2.csv
    * IN S3: RESULTATS_SIRETISATION/TABLES_SIRET
      * index_id, 
      * sequence_id, 
      * siren, 
      * siret
      * test_sequence_siret,
      * test_index_siret, 
      *  test_list_num_voie, 
      *  test_date, 
      *  test_status_admin,
      *  test_siege
      *  test_code_commune
      *  test_adresse_cas_1_3_4, 
      *  test_duplicates_is_in_cas_1_3_4
      *  test_enseigne  

  - Il faut aussi filtrer les lignes qui sont à éjecter du test test_duplicates_is_in_cas_1_3_4 a savoir les TO_REMOVE 

  - Include DataStudio (tick if yes): false

## Metadata 

- Metadata parameters are available here: [Ressources_suDYJ#_luZqd](http://Ressources_suDYJ#_luZqd)

- Task type:
    - Jupyter Notebook
- Users: :
    - [Thomas Pernet](mailto:t.pernetcoudrier@gmail.com)
- Watchers:

  - [Thomas Pernet](mailto:t.pernetcoudrier@gmail.com)
- Estimated Log points:
 - One being a simple task, 15 a very difficult one
    -  7
- Task tag
    -  \#sql-query,#matching,#siretisation,#regle-de-gestion,#cas-1
- Toggl Tag
   - \#data-analysis 
  
## Input Cloud Storage [AWS/GCP]

If link from the internet, save it to the cloud first

### Tables [AWS/BigQuery]

- Batch 1:

    - Select Provider: Athena

    - Select table(s): ets_inpi_insee_cases_filtered

      - Select only tables created from the same notebook, else copy/paste selection to add new input tables

    - Information:

        - Region: 

          - NameEurope (Paris)
          - Code: eu-west-3

        - Database: inpi

          - Notebook construction file: [08_Cas_1_technique_Siretisation_INSEE_INPI](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/02_siretisation/08_Cas_1_technique_Siretisation_INSEE_INPI.md)
    
## Destination Output/Delivery

- AWS

  - Athena: 

      - Region: Europe (Paris)
      - Database: inpi
      - Tables (Add name new table): ets_inpi_insee_cases_filtered
      - List new tables
      - ets_inpi_insee_cases_filtered
- S3(Add new filename to Database: [Ressources](https://coda.io/d/CreditAgricole_dCtnoqIftTn/Ressources_suDYJ))

    - Origin: Jupyter notebook

    - Bucket: calfdata

    - Key: RESULTATS_SIRETISATION/LOGS_ANALYSE

    - Filename(s): LOGS_CAS_2.json,RESULTAT_CAS_2.csv
        - [RESULTATS_SIRETISATION/TABLES_SIRET/RESULTAT_CAS_2.csv](https://s3.console.aws.amazon.com/s3/buckets/calfdata/RESULTATS_SIRETISATION/TABLES_SIRET)
        - [RESULTATS_SIRETISATION/LOGS_ANALYSE/LOGS_CAS_2.json](https://s3.console.aws.amazon.com/s3/buckets/calfdata/RESULTATS_SIRETISATION/LOGS_ANALYSE)




## Things to know (Steps, Attention points or new flow of information)

### Sources of information  (meeting notes, Documentation, Query, URL)



## Connexion serveur

```python
from awsPy.aws_authorization import aws_connector
from awsPy.aws_athena import service_athena
from awsPy.aws_s3 import service_s3
from pathlib import Path
import pandas as pd
import numpy as np
import os, shutil
bucket = 'calfdata'
path = os.getcwd()
parent_path = str(Path(path).parent)
path_cred = r"{}/credential_AWS.json".format(parent_path)

region = 'eu-west-3'
bucket = 'calfdata'
s3_output = 'INPI/sql_output'
```

```python
con = aws_connector.aws_instantiate(credential = path_cred,
                                       region = region)
client= con.client_boto()
s3 = service_s3.connect_S3(client = client,
                      bucket = bucket, verbose = False) 
#athena = service_athena.connect_athena(client = client,
#                      bucket = bucket) 
```

```python
import seaborn as sns

cm = sns.light_palette("green", as_cmap=True)
pd.set_option('display.max_columns', None)
```

# Creation tables

La première étape consiste à créer la table `ets_inpi_insee_cases_filtered` qui filtre la table `ets_inpi_insee_cases` des lignes lorsque la variable `test_duplicates_is_in_cas_1_3_4` est différent de `'TO_REMOVE'`

## Steps

- Creation de la table  `ets_inpi_insee_cases_filtered`
- Compter le nombre d'observations
- Brève analyse des résultats des tests
- Séparation des lignes


### Count ligne a trouver


Le nombre d'index a trouver sur l'ensemble de la base est de:

```python
query = """
SELECT COUNT(*) AS total_index
FROM ets_inpi_insee_cases_filtered 
"""
total_index = s3.run_query(
            query=query,
            database='inpi',
            s3_output=s3_output,
    filename = 'count_ets_inpi_insee_cases_to_find',
    destination_key = None
        )
total_index
```

Le nombre d'observations est de:

```python
query = """
SELECT COUNT(*) AS count_rows
FROM ets_inpi_insee_cases_filtered 
WHERE index_id_duplicate = 'True' AND test_adresse_cas_1_3_4 ='True'
"""
total_index_filtre = s3.run_query(
            query=query,
            database='inpi',
            s3_output=s3_output,
    filename = 'cas_1_count_ets_inpi_insee_cases_filtered',
    destination_key = None
        )
total_index_filtre
```

### Brève analyse des résultats des tests

```python
#top = """
#WITH test AS (
#  SELECT test_list_num_voie  AS groups, COUNT(*) as cnt_test_list_num_voie
#FROM ets_inpi_insee_cases_filtered 
#WHERE index_id_duplicate = 'False' AND test_adresse_cas_1_3_4 ='True'
#GROUP BY test_list_num_voie
#  )
#"""

top_1 = "SELECT groups_true_false_null.groups, "
top_2 = ""
middle_2 = " FROM groups_true_false_null "

bottom_1 = ""
bottom_2 = """

-- {0}

LEFT JOIN (
    SELECT {0}  AS groups, COUNT(*) as cnt_{0}
    FROM ets_inpi_insee_cases_filtered 
    WHERE index_id_duplicate = 'True' AND test_adresse_cas_1_3_4 ='True'
    GROUP BY {0}
    ) AS tb_{0}
  ON groups_true_false_null.groups = tb_{0}.groups
"""

tests = [
    "test_sequence_siret",
    "test_index_siret",
    "test_siren_insee_siren_inpi",
    "test_sequence_siret_many_cas",
    'test_list_num_voie',
    "test_siege",
    "test_date",
    "test_status_admin",
    "test_code_commune",
    "test_type_voie",
    "test_enseigne"] 

for i, test in enumerate(tests):
    var = 'cnt_{}'.format(test)
    if i == len(tests) -1:
        top_2 += '{}'.format(var)
    else:
        top_2+='{},'.format(var)
        
    bottom_1+= bottom_2.format(test)  

query = top_1 + top_2 + middle_2 + bottom_1
tb = s3.run_query(
            query=query,
            database='inpi',
            s3_output=s3_output,
    filename = 'cas_2_results_tests',
    destination_key = 'ANALYSE_POST_SIRETISATION'
        )
```

- `test_sequence_siret`: Pertinence faible
    - `count_inpi_sequence_siret = 1 THEN ‘True’ ELSE ‘False``
    - Si la variable est ‘True’ alors, il n’y a pas de duplicate pour une séquence
- `test_index_siret`: Pertinence faible
    - `count_inpi_index_id_stat_cas_siret = 1 THEN 'True' ELSE 'False'`
    - Si la variable est true, alors, il n’y a qu”un seul cas de figure par index 
- `test_siren_insee_siren_inpi`: Pertinence elevée
    - `count_initial_insee = count_inpi_siren_siret THEN 'True' ELSE 'False'`
    - Si la variable est ‘True’ alors tous les établissements ont été trouvé
- `test_sequence_siret_many_cas`: Pertinence faible
    - `count_inpi_sequence_siret = count_inpi_sequence_stat_cas_siret THEN 'True' ELSE 'False`
    - test si la séquence appartient a plusieurs cas
- `test_date`: Pertinence moyenne
    - `WHEN datecreationetablissement = date_debut_activite THEN 'True' WHEN datecreationetablissement IS NULL 
        OR date_debut_activite IS NULL THEN 'NULL' --WHEN datecreationetablissement = '' 
        ELSE 'False'``
    - Test si la date de création de l'établissement est égale à la date de création. 
- `test_status_admin`: Pertinence moyenne
    - `WHEN etatadministratifetablissement = status_admin THEN 'True' WHEN etatadministratifetablissement IS NULL 
        OR status_admin IS NULL THEN 'NULL' WHEN etatadministratifetablissement = '' 
        OR status_admin = '' THEN 'NULL' ELSE 'False'``
    - Test si l'établissement est fermé ou non. Pas radié mais fermé. L'INSEE n'indique pas les radiations, et le fichier ETS de l'INPI n'indique pas les radiations et n'indique pas les fermetures resultants de radiation. Pour cela il faut construire la variable via la table PM ou PP.
- `test_siege`: Pertinence elevée
    - `etablissementsiege = status_ets THEN 'True' WHEN etablissementsiege IS NULL 
        OR status_ets IS NULL THEN 'NULL' WHEN etablissementsiege = '' 
        OR status_ets = '' THEN 'NULL' ELSE 'False'``
    - Test si le siret est un siège ou non. 
- `test_code_commune`: Pertinence faible
    - `codecommuneetablissement = code_commune THEN 'True' WHEN codecommuneetablissement IS NULL 
        OR code_commune IS NULL THEN 'NULL' WHEN codecommuneetablissement = '' 
        OR code_commune = '' THEN 'NULL' ELSE 'False'``
    - Test si le code commune est identique entre l'INPI et l'INSEE. Pas suffisament fiable
- `test_type_voie`: Pertinence faible
    - `numerovoieetablissement = numero_voie_matching THEN 'True' WHEN numerovoieetablissement IS NULL 
        OR numero_voie_matching IS NULL THEN 'NULL' WHEN numerovoieetablissement = '' 
        OR numero_voie_matching = '' THEN 'NULL' ELSE 'False'`
    - Test si le type de voie est identique entre les deux variables. Methode d'extraction que nous avons utilisé n'est pas suffisement pertinente
- `test_enseigne`: Pertinence moyenne
    - `WHEN cardinality(test) = 0 THEN 'NULL' WHEN enseigne = '' THEN 'NULL' WHEN temp_test_enseigne = TRUE THEN 'True' ELSE 'False'`
    - Test si l'enseigne est identique entre les variables. Aucun retraitement si ce n'est mise en majuscule et exclusion des accents. Ne regardepas les fautes d'orthographe

```python
(tb.assign(
    groups = lambda x: x['groups'].fillna('NULL'),
)
 .replace({'groups':{True:'True', False:'False'}})
 .set_index('groups').T
 .assign(total_row = lambda x : x.sum(axis = 1))
 #.columns
 .sort_values(by = ['True'], ascending = False)
 .style
 .format("{:,.0f}")
 .bar(subset= ['False'],color='#d65f5f')
 .bar(subset= ['True'],color='#008000')
)
```

Nous pouvons voir dans le tableau précédent qu'il y a 104,051 lignes qui ont plusieurs siret pour la même séquence. Une des explication possible est un déménagement de l'établissement sans fermeture coté greffe. En effet, un déménagement entraine automatiquement une nouvelle attribution de siret coté INSEE. Concernant l'INPI, le déménagement déclenche un événement de modification, et non une suppression. Dès lors, l'INSEE va indiquer la première adresse comme fermée alors que l'INPI va considérer les deux adresses comme étant ouvertes. 

Une autre explication peut être le changement de nom de la rue. 

Si dessous, deux exemples de cas de figure avec plusieurs adresses pour une même séquence.

```python
pd.set_option('display.max_columns', None)
```

```python
query = """ 
SELECT * 
FROM ets_final_sql  
WHERE sequence_id = 4721741
"""
s3.run_query(
            query=query,
            database='inpi',
            s3_output=s3_output,
    filename = 'exemple_changement_adresse_4721741',
    destination_key = None
        )
```

```python
query = """ 
SELECT * 
FROM ets_final_sql  
WHERE sequence_id = 2235206
"""
s3.run_query(
            query=query,
            database='inpi',
            s3_output=s3_output,
    filename = 'exemple_changement_adresse_2235206',
    destination_key = None
        )
```

Le tableau indique aussi 64,790 lignes avec des enseignes différentes. C'est le cas lorsque l'enseigne n'est pas écrit de manière identique coté INPI et coté INSEE. Par exemple, l'INPI indique `&` alors que l'INSEE écrit `ET`.  Une autre explication réside dans le changement d'enseigne. L'INSEE informe du dernier état connu de l'enseigne, alors que l'INPI fournit toutes les modifications. L'enseigne peut dès lors être différente pour les lignes historiques. Prenons l'exemple ci dessous, l'établissement a modifié son enseigne au cours du temps, induisant un échec du test, mais la variable `count_inpi_sequence_siret` est égale à 1, donc un seul siret possible

```python
query = """
SELECT index_id, sequence_id, siren, siret,test_sequence_siret, count_inpi_sequence_siret, enseigne, enseigne1etablissement, enseigne2etablissement, enseigne3etablissement, test_enseigne
FROM ets_inpi_insee_cases_filtered 
WHERE sequence_id = 9133786
"""
s3.run_query(
            query=query,
            database='inpi',
            s3_output=s3_output,
    filename = 'exemple_changement_enseigne_9133786',
    destination_key = None
        )
```

Ci dessous, nous vérifions que le résultat des tests en prenant en compte le test `test_sequence_siret` comme référence, a savoir si la séquence a plusieurs siret. Le test impacte 104,051 lignes, comme indiqué dans le tableau ci dessus

```python
query ="""
SELECT test_enseigne,test_sequence_siret, COUNT(test_enseigne) as cnt
FROM ets_inpi_insee_cases_filtered 
WHERE index_id_duplicate = 'True' AND test_adresse_cas_1_3_4 ='True' --AND test_sequence_siret = 'False'
GROUP BY test_sequence_siret, test_enseigne
"""
tb = s3.run_query(
            query=query,
            database='inpi',
            s3_output=s3_output,
    filename = 'cas_2_exemple_many_siret_sequence_test_enseigne',
    destination_key = 'ANALYSE_POST_SIRETISATION'
        )
```

Il y a 1894 lignes qui ont la même adresse, mais qui a été modifié au cours du temps, et en plus avec une modification de nom d'enseigne qui n'a pas donné lieu a une fermeture de l'établissement côté INPI.

```python
(tb
 .set_index(['test_enseigne', 'test_sequence_siret'])
 .unstack(-1)
 .assign(
     total_row = lambda x : x.sum(axis = 1),
     pct_false = lambda x: x[('cnt',False)]/x['total_row'],
     pct_true = lambda x: x[('cnt',True)]/x['total_row']
 )
 .style
 .format("{0:,.2%}", subset=["pct_false", "pct_true"])
)
```

Un autre test peut être de regarder le nombre de séquence avec plusieurs siret, et qui ont des divergences aevc le statut de siège entre l'INSEE et l'INPI.

```python
query ="""
SELECT test_siege,test_sequence_siret, COUNT(test_siege) as cnt
FROM ets_inpi_insee_cases_filtered 
WHERE index_id_duplicate = 'True' AND test_adresse_cas_1_3_4 ='True'
GROUP BY test_sequence_siret, test_siege
"""
tb = s3.run_query(
            query=query,
            database='inpi',
            s3_output=s3_output,
    filename = 'cas_2_exemple_many_siret_sequence_test_siege',
    destination_key = 'ANALYSE_POST_SIRETISATION'
        )
```

Lorsque le `test_siege` et `test_sequence_siret` sont tous les deux égals à `False` c'est que l'INPI n'a pas modifié correctement les informations. L'établissement a gardé la même séquence, mais a changé d'adresse. La modification n'a pas donné lieu a de modification du type d'établissement, a savoir siège, principal ou secondaire. Un changement d'adresse peut en même temps être accompagné d'un changement de type. L'INSEE peut par exemple, modifier l'adresse, créer un nouveau siret et modifier le type de siège a secondaire. L'INPI ne va pas faire tous ses changements. L'INPI va créer un événenement de modification de l'adresse, sans changement de type. 

```python
(tb
 .set_index(['test_siege', 'test_sequence_siret'])
 .unstack(-1)
 .assign(
     total_row = lambda x : x.sum(axis = 1),
     pct_false = lambda x: x[('cnt',False)]/x['total_row'],
     pct_true = lambda x: x[('cnt',True)]/x['total_row']
 )
 .style
 .format("{0:,.2%}", subset=["pct_false", "pct_true"])
)
```

C'est le cas pour le fichier ci dessous, la séquence est la même coté INPI, mais à l'INSEE le nouvel établissement est devenu secondaire, or il est resté siège à l'INPI

```python
query = """
SELECT row_id, index_id, sequence_id, siren, siret, count_inpi_siren_siret, test_sequence_siret, etablissementsiege, status_ets, test_siege
FROM ets_inpi_insee_cases_filtered 
WHERE sequence_id = 4736523
"""
s3.run_query(
            query=query,
            database='inpi',
            s3_output=s3_output,
    filename = 'cas_1_exemple_erreur_inpi_4736523',
    destination_key = None
        )
```

```python
query = """
SELECT index_id, sequence_id, siren, code_greffe, nom_greffe, numero_gestion, id_etablissement, origin, date_greffe, libelle_evt, type
FROM ets_final_sql  
WHERE sequence_id = 4736523
"""
s3.run_query(
            query=query,
            database='inpi',
            s3_output=s3_output,
    filename = 'cas_1_exemple_erreur_inpi_4736523',
    destination_key = None
        )
```

```python
query = """
SELECT siren, siret, datederniertraitementetablissement, etablissementsiege
FROM insee_rawdata_juillet
WHERE siren = '512619941'
"""
s3.run_query(
            query=query,
            database='inpi',
            s3_output=s3_output,
    filename = 'cas_1_exemple_erreur_inpi_4736523',
    destination_key = None
        )
```

# Export vers S3

La liste des séquences avec les siret est maintenant prête, nous pouvons l'exporter dans le S3 afin de reconstruire l'ensemble des siret de la base. 

A partir du moment ou nous connaissons le siret d'une séquence, il est possible de l'attribuer aux autres lignes. Toutefois, nous avons vu qu'une sequence peut avoir plusieurs siret, de fait, il faut être très attentif lors de la siretisation de nouvelles valeurs.

Dans ce fichier csv, nous allons garder les tests suivants:

- test_sequence_siret,
- test_index_siret, 
- test_list_num_voie, 
- test_date, 
- test_status_admin,
- test_siege
- test_code_commune
- test_adresse_cas_1_3_4, 
- test_duplicates_is_in_cas_1_3_4
- test_enseigne  

```python
query = """
SELECT index_id, sequence_id, siren, siret, 
test_sequence_siret, test_index_siret, test_list_num_voie, test_date, 
test_status_admin, test_siege, test_code_commune, test_adresse_cas_1_3_4, 
test_duplicates_is_in_cas_1_3_4, test_enseigne  
FROM ets_inpi_insee_cases_filtered 
WHERE index_id_duplicate = 'True' AND test_adresse_cas_1_3_4 ='True'
"""
output = s3.run_query(
            query=query,
            database='inpi',
            s3_output=s3_output,
    filename = None,
    destination_key = None
        )

results = False
filename_ligne = 'RESULTAT_CAS_2.csv'

while results != True:
    source_key = "{}/{}.csv".format(
                                'INPI/sql_output',
                                output['QueryExecutionId']
                                       )
    destination_key = "{}/{}".format(
                                    'RESULTATS_SIRETISATION/TABLES_SIRET',
                                    filename_ligne
                                )

    results = s3.copy_object_s3(
                                    source_key = source_key,
                                    destination_key = destination_key,
                                    remove = True
                                )  
```

Finalement, nous allons créer un fichier json avec un résumé de la siretisation

```python
query = """
SELECT COUNT(*) 
FROM ets_inpi_insee_cases_filtered 
WHERE index_id_duplicate = 'True' AND test_adresse_cas_1_3_4 ='True'
"""
total_index_found = s3.run_query(
            query=query,
            database='inpi',
            s3_output=s3_output,
    filename = 'cas_2_count_ets_inpi_insee_cases_found',
    destination_key = None
        )
total_index_found
```

```python
dic_ = {
    
    'CAS': 'CAS_2',
    'count_all_index':total_index.values[0][0],
    'count_filtered_in': total_index_filtre.values[0][0],
    'count_filtered_out':total_index_found.values[0][0],
    'percentage_found': total_index_found.values[0][0]/total_index.values[0][0]
}
dic_
```

```python
import json
def convert(o):
        if isinstance(o, np.int64): return int(o)  
        raise TypeError
```

```python
with open('LOGS_CAS_2.json', 'w') as outfile:
    json.dump(dic_, outfile, default=convert)
```

```python
s3.upload_file(file_to_upload = 'LOGS_CAS_2.json',
            destination_in_s3 = 'RESULTATS_SIRETISATION/LOGS_ANALYSE')
os.remove('LOGS_CAS_2.json')
```

# Generation report

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
    os.system('jupyter nbconvert --no-input --to {} {}'.format(
    extension,notebookname))
    
    ### Move notebook to report folder
    #time.sleep(5)
    shutil.move(source_to_move, dest)
    print("Report Available at this adress:\n {}".format(dest))
```

```python
create_report(extension = "html")
```
