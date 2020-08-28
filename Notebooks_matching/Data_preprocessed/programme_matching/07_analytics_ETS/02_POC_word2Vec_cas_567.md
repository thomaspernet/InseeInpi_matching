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

<!-- #region -->
# Test similarite exception list mots INSEE et INPI siretisation

Objective(s)

*  L’objectif de cette tache est de trouver une solution pour retourner la distance donnée par le Word2Vec entre 2 listes contenant des mots qui ne sont pas communs dans l’adresse INSEE et INPI
* Il faut faire le test lorsque la variable status_cas est egal a CAS_5,6 ou 7
* Par exemple:
    * inpi_except: [A, B]
    * insee_except: [A,C]
    * Le test: [[A,A], [A,C], [B,A],[B,C]]
    * Output: [p1, p2, p3, p4]
    * Recupération max list output
    * Variables nécéssaire:
        * inpi_except 
        * insee_except 
        * status_cas

## Metadata

* Metadata parameters are available here: Ressources_suDYJ#_luZqd
* Task type:
  * Jupyter Notebook
* Users: :
  * Thomas Pernet
* Watchers:
  * Thomas Pernet
* Estimated Log points:
  * One being a simple task, 15 a very difficult one
  *  14
* Task tag
  *  #sql-query,#matching,#siretisation,#machine-learning,#word2vec
* Toggl Tag
  * #poc
  
## Input Cloud Storage [AWS/GCP]

If link from the internet, save it to the cloud first

### Tables [AWS/BigQuery]

1. Batch 1:
    * Select Provider: Athena
      * Select table(s): ets_inpi_insee_cases
        * Select only tables created from the same notebook, else copy/paste selection to add new input tables
        * If table(s) does not exist, add them: Add New Table
        * Information:
          * Region: 
            * NameEurope (Paris)
            * Code: eu-west-3
          * Database: inpi
          * Notebook construction file: [07_pourcentage_siretisation_v3](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/02_siretisation/07_pourcentage_siretisation_v3.md)
2. Batch 2:
  * Select Provider: Athena
  * Select table(s): list_mots_insee_inpi_word2vec_weights
    * Select only tables created from the same notebook, else copy/paste selection to add new input tables
    * If table(s) does not exist, add them: Add New Table
    * Information:
      * Region: 
        * NameEurope (Paris)
        * Code: eu-west-3
      * Database: machine_learning
      * Notebook construction file: 03_POC_word2Vec_weights_computation
    
## Destination Output/Delivery

* Athena: 
    * Region: Europe (Paris)
    * Database: inpi
    * Tables (Add name new table): ets_inpi_insee_wordvec

  
## Things to know (Steps, Attention points or new flow of information)

### Sources of information  (meeting notes, Documentation, Query, URL)

1. Jupyter Notebook (Github Link)
  1. md : [Test_word2Vec.md](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/02_siretisation/Test_word2Vec.md)
<!-- #endregion -->

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
con = aws_connector.aws_instantiate(credential = path_cred,
                                       region = 'eu-west-3')
client= con.client_boto()
s3 = service_s3.connect_S3(client = client,
                      bucket = 'calfdata', verbose = False) 
athena = service_athena.connect_athena(client = client,
                      bucket = 'calfdata') 
```

# Etape de creation

- Filtrer les cas 5 à 7 et `test_list_num_voie != 'False'`. Il est inutile d'appliquer le test sur des lignes dont on est sur que les numéros ne sont pas identiques. On garde les `NULL` car ils correspondent aux lignes sans numéro.
- créer deux colonnes avec le pseudo-produit cartésien (table INPI vers table INSEE). Autrement dit, on ne souhaite pas comparer les mots au sein de la même liste, mais entre les listes. 
    - Table `ets_inpi_insee_cases` 
- Merge la liste des poids dans la table `list_mots_insee_inpi_word2vec_weights` 
- Calcul de la Cosine distance (dot product sur la magnitude)
- Calcul de la Cosine distance maximum par group `index_id` et `unzip_inpi`
- Recupération de la combinaison maximum par group
- Calculer le nombre de Cosine distance supérieure à .6.
    - Sert pour determiner combien de mots ont été retrouvé
- Création de la table `ets_inpi_insee_word2vec` pour analyse

On travaille sur une table de `1250134` lignes.  [Query](https://eu-west-3.console.aws.amazon.com/athena/home?region=eu-west-3#query/history/a4450f46-1f00-4e73-bb5d-8b41bf6a7125)

Attention, si il y a des valeurs nulles dans le calcul de la Cosine distance, c'est que le modèle n'a pas calculer de poid car pas suffisament d'occurences.

## Detail

La table `ets_inpi_insee_word2vec` est créé via une seule query, toutefois, ci dessous se trouve le detail de chaque étape

### Filtre et pseudo produit cartesien + merge

```
WITH
dataset AS (
SELECT index_id,inpi_except, insee_except, 
  transform(sequence(1, CARDINALITY(insee_except)), x-> insee_except),
  ZIP(inpi_except,
      
        transform(sequence(1, CARDINALITY(inpi_except)), x-> insee_except)
     ) as test
FROM inpi.ets_inpi_insee_cases
where status_cas = 'CAS_6'
LIMIT 10
  )
  SELECT *
  FROM (
    WITH weight_inpi AS (
  SELECT index_id, inpi_except, insee_except, unzip_inpi, unzip_insee, list_weights as list_weights_inpi
  FROM (
  SELECT
  index_id, inpi_except, insee_except,
    unzip.field0 as unzip_inpi,
    unzip.field1 as insee,
    test
  FROM dataset
  CROSS JOIN UNNEST(test) AS new (unzip)
    )
   CROSS JOIN UNNEST(insee) as test (unzip_insee)
   LEFT JOIN list_mots_insee_inpi_word2vec_weights 
   ON unzip_inpi = list_mots_insee_inpi_word2vec_weights.words
  )
SELECT
    
    index_id, inpi_except, insee_except, unzip_inpi, unzip_insee, list_weights_inpi,
    list_weights as list_weights_insee
    FROM weight_inpi
    LEFT JOIN list_mots_insee_inpi_word2vec_weights 
    ON unzip_insee = list_mots_insee_inpi_word2vec_weights.words
    
)   
``` 

### Calcul dot

```
REDUCE(
zip_with(list_weights_inpi, list_weights_insee, 
         (x, y) -> x * y
         ),
  CAST(ROW(0.0) AS ROW(sum DOUBLE)),
  (s, x) -> CAST(ROW(x + s.sum)  AS ROW(sum DOUBLE)),
  s -> s.sum
  ) AS dot_product,
```

### Calcul magnitude

```
SQRT(
REDUCE(
transform(list_weights_inpi,
         (x) -> POW(x, 2)
         )
         ,
  CAST(ROW(0.0) AS ROW(sum DOUBLE)),
  (s, x) -> CAST(ROW(x + s.sum)  AS ROW(sum DOUBLE)),
  s -> s.sum
  )
  ) *
SQRT(
REDUCE(
transform(list_weights_insee,
         (x) -> POW(x, 2)
         )
         ,
  CAST(ROW(0.0) AS ROW(sum DOUBLE)),
  (s, x) -> CAST(ROW(x + s.sum)  AS ROW(sum DOUBLE)),
  s -> s.sum
  )
  )  
AS denominator,
```


# Creation table analyse


```python
query = """
CREATE TABLE inpi.ets_inpi_insee_word2vec
WITH (
  format='PARQUET'
) AS
WITH dataset AS (
  SELECT 
    row_id, 
    index_id, 
    status_cas, 
    inpi_except, 
    insee_except, 
    pct_intersection, 
    len_inpi_except,
    len_insee_except, 
    transform(
      sequence(
        1, 
        CARDINALITY(insee_except)
      ), 
      x -> insee_except
    ), 
    ZIP(
      inpi_except, 
      transform(
        sequence(
          1, 
          CARDINALITY(inpi_except)
        ), 
        x -> insee_except
      )
    ) as test 
  FROM 
    inpi.ets_inpi_insee_cases 
  where 
    (
      status_cas = 'CAS_5' 
      OR status_cas = 'CAS_6' 
      OR status_cas = 'CAS_7'
    ) 
    AND test_list_num_voie != 'False'
     -- AND index_id = 3980536
) 
SELECT 
  * 
FROM 
  (
    WITH test AS (
      SELECT 
        * 
      FROM 
        (
          WITH max_data AS (
            SELECT 
              row_id, 
              dataset.index_id, 
              status_cas, 
              inpi_except, 
              insee_except, 
              pct_intersection,
              len_inpi_except,
              len_insee_except,
              test, 
              max_pct_intersection 
            FROM 
              dataset 
              INNER JOIN (
                SELECT 
                  index_id, 
                  MAX(pct_intersection) as max_pct_intersection 
                FROM 
                  dataset 
                GROUP BY 
                  index_id
              ) AS max_pct ON dataset.index_id = max_pct.index_id 
              AND dataset.pct_intersection = max_pct.max_pct_intersection
          ) 
          SELECT 
            * 
          FROM 
            (
              WITH weight_inpi AS (
                SELECT 
                  row_id,
                  index_id, 
                  status_cas, 
                  inpi_except, 
                  insee_except,
                  len_inpi_except,
                  len_insee_except,
                  unzip_inpi, 
                  unzip_insee, 
                  list_weights as list_weights_inpi 
                FROM 
                  (
                    SELECT 
                    row_id,
                      index_id, 
                      status_cas, 
                      inpi_except, 
                      insee_except,
                      len_inpi_except,
                      len_insee_except,
                      unzip.field0 as unzip_inpi, 
                      unzip.field1 as insee, 
                      test 
                    FROM 
                      max_data CROSS 
                      JOIN UNNEST(test) AS new (unzip)
                  ) CROSS 
                  JOIN UNNEST(insee) as test (unzip_insee) 
                  LEFT JOIN machine_learning.list_mots_insee_inpi_word2vec_weights ON unzip_inpi = list_mots_insee_inpi_word2vec_weights.words
              ) 
              SELECT 
              row_id,
                index_id, 
                status_cas, 
                inpi_except, 
                insee_except, 
                unzip_inpi, 
                unzip_insee,
                len_inpi_except,
                len_insee_except,
                REDUCE(
                  zip_with(
                    list_weights_inpi, 
                    list_weights_insee, 
                    (x, y) -> x * y
                  ), 
                  CAST(
                    ROW(0.0) AS ROW(sum DOUBLE)
                  ), 
                  (s, x) -> CAST(
                    ROW(x + s.sum) AS ROW(sum DOUBLE)
                  ), 
                  s -> s.sum
                ) / (
                  SQRT(
                    REDUCE(
                      transform(
                        list_weights_inpi, 
                        (x) -> POW(x, 2)
                      ), 
                      CAST(
                        ROW(0.0) AS ROW(sum DOUBLE)
                      ), 
                      (s, x) -> CAST(
                        ROW(x + s.sum) AS ROW(sum DOUBLE)
                      ), 
                      s -> s.sum
                    )
                  ) * SQRT(
                    REDUCE(
                      transform(
                        list_weights_insee, 
                        (x) -> POW(x, 2)
                      ), 
                      CAST(
                        ROW(0.0) AS ROW(sum DOUBLE)
                      ), 
                      (s, x) -> CAST(
                        ROW(x + s.sum) AS ROW(sum DOUBLE)
                      ), 
                      s -> s.sum
                    )
                  )
                ) AS cosine_distance 
              FROM 
                (
                  SELECT 
                  row_id,
                    index_id, 
                    status_cas, 
                    inpi_except, 
                    insee_except, 
                    unzip_inpi, 
                    unzip_insee,
                    len_inpi_except,
                    len_insee_except,
                    list_weights_inpi, 
                    list_weights as list_weights_insee 
                  FROM 
                    weight_inpi 
                    LEFT JOIN machine_learning.list_mots_insee_inpi_word2vec_weights ON unzip_insee = list_mots_insee_inpi_word2vec_weights.words
                )
            )
        )
    ) 
    SELECT 
      row_id,
      test.index_id, 
      test.status_cas, 
      test.unzip_inpi, 
      unzip_insee,
      len_inpi_except,
       len_insee_except,
      max_cosine_distance,
    count_found
    FROM 
      test 
      INNER JOIN (
        SELECT 
          index_id, 
          status_cas, 
          unzip_inpi, 
          MAX(cosine_distance) as max_cosine_distance 
        FROM 
          test 
        GROUP BY 
          index_id, 
          status_cas, 
          unzip_inpi
      ) AS max_cosine_by_group ON test.index_id = max_cosine_by_group.index_id 
      AND test.status_cas = max_cosine_by_group.status_cas 
      AND test.unzip_inpi = max_cosine_by_group.unzip_inpi 
      AND test.cosine_distance = max_cosine_by_group.max_cosine_distance
      LEFT JOIN (
        SELECT 
          index_id, 
          status_cas, 
          COUNT(*) as count_found 
        FROM 
          (
            SELECT 
          index_id, 
          status_cas, 
          unzip_inpi, 
          MAX(cosine_distance) as max_cosine_distance 
        FROM 
          test 
        GROUP BY 
          index_id, 
          status_cas, 
          unzip_inpi
      ) 
        WHERE max_cosine_distance >= .6
        GROUP BY index_id, status_cas
      ) AS above_threshold 
      ON test.index_id = above_threshold.index_id 
      AND test.status_cas = above_threshold.status_cas 
  )
"""
output = athena.run_query(
        query=query,
        database='inpi',
        s3_output='INPI/sql_output'
    )
```

# Analyse



## Calcul decile

```python
query = """
SELECT approx_percentile(
  max_cosine_distance, ARRAY[
    0.25,
    0.50,
    0.60,
    0.70,
    0.75,
    0.80,
    0.85,
    0.95,
    0.99]
  )
  FROM ets_inpi_insee_word2vec 
"""
#[0.15966796874999997, 0.4188717631017009, 0.5195312499999999,
#0.6388182005831299, 0.6618696474706767, 0.725784156601784, 0.769106062989077, 0.8180236405537482, 0.8996922673858876]




```

## Distribtution combinaison 

Lorsqu'il y a des mots qui n'ont aucun rapport, mais avec à la fois un cosine élevée et un count unique, c'est du aux entreprises avec beaucoup de doublons dans la base, engendrant ainsi une augmentation artificielle du count lors de l'entrainement du modèle. Il faudrait tuner le modèle différement et voir l'impact sur ce genre de situation, voir modifier le jeu d'entrainement.


```python
def count_combinaison(from_ = .6, to_ = .7):
    query = """
    SELECT combinaison, COUNT(combinaison) as count_combinaison, AVG(max_cosine_distance) avg_cosine
    FROM (
    SELECT CONCAT(ARRAY[unzip_inpi],ARRAY[unzip_insee]) as combinaison, max_cosine_distance  
    FROM ets_inpi_insee_word2vec 
    WHERE max_cosine_distance >= {0} AND max_cosine_distance < {1}
      )
      GROUP BY combinaison
      ORDER BY count_combinaison DESC

    """.format(from_, to_)

    output = athena.run_query(
            query=query,
            database='inpi',
            s3_output='INPI/sql_output'
        )

    results = False
    filename = 'combinaison_word2vec_{0}_{1}.csv'.format(from_, to_)

    while results != True:
        source_key = "{}/{}.csv".format(
                                'INPI/sql_output',
                                output['QueryExecutionId']
                                       )
        destination_key = "{}/{}".format(
                                    'MACHINE_LEARNING/NLP/RESULTAT_WORD2VEC',
                                    filename
                                )

        results = s3.copy_object_s3(
                                    source_key = source_key,
                                    destination_key = destination_key,
                                    remove = True
                                )
        
    test_1 = (s3.read_df_from_s3(
            key = 'MACHINE_LEARNING/NLP/RESULTAT_WORD2VEC/{}'.format(filename), sep = ',')
              .assign(
                  pct_total = lambda x: x['count_combinaison']/x['count_combinaison'].sum(),
                  pct_total_cum = lambda x: x['pct_total'].cumsum(),
              
              )
             )
    
    return test_1
```

### Cosine de .6 a .95

- 70 pairs regroupe environ 75% des lignes avec test lignes et cas 5 à 7.

```python
full = count_combinaison(from_ = .6, to_ = .95)
```

```python
full.shape
```

```python
full.head(50)
```

```python
full.loc[lambda x: x['pct_total']>.00001]#.to_excel('test.xlsx')
```

### Cosine De .6 a .7

```python
count_combinaison(from_ = .6, to_ = .65)
```

### Cosine De .65 a .7

```python
count_combinaison(from_ = .65, to_ = .7)
```

### Cosine De .7 a .75

```python
count_combinaison(from_ = .7, to_ = .75)
```

### Cosine De .75 a .8

```python
count_combinaison(from_ = .75, to_ = .8)
```

### Cosine De .8 a .85

```python
count_combinaison(from_ = .8, to_ = .85)
```

### Cosine De .85 a .9

```python
count_combinaison(from_ = .85, to_ = .9)
```

```python
count_combinaison(from_ = .9, to_ = .95)
```

## Analyse duplicate

Pour trouver une méthode de séparation lorsque l'`index_id` a de nombreuses possibilités, nous pouvons regarder les duplicates. 

Dans un premier temps, on va compter le nombre d'index par nombre de combinaison possible. 


### Compte index par combinaison

```python
query = """
SELECT count_id AS combinaison, COUNT(count_id) as count_duplicate
FROM (
SELECT index_id, COUNT(*) as count_id
FROM ets_inpi_insee_word2vec 
GROUP BY index_id
)
GROUP BY count_id
ORDER BY count_duplicate DESC, combinaison
"""
output = athena.run_query(
            query=query,
            database='inpi',
            s3_output='INPI/sql_output'
        )

results = False
filename = 'duplicate_index_combinaison_word2vec.csv'

while results != True:
    source_key = "{}/{}.csv".format(
                                'INPI/sql_output',
                                output['QueryExecutionId']
                                       )
    destination_key = "{}/{}".format(
                                    'MACHINE_LEARNING/NLP/RESULTAT_WORD2VEC',
                                    filename
                                )

    results = s3.copy_object_s3(
                                    source_key = source_key,
                                    destination_key = destination_key,
                                    remove = True
                                )
        
test_1 = (s3.read_df_from_s3(
            key = 'MACHINE_LEARNING/NLP/RESULTAT_WORD2VEC/{}'.format(filename), sep = ',')
              .assign(
                  pct_total = lambda x: x['count_duplicate']/x['count_duplicate'].sum(),
                  pct_total_cum = lambda x: x['pct_total'].cumsum(),
              
              )
             )
test_1
```

## Analyse des cas

```python
query = """
SELECT status_cas, approx_percentile(
  pct_found, ARRAY[
    0.25,
    0.50,
    0.60,
    0.70,
    0.75,
    0.80,
    0.85,
    0.95,
    0.99]
  ) as avg_pct_found_cas
FROM (
SELECT row_id, index_id, status_cas, AVG(pct_found) as pct_found
FROM(
SELECT *, 
CASE WHEN count_found IS NULL THEN 0 ELSE
cast(count_found as decimal(7,2))/ cast(len_insee_except as decimal(7,2)) END AS pct_found 
FROM "inpi"."ets_inpi_insee_word2vec"
)
GROUP BY row_id, index_id, status_cas
)
GROUP BY status_cas
"""

output = athena.run_query(
            query=query,
            database='inpi',
            s3_output='INPI/sql_output'
        )

results = False
filename = 'distribution_pct_combinaison_word2vec.csv'

while results != True:
    source_key = "{}/{}.csv".format(
                                'INPI/sql_output',
                                output['QueryExecutionId']
                                       )
    destination_key = "{}/{}".format(
                                    'MACHINE_LEARNING/NLP/RESULTAT_WORD2VEC',
                                    filename
                                )

    results = s3.copy_object_s3(
                                    source_key = source_key,
                                    destination_key = destination_key,
                                    remove = True
                                )
    
test_1 = (s3.read_df_from_s3(
            key = 'MACHINE_LEARNING/NLP/RESULTAT_WORD2VEC/{}'.format(filename), sep = ',')
         )
test_1
#[0.25,0.50,0.60,0.70,0.75,0.80,0.85,0.95,0.99]
```

Exemple de relation, overfitting? Dans cette adresse, il y a deux mots, ALLUES et MUSILLON. Le machine a trouvé une similarité de .78 (très élevé). Ca m'a paru bizare. J'ai regardé sur Google, en fait, c'est un chalet à Méribel

```python
query = """
SELECT *
FROM ets_inpi_insee_word2vec  
WHERE index_id = 423494
"""

output = athena.run_query(
            query=query,
            database='inpi',
            s3_output='INPI/sql_output'
        )

results = False
filename = 'index_423494_combinaison_word2vec.csv'

while results != True:
    source_key = "{}/{}.csv".format(
                                'INPI/sql_output',
                                output['QueryExecutionId']
                                       )
    destination_key = "{}/{}".format(
                                    'MACHINE_LEARNING/NLP/RESULTAT_WORD2VEC',
                                    filename
                                )

    results = s3.copy_object_s3(
                                    source_key = source_key,
                                    destination_key = destination_key,
                                    remove = True
                                )
    
test_1 = (s3.read_df_from_s3(
            key = 'MACHINE_LEARNING/NLP/RESULTAT_WORD2VEC/{}'.format(filename), sep = ',')
         )
test_1
```

Count nombre de test si pct_found supérieur à .5

```python
query = """
SELECT status_cas, test_exception, COUNT(*) as count_test_exception
FROM(
SELECT row_id, index_id, status_cas, pct_found, CASE WHEN pct_found >=.5 THEN 'True' ELSE 'False' END as test_exception
FROM (
SELECT row_id, index_id, status_cas, AVG(pct_found) as pct_found
FROM(
SELECT *, 
CASE WHEN count_found IS NULL THEN 0 ELSE
cast(count_found as decimal(7,2))/ cast(len_insee_except as decimal(7,2)) END AS pct_found 
FROM "inpi"."ets_inpi_insee_word2vec"
)
GROUP BY row_id, index_id, status_cas
)
  )
  GROUP BY status_cas, test_exception
  ORDER BY status_cas, test_exception

"""
output = athena.run_query(
            query=query,
            database='inpi',
            s3_output='INPI/sql_output'
        )

results = False
filename = 'resultat_test_combinaison_word2vec.csv'

while results != True:
    source_key = "{}/{}.csv".format(
                                'INPI/sql_output',
                                output['QueryExecutionId']
                                       )
    destination_key = "{}/{}".format(
                                    'MACHINE_LEARNING/NLP/RESULTAT_WORD2VEC',
                                    filename
                                )

    results = s3.copy_object_s3(
                                    source_key = source_key,
                                    destination_key = destination_key,
                                    remove = True
                                )
    
test_1 = (s3.read_df_from_s3(
            key = 'MACHINE_LEARNING/NLP/RESULTAT_WORD2VEC/{}'.format(filename), sep = ',')
         )
(
    test_1.set_index(['status_cas', 'test_exception']).unstack(-1)
    .assign(cumsum_true = lambda x: x[('count_test_exception',True)].cumsum())
)
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
