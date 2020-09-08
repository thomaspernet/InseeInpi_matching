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

# Création table poids obtenus via le Word2Vec

Copy paste from Coda to fill the information

## Objective(s)

La siretisation repose sur une matrice de règles de gestion classée de manière ordonnée. Lors des US précédents, nous avons calculé 10 variables tests sur les 12. Voici les 10 tests calculés au préalable, avec leur dépendence:

| Rang | Nom_variable                              | Dependence                                    | Notebook                           | Difficulte | Table_input                                                                                                                                                            | Variables_crees_US                                                                 | Possibilites                  |
|------|-------------------------------------------|-----------------------------------------------|------------------------------------|------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------|-------------------------------|
| 1    | status_cas                                |                                               | 02_cas_de_figure                   | Moyen      | ets_insee_inpi_status_cas                                                                                                                                              | status_cas,intersection,pct_intersection,union_,inpi_except,insee_except           | CAS_1,CAS_2,CAS_3,CAS_4,CAS_5 |
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

Il reste encore deux tests a calculer afin de pouvoir matcher avec la matrice des règles. Les deux règles manquantes sont `test_distance_cosine` et `test_distance_levhenstein`. Néanmoins, ses deux tests requièrent le calcul de deux variables, dont l'une d'elle `max_cosine_distance` dépend des résultats de l'algorithme de Word2Vec. C'est l'object de cette US.

Dans cette US, nous allons calculer les poids permettants de mettre en avant la similarité entre deux mots. La similarité est calculé via le Word2Vec embedding.

## Metadata 

* Metadata parameters are available here: 
* US Title: Création table poids obtenus via le Word2Vec
* Epic: Epic 5
* US: US 7
* Date Begin: 9/7/2020
* Duration Task: 1
* Status:  
* Source URL:US 07 Preparation tables et variables tests
* Task type:
  * Jupyter Notebook
* Users: :
  * Thomas Pernet
* Watchers:
  * Thomas Pernet
* Estimated Log points:
  * One being a simple task, 15 a very difficult one
  *  7
* Task tag
  *  #machine-learning,#word2vec,#siretisation,#sql-query,#similarite,#preparation-tableword3vec
* Toggl Tag
  * #data-preparation
  
## Input Cloud Storage [AWS]

If link from the internet, save it to the cloud first

### Tables [AWS]

1. Batch 1:
  * Select Provider: Athena
  * Select table(s): ets_insee_inpi
    * Select only tables created from the same notebook, else copy/paste selection to add new input tables
    * If table(s) does not exist, add them: Add New Table
    * Information:
      * Region: 
        * NameEurope (Paris)
        * Code: eu-west-3
      * Database: inpi
      * Notebook construction file: 
        * https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/03_ETS_add_variables.md
    
## Destination Output/Delivery

* AWS
    1. Athena: 
      * Region: Europe (Paris)
      * Database: siretisation
      * Tables (Add name new table): list_pairs_mots_insee_inpi,weights_mots_insee_inpi_word2vec
      * List new tables
          * list_pairs_mots_insee_inpi
          * weights_mots_insee_inpi_word2vec

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

Pour cacluler la similarité entre l'adresse de l'INSEE et de l'INPI, nous devons créer une liste de combinaison unique entre les deux adresses. Pour cela, nous utilisons les variables `adresse_distance_inpi` et `adresse_distance_insee` qui ont été, au préalable, néttoyée, puis nous les concatènons en prenant le soin d'enlever les mots redondants. Dit autrement, si deux mots sont présents dans les deux adresses, alors, nous n'en gardons qu'un seul.

La table contient environ 2,836,384 combinaisons possibles.

```python
s3_output = 'inpi/sql_output'
database = 'siretisation'
```

```python
query = """
/*Combinaison mots insee inpi*/
CREATE TABLE siretisation.list_pairs_mots_insee_inpi
WITH (
  format='PARQUET'
) AS
SELECT unique_combinaition,
COUNT(*) AS CNT
FROM (SELECT
array_distinct(
    concat(
    array_distinct(
      split(adresse_distance_inpi, ' ')
      ),
    array_distinct(
      split(adresse_distance_insee, ' ')
    )    )
  ) unique_combinaition
FROM siretisation.ets_insee_inpi 
      )
      GROUP BY unique_combinaition
"""

output = s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = None, ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
output
```

<!-- #region -->
## Calcul model

### [What Are Word Embeddings for Text?](https://machinelearningmastery.com/what-are-word-embeddings/)

- by [[Jason Brownlee]]

- A word embedding is a learned representation for text where words that have the same meaning have a similar representation
- It is this approach to representing words and documents that may be considered one of the key breakthroughs of deep learning on challenging natural language processing problems.

    -> One of the benefits of using dense and low-dimensional vectors is computational
    
- The main benefit of the dense representations is generalization power
- if we believe some features may provide similar clues, it is worthwhile to provide a representation that is able to capture these similarities
- Word embeddings are in fact a class of techniques where individual words are represented as real-valued vectors in a predefined vector space
- Each word is mapped to one vector and the vector values are learned in a way that resembles a neural network, and hence the technique is often lumped into the field of deep learning
- Key to the approach is the idea of using a dense distributed representation for each word
- Each word is represented by a real-valued vector, often tens or hundreds of dimensions. This is contrasted to the thousands or millions of dimensions required for sparse word representations, such as a one-hot encoding
- The distributed representation is learned based on the usage of words
- This allows words that are used in similar ways to result in having similar representations, naturally capturing their meaning
- This can be contrasted with the crisp but fragile representation in a bag of words model where, unless explicitly managed, different words have different representations, regardless of how they are used

## Word Embedding Algorithms
    
- Word embedding methods learn a real-valued vector representation for a predefined fixed sized vocabulary from a corpus of text
- The learning process is either joint with the neural network model on some task, such as document classification, or is an unsupervised process, using document statistics.


### Word2Vec

- Word2Vec is a statistical method for efficiently learning a standalone word embedding from a text corpus
- It was developed by Tomas Mikolov, et al. at Google in 2013 as a response to make the neural-network-based training of the embedding more efficient and since then has become the de facto standard for developing pre-trained word embedding
- Two different learning models were introduced that can be used as part of the word2vec approach to learn the word embedding; they are:
    - Continuous Bag-of-Words, or CBOW model 
    - Continuous Skip-Gram Model. 
    - The CBOW model learns the embedding by predicting the current word based on its context
    - The continuous skip-gram model learns by predicting the surrounding words given a current word
    - Both models are focused on learning about words given their local usage context, where the context is defined by a window of neighboring words
    - The key benefit of the approach is that high-quality word embeddings can be learned efficiently (low space and time complexity), allowing larger embeddings to be learned (more dimensions) from much larger corpora of text (billions of words).
<!-- #endregion -->

```python
query = """
SELECT *
FROM siretisation.list_pairs_mots_insee_inpi
"""
### run query
output = s3.run_query(
        query=query,
        database='siretisation',
        s3_output='INPI/sql_output',
    filename = 'list_pairs', ## Add filename to print dataframe
  #destination_key = 'INPI/list_pairs_mots_insee_inpi' ### Add destination key if need to copy output
    )
```

```python
output.sort_values(by = ['cnt']).tail(10)
```

## Train model

Il y a plusieurs paramètres dans la librairie que nous allons utilisé, voici la configuration que nous allons prendre:

- size: (default 100) The number of dimensions of the embedding, e.g. the length of the dense vector to represent each token (word).
- window: (default 5) The maximum distance between a target word and words around the target word.
- min_count: (default 5) The minimum count of words to consider when training the model; words with an occurrence less than this count will be ignored.
- workers: (default 3) The number of threads to use while training.
- sg: (default 0 or CBOW) The training algorithm, either CBOW (0) or skip gram (1).

```python
from gensim.models import Word2Vec
import re
```

```python
def basic_clean(text):
    return re.sub(r'[^\w\s]|[|]', '', text).split()
```

```python
df_text = output['unique_combinaition'].apply(lambda x: basic_clean(x))
```

```python
df_text.head()
```

Pour le POC, nous utilisons les paramètres par défault

```python
%%time 
model = Word2Vec(df_text.tolist(),
                 size = 100,
                 window = 5,
                 min_count=5,
                 sg = 0)
```

Nous devons calculer la similarité entre les mots communs dans l'adresse INPI/INSEE, donc nous pouvons utiliser les poids du modèles et ensuite calculer la similarité avec la méthode du cosine. 

La librarie `gensim` permet d'exporter les poids en `.txt`. Toutefois, il n'est pas concevable de calculer l'ensembles des similarités entre toutes les occurences (environ 90.000), donc lors des traitements dans Athena, nous calculerons le cosines à la demande. 


Sauvegarde l'ensemble des poids dans des colonnes

```python
model.wv.save_word2vec_format('word2vec_weights_100.txt', binary=False)
```

```python
list_header = ['Words'].extend(list(range(1, 101)))
```

```python
model_weights = pd.read_csv('word2vec_weights_100.txt',
                            sep = ' ', skiprows= 1,
                           header= list_header)
```

```python
model_weights.to_csv('word2vec_weights_100_v2.csv', index = False)  
```

Le modèle se trouve à l'adresse suivante [MACHINE_LEARNING/NLP/WORD2VEC_WEIGHTS/V2](https://s3.console.aws.amazon.com/s3/buckets/calfdata/MACHINE_LEARNING/NLP/WORD2VEC_WEIGHTS/V2?region=eu-west-3&tab=overview)

```python
s3.upload_file('word2vec_weights_100_v2.csv',
               'MACHINE_LEARNING/NLP/WORD2VEC_WEIGHTS/V2')
```

## Create tables weights

Athena ne peut pas créer des array float a partir de fichier csv, du coup on utilise la fonction concat. C'est une solution pour le poc.

On créer une table temporaire qui contient l'ensemble des poids en colonnes list_mots_insee_inpi_word2vec_weights_temp puis on canct les colonnes dans la table list_mots_insee_inpi_word2vec_weights

```python
top = """
CREATE EXTERNAL TABLE IF NOT EXISTS siretisation.weights_mots_insee_inpi_word2vec (

`Words` string,
"""
middle = ""

for i in range(0,100):
    if i == 99:
        middle += "vec_{} float )".format(i)
    else:
        middle += "vec_{} float,".format(i)
bottom = """
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
   'separatorChar' = ',',
   'quoteChar' = '"'
   ) 
     LOCATION 's3://calfdata/MACHINE_LEARNING/NLP/WORD2VEC_WEIGHTS/V2'
     TBLPROPERTIES ('has_encrypted_data'='false',
              'skip.header.line.count'='1');
""" 
query = top + middle +bottom
output = s3.run_query(
        query=query,
        database='siretisation',
        s3_output='INPI/sql_output'
    )
```

```python
query = """
SELECT * FROM "siretisation"."weights_mots_insee_inpi_word2vec" limit 10;
"""
s3.run_query(
        query=query,
        database='siretisation',
        s3_output='INPI/sql_output',
    filename = 'weights'
    )

```

```python
query = """

CREATE TABLE siretisation.list_weight_mots_insee_inpi_word2vec
WITH (
  format='PARQUET'
) AS
SELECT words,
CONCAT(

"""
middle = ""
for i in range(0, 100):
    if i ==99:
        middle  = "ARRAY[vec_{}]) as list_weights".format(i)
    else:
        middle  = "ARRAY[vec_{}],".format(i)
    query += middle
bottom = """
FROM "siretisation"."weights_mots_insee_inpi_word2vec"
"""
query += bottom
output = s3.run_query(
        query=query,
        database='machine_learning',
        s3_output='INPI/sql_output'
    )
```

```python
query = """
SELECT * FROM "siretisation"."list_weight_mots_insee_inpi_word2vec" limit 10;
"""
s3.run_query(
        query=query,
        database='siretisation',
        s3_output='INPI/sql_output',
    filename = 'list_weights'
    ).head(3)
```

<!-- #region -->
# Test acceptance/Analyse du modèle

La librairie `gensim` a une fonction integrée pour calculer la similarité. Toutefois, nous devons cacluler le cosine manuelement dans Athena car il n'y a pas de fonction SQL prévue a cet effet. 


## Similarité - cosine

La fonction cosine se calcule de la facon suivante:

$$\frac{u \cdot v}{\|u\|_{2}\|v\|_{2}}$$

- Source 1: Calcul cosine:
  - [Cosine similarity](https://en.wikipedia.org/wiki/Cosine_similarity)
  - [Magnitude (mathematics)](https://en.wikipedia.org/wiki/Magnitude_(mathematics)#Euclidean_vector_space)
  - [Scipy Cosine](https://docs.scipy.org/doc/scipy/reference/generated/scipy.spatial.distance.cosine.html)
<!-- #endregion -->

Dans ce notebook, nous allons expliquer comment calculer la distance de cosine en SQL, via les fonctions Presto


1. Creation table exemple

On va récupérer deux mots, RTE et ROUTE

```python
query = """
CREATE TABLE siretisation.exemple_cosine_sql
WITH (
  format='PARQUET'
) AS
WITH db AS (
SELECT 'PAIR' AS pairs,words as words_1, list_weights as list_weights_1
FROM list_weight_mots_insee_inpi_word2vec 
WHERE words = 'RTE'
)
SELECT db.pairs, words_1,words_2, list_weights_1,list_weights_2
FROM db
LEFT JOIN (
  
  SELECT 'PAIR' AS pairs,words as words_2, list_weights as list_weights_2
FROM list_weight_mots_insee_inpi_word2vec 
WHERE words = 'ROUTE'
  
  ) AS tb_list2
  ON db.pairs = tb_list2.pairs

"""
output = s3.run_query(
        query=query,
        database='siretisation',
        s3_output='INPI/sql_output'
    )
```

```python
query = """
SELECT * FROM "siretisation"."exemple_cosine_sql"
"""
tb_rte_route = s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = 'exemple_rte_route', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
tb_rte_route
```

2. Calcul du nominateur 

Le nominateur est simplement le dot product entre la liste 1 et la liste 2:

$$u \cdot v$$

Pour rappel, le dot product s'effectue de la manière suivante: $a · b = ax × bx + ay × by$

Le dot product doit, par définition, retourner un scalaire!

Fonction presto:

- `REDUCE`: https://prestodb.io/docs/0.172/functions/lambda.html#reduce
    - Va permettre de sommer le produit x*y
- `zip_with`: https://prestodb.io/docs/0.172/functions/lambda.html#zip_with
    - Va permettre de calculer produit x*y


```python
query = """
SELECT 
pairs, words_1, words_2, 
REDUCE(
              zip_with(
                list_weights_1, 
                list_weights_2, 
                (x, y) -> x * y
              ), 
              CAST(
                ROW(0.0) AS ROW(sum DOUBLE)
              ), 
              (s, x) -> CAST(
                ROW(x + s.sum) AS ROW(sum DOUBLE)
              ), 
              s -> s.sum
            ) AS dot_product,
            list_weights_1, list_weights_2

FROM "siretisation"."exemple_cosine_sql"
"""
s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = 'exemple_dotproruct', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

On va vérifier notre résultat avec numpy

```python
def sql_array_to_array(x):
    """"""
    return (tb_rte_route[x]
 .str
 .replace('\[|\]|,', '', regex=True)
 .str
 .split(expand = True)
 .loc[0]
 .astype(float)
)
```

```python
import numpy as np
np.dot(sql_array_to_array('list_weights_1'),sql_array_to_array('list_weights_2'))
```

3. Calcul du dénominateur

Le denominateur est $\|u\|_{2}\|v\|_{2}$, ce qui correspond à la norme. La norme est une extension de la valeur absolue des nombres aux vecteurs. Elle permet de mesurer la longueur commune à toutes les représentations d'un vecteur dans un espace affine, mais définit aussi une distance entre deux vecteurs invariante par translation et compatible avec la multiplication externe.

La norme usuelle (euclidienne) d'un vecteur peut se calculer à l'aide de ses coordonnées dans un repère orthonormé à l'aide du théorème de Pythagore.
Dans le plan, si le vecteur ${\displaystyle {\vec {u}}}{\vec {u}}$ a pour coordonnées ${\displaystyle (x,y)}(x,y)$, sa norme s'écrire:

$$\|{\vec  u}\|={\sqrt  {x^{2}+y^{2}}}.$$

```python
query = """
SELECT 
pairs, words_1, 
SQRT(
                REDUCE(
                  transform(
                    list_weights_1, 
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
              ) as norme_w1 

FROM "siretisation"."exemple_cosine_sql"
"""
s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = 'exemple_cosine_sql', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

Résultat via numpy

```python
np.linalg.norm(sql_array_to_array('list_weights_1'))
```

On calcule du cosine

```python
query =""" 
SELECT 
pairs, words_1, words_2, 
 REDUCE(
              zip_with(
                list_weights_1, 
                list_weights_2, 
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
                    list_weights_1, 
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
                    list_weights_2, 
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

FROM "siretisation"."exemple_cosine_sql"
"""
s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = 'exemple_cosine', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
```

```python
from scipy.spatial import distance

1 - distance.cosine(
     sql_array_to_array('list_weights_1'),
     sql_array_to_array('list_weights_2')
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
