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
# Creation méthodologie calcul poids similarité adresse via word2vec

Objective(s)

- Lors du POC de l’US [US 06 Union et Intersection](https://coda.io/d/CreditAgricole_dCtnoqIftTn/US-06-Union-et-Intersection_sucns), nous avons besoin d’une table contenant les poids indiquant la similarité entre deux mots. Dès lors, il est indispensable de créer un notebook avec de la documentation sur la création de ces poids et de la technique utilisée. 

  - Une table sera créé dans Athena avec deux colonnes pour chacun des mots et un poids. Les trois nouvelles variables seront appelées:

  - Mot_A
  - Mot_B
  - Index_relation

- Pour calculer les poids, il faut utiliser la table suivante XX avec les variables:

  -  `adresse_distance_inpi` 
  -  `adresse_distance_inpi` 

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

  - \#machine-learning,#word2vec,#documentation,#similarite

- Toggl Tag

  - \#variable-computation
 
  
## Input Cloud Storage [AWS/GCP]

If link from the internet, save it to the cloud first

### Tables [AWS/BigQuery]

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
      * Notebook construction file: https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/03_ETS_add_variables.md
    
## Destination Output/Delivery

1. Athena: 
      * Region: Europe (Paris)
      * Database: machine_learning
      * Tables (Add name new table):
          - list_mots_insee_inpi
          - list_mots_insee_inpi_word2vec_weights

2. S3(Add new filename to Database: Ressources)
      * Origin: Jupyter notebook
      * Bucket: calfdata
      * Key: MACHINE_LEARNING/NLP/WORD2VEC_WEIGHTS
      * Filename(s): word2vec_weights_100

  
## Things to know (Steps, Attention points or new flow of information)

### Sources of information  (meeting notes, Documentation, Query, URL)

- Query [Athena/BigQuery]

  1. Link 1: [Liste ngrams](https://eu-west-3.console.aws.amazon.com/athena/home?region=eu-west-3#query/history/79a481c2-df9c-4785-993b-4c6813947770)

    - Description: Query utilisée précédemment pour créer la liste des combinaisons INSEE-INPI
    
1. GitHub
  * Repo: https://github.com/thomaspernet/InseeInpi_matching
  * Folder name: Notebooks_matching/Data_preprocessed/programme_matching/02_siretisation
  * Source code:  Test_word2Vec.md
2. Python Module [Module name](link)
  * Library 1: gensim
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

# Creation database

```python
query = """CREATE DATABASE IF NOT EXISTS machine_learning
  COMMENT 'DB for machine learning tests'
  LOCATION 's3://calfdata/MACHINE_LEARNING/NLP/'
  """
```

# Creation Tables

Pour cacluler la similarité entre l'adresse de l'INSEE et de l'INPI, nous devons créer une liste de combinaison unique entre les deux adresses. Pour cela, nous utilisons les variables `adresse_distance_inpi`  et `adresse_distance_insee` qui ont été, au préalable, néttoyée, puis nous les concatènons en prenant le soin d'enlever les mots redondants. Dit autrement, si deux mots sont présents dans les deux adresses, alors, nous n'en gardons qu'un seul. 

La table contient environ 2,836,384 combinaisons possibles. 

```python
create table = False
query_combination = """
/*Combinaison mots insee inpi*/
CREATE TABLE machine_learning.list_mots_insee_inpi
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
FROM inpi.ets_insee_inpi 
      )
      GROUP BY unique_combinaition
"""
```

```python
if create_table:
    output = athena.run_query(
            query=query_combination,
            database='inpi',
            s3_output='INPI/sql_output'
        )
```

<!-- #region -->
# Calcul model

Dès lors que la table d'entrainement est prète, nous allons calculer un vecteur de poid pour chaque mot. Par défaut, nous calculons 100 poids pour chacune des occurences. Les poids sont calculés grace a la technique de Word2Vec

## [What Are Word Embeddings for Text?](https://machinelearningmastery.com/what-are-word-embeddings/) 

- by [[Jason Brownlee]]

## What Are Word Embeddings?

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
FROM machine_learning.list_mots_insee_inpi
"""

### run query
output = athena.run_query(
        query=query,
        database='machine_learning',
        s3_output='INPI/sql_output'
    )

results = False
filename = 'combinaison_adresses_insee_inpi.csv'
    
while results != True:
    source_key = "{}/{}.csv".format(
                            'INPI/sql_output',
                            output['QueryExecutionId']
                                   )
    destination_key = "{}/{}".format(
                                'MACHINE_LEARNING/NLP/LISTE_INSEE_INPI',
                                filename
                            )
        
    results = s3.copy_object_s3(
                                source_key = source_key,
                                destination_key = destination_key,
                                remove = True
                            )
```

Load dataframe

```python
list_insee_inpi = (s3.read_df_from_s3(
            key = 'MACHINE_LEARNING/NLP/LISTE_INSEE_INPI/{}'.format(filename), sep = ',')
             )
```

## Train model

There are many parameters on this constructor; a few noteworthy arguments you may wish to configure are:

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
df_text = list_insee_inpi['unique_combinaition'].apply(lambda x: basic_clean(x))
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

Pour cela, nous allons créer un csv avec deux colonnes, `words` et `list_weights`. Attention, cette dernière n'est pas une liste dans le csv, mais le sera dans Athena. Athena permet d'importer un ensemble de valeur dans un array. Si on crée une liste dans le csv, Athena va créer une liste de liste. Ainsi, il est plus simple dans le csv de créer uniquement deux colonnes, les mots et les poids. Le séparateur `|` sera utilisé. Le csv ressemble a ca:

```
Words | list_of_weights
RUE | .1, .4 ......
AVENUE | .2, .9 ......
```

```python
model.wv.save_word2vec_format('word2vec_weights_100.txt', binary=False)
```

```python
list_header = ['Words'].extend(list(range(1, 101)))
```

```python
model_wieghts = pd.read_csv('word2vec_weights_100.txt',
                            sep = ' ', skiprows= 1,
                           header= list_header)
```

```python
zipped_weight = list(
    zip(
    model_wieghts.set_index(0).index.values.tolist(),
    model_wieghts.set_index(0).values.tolist()
)
    )
```

```python
(pd.DataFrame(zipped_weight)
  .rename(columns= {0:'words', 1: 'list_weights'})
  .assign(list_weights = lambda x:x['list_weights'].apply(lambda x: ','.join(map(str, x))))
  .to_csv('word2vec_weights_100.csv', index = False, sep = "|")       
 )
```

Le modèle se trouve à l'adresse suivante [MACHINE_LEARNING/NLP/WORD2VEC_WEIGHTS](https://s3.console.aws.amazon.com/s3/buckets/calfdata/MACHINE_LEARNING/NLP/WORD2VEC_WEIGHTS/?region=eu-west-3&tab=overview)

```python
s3.upload_file('word2vec_weights_100.csv',
               'MACHINE_LEARNING/NLP/WORD2VEC_WEIGHTS')
```

## Create tables weights

Pour convertir un string en array, il suffit d'utiliser `array<string>`  

```python
query = """
CREATE EXTERNAL TABLE IF NOT EXISTS machine_learning.list_mots_insee_inpi_word2vec_weights (

`Words` string,
`list_weights` array<string>
  )

ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
     WITH SERDEPROPERTIES (
      'serialization.format' = ',',
      'field.delim' = '|') 
     LOCATION 's3://calfdata/MACHINE_LEARNING/NLP/WORD2VEC_WEIGHTS'
     TBLPROPERTIES ('has_encrypted_data'='false', 
     'skip.header.line.count'='1')
     
"""
output = athena.run_query(
        query=query,
        database='machine_learning',
        s3_output='INPI/sql_output'
    )
```

# Analyse du modèle

La librairie `gensim` a une fonction integrée pour calculer la similarité. Toutefois, nous devons cacluler le cosine manuelement dans Athena car il n'y a pas de fonction SQL prévue a cet effet. 


## Similarité - cosine

La fonction coseine se calcule de la facon suivante:

$$\frac{u \cdot v}{\|u\|_{2}\|v\|_{2}}$$

- Source 1: Calcul cosine:
  - [Cosine similarity](https://en.wikipedia.org/wiki/Cosine_similarity)
  - [Magnitude (mathematics)](https://en.wikipedia.org/wiki/Magnitude_(mathematics)#Euclidean_vector_space)
  - [Scipy Cosine](https://docs.scipy.org/doc/scipy/reference/generated/scipy.spatial.distance.cosine.html)

```python
from scipy.spatial import distance
from sklearn.manifold import TSNE
import numpy as np
import matplotlib.pyplot as plt
```

```python
import warnings
warnings.filterwarnings('ignore')
```

Ci dessous, un exemple des 10 premiers poids des mots `BOULEVARD`  et `BD`

```python
model['BOULEVARD'][:10]
```

```python
model['BD'][:10]
```

```python
1 - distance.cosine(model['BOULEVARD'], model['BD'])
```

```python
model.wv.similarity('BOULEVARD', 'BD')
```

Calcul à la main

```python
np.dot(model['BD'], model['BOULEVARD'])/ \
(np.sqrt(np.sum(np.square(model['BD']))) * np.sqrt(np.sum(np.square(model['BOULEVARD']))))
```

T-SNE plot pour les similarités entre les mots

```python
def display_closestwords_tsnescatterplot(model, word, size):
    
    fig= plt.figure(figsize=(10,10))
    
    arr = np.empty((0,size), dtype='f')
    word_labels = [word]
    close_words = model.similar_by_word(word)
    arr = np.append(arr, np.array([model[word]]), axis=0)
    for wrd_score in close_words:
        wrd_vector = model[wrd_score[0]]
        word_labels.append(wrd_score[0])
        arr = np.append(arr, np.array([wrd_vector]), axis=0)

    tsne = TSNE(n_components=2, random_state=0)
    np.set_printoptions(suppress=True)
    Y = tsne.fit_transform(arr)
    x_coords = Y[:, 0]
    y_coords = Y[:, 1]
    plt.scatter(x_coords, y_coords)
    for label, x, y in zip(word_labels, x_coords, y_coords):
            plt.annotate(label, xy=(x, y), xytext=(0, 0), textcoords='offset points')
    plt.xlim(x_coords.min()+0.00005, x_coords.max()+0.00005)
    plt.ylim(y_coords.min()+0.00005, y_coords.max()+0.00005)
    plt.show()
```

```python
display_closestwords_tsnescatterplot(model = model,
                                     word = 'BOULEVARD',
                                     size = 100)
```

```python
display_closestwords_tsnescatterplot(model = model,
                                     word = 'AVENUE',
                                     size = 100)
```

```python
display_closestwords_tsnescatterplot(model = model,
                                     word = 'ZI',
                                     size = 100)
```

```python
display_closestwords_tsnescatterplot(model = model,
                                     word = 'APPART',
                                     size = 100)
```

```python
display_closestwords_tsnescatterplot(model = model,
                                     word = 'CDT',
                                     size = 100)
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
