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

# Repliquer calcul cosine en Spark

# Objective(s)

Repliquer le code du calcul de la distance entre 2 listes de mots en Spark. Plus précisément, faire:
* créer 6 variables qui vont permettre a la réalisation des tests test_similarite_exception_words et test_distance_levhenstein_exception_words. Les six variables sont les suivantes:
  * unzip_inpi: Mot comparé coté inpi
  * unzip_insee: Mot comparé coté insee
  * max_cosine_distance: Score de similarité entre le mot compaté coté inpi et coté insee
  * levenshtein_distance: Nombre d'édition qu'il faut réaliser pour arriver à reproduire les deux mots
  * key_except_to_test: Champs clé-valeur pour toutes les possibiltés des mots qui ne sont pas en communs entre l'insee et l'inp

Metadata
* Epic: Epic 1
* US: US 1
* Date Begin: 10/13/2020
* Duration Task: 4
* Description: Traduire les codes SQL du calcul du Cosine en Spark
* Step type:  
* Status: Active
* Source URL: US 01 Transfert Spark
* Task type: Jupyter Notebook
* Users: Thomas Pernet
* Watchers: Thomas Pernet
* User Account: https://937882855452.signin.aws.amazon.com/console
* Estimated Log points: 10
* Task tag: #computation,#spark
* Toggl Tag: #data-preparation

# Knowledge

## List of candidates
* Calcul from scratch de la distance de cosine entre deux listes


## Connexion serveur

```python
from awsPy.aws_authorization import aws_connector
from awsPy.aws_s3 import service_s3
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

# Creation tables

## Steps

```python
from pyspark.sql import SparkSession

spark = (SparkSession 
    .builder 
    .appName("Python Spark SQL basic example") 
    .config('spark.executor.memory', '4G') 
    .getOrCreate()
        )
```

```python
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.debug.maxToStringFields", 1000)
```

```python
s3_output = 'SQL_OUTPUT_ATHENA'
database = 'ets_siretisation'

query = """
SELECT row_id, inpi_except, insee_except
FROM "ets_siretisation"."ets_insee_inpi_statut_cas"
WHERE inpi_except IS NOT NULL AND insee_except IS NOT NULL
LIMIT 10

"""

output = s3.run_query(
            query=query,
            database=database,
            s3_output=s3_output,
  filename = 'test_list_spark', ## Add filename to print dataframe
  destination_key = None ### Add destination key if need to copy output
        )
output.head()
```

```python
result = output.to_json(orient="records")
parsed = json.loads(result)
parsed
```

```python
list_ = []
for key, value in enumerate(parsed):
    dic = {
        'row_id':value['row_id'],
        'inpi_except':value['inpi_except'].strip('][').split(', ') ,
        'insee_except': value['insee_except'].strip('][').split(', ')
    }
    list_.append(dic)
with open('test_list.json', 'w') as outfile:
    json.dump(list_, outfile)
```

Recupération premier ID

```python
test_id = parsed[0]['row_id']
```

```python
df = spark.createDataFrame(list_)
df.printSchema()
```

```python
df.first()
```

Load weights

```python
from pyspark.sql.types import StructType, ArrayType, StringType, FloatType, MapType
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType
from pyspark.mllib.linalg import DenseVector, Vectors, VectorUDT
```

```python
list_to_vector_udf = F.udf(lambda x: Vectors.dense(x), VectorUDT())
```

```python
path_list = 'word2vec_weights_100_v2.csv'
schema  = (
    StructType()
    .add('words', StringType(),True)
    .add("list_weigths", ArrayType(FloatType(), True))
)

cols = [str(i) for i in range(1, 101)]
weights = (spark.read.csv(path_list, header = True)
           .select('0',(F.array(cols)).cast(ArrayType(FloatType(), True)).alias('list_weights'))
           .withColumnRenamed("0","words")
           #.select('words', list_to_vector_udf("list_weights").alias('list_weights'))
          )
weights.dtypes
```

```python
weights.show()
```

Function UDF

https://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=udf#pyspark.sql.functions.udf

```python
slen = F.udf(lambda s: len(s), IntegerType())
```

```python
(
    weights
    .select(
        'words',
        slen('words')
    )
    .show()
)
```

```python
#@F.udf(returnType=FloatType())
#def dot(x, y):
#    return Vectors.dense(x).dot(Vectors.dense(y))
```

# Calcul Cosine depuis deux listes en Spark 3.0


Comme la fonction du cosine est assez simple, il n'y a pas besoin de créer une fonction (et le décorateur). Une fonction lambda est amplement suffisante

```python
cosine = F.udf(lambda x, y: 
               (np.dot(x, y)/ (np.linalg.norm(x) * np.linalg.norm(y))).item(),
               FloatType())
```

```python
test = (
    df
    .filter("row_id = {}".format(test_id))
    .select(
        'row_id',
        F.expr(
        """
explode(
map_from_entries(    
 arrays_zip(
  inpi_except, 
  transform(
    sequence(
      1, 
      size(inpi_except)
    ), 
    x -> insee_except
    )
    )
  )
)
      """
                        )
         .alias("inpi", "value")
    )
       
    .select(
        'row_id',
        "inpi",
        F.explode_outer("value")
        .alias("insee")
   )
    .join((weights.withColumnRenamed("words","inpi")),
        on = ['inpi'], how = 'left')
    .withColumnRenamed("list_weights","list_weights_inpi")
    .join((weights.withColumnRenamed("words","insee")),
       on = ['insee'], how = 'left')
    .withColumnRenamed("list_weights","list_weights_insee")
    .select('row_id',
            'inpi',
            'insee',
            "list_weights_inpi",
            "list_weights_insee",
            cosine("list_weights_inpi", "list_weights_insee").alias("cosine"),
           )
)
```

```python
test.dtypes
```

```python
test.show(truncate = True)
```

Verification 

```python
df_pandas = test.toPandas()
```

```python
df_pandas.head()
```

# Calcul Cosine depuis deux listes en Spark, version < 2.2

```python
list_a = ["RUE", "CHARLES", "GILLE"]
list_b = ["BOULEVARD", "PREUILLY"]

test_list =[dict(zip([i], [list_b])) for i in list_a]
test_list
```

```python
zip_except = F.udf(lambda x, y: [dict(zip([i], [y])) for i in x],
                   ArrayType(MapType(StringType(), ArrayType(StringType()))))
```

```python
cosine = F.udf(lambda x, y: 
               (np.dot(x, y)/ (np.linalg.norm(x) * np.linalg.norm(y))).item(),
               FloatType())
```

```python
test = (
    df
    .filter("row_id = {}".format(test_id))
    .select(
        'row_id',
        'inpi_except',
        'insee_except',
        F.explode(zip_except("inpi_except","insee_except")).alias("zip_except")
    )
    .select(
    'row_id',
        'inpi_except',
        'insee_except',
        F.explode("zip_except").alias("inpi", "value")
    )
    .select(
    'row_id',
        'inpi_except',
        'insee_except',
        'inpi',
         F.explode("value")
        .alias("insee")
    )
    .join((weights.withColumnRenamed("words","inpi")),
        on = ['inpi'], how = 'left')
    .withColumnRenamed("list_weights","list_weights_inpi")
    .join((weights.withColumnRenamed("words","insee")),
       on = ['insee'], how = 'left')
    .withColumnRenamed("list_weights","list_weights_insee")
    .select('row_id',
            'inpi',
            'insee',
            "list_weights_inpi",
            "list_weights_insee",
            cosine("list_weights_inpi", "list_weights_insee").alias("cosine"),
           )
)

```

```python
test
```

```python
test.show(truncate =True)
```
