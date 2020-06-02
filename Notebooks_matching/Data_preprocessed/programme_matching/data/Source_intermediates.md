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

<!-- #region Collapsed="false" -->
# Source intermediate

Dans ce notebook, il y a les informations relatives à la création des fichiers intermediares:

- Communes
<!-- #endregion -->

```python Collapsed="false"
import dask.dataframe as dd
import pandas as pd
#import Match_inpi_insee.aws_connectors as aws
#from tqdm.notebook import tqdm
#import tqdm
%load_ext autoreload
%autoreload 2
```

```python Collapsed="false"
path_commune = 'https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/raw' \
'/master/Notebooks_matching/Data_preprocessed/programme_matching/data/input' \
'/RawParameters/communes-01012019.csv'


```

<!-- #region Collapsed="true" -->
## Créer fichier toutes les possibilités communes

Le fichier provient de l'[INSEE](https://www.insee.fr/fr/information/3720946)
<!-- #endregion -->

```python Collapsed="false"
communes = (pd.read_csv(path_commune)
            .set_index('ncc')
            .reindex(columns=['nccenr', 'libelle'])
            .assign(
    noaccent=lambda x: x['nccenr'].str.normalize('NFKD')
    .str.encode('ascii', errors='ignore')
    .str.decode('utf-8'),
    nccenr_noponc=lambda x: x['nccenr'].str.replace('[^\w\s]', ' '),
    libelle_noponc=lambda x: x['libelle'].str.replace('[^\w\s]', ' '),
    noaccent_noponc=lambda x: x['noaccent'].str.replace('[^\w\s]', ' '),
    uppercase=lambda x: x.index,
    nccenr_uppercase=lambda x: x['nccenr'].str.upper(),
    libelle_uppercase=lambda x: x['libelle'].str.upper(),
    noaccent_uppercase=lambda x: x['noaccent'].str.upper(),
    nccenr_noponc_uppercase=lambda x: x['nccenr_noponc'].str.upper(),
    libelle_noponc_uppercase=lambda x: x['libelle_noponc'].str.upper(),
    noaccent_noponc_uppercase=lambda x: x['noaccent_noponc'].str.upper(),
    nccenr_lowercase=lambda x: x['nccenr'].str.lower(),
    libelle_lowercase=lambda x: x['libelle'].str.lower(),
    noaccent_lowercase=lambda x: x['noaccent'].str.lower(),
    nccenr_noponc_lowercase=lambda x: x['nccenr_noponc'].str.lower(),
    libelle_noponc_lowercase=lambda x: x['libelle_noponc'].str.lower(),
    noaccent_noponc_lowercase=lambda x: x['noaccent_noponc'].str.lower(),
    nccenr_noarrond1=lambda x: x['nccenr'].str.replace(
        'er Arrondissement', ''),
    uppercase_noarrond1=lambda x: x['uppercase'].str.replace(
        'ER ARRONDISSEMENT', ''),
    lowercase_noarrond1=lambda x: x['nccenr_lowercase'].str.replace(
        'er arrondissement', ''),
    nccenr_noarrond=lambda x: x['nccenr'].str.replace('e Arrondissement', ''),
    uppercase_noarrond=lambda x: x['uppercase'].str.replace(
        'E ARRONDISSEMENT', ''),
    lowercase_noarrond=lambda x: x['nccenr_lowercase'].str.replace(
        'e arrondissement', ''),
)
)

for n in communes.columns:
    var_ = '{}_ST'.format(n)
    var_1 = '{}_st'.format(n)
    var_2 = '{}_St'.format(n)
    
    communes[var_] = communes[n].str.replace('SAINT', 'ST')
    communes[var_1] = communes[n].str.replace('Saint', 'st')
    communes[var_2] = communes[n].str.replace('Saint', 'St')
    
    var_ = '{}_Sbar'.format(n)
    var_1 = '{}_sbar'.format(n)
    
    communes[var_] = communes[n].str.replace('SUR', 'S/')
    communes[var_1] = communes[n].str.replace('sur', 's/')
    
communes = (communes
            .stack()
            .rename('possibilite')
            .reset_index()
            .drop(columns='level_1')
            .drop_duplicates(subset=['possibilite']))
communes.head()
```

```python Collapsed="false"
#communes.to_csv('data\input\communes_france.csv', index = False)
```

<!-- #region Collapsed="true" -->
## Creation libelleVoieEtablissement

`libelleVoieEtablissement.csv`:
CSV recréé via extraction des données depuis site https://www.sirene.fr/sirene/public/variable/libelleVoieEtablissement
<!-- #endregion -->

```python
libelle = 'https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/raw' \
'/master/Notebooks_matching/Data_preprocessed/programme_matching/data/input' \
'/RawParameters/libelleVoieEtablissement.csv'

```

```python Collapsed="false"
voie = pd.read_csv(libelle)
voie.head()
```

```python Collapsed="false"
(voie.assign(upper_clean = lambda x: x['possibilite'].str.normalize(
                'NFKD')
            .str.encode('ascii', errors='ignore')
            .str.decode('utf-8')
            .str.replace('[^\w\s]|\d+', ' ')
            .str.upper(),
            lower_clean = lambda x: x['possibilite'].str.normalize(
                'NFKD')
            .str.encode('ascii', errors='ignore')
            .str.decode('utf-8')
            .str.replace('[^\w\s]|\d+', ' ')
            .str.lower()
        )
 .set_index('INSEE')
 .unstack() 
 .reset_index()
 #.drop(columns = 'level_0')
 .iloc[:,1:]
 .rename(columns = {0: 'possibilite'})
 .sort_values(by = 'INSEE')
 #.to_csv(r'data\input\voie.csv', index = False)
)
```

# Formz juridique INSEE/INPI

Pour siretiser les personnes morales, il faut utliser le champs forme juridique qui est sous forme de code à l'INSEE mais sous forme textuelle à l'INPI.

- La donnée de l'insee vient de cette URL https://www.insee.fr/fr/information/2028129
    - A noter qu'il y a un espace dans le libellé a la fin, du coup on l'a enlevé avant de renvoyer dans le S3
- La donnée de l'INPI vient de cette query https://eu-west-3.console.aws.amazon.com/athena/home?region=eu-west-3#query/history/870c671d-d51c-473a-9478-64218961d91a

```python
from awsPy.aws_authorization import aws_connector
from awsPy.aws_s3 import service_s3
from awsPy.aws_athena import service_athena
import os, time
from pathlib import Path
```

```python
bucket = 'calfdata'
path = os.getcwd()
parent_path = str(Path(path).parent)
path_cred = "{}/credential_AWS.json".format(parent_path)
con = aws_connector.aws_instantiate(credential = path_cred,
                                        region = 'eu-west-3')
client= con.client_boto()
s3 = service_s3.connect_S3(client = client,
                      bucket = 'calfdata') 
athena = service_athena.connect_athena(client = client,
                      bucket = 'calfdata') 
```

```python
inpi = s3.read_df_from_s3(key = 'INPI/TC_1/02_preparation_donnee/intermediate_file/forme_juridique_inpi.csv',
                sep = ',',encoding = None)
inpi.shape
```

```python
inpi_raw = s3.read_df_from_s3(
    'INPI/TC_1/02_preparation_donnee/intermediate_file/forme_juridique_inpi_origin.csv',
sep = ',',encoding = None)
inpi_raw.shape
```

```python
inpi.assign(Libellé = lambda x: 
         x['Libellé'].str.rstrip()).merge(inpi_raw,
           left_on = ['Libellé'],
          right_on = ['forme_juridique'],
                                          how = 'outer',
                                          indicator = True
                                         ).to_excel('missing_forme_juridique.xlsx', 
                                                  index= False)
```

```python
insee = s3.read_df_from_s3(key = 'INPI/TC_1/02_preparation_donnee/intermediate_file/forme_juridique_insee.csv',
                sep = ',',encoding = None)
insee.shape
```

```python
(insee
 .assign(Libellé = lambda x: 
         x['Libellé'].str.rstrip())
 .merge(inpi, left_on = ['Libellé'], right_on = ['forme_juridique'], 
        how = 'right',
       indicator = True))
```

```python
insee.assign(Libellé = lambda x: 
         x['Libellé'].str.rstrip()
            ).to_csv('forme_juridique_insee_cleaned.csv', index = False)
s3.upload_file('forme_juridique_insee_cleaned.csv',
            destination_in_s3=  'INPI/TC_1/02_preparation_donnee/intermediate_for_athena/forme_juridique_insee_cleaned.csv'
           )
```

```python
insee.head()
```

```python
query_tb = """CREATE EXTERNAL TABLE IF NOT EXISTS inpi.forme_juridique (
    `Libellé` string,
    `Code` string
    )
     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
   'separatorChar' = ',',
   'quoteChar' = '"'
   )
     LOCATION 's3://calfdata/INPI/TC_1/02_preparation_donnee/intermediate_for_athena'
     TBLPROPERTIES ('has_encrypted_data'='false',
              'skip.header.line.count'='1');"""
athena.run_query(query_tb,
                 database='inpi',
                 s3_output='INSEE/sql_output'
                )
```

```python

```
