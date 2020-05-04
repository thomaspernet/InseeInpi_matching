---
jupyter:
  jupytext:
    formats: ipynb,md
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.2'
      jupytext_version: 1.4.0+dev
  kernelspec:
    display_name: Python 3
    language: python
    name: python3
---

# Prepare ETS Data : normalize address

```python
import os, shutil
os.chdir('../')
current_dir = os.getcwd()
from inpi_insee import preparation_data
from awsPy.aws_authorization import aws_connector
from awsPy.aws_s3 import service_s3
from pathlib import Path
import pandas as pd
%load_ext autoreload
%autoreload 2
```

```python
#list(pd.read_csv('720df3ce-6e71-403d-b82f-845a5d82ac9d.csv'))
```

## Download from S3

```python
bucket = 'calfdata'
path = os.getcwd()
parent_path = str(Path(path).parent)
path_cred = r"{}/programme_matching/credential_AWS.json".format(parent_path)
con = aws_connector.aws_instantiate(credential = path_cred,
                                        region = 'eu-west-3')
client= con.client_boto()
s3 = service_s3.connect_S3(client = client,
                      bucket = 'calfdata') 
#s3.download_file(key= 'INPI/TC_1/02_preparation_donnee/ETS/initial_partiel_evt_new_ets_status_final.csv')
```

```python
#shutil.move(r"initial_partiel_evt_new_ets_status_final.csv",
#            r"data\RawData\INPI\Stock")
```

```python
param = {
    'communes_insee': r'data\input\Parameters\communes_france.csv',
    'upper_word':r'data\input\Parameters\upper_stop.csv',
     "voie":r'data\input\Parameters\voie.csv',
    'insee': r"data\RawData\INSEE\Stock\ETS\StockEtablissement_utf8.csv",
    'inpi_etb': r"data\RawData\INPI\Stock\initial_partiel_evt_new_ets_status_final.csv",
    'date_end':"2020-01-01"
}
prep_data = preparation_data.preparation(param)
```

Origin:

- Initial
- Partiel
- NEW
- EVT

```python
%%time
prep_data.normalize_inpi(
    origin ='EVT',
    save_gz = True,
    save_sql = False)
```

```python
%%time
prep_data.normalize_insee(
   "data\input\SIREN_INPI\inpi_SIREN_stock_initial.gz",
    save_gz = True,
    save_sql = False)
```

# AWS service


## Move to S3

```python
s3.upload_file(
    file_to_upload = r'data\input\INPI\inpi_stock_initial_0.csv',
    destination_in_s3 = 'INPI/TC_1/02_preparation_donnee/Stock/ETB')
```

```python
s3.upload_file(
    file_to_upload = r'data\input\INSEE\insee_2017_7649840.csv',
    destination_in_s3 = 'INPI/TC_1/02_preparation_donnee/Stock/ETB')
```

## Move to Athena

Lien vers [Athena](https://eu-west-3.console.aws.amazon.com/athena/home?force&region=eu-west-3#query)

```python
from awsPy.aws_athena import service_athena
import pandas as pd
```

```python
athena = service_athena.connect_athena(client = client,
                      bucket = 'calfdata') 
```

```python
df = pd.read_csv(r'data\input\INPI\inpi_stock_initial_0.gz', low_memory=False)
```

A automatiser please...

```python
for i in df.columns:
    if df[i].dtype == 'int64':
        print("`{}` int,".format(i))
    else:
        print("`{}` string,".format(i))
```

```python
# Athena database and table definition
database = 'inpi'
table = 'stock_partiel_prepared'
s3_input = 's3://calfdata/INPI/TC_1/02_preparation_donnee/Stock/ETB'
s3_ouput = 'INPI/sql_output'

create_database = "CREATE DATABASE IF NOT EXISTS %s;" % (database)
create_table = \
    """CREATE EXTERNAL TABLE IF NOT EXISTS %s.%s (
`code greffe` int,
`nom_greffe` string,
`numero_gestion` string,
`rcs_registre` string,
`date_greffe` string,
`libelle_evt` string,
`id_etablissement` int,
`siren` int,
`nom_commercial` string,
`enseigne` string,
`date_début_activité` string,
`domiciliataire_nom` string,
`domiciliataire_siren` string,
`count_initial_inpi` int,
`domiciliataire_greffe` string,
`domiciliataire_complément` string,
`type` string,
`siège_pm` string,
`activité` string,
`origine_fonds` string,
`origine_fonds_info` string,
`type_exploitation` string,
`pays` string,
`ville` string,
`ncc` string,
`code_postal` string,
`code_commune` string,
`adresse_ligne1` string,
`adresse_ligne2` string,
`adresse_ligne3` string,
`adress_new` string,
`adresse_new_clean_reg` string,
`possibilite` string,
`insee` string,
`digit_inpi` string,
`list_digit_inpi` string,
`len_digit_address_inpi` int,
`siege_domicile_représentant` string,
`activité_ambulante` string,
`activité_saisonnière` string,
`activité_non_sédentaire` string,
`index` int
    )
     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
   'separatorChar' = ',',
   'quoteChar' = '"'
   )
     LOCATION '%s'
     TBLPROPERTIES ('has_encrypted_data'='false',
              'skip.header.line.count'='1');""" % (
    database,
    table,
    s3_input
)
athena.run_query(
    query=create_database,
    database=database,
    s3_output=s3_ouput
)
athena.run_query(
    query=create_table,
    database=database,
    s3_output=s3_ouput
)
```
