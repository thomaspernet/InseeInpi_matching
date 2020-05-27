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

# Insee UL to Athena

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
varname = s3.read_df_from_s3(key = 'INSEE/variables_name/dessinstockunitelegale.csv',
                sep = ',',encoding = None)
```

## Create Table

```python
output = athena.run_query(
                        query="DROP TABLE `insee_ul`",
                        database = "inpi",
    s3_output='INSEE/sql_output')
```

```python
query_tb = \
    """CREATE EXTERNAL TABLE IF NOT EXISTS inpi.insee_ul (
    {0}

    )
     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
   'separatorChar' = ',',
   'quoteChar' = '"'
   )
     LOCATION 's3://calfdata/INSEE/00_rawData/UL'
     TBLPROPERTIES ('has_encrypted_data'='false',
              'skip.header.line.count'='1');"""
```

```python
varname.iloc[i, 0]
```

```python
field = ""
for i, v in enumerate(varname.sort_values(by = 'Ordre')['Type']):
    if i != varname.shape[0] -1:
        if v == "Numérique":
            field += "`{}` integer, \n".format(varname.sort_values(
                by = 'Ordre').iloc[i, 0])
        else:
             field += "`{}` string, \n".format(varname.sort_values(
                 by = 'Ordre').iloc[i, 0])
    else:
        if v == "Numérique":
            field += "`{}` integer \n".format(varname.sort_values(
                by = 'Ordre').iloc[i, 0])
        else:
             field += "`{}` string \n".format(varname.sort_values(
                 by = 'Ordre').iloc[i, 0])
```

```python
print(query_tb.format(field))
```

```python
athena.run_query(query_tb.format(field),
                 database='inpi',
                 s3_output='INSEE/sql_output'
                )
```

```python
print(query_tb.format(field))
```
