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

# Logs SIRETISATION

```python
import os, glob, shutil, json
os.chdir('../')
current_dir = os.getcwd()
from tqdm import tqdm
import pandas as pd
import numpy as np
from pathlib import Path
from inpi_insee import preparation_data
path = os.getcwd()
parent_path = str(Path(path).parent)
```

```python
from awsPy.aws_authorization import aws_connector
from awsPy.aws_s3 import service_s3
from awsPy.aws_athena import service_athena
bucket = 'calfdata'
path_cred = "{}/programme_matching/credential_AWS.json".format(parent_path)
con = aws_connector.aws_instantiate(credential = path_cred,
                                        region = 'eu-west-3')
client= con.client_boto()
s3 = service_s3.connect_S3(client = client,
                      bucket = 'calfdata') 
```

```python
path ="INPI/TC_1/03_siretisation/logs/ETS"
name = "inpi_initial_partiel_evt_new_ets_status_final_InitialPartielEVTNEW_logs.json"
for i in range(0, 7):
    s3.download_file(key= '{0}/{1}_{2}'.format(path, i, name))
```

```python
for i in range(0, 7):
    shutil.move("{0}/programme_matching/{1}_{2}".format(path,i, name),
            "/programme_matching/data/logs")
```

```python
data = []
for file in glob.glob(
    "{}/Data_preprocessed/programme_matching/data/logs/*.json".format(path)):
    with open(file) as f:
        for line in f:
            data.append(json.loads(line))
```

```python
logs = pd.json_normalize(data).sort_values(by = 'key')
logs
```

```python
logs[['total_match_rows_current']].sum()
```

```python
logs[['perc_total_match_rows_initial']].sum()
```

```python
logs[['perc_total_match_siren_initial']].sum()
```

```python
logs[['key',
    'perc_total_match_rows_initial',
      'perc_total_match_siren_initial']].set_index('key').plot.bar(stacked=False)
```

```python
logs[['total_match_rows_current']].plot.bar(stacked=False)
```

```python

```
