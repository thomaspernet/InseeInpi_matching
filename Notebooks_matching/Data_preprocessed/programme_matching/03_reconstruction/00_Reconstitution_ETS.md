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

# Reconstitution INPI


## Recreation du fichier INPI 

Le notebook comporte 4 parties:

- Description champs [INPI ETS](https://github.com/thomaspernet/InseeInpi_matching/tree/master/Documentation/IMR#etablissements)

1. Reconsitution Data
    - Matched:
        - Input:
        - output: 
            - CSV: [03_siretisation/match/ETS](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/TC_1/03_siretisation/match/ETS/)
            - Table: `inpi_siret_initial_partiel_ets_matched` 
    - Unmatched
        - Input:
        - output:
            - CSV: [03_siretisation/Non_match/ETS](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/TC_1/03_siretisation/Non_match/ETS/)
            - Table: `inpi_siret_initial_partiel_ets_unmatched`
    - Treatement speciaux
        - Input:
        - output:
            - CSV: [03_siretisation/special_treatment/ETS](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/TC_1/03_siretisation/special_treatment/ETS/)
            - Table: `inpi_siret_initial_partiel_ets_ts`
2. Details sequence
3. Rapport logs

```python
import os
os.chdir('../')
current_dir = os.getcwd()
from tqdm import tqdm
import pandas as pd
import numpy as np
from pathlib import Path
from inpi_insee import preparation_data
path = os.getcwd()
parent_path = str(Path(path).parent)
pd.set_option('display.max_columns', None)

%load_ext autoreload
%autoreload 2
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
athena = service_athena.connect_athena(client = client,
                      bucket = 'calfdata') 
```

# Paramètres

```python
origin = "InitialPartielEVTNEW"
filename = "inpi_initial_partiel_evt_new_ets_status_final_InitialPartielEVTNEW_0.csv"
#origin = "NEW"
#filename = "inpi_initial_partiel_evt_new_ets_status_final_NEW_0.csv"
path_data_merge = "programme_matching/data/output"
path_data_siren_inpi = "programme_matching/data/input/SIREN_INPI"
path_data_initial = "programme_matching/data/input/INPI"
path_data_st = "programme_matching/data/input/INPI/special_treatment"
```

```python
dtypes = {
    "siren"           :                    "object",
"type"                 :                   "object",
"adress_new"            :                  "object",
"adresse_new_clean_reg"  :                 "object",
"INSEE"                   :                "object",
"digit_inpi"               :               "object",
"list_digit_inpi"           :              "object",
"len_digit_address_inpi"     :              "int64",
"code_postal"                 :           "object",
"ville"                        :           "object",
"ncc"                           :          "object",
"code_commune"                   :         "object",
"pays"                            :        "object",
"count_initial_inpi"               :        "int64",
"date_debut_activite"             :        "object",
"index"                            :        "int64",
"siret"                             :       "object",
"dateCreationEtablissement"          :     "object",
"count_initial_insee"                 :   "int64",
"etablissementSiege"                   :     "object",
"complementAdresseEtablissement"        :  "object",
"numeroVoieEtablissement"               : "object",
"indiceRepetitionEtablissement"         :  "object",
"typeVoieEtablissement"                 :  "object",
"libelleVoieEtablissement"              :  "object",
"len_digit_address_insee"                :"int64",
"list_digit_insee"                       : "object",
"codePostalEtablissement"                :"object", #27
"libelleCommuneEtablissement"            : "object",
"libelleCommuneEtrangerEtablissement"    : "object",
"distributionSpecialeEtablissement"      : "object",
"codeCommuneEtablissement"               : "object",
"codeCedexEtablissement"                 :"object",
"libelleCedexEtablissement"              : "object",
"codePaysEtrangerEtablissement"          :"object",
"libellePaysEtrangerEtablissement"       : "object",
"etatAdministratifEtablissement"         : "object",
"origin_test"                            : "object",
"count_siren_siret"                      :"object",
"test_address_libelle"                   :   "object",
"test_address_complement"                :   "object",
"test_join_address"                      :   "object",
"test_date"                              :   "object",
"test_1"                                 :"object",
"test_siege"                             :   "object",
"test_voie"                              :   "object",
"test_numero"                            :   "object",
"count_duplicates_final"                 :  "int64",
"count_duplicates_"                      :  "int64",
"test"                                   : "object"
}

dtypes_or = {
'siren': 'string',
 'code_greffe': 'string',
 'nom_greffe': 'string',
 'numero_gestion': 'string',
 'id_etablissement': 'string',
 'status': 'string',
 'origin': 'string',
 'file_timestamp': 'string',
 'date_greffe': 'string',
 'libelle_evt': 'string',
 'type': 'string',
 'siege_pm': 'string',
 'rcs_registre': 'string',
 'adresse_ligne1': 'string',
 'adresse_ligne2': 'string',
 'adresse_ligne3': 'string',
 'adress_new': 'string',
 'adresse_new_clean_reg': 'string',
 'possibilite': 'string',
 'INSEE': 'string',
 'digit_inpi': 'string',
 'list_digit_inpi': 'string',
 'len_digit_address_inpi': 'int64',
 'code_postal': 'string',
 'ville': 'string',
 'ncc': 'string',
 'code_commune': 'string',
 'pays': 'string',
 'domiciliataire_nom': 'string',
 'domiciliataire_siren': 'string',
 'count_initial_inpi': 'int64',
 'domiciliataire_greffe': 'string',
 'domiciliataire_complement': 'string',
 'Siege_domicile_representant': 'string',
 'nom_commercial': 'string',
 'enseigne': 'string',
 'activite_ambulante': 'string',
 'activite_saisonniere': 'string',
 'activite_Non_Sedentaire': 'string',
 'date_debut_activite': 'string',
 'activite': 'string',
 'origine_fonds': 'string',
 'origine_fonds_info': 'string',
 'type_exploitation': 'string',
 'csv_source': 'string',
 'index': 'int64'
}
```

# Reconsitution Data

Pour chaque categorie (Matched, Umatched, TS), 3 steps sont réalisées:

- Append des csv/gz
- Sauvegarde csv local
- Upload dans le S3
- Creation table Athena


## Matched

- Input:
    - CSV: 
        - `data/input/INPI` + `ORIGIN` + [`FILENAME` + `ORIGIN` +  `_O.csv`]
            - ex: `data/input/INPI/NEW/inpi_initial_partiel_evt_new_ets_status_final_NEW_0.csv`
        - `data/output/` + `ORIGIN` + [`i_FILENAME` + `ORIGIN` +  `_+[not_duplicate/pure_match].csv`]
            - `data/output/NEW/0_inpi_initial_partiel_evt_new_ets_status_final_InitialPartielEVT_not_duplicate.gz`
- output: 
    - CSV: [03_siretisation/match/ETS/](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/TC_1/03_siretisation/match/ETS/)
    - Table: `inpi_siret_initial_partiel_ets_matched`


Append des csv/gz

```python
%%time
list_issue = []
df_matched = pd.DataFrame()
for root, dirs, files in os.walk(os.path.join(parent_path,path_data_merge,origin)):
    for name in tqdm(files):
        if name.endswith((".gz")):
            path_gz = '{}/{}'.format(root, name)
            df_ = pd.read_csv(path_gz, compression = 'gzip',low_memory = False, dtype = dtypes)
            df_matched = df_matched.append(
            df_
            )

```

```python
%%time
df_inpi = pd.read_csv(
    os.path.join(parent_path,path_data_initial,origin, filename),
    low_memory = False, 
    dtype = {
        'siren': 'string',
 'code_greffe': 'string'
        
    }
)
#df_inpi.shape[0] - df_matched.shape[0]
```

```python
df_inpi.loc[lambda x:
                   x['siren'].isin(['849981527'])]
```

```python
df_inpi.dtypes
```

Sauvegarde csv local

```python
reindex = ['siren','siret','code_greffe','nom_greffe','numero_gestion',
 'id_etablissement','status','origin','file_timestamp','date_greffe',
 'libelle_evt','type','siege_pm','rcs_registre','adresse_ligne1',
 'adresse_ligne2','adresse_ligne3','adress_new','adresse_new_clean_reg',
 'possibilite','INSEE','digit_inpi','list_digit_inpi','len_digit_address_inpi',
 'code_postal','ville','ncc','code_commune','pays','domiciliataire_nom',
 'domiciliataire_siren','count_initial_inpi','domiciliataire_greffe',
 'domiciliataire_complement','Siege_domicile_representant','nom_commercial',
 'enseigne', 'activite_ambulante','activite_saisonniere',
 'activite_Non_Sedentaire','date_debut_activite','activite','origine_fonds',
 'origine_fonds_info','type_exploitation','csv_source','index','origin_test',
 'count_initial_insee','count_siren_siret','test_address_libelle',
 'test_address_complement','test_join_address','test_date','test_1',
 'test_siege','test_voie','test_numero','count_duplicates_final',
 'count_duplicates_','test']


df_matched_full = df_inpi.merge(df_matched[
    ['index','siret','origin_test','count_initial_insee', 'count_siren_siret',
     'test_address_libelle','test_address_complement','test_join_address',
     'test_date','test_1','test_siege','test_voie','test_numero',
     'count_duplicates_final','count_duplicates_','test'
     ]
], on='index', how='inner').reindex(columns = reindex)

path_save = '{}/programme_matching/data/' \
'inpi_initial_partiel_evt_ets_status_final_{}.csv'.format(parent_path,
                                                        df_matched_full.shape[0]
                                                        )

```

```python
df_matched_full.dtypes
```

```python
df_matched.loc[lambda x:
                   x['siren'].isin(['849981527'])]
```

```python
df_matched_full.loc[lambda x:
                   x['siren'].isin(['849981527'])]
```

```python
df_matched_full.to_csv(
    path_save
    .format(
    df_matched_full.shape[0]),
    index = False) 
```

```python
df_matched_full.shape
```

Upload data dans le S3: [03_siretisation/match/ETS/](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/TC_1/03_siretisation/match/ETS/) et creation table dans Athena

```python
key = 'INPI/TC_1/03_siretisation/match/ETS'
s3.remove_file(key)
```

```python
s3.upload_file(
    file_to_upload = path_save,
    destination_in_s3 = key)
```

```python
os.remove(path_save)
```

```python
db = "inpi"
table_name = "inpi_siret_initial_partiel_ets_matched"
key_input = "s3://calfdata/INPI/TC_1/03_siretisation/match/ETS"

query = """
DROP TABLE `{}`;
""".format(table_name)
athena.run_query(
    query=query,
    database='inpi',
    s3_output='INPI/sql_output'
                )
```

```python
query_tb = \
    """CREATE EXTERNAL TABLE IF NOT EXISTS {0}.{1} (
    `siren` string , `siret` string , `code_greffe` string ,
    `Nom_greffe` string , `numero_gestion` string ,
    `id_etablissement` string , `status` string ,
    `origin` string , `file_timestamp` string ,
    `date_greffe` string , `libelle_evt` string ,
    `type` string , `siege_pm` string , `rcs_registre` string ,
    `adresse_ligne1` string , `adresse_ligne2` string ,
    `adresse_ligne3` string , `adress_new` string ,
    `adresse_new_clean_reg` string , `possibilite` string ,
    `INSEE` string , `digit_inpi` string ,
    `list_digit_inpi` string , `len_digit_address_inpi` string ,
    `code_postal` string , `ville` string , `ncc` string ,
    `code_commune` string , `pays` string ,
    `domiciliataire_nom` string , `domiciliataire_siren` string ,
    `count_initial_inpi` string , `domiciliataire_greffe` string ,
    `domiciliataire_complement` string ,
    `Siege_domicile_representant` string ,
    `nom_commercial` string , `enseigne` string ,
    `activite_ambulante` string , `activite_saisonniere` string ,
    `activite_Non_Sedentaire` string , `date_debut_activite` string ,
    `activite` string , `origine_fonds` string ,
    `origine_fonds_info` string , `type_exploitation` string ,
    `csv_source` string , `index` string , `origin_test` string ,
    `count_initial_insee` string , `count_siren_siret` string , 
    `test_address_libelle` string , `test_address_complement` string ,
    `test_join_address` string , `test_date` string ,
    `test_1` string , `test_siege` string , `test_voie` string ,
    `test_numero` string , `count_duplicates_final` string , 
    `count_duplicates_` string , `test` string )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
    WITH SERDEPROPERTIES ( 'separatorChar' = ',', 'quoteChar' = '"' ) 
    LOCATION '{2}' 
    TBLPROPERTIES ('has_encrypted_data'='false', 'skip.header.line.count'='1');
""".format(db,table_name,key_input)
athena.run_query(
    query=query_tb,
    database='inpi',
    s3_output='INPI/sql_output'
                )
```

### non matched Treatment speciaux

- Input:
    - CSV: 
        - `data/INPI/special_treatment` + `ORIGIN` + [`i_ FILENAME` + `ORIGIN` +`.gz`]
            - `data/INPI/special_treatment/`
            - `0_inpi_initial_partiel_evt_new_ets_status_final_InitialPartielEVT_special_treatment.gz`
- output: 
    - CSV: [03_siretisation/special_treatment/ETS/](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/TC_1/03_siretisation/special_treatment/ETS/)
    - Table: `inpi_siret_initial_partiel_ets_TS`

```python
df_ts = pd.DataFrame()
for root, dirs, files in os.walk(os.path.join(parent_path,path_data_st,origin)):
    for name in tqdm(files):
        if name.endswith((".gz")):
            path_gz = '{}/{}'.format(root, name)
            df_ = pd.read_csv(path_gz,
                              compression = 'gzip',
                              low_memory = False,
                              dtype = dtypes)
            df_ts = df_ts.append(
            df_
            )
```

Sauvegarde csv local

```python
path_save = '{}/programme_matching/data/' \
'inpi_initial_partiel_evt_ets_status_final_TS_{}.csv'.format(parent_path,
                                                        df_ts.shape[0]
                                                        )
df_ts.to_csv(
    path_save
    .format(
    df_ts.shape[0]),
    index = False) 
```

Upload data dans le S3: [03_siretisation/special_treatment/ETS/](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/TC_1/03_siretisation/special_treatment/ETS/) et creation table dans Athena

```python
"INPI/TC_1/03_siretisation/special_treatment/ETS/" \
"inpi_initial_partiel_evt_ets_status_final_TS_205004.csv"
s3.remove_file(key)
```

```python
key = 'INPI/TC_1/03_siretisation/special_treatment/ETS'
s3.upload_file(
    file_to_upload = path_save,
    destination_in_s3 = key)
os.remove(path_save)
```

```python
db = "inpi"
table_name = "inpi_siret_initial_partiel_ets_TS"
key_input = "s3://calfdata/INPI/TC_1/03_siretisation/special_treatment/ETS"

query = """
DROP TABLE `{}`;
""".format(table_name)
athena.run_query(
    query=query,
    database='inpi',
    s3_output='INPI/sql_output'
                )
```

```python
query_tb = \
    """CREATE EXTERNAL TABLE IF NOT EXISTS {0}.{1} (
    `siren` string , `siret` string , `code_greffe` string ,
    `Nom_greffe` string , `numero_gestion` string ,
    `id_etablissement` string , `status` string ,
    `origin` string , `file_timestamp` string ,
    `date_greffe` string , `libelle_evt` string ,
    `type` string , `siege_pm` string , `rcs_registre` string ,
    `adresse_ligne1` string , `adresse_ligne2` string ,
    `adresse_ligne3` string , `adress_new` string ,
    `adresse_new_clean_reg` string , `possibilite` string ,
    `INSEE` string , `digit_inpi` string ,
    `list_digit_inpi` string , `len_digit_address_inpi` string ,
    `code_postal` string , `ville` string , `ncc` string ,
    `code_commune` string , `pays` string ,
    `domiciliataire_nom` string , `domiciliataire_siren` string ,
    `count_initial_inpi` string , `domiciliataire_greffe` string ,
    `domiciliataire_complement` string ,
    `Siege_domicile_representant` string ,
    `nom_commercial` string , `enseigne` string ,
    `activite_ambulante` string , `activite_saisonniere` string ,
    `activite_Non_Sedentaire` string , `date_debut_activite` string ,
    `activite` string , `origine_fonds` string ,
    `origine_fonds_info` string , `type_exploitation` string ,
    `csv_source` string , `index` string , `origin_test` string ,
    `count_initial_insee` string , `count_siren_siret` string , 
    `test_address_libelle` string , `test_address_complement` string ,
    `test_join_address` string , `test_date` string ,
    `test_1` string , `test_siege` string , `test_voie` string ,
    `test_numero` string , `count_duplicates_final` string , 
    `count_duplicates_` string , `test` string )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
    WITH SERDEPROPERTIES ( 'separatorChar' = ',', 'quoteChar' = '"' ) 
    LOCATION '{2}' 
    TBLPROPERTIES ('has_encrypted_data'='false', 'skip.header.line.count'='1');
""".format(db,table_name,key_input)
athena.run_query(
    query=query_tb,
    database='inpi',
    s3_output='INPI/sql_output'
                )
```

### Unmatched

- Input:
    - Pandas DataFrame:
        - `df_inpi`: Créer en step 1
    - List `index` des matches: via DataFrame step 1
        - `df_matched_full['index']`
- output: 
    - CSV: [03_siretisation/Non_match/ETS/](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/TC_1/03_siretisation/Non_match/ETS/)
    - Table: `inpi_siret_initial_partiel_ets_Unmatched`


Sauvegarde csv local

```python
path_save = '{}/programme_matching/data/' \
'inpi_initial_partiel_evt_ets_status_final_unmatched_{}.csv'.format(parent_path,
                                                        df_inpi.shape[0] - df_matched.shape[0]
                                                        )
df_inpi.loc[lambda x: 
            ~x['index'].isin(df_matched_full['index'].to_list())
           ].to_csv(
    path_save
    .format(
    df_ts.shape[0]),
    index = False) 
```

Upload data dans le S3: [03_siretisation/Non_match/ETS/](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/TC_1/03_siretisation/Non_match/ETS/) et creation table dans Athena

```python
key = "INPI/TC_1/03_siretisation/Non_match/ETS/" \
"inpi_initial_partiel_evt_ets_status_final_unmatched_558898.csv"

s3.remove_file(key)
```

```python
key = 'INPI/TC_1/03_siretisation/Non_match/ETS'
s3.upload_file(
    file_to_upload = path_save,
    destination_in_s3 = key)
os.remove(path_save)
```

```python
db = "inpi"
table_name = "inpi_siret_initial_partiel_ets_Unmatched"
key_input = "s3://calfdata/INPI/TC_1/03_siretisation/Non_match/ETS"

query = """
DROP TABLE `{}`;
""".format(table_name)
athena.run_query(
    query=query,
    database='inpi',
    s3_output='INPI/sql_output'
                )
```

```python
query_tb = \
    """CREATE EXTERNAL TABLE IF NOT EXISTS {0}.{1} (
`siren` string,`code_greffe` string,`Nom_greffe` string,
 `numero_gestion` string,`id_etablissement` string,`status` string,
 `origin` string,`file_timestamp` string, `date_greffe` string,
 `libelle_evt` string,`type` string,`siege_pm` string,
 `rcs_registre` string,`adresse_ligne1` string,`adresse_ligne2` string,
 `adresse_ligne3` string,`adress_new` string,`adresse_new_clean_reg` string,
 `possibilite` string,`INSEE` string,`digit_inpi` string,
 `list_digit_inpi` string,`len_digit_address_inpi` string,`code_postal` string,
 `ville` string, `ncc` string,`code_commune` string, `pays` string,
 `domiciliataire_nom` string,`domiciliataire_siren` string,
 `count_initial_inpi` string,`domiciliataire_greffe` string,
 `domiciliataire_complement` string,`Siege_domicile_representant` string,
 `nom_commercial` string,`enseigne` string,`activite_ambulante` string,
 `activite_saisonniere` string,`activite_Non_Sedentaire` string,
 `date_debut_activite` string,`activite` string,`origine_fonds` string,
 `origine_fonds_info` string,`type_exploitation` string,`csv_source` string,
 `index` string
    )
     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
   'separatorChar' = ',',
   'quoteChar' = '"'
   )
     LOCATION '{2}'
     TBLPROPERTIES ('has_encrypted_data'='false',
              'skip.header.line.count'='1');""".format(db,table_name,key_input)
                                                    
athena.run_query(
    query=query_tb,
    database='inpi',
    s3_output='INPI/sql_output'
                )
```

### Remove Files

```python
for root, dirs, files in os.walk(os.path.join(parent_path,path_data_merge,origin)):
    for name in tqdm(files):
        if name.endswith((".gz")):
            path_gz = '{}/{}'.format(root, name)
            os.remove(path_gz)
```

```python
for root, dirs, files in os.walk(os.path.join(parent_path,path_data_st,origin)):
    for name in tqdm(files):
        if name.endswith((".gz")):
            path_gz = '{}/{}'.format(root, name)
            os.remove(path_gz)
```

```python
for root, dirs, files in os.walk(os.path.join(parent_path,path_data_initial,origin)):
    for name in tqdm(files):
        if name.endswith((".csv")):
            path_gz = '{}/{}'.format(root, name)
            os.remove(path_gz)
```

```python
for root, dirs, files in os.walk(os.path.join(parent_path,path_data_siren_inpi,origin)):
    for name in tqdm(files):
        if name.endswith((".csv")):
            path_gz = '{}/{}'.format(root, name)
            os.remove(path_gz)
```

### Remove folder

```python
import shutil
```

```python
shutil.rmtree(os.path.join(parent_path,path_data_initial,origin))
```

```python
shutil.rmtree(os.path.join(parent_path,path_data_st,origin))
```

```python
shutil.rmtree(os.path.join(parent_path,path_data_merge,origin))
```

```python
shutil.rmtree(os.path.join(parent_path,path_data_siren_inpi,origin))
```

## Insee


### Raw Data

- Input: Déjà dans le S3
    - CSV: 
        - `INSEE/00_rawData/StockEtablissement_utf8.csv`
- output: 
    - CSV: [01_preparation/ETS/](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INSEE/00_rawData/)
    - Table: `insee_rawdata`

```python
db = "inpi"
table_name = "insee_rawdata"
key_input = "s3://calfdata/INSEE/00_rawData"

query_tb = \
    """CREATE EXTERNAL TABLE IF NOT EXISTS {0}.{1} (
`siren`   string, 
 `nic`   string, 
 `siret`   string, 
 `statutDiffusionEtablissement`   string, 
 `dateCreationEtablissement`   string, 
 `trancheEffectifsEtablissement`   string, 
 `anneeEffectifsEtablissement`   string, 
 `activitePrincipaleRegistreMetiersEtablissement`   string, 
 `dateDernierTraitementEtablissement`   string, 
 `etablissementSiege`   string, 
 `nombrePeriodesEtablissement`   string, 
 `complementAdresseEtablissement`   string, 
 `numeroVoieEtablissement`   string, 
 `indiceRepetitionEtablissement`   string, 
 `typeVoieEtablissement`   string, 
 `libelleVoieEtablissement`   string, 
 `codePostalEtablissement`   string, 
 `libelleCommuneEtablissement`   string, 
 `libelleCommuneEtrangerEtablissement`   string, 
 `distributionSpecialeEtablissement`   string, 
 `codeCommuneEtablissement`   string, 
 `codeCedexEtablissement`   string, 
 `libelleCedexEtablissement`   string, 
 `codePaysEtrangerEtablissement`   string, 
 `libellePaysEtrangerEtablissement`   string, 
 `complementAdresse2Etablissement`   string, 
 `numeroVoie2Etablissement`   string, 
 `indiceRepetition2Etablissement`   string, 
 `typeVoie2Etablissement`   string, 
 `libelleVoie2Etablissement`   string, 
 `codePostal2Etablissement`   string, 
 `libelleCommune2Etablissement`   string, 
 `libelleCommuneEtranger2Etablissement`   string, 
 `distributionSpeciale2Etablissement`   string, 
 `codeCommune2Etablissement`   string, 
 `codeCedex2Etablissement`   string, 
 `libelleCedex2Etablissement`   string, 
 `codePaysEtranger2Etablissement`   string, 
 `libellePaysEtranger2Etablissement`   string, 
 `dateDebut`   string, 
 `etatAdministratifEtablissement`   string, 
 `enseigne1Etablissement`   string, 
 `enseigne2Etablissement`   string, 
 `enseigne3Etablissement`   string, 
 `denominationUsuelleEtablissement`   string, 
 `activitePrincipaleEtablissement`   string, 
 `nomenclatureActivitePrincipaleEtablissement`   string, 
 `caractereEmployeurEtablissement` string

    )
     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
   'separatorChar' = ',',
   'quoteChar' = '"'
   )
     LOCATION '{2}'
     TBLPROPERTIES ('has_encrypted_data'='false',
              'skip.header.line.count'='1');""".format(db,table_name,key_input)
athena.run_query(
    query=query_tb,
    database='inpi',
    s3_output='INPI/sql_output'
                )
```

### Data préparée

- Input:
    - CSV: 
        - `data/input/INSEE/` + `ORIGIN` + [`insee` + `size`+`ORIGIN` +`.csv`]
            - `data/input/INSEE/InitialPartielEVT/insee_8272605_InitialPartielEVT.csv`
- output: 
    - CSV: [01_preparation/ETS/](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INSEE/01_preparation/ETS/)
    - Table: `insee_siret_initial_partiel_ets`

```python
dic_ = {
    'InitialPartielEVTNEW':"insee_9428972_InitialPartielEVTNEW",
    #'NEW':"insee_1745311_NEW",
}
key_s3 = "INSEE/01_preparation/ETS"

for key, value in dic_.items():
    path_insee = 'programme_matching/' \
'data/input/INSEE/{0}/{1}.csv'.format(key, value)
    s3.upload_file(
    file_to_upload = os.path.join(parent_path, path_insee),
    destination_in_s3 = key_s3)
```

```python
os.remove(os.path.join(parent_path, path_insee))
```

```python
shutil.rmtree(os.path.join(parent_path, "data/input/INSEE", origin))
```

```python
db = "inpi"
table_name = "insee_siret_initial_partiel_ets"
key_input = "s3://calfdata/INSEE/01_preparation/ETS"

query = """
DROP TABLE `{}`;
""".format(table_name)
athena.run_query(
    query=query,
    database='inpi',
    s3_output='INPI/sql_output'
                )
```

```python
query_tb = \
    """CREATE EXTERNAL TABLE IF NOT EXISTS {0}.{1} (
`siren`   string, 
 `siret`   string, 
 `dateCreationEtablissement`   string, 
 `count_initial_insee`   string, 
 `etablissementSiege`   string, 
 `complementAdresseEtablissement`   string, 
 `numeroVoieEtablissement`   string, 
 `indiceRepetitionEtablissement`   string, 
 `typeVoieEtablissement`   string, 
 `libelleVoieEtablissement`   string, 
 `len_digit_address_insee`   string, 
 `list_digit_insee`   string, 
 `codePostalEtablissement`   string, 
 `libelleCommuneEtablissement`   string, 
 `libelleCommuneEtrangerEtablissement`   string, 
 `distributionSpecialeEtablissement`   string, 
 `codeCommuneEtablissement`   string, 
 `codeCedexEtablissement`   string, 
 `libelleCedexEtablissement`   string, 
 `codePaysEtrangerEtablissement`   string, 
 `libellePaysEtrangerEtablissement`   string, 
 `etatAdministratifEtablissement`   string, 
 `index` string

    )
     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
   'separatorChar' = ',',
   'quoteChar' = '"'
   )
     LOCATION '{2}'
     TBLPROPERTIES ('has_encrypted_data'='false',
              'skip.header.line.count'='1');""".format(db,table_name,key_input)

athena.run_query(
    query=query_tb,
    database='inpi',
    s3_output='INPI/sql_output'
                )
```

## Génération Detail sequence


```python
from inpi_insee import siretisation
import matplotlib.pyplot as plt
```

```python
siretisation.crate_graph_report_test(df_matched_full)
```

```python
siretisation.create_graph_report(df_matched_full)
```

## Rapport sur les logs

```python
import glob, os, json
import pandas as pd
data = []
#os.chdir(r"data\logs\")
for file in glob.glob("{}/programme_matching/data/logs/{}/*.json".format(parent_path,
                                                                         origin)):
    with open(file) as f:
        for line in f:
            data.append(json.loads(line))
```

```python
logs = pd.json_normalize(data)
logs
```

Nombre de lignes sirétisées

```python
logs[['total_match_rows_current']].sum()
```

Pourcentage de lignes sirétisées

```python
logs[['perc_total_match_rows_initial']].sum()
```

Nombre de lignes ayant été trouvé à l'INSEE

```python
logs[['perc_total_match_siren_initial']].sum()
```

Graphique sur la séquence ayant aidée à la sirétisation

```python
logs[['perc_total_match_rows_initial',
      'perc_total_match_siren_initial']].plot.bar(stacked=False)
```

Graphique sur la séquence avec le nombre de lignes sirétisées

```python
logs[['total_match_rows_current']].plot.bar(stacked=False)
```

Nombre de SIREN et index non sirétisés

```python
logs[["df_duplication.df_sp_index.nb_index",
      'df_duplication.df_sp_index.unique_siren']].plot.bar(stacked=False)
```

### Test generation SIREN aléatoire

Utile pour faire des vérifications dans l'App

```python
pd.set_option('display.max_rows', None)
df_matched_full.loc[np.random.randint(low= 1, high= 7000000, size=1)[0]].reset_index()
```

```python
df_matched_full.loc[lambda x: x['siren'].isin(['448416636'])].reset_index()#.T
```
