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

# Creation table INSEE

- Donnee Brute
    - https://www.data.gouv.fr/en/datasets/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret/
- Donnée S3
    - https://s3.console.aws.amazon.com/s3/buckets/calfdata/INSEE/00_rawData/ETS_01_07_2020/
- Champs:
    - 

```python
from awsPy.aws_authorization import aws_connector
from awsPy.aws_s3 import service_s3
from awsPy.aws_athena import service_athena
bucket = 'calfdata'
path_cred = "../credential_AWS.json"#.format(parent_path)
con = aws_connector.aws_instantiate(credential = path_cred,
                                        region = 'eu-west-3')
client= con.client_boto()
s3 = service_s3.connect_S3(client = client,
                      bucket = 'calfdata') 
athena = service_athena.connect_athena(client = client,
                      bucket = 'calfdata') 
```

```python
db = "inpi"
table_name = "insee_rawdata_juillet"
key_input = "s3://calfdata/INSEE/00_rawData/ETS_01_07_2020"

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

```

```python
athena.run_query(
    query=query_tb,
    database='inpi',
    s3_output='INPI/sql_output'
                )
```

```python
db = "inpi"
table_name = "insee_rawdata"
key_input = "s3://calfdata/INSEE/00_rawData/ETS"

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

```

```python
athena.run_query(
    query=query_tb,
    database='inpi',
    s3_output='INPI/sql_output'
                )
```

```python

```
