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

# Preparation INSEE/INPI PM

* Objective(s): 
  * Preparer les tables INSEE et INPI PM pour siretiser
* GitHub Branch: master 
* Notebook:04_prep_insee_pm 
* Steps:
  * Prepare l’insee selon le schéma défini INPI_PM
    * nommer la nouvelle table: insee_ul_prep
  * Prepare l’inpi pm selon le schéma défini INPI_PM
    * nommer la nouvelle table: inpi_pm_prep
  * Ajouter toutes les anomalies ici
  
![](https://drive.google.com/uc?export=view&id=1I3U83Y94z_z_EQA7KC1etPci_UbU9Nnm)

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

# Preparation INSEE

```python
output = athena.run_query(
                        query="DROP TABLE `insee_ul_prep`",
                        database = "inpi",
    s3_output='INSEE/sql_output')
```

```python
#Preparation table insee UL
query_tb = """
CREATE TABLE inpi.insee_ul_prep
WITH (
  format='PARQUET',
  external_location='s3://calfdata/INSEE/01_preparation/PM'
) AS
SELECT "siren", 
CONCAT(siren, nicSiegeUniteLegale) as siret,
"statutDiffusionUniteLegale", 
"unitePurgeeUniteLegale", 
"dateCreationUniteLegale", 
"sigleUniteLegale", 
"sexeUniteLegale", 
"prenom1UniteLegale", 
"prenom2UniteLegale", 
"prenom3UniteLegale", 
"prenom4UniteLegale", 
"prenomUsuelUniteLegale", 
"pseudonymeUniteLegale", 
"identifiantAssociationUniteLegale", 
"trancheEffectifsUniteLegale", 
"anneeEffectifsUniteLegale", 
"dateDernierTraitementUniteLegale", 
"nombrePeriodesUniteLegale", 
"categorieEntreprise", 
"anneeCategorieEntreprise", 
"dateDebut", 
"etatAdministratifUniteLegale", 
"nomUniteLegale", 
"nomUsageUniteLegale", 
UPPER(denominationUniteLegale) as denominationUniteLegale,
"denominationUsuelle1UniteLegale", 
"denominationUsuelle2UniteLegale", 
"denominationUsuelle3UniteLegale", 
"categorieJuridiqueUniteLegale", 
"activitePrincipaleUniteLegale", 
"nomenclatureActivitePrincipaleUniteLegale", 
"nicSiegeUniteLegale", 
"economieSocialeSolidaireUniteLegale", 
"caractereEmployeurUniteLegale"
FROM "inpi"."insee_ul" 
"""
athena.run_query(query_tb,
                 database='inpi',
                 s3_output='INSEE/sql_output'
                )
```
