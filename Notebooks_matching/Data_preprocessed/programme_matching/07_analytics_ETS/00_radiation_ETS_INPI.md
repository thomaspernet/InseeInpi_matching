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

# Radiation ETS 

## Objectif

* L’information sur les radiations n’est pas présente dans la table ETS. Il faut regarder les tables PM et PP pour savoir si l’entreprise (ou établissement) est radiée. Dès lors, se pose les questions suivantes:
  * Pourquoi l’INPI ne crée pas de notification dans les CSV ETS lors d’une radiation.
  * On c’est qu’une PM ou PP peut avoir plusieurs inscription selon si elle agit dans plusieurs territoires (plusieurs greffes). 
    * Si une PM/PP est radiée sur un territoire, est ce que cela affecte l’ensemble des établissements qu’elle a au niveau national ou uniquement sur le territoire d’implantation des établissements (niveau greffe).
      * Etablissements
* Définir avec clarté la différence entre une suppression et une radiation 
* Création d’une table appelée ets_pm_pp_inpi avec les champs suivants:
  *   ets_final_sql.siren, 
  *   ets_final_sql.code_greffe , 
  *   ets_final_sql.nom_greffe , 
  *   ets_final_sql.numero_gestion , 
  *   id_etablissement  , 
  *   type , 
  *   type_inscription ,
  *   ets_final_sql.status , 
  *   date_radiation , 
  *   last_libele_evt , 
  *   origin , 
  *   ets_final_sql.date_greffe , 
  *   ets_final_sql.file_timestamp , 
  *   adresse_reconstituee_inp 
  * et la création des champs suivants:
    *  date_radiation_min,
    * entreprise_radiee 
    * divergence_radiation_suppression
    
## Metadata

* Input Cloud Storage [AWS/GCP]: If link from the internet, save it to the cloud first
  * S3
    *  File (csv/json): 
      * Notebook construction file (data lineage, md link from Github) 
        * md :
        * py :
  * Athena 
    * Region: eu-west-3 
    * Database: inpi 
    *  Table: ets_final_sql 
    * Notebook construction file (data lineage) 
      * md : [03_ETS_add_variables.md](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/03_ETS_add_variables.md)
    * Table: pp_test_filtered 
    * Notebook construction file (data lineage) 
      * md : [01_Athena_concatenate_PP.md](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/01_Athena_concatenate_PP.md)
    * Table: pm_test_filtered 
    * Notebook construction file (data lineage) 
      * md : [01_Athena_concatenate_PM.md](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/01_Athena_concatenate_PM.md)

## Output

* Athena: 
    * Region: eu-west-3 
    * Database: inpi 
    *  Table: `ets_pm_pp_inpi`


## Radiation PM

```python
query = """
CREATE TABLE inpi.ets_pm_radiation_analytics
WITH (
  format='PARQUET'
) AS

SELECT 
  ets_final_sql.siren, 
  ets_final_sql.code_greffe, 
  ets_final_sql.nom_greffe, 
  ets_final_sql.numero_gestion, 
  id_etablissement, 
  type, 
  type_inscription,
  ets_final_sql.status, 
  date_radiation_min,
  date_radiation, 
  last_libele_evt, 
  origin, 
  ets_final_sql.date_greffe, 
  ets_final_sql.file_timestamp, 
  adresse_reconstituee_inpi,
  CASE WHEN date_radiation_min IS NOT NULL THEN TRUE ELSE FALSE END AS entreprise_radie,
  
  CASE WHEN date_radiation_min IS NOT NULL AND last_libele_evt != 'Etablissement supprimé' 
  THEN TRUE ELSE FALSE END AS divergence_radiation_suppression
  
FROM 
  ets_final_sql 
  INNER JOIN (
    WITH pm AS (
  select 
    initial_partiel_evt_new_pm_status_final.siren, 
    initial_partiel_evt_new_pm_status_final."code greffe" as code_greffe, 
    initial_partiel_evt_new_pm_status_final.nom_greffe, 
    initial_partiel_evt_new_pm_status_final.numero_gestion, 
    status, 
    origin, 
    initial_partiel_evt_new_pm_status_final.date_greffe, 
    file_timestamp, 
    max_timestamp, 
    type_inscription, 
    date_immatriculation, 
    date_1re_immatriculation, 
    date_radiation, 
    date_transfert, 
    "sans_activité", 
    "date_debut_activité", 
    "date_début_1re_activité", 
    "date_cessation_activité", 
    denomination, 
    sigle, 
    forme_juridique, 
    "associé_unique", 
    "activité_principale", 
    type_capital, 
    capital, 
    capital_actuel, 
    devise, 
    date_cloture, 
    date_cloture_except, 
    economie_sociale_solidaire, 
    "durée_pm", 
    libelle_evt, 
    csv_source 
  FROM 
    initial_partiel_evt_new_pm_status_final 
    INNER JOIN (
      select 
        siren, 
        "code greffe", 
        numero_gestion, 
        date_greffe, 
        max(file_timestamp) as max_timestamp 
      from 
        initial_partiel_evt_new_pm_status_final 
      -- WHERE 
      --  siren = '317253433' 
      GROUP BY 
        siren, 
        "code greffe", 
        numero_gestion, 
        date_greffe
    ) as max_time ON initial_partiel_evt_new_pm_status_final.siren = max_time.siren 
    AND initial_partiel_evt_new_pm_status_final."code greffe" = max_time."code greffe" 
    AND initial_partiel_evt_new_pm_status_final.numero_gestion = max_time.numero_gestion 
    AND initial_partiel_evt_new_pm_status_final.date_greffe = max_time.date_greffe 
  WHERE 
    file_timestamp = max_timestamp 
  ORDER BY 
    siren, 
    code_greffe, 
    numero_gestion, 
    date_greffe
) 
SELECT 
  pm.siren, 
    date_radiation,
  date_radiation_min, 
  code_greffe, 
  nom_greffe, 
  numero_gestion,
    type_inscription
FROM pm 
INNER JOIN (
SELECT 
  siren, 
  MIN(
    Coalesce(
      try(
        date_parse(date_radiation, '%Y-%m-%d')
      ), 
      try(
        date_parse(
          date_radiation, '%Y-%m-%d %hh:%mm:%ss.SSS'
        )
      ), 
      try(
        date_parse(
          date_radiation, '%Y-%m-%d %hh:%mm:%ss'
        )
      ), 
      try(
        cast(date_radiation as timestamp)
      )
    )
  ) as date_radiation_min 
FROM 
  pm 
GROUP BY 
  siren
) as temp
ON pm.siren = temp.siren
    
    
  ) as pm 
  ON ets_final_sql.siren = pm.siren 
  AND ets_final_sql.code_greffe = pm.code_greffe 
  AND ets_final_sql.nom_greffe = pm.nom_greffe 
  AND ets_final_sql.numero_gestion = pm.numero_gestion -- WHERE 
  --  siren = '317253433' 
ORDER BY 
  siren, 
  code_greffe, 
  nom_greffe, 
  numero_gestion, 
  type,
  date_greffe, 
  file_timestamp
"""
```

```python

```

```python

```

## Analytics




Nombre d'établissements radié

```python
query = """
SELECT entreprise_radiee, COUNT(entreprise_radiee) as count_
FROM (
SELECT siren, 
    entreprise_radiee, 
    COUNT(*) AS CNT
FROM "inpi"."ets_pm_radiation_analytics" 
GROUP BY siren, entreprise_radiee
ORDER BY siren DESC
  ) 
  GROUP BY entreprise_radiee
  ORDER BY count_ DESC
"""
```

Nombre de divergence, a savoir le nombre de pm qui sont des libéllé fermé mais pas supprimé

```python
query = """
SELECT divergence_radiation_suppression, COUNT(divergence_radiation_suppression) as count_
FROM (
SELECT siren, 
    divergence_radiation_suppression, 
    COUNT(*) AS CNT
FROM "inpi"."ets_pm_radiation_analytics" 
GROUP BY siren, divergence_radiation_suppression
ORDER BY siren DESC
  ) 
  GROUP BY divergence_radiation_suppression
  ORDER BY count_ DESC
"""
```

Liste entreprise radié mais encore ouvert dans la table établissement

```python
query = """
SELECT DISTINCT(siren)
FROM ets_pm_radiation_analytics 
WHERE entreprise_radiee = TRUE and divergence_radiation_suppression = TRUE
"""
```

Nombre entreprise radié mais pas dans tous les greffes

```python
query = """
SELECT count(*) as nb_ent_pas_complement_ferme
FROM (
SELECT siren, COUNT(DISTINCT(nom_greffe))
FROM ets_pm_radiation_analytics 
WHERE  entreprise_radiee = TRUE and date_radiation = ''
GROUP BY siren, nom_greffe
  )

"""
```

Nb entreprise pas complement ferme par greffe

```python
query = """
SELECT nb_greffe_pas_complement_ferme, count(nb_greffe_pas_complement_ferme)  as count_
FROM(
SELECT siren, count(siren) as nb_greffe_pas_complement_ferme
FROM (
SELECT siren, COUNT(DISTINCT(nom_greffe))
FROM ets_pm_radiation_analytics 
WHERE  entreprise_radiee = TRUE and date_radiation = ''
GROUP BY siren, nom_greffe
  )
  GROUP BY siren
  )
  GROUP BY nb_greffe_pas_complement_ferme
  ORDER BY count_
"""
```
