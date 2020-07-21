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

<!-- #region -->
# Rapprochement INSEE UL ETS

```
Entant que {X} je souhaite {normaliser la variable pays} afin de {pouvoir la faire correspondre à l'INSEE}
```

**Metadatab**

- Taiga:
    - Numero US: []()
- Gitlab
    - Notebook: []()
    - Markdown: []()
    - Data:
        - []()
        - 

# Contexte


# US / ISSUES liées

[PO & DEV : s'il existe des références, les inscrire]

# Besoin

Dans cette US, le besoin est le suivant:

- créer une table contenant les informations sur les PP/PM et établissements a l'INPI et les informations sur les UL et etablissments à l'INSEE

![](https://app.lucidchart.com/publicSegments/view/2a1a8c67-097c-4022-931e-6ee13d24371b/image.png)
<!-- #endregion -->

# Spécifications

### Origine information (si applicable) 

- Metadata:
    - Type
    - Source
    - Summary
    
## Input

[PO : dans le cas de transformation de données, préciser ,les sources :

*   Applications
*   Schémas
*   Tables: `inpi_etablissement_historique`
*   CSV: 
*   Champs: 




### Exemple Input 1

XXX

**Snippet**

- [Snippet 1]()

```python
import pandas as pd
import numpy as np
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
query = """
WITH remove_empty_siret AS (
  SELECT 
  siren, nic, siret,regexp_like(siret, '\d+') as test_siret, statutdiffusionetablissement, datecreationetablissement, trancheeffectifsetablissement, anneeeffectifsetablissement, activiteprincipaleregistremetiersetablissement, datederniertraitementetablissement, etablissementsiege, nombreperiodesetablissement, complementadresseetablissement, numerovoieetablissement, indicerepetitionetablissement, typevoieetablissement, libellevoieetablissement, codepostaletablissement, libellecommuneetablissement, libellecommuneetrangeretablissement, distributionspecialeetablissement, codecommuneetablissement, codecedexetablissement, libellecedexetablissement, codepaysetrangeretablissement, libellepaysetrangeretablissement, complementadresse2etablissement, numerovoie2etablissement, indicerepetition2etablissement, typevoie2etablissement, libellevoie2etablissement, codepostal2etablissement, libellecommune2etablissement, libellecommuneetranger2etablissement, distributionspeciale2etablissement, codecommune2etablissement, codecedex2etablissement, libellecedex2etablissement, codepaysetranger2etablissement, libellepaysetranger2etablissement, datedebut, etatadministratifetablissement, enseigne1etablissement, enseigne2etablissement, enseigne3etablissement, denominationusuelleetablissement, activiteprincipaleetablissement, nomenclatureactiviteprincipaleetablissement, caractereemployeuretablissement
  FROM 
  insee_rawdata -- WHERE siren = '797406154'  
    -- WHERE siren = '797406188'
    ) 
SELECT *
FROM 
  (
    WITH concat_adress AS(
      SELECT 
      siren, nic, siret, statutdiffusionetablissement, datecreationetablissement, trancheeffectifsetablissement, anneeeffectifsetablissement, activiteprincipaleregistremetiersetablissement, datederniertraitementetablissement, etablissementsiege, nombreperiodesetablissement, complementadresseetablissement, numerovoieetablissement, indicerepetitionetablissement, typevoieetablissement, libellevoieetablissement, codepostaletablissement, libellecommuneetablissement, libellecommuneetrangeretablissement, distributionspecialeetablissement, codecommuneetablissement, codecedexetablissement, libellecedexetablissement, codepaysetrangeretablissement, libellepaysetrangeretablissement, complementadresse2etablissement, numerovoie2etablissement, indicerepetition2etablissement, typevoie2etablissement, libellevoie2etablissement, codepostal2etablissement, libellecommune2etablissement, libellecommuneetranger2etablissement, distributionspeciale2etablissement, codecommune2etablissement, codecedex2etablissement, libellecedex2etablissement, codepaysetranger2etablissement, libellepaysetranger2etablissement, datedebut, etatadministratifetablissement, enseigne1etablissement, enseigne2etablissement, enseigne3etablissement, denominationusuelleetablissement, activiteprincipaleetablissement, nomenclatureactiviteprincipaleetablissement, caractereemployeuretablissement,
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              REGEXP_REPLACE(
                CONCAT(
                  COALESCE(numeroVoieEtablissement, ''), 
                  COALESCE(
                    indiceRepetitionEtablissement, ''
                  ), 
                  ' ', 
                  COALESCE(voie_clean, ''), 
                  ' ', 
                  -- besoin sinon exclu
                  COALESCE(libelleVoieEtablissement, ''), 
                  ' ', 
                  COALESCE(
                    complementAdresseEtablissement, 
                    ''
                  )
                ), 
                '[^\w\s]| +', 
                ' '
              ), 
              '(?:^|(?<= ))(AU|AUX|AVEC|CE|CES|DANS|DE|DES|DU|ELLE|EN|ET|EUX|IL|ILS|LA|LE|LES)(?:(?= )|$)', 
              ''
            ), 
            '\s\s+', 
            ' '
          ), 
          '^\s+|\s+$', 
          ''
        ) AS adress_reconstituee_insee, ville_matching,voie_clean
      FROM 
        (
          SELECT 
          siren, nic, siret, statutdiffusionetablissement, datecreationetablissement, trancheeffectifsetablissement, anneeeffectifsetablissement, activiteprincipaleregistremetiersetablissement, datederniertraitementetablissement, etablissementsiege, nombreperiodesetablissement, complementadresseetablissement, numerovoieetablissement, indicerepetitionetablissement, typevoieetablissement, libellevoieetablissement, codepostaletablissement, libellecommuneetablissement, libellecommuneetrangeretablissement, distributionspecialeetablissement, codecommuneetablissement, codecedexetablissement, libellecedexetablissement, codepaysetrangeretablissement, libellepaysetrangeretablissement, complementadresse2etablissement, numerovoie2etablissement, indicerepetition2etablissement, typevoie2etablissement, libellevoie2etablissement, codepostal2etablissement, libellecommune2etablissement, libellecommuneetranger2etablissement, distributionspeciale2etablissement, codecommune2etablissement, codecedex2etablissement, libellecedex2etablissement, codepaysetranger2etablissement, libellepaysetranger2etablissement, datedebut, etatadministratifetablissement, enseigne1etablissement, enseigne2etablissement, enseigne3etablissement, denominationusuelleetablissement, activiteprincipaleetablissement, nomenclatureactiviteprincipaleetablissement, caractereemployeuretablissement,
            REGEXP_REPLACE(
              REGEXP_REPLACE(
                REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          libelleCommuneEtablissement, 
                          '^\d+\s|\s\d+\s|\s\d+$', 
                          -- digit
                          ''
                        ), 
                        '^LA\s+|^LES\s+|^LE\s+|\\(.*\\)|^L(ES|A|E) | L(ES|A|E) | L(ES|A|E)$|CEDEX | CEDEX | CEDEX|^E[R*] | E[R*] | E[R*]$', 
                        ''
                      ), 
                      '^STE | STE | STE$|^STES | STES | STES', 
                      'SAINTE'
                    ), 
                    '^ST | ST | ST$', 
                    'SAINT'
                  ), 
                  'S/|^S | S | S$', 
                  'SUR'
                ), 
                '/S', 
                'SOUS'
              ), 
              '[^\w\s]|\([^()]*\)|ER ARRONDISSEMENT|E ARRONDISSEMENT|" \
"|^SUR$|CEDEX|[0-9]+|\s+', 
              ''
            ) as ville_matching, test_siret 
          FROM 
            remove_empty_siret
        ) 
        LEFT JOIN type_voie ON typevoieetablissement = type_voie.voie_matching 
      WHERE 
        test_siret = true
    ) 
    SELECT 
    count_initial_insee,
    concat_adress.siren, 
    statutdiffusionetablissement, 
    dateCreationEtablissement, 
    trancheeffectifsetablissement, 
    anneeeffectifsetablissement, 
    activiteprincipaleregistremetiersetablissement, 
    datederniertraitementetablissement, 
    nombreperiodesetablissement, 
    etatAdministratifEtablissement,
    etablissementSiege, 
    codePostalEtablissement, 
    codeCommuneEtablissement, 
    libelleCommuneEtablissement, 
    ville_matching, 
    numeroVoieEtablissement, 
    typeVoieEtablissement, 
    voie_clean, 
    libelleVoieEtablissement, 
    complementAdresseEtablissement, 
    adress_reconstituee_insee
    indiceRepetitionEtablissement, 
    enseigne1Etablissement, 
    enseigne2Etablissement, 
    enseigne3Etablissement, 
    distributionspecialeetablissement, 
    codecedexetablissement, 
    libellecedexetablissement, 
    codepaysetrangeretablissement, 
    libellepaysetrangeretablissement, 
    complementadresse2etablissement, 
    numerovoie2etablissement, 
    indicerepetition2etablissement, 
    typevoie2etablissement, 
    libellevoie2etablissement, 
    codepostal2etablissement, 
    libellecommune2etablissement, 
    libellecommuneetranger2etablissement, 
    distributionspeciale2etablissement, 
    codecommune2etablissement, 
    codecedex2etablissement, 
    libellecedex2etablissement, 
    codepaysetranger2etablissement, 
    libellepaysetranger2etablissement, 
    datedebut, 
    denominationusuelleetablissement, 
    activiteprincipaleetablissement, 
    nomenclatureactiviteprincipaleetablissement, 
    caractereemployeuretablissement 
    FROM 
      concat_adress 
      LEFT JOIN (
        SELECT 
          siren, 
          COUNT(siren) as count_initial_insee 
        FROM 
          concat_adress 
        GROUP BY 
          siren
      ) as count_siren ON concat_adress.siren = count_siren.siren
  ) as temp 
 LEFT JOIN insee_ul 
 ON insee_ul.siren = temp.siren
 limit 10 
"""
```

### Exemple Input 2

XXX

**Snippet**

- [Snippet 2]()


## Output

[PO : dans le cas de transformation de données, préciser les sorties :

*   BDD cibles
*   Tables: `inpi_etablissement_historique`
*   Champs: 

]

XXX

<!-- #region -->
## Règles de gestion applicables

[PO : Formules applicables]

Si nouvelle règle, ajouter ici.

# Charges de l'équipe

[

PO : Si des étapes particulières / des points d'attention sont attendus, être aussi explicite que possible

Spécifiquement pour l'intégration de nouvelles données dans DATUM :

*   Nombre de lignes chargées pour chaque nouvelle table
*   Poids de chaque nouvelle table
*   Durée du traitement ajouté (+ durée avant et après)

]

# Tests d'acceptance

[PO : comment contrôler que la réalisation est conforme]

**Code reproduction**

```
```


# CONCEPTION

Conception réalisée par ............. et ..................

[DEV :

Important :

*   Ce chapitre doit impérativement être complété **avant de basculer l'US à 'développement en cours'**
*   La conception doit systématiquement être **faite à deux**
*   Il ne doit **pas y avoir de code dans ce chapitre**
*   Tout au long du développement, ce chapitre doit être enrichi
*   Le nom du binôme ayant participé à la conception doit être précisé dans l'US

Contenu :

*   Décrire les traitements nouveaux / modifiés : emplacement des fichiers (liens vers GIT), mise en avant des évolutions fortes, impacts dans la chaîne d'exploitation
*   Points d'attention spécifiques : notamment sur les règles de gestion et leur mise en oeuvre technique

]

# Evolution de la documentation

[DEV :

*   Identifier les champs enrichis dans le dictionnaire de données
*   Identifier les impacts dans les documents pérennes DTA, DEXP, Consignes de supervision
*   Identifier les impacts dans les documents de MEP (FI)

]

# Tests réalisés

[DEV : préciser les tests réalisés pour contrôler le bon fonctionnement, et les résultats obtenus]

# Tests automatiques mis en oeuvre

[DEV : préciser les TA et expliciter leur fonctionnement]

# Démonstration

[DEV : suivant le cas, publier sur le sharepoint et mettre un lien ici soit :

*   Capture d'écran
*   Vidéo publiée

]
<!-- #endregion -->

# Creation markdown

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
    extension: string. Can be "html", "pdf", "markdown"
    
    
    """
    
    ### Get notebook name
    connection_file = os.path.basename(ipykernel.get_connection_file())
    kernel_id = connection_file.split('-', 1)[1].split('.')[0]

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
    parent_path = str(Path(path).parent)
    
    ### Path report
    #path_report = "{}/Reports".format(parent_path)
    #path_report = "{}/Reports".format(path)
    
    ### Path destination
    name_no_extension = notebookname.split(sep, 1)[0]
    if extension == 'markdown':
        #extension = 'md'
        os.remove(name_no_extension +'.{}'.format('md'))
        source_to_move = name_no_extension +'.{}'.format('md')
    else:
        source_to_move = name_no_extension +'.{}'.format(extension)
    dest = os.path.join(path,'US_md', source_to_move)
    
    print('jupyter nbconvert --no-input --to {} {}'.format(
    extension,notebookname))
    
    ### Generate notebook
    os.system('jupyter nbconvert --no-input --to {} {}'.format(
    extension,notebookname))
    
    ### Move notebook to report folder
    #time.sleep(5)
    shutil.move(source_to_move, dest)
    print("Report Available at this adress:\n {}".format(dest))
```

```python
create_report(extension = "markdown")
```
