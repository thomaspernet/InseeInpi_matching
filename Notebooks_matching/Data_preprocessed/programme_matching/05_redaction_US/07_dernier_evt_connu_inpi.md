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

# Ajouter le dernier type d’événement connu 

```
En tant que {X} je souhaite {récupérer le dernier événement connu de l'INPI} afin de {pouvoir rattacher cette événement aux valeurs historisées d'une séquence}
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

## Règles de gestion

*   Définition partiel

    *   si csv dans le dossier Stock, année > 2017, alors partiel, c'est a dire, modification complète du dossier due a une anomalie.
    *   la date d’ingestion est indiquée dans le path, ie comme les flux
*   Une séquence est un classement chronologique pour le quadruplet suivant:

    *   _siren_ + _code greffe_ + _numero gestion_ + _ID établissement_
*  Une création d'une séquence peut avoir plusieurs transmission a des intervalles plus ou moins long
    *   Si plusieurs transmissions avec le libellé “création établissement” ou “création" , alors il faut prendre la dernière date de transmission
    *   Il y a certains cas ou les lignes de créations doublons sont de faux événements (mauvais envoie de la part du greffier)
        *   Si le timestamp entre la première ligne et dernière ligne est supérieures a 31 jour (exclut), il faut:
            *   Récupération de la dernière ligne, et créer une variable flag, comme pour le statut
*   Evénement 1
    *   Les événements doivent impérativement suivre l'ordre d'apparition dans le csv du FTP
        *   Pour les événements, il est possible d'avoir plusieurs informations renseignées pour une même date de transmission pour une même séquence
    *   Le remplissage doit se faire de la manière suivante pour la donnée brute
        *   Pour une date de transmission donnée, c'est la dernière ligne de la séquence qui doit être utilisée, remplie des valeurs manquantes extraites des lignes précédentes. Si la dernière ligne de la séquence contient un champs non vide, il ne faut pas la remplacer par la ligne précédente.
- Partiel
  - En cas de corrections majeures, la séquence annule et remplace la création et événements antérieurs. Dans ce cas, toutes les données qui ont pu être transmises antérieurement via le stock initial ou le flux doivent donc être ignorées (prendre en compte la date de transmission indiquée dans le nom des sous-répertoires du stock et des fichiers
-  Siren sans Siège ou Principal
  - Il est possible qu'un SIREN n'ai pas de siege/principal. Normalement, cela doit être corrigé par un partiel
-  Etablissement sans création
  - Il arrive que des établissements soient supprimés (EVT) mais n'ont pas de ligne "création d'entreprise". Si cela, arrive, Infogreffe doit envoyer un partiel pour corriger. Il arrive que le greffe envoie seulement une ligne pour SEP, lorsque le Principal est fermé, le siège est toujours ouvert. Mais pas de nouvelle ligne dans la base. Le partiel devrait corriger cela.
- La variable `ville` de l'INPI n'est pas normalisée. C'est une variable libre de la créativité du greffier, qui doit être formalisée du mieux possible afin de permettre la jointure avec l'INSEE. Plusieurs règles regex ont été recensé comme la soustraction des numéros, caractères spéciaux, parenthèses, etc. Il est possible d'améliorer les règles si nécessaire
- Le code postal doit être formalisé correctement, a savoir deux longueurs possibles: zero (Null) ou cinq. Dans certains cas, le code postal se trouve dans la variable de la ville.
- La variable pays doit être formalisée, a savoir correspondre au code pays de l'INSEE. Bien que la majeure partie des valeurs soit FRANCE ou France, il convient de normaliser la variable pour récuperer les informations des pays hors France.
- Les variables de l'adresse de l'INPI ne sont pas normalisées, et ne peuvent être utilisées en l'état. Il est donc indispensable de retravailler les variables adresse pour pouvoir les comparer avec l'INSEE. Nous utilisons une règle (pattern) regex pour vérifier si les mots contenus dans l'adresse de l'INPI sont aussi contenus à l'INSEE.
- L'INSEE codifie le type de voie de la manière suivante:
    - Si le type de voie est d'une longueur inférieure ou égale à 4 caractères, le type de voie n'est pas abrégé. Ainsi, RUE ou QUAI sont écrits tels quels, alors que AVENUE est abrégée en AV.
    - La codification de l'INSEE va donc être utilisé ppur créer la variable `voie_matching`
- Pour ne pas complexifié le processus de siretisation, seule le premier numéro de voie contenu dans l'adresse nettoyée est extraite pour ensuite etre matché avec l'INSEE.

Workflow US (via stock)

![workflow](https://www.lucidchart.com/publicSegments/view/d9e4494d-bfaf-4d0e-9e0f-53011cda7eb9/image.png)

# US / ISSUES liées

[PO & DEV : s'il existe des références, les inscrire]

# Besoin

Dans cette US, le besoin est le suivant:

- Création de la variables `last_libelle_evt`:
    - Extraction du dernier libellé de l'événement connu pour une séquence, et appliquer cette information à l'ensemble de la séquence


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
        - `siren`
        - `code_greffe`
        - `nom_greffe`
        - `numero_gestion`
        - `id_etablissement`
        - `date_greffe`
        - `libelle_evt`




### Exemple Input 1

Dans l'exemple ci dessous, nous avons une entreprise avec une création et suppression.

| siren     | code_greffe | nom_greffe      | numero_gestion | id_etablissement | date_greffe             | libelle_evt             |
|-----------|-------------|-----------------|----------------|------------------|-------------------------|-------------------------|
| 797407582 | 1301        | Aix-en-Provence | 2013B01887     | 1                | 2017-02-22 00:00:00.000 | Etablissement ouvert    |
| 797407582 | 1301        | Aix-en-Provence | 2013B01887     | 1                | 2019-01-09 00:00:00.000 | Etablissement supprimÃ© |


## Output

[PO : dans le cas de transformation de données, préciser les sorties :

*   BDD cibles
*   Tables: `inpi_etablissement_historique`
*   Champs: `last_libele_evt`

]

Après avoir récupérer le dernier événement connu, nous avons pu remplir l'information pour l'ensemble de la séquence

| siren     | code_greffe | nom_greffe      | numero_gestion | id_etablissement | date_greffe             | libelle_evt             | last_libele_evt         |
|-----------|-------------|-----------------|----------------|------------------|-------------------------|-------------------------|-------------------------|
| 797407582 | 1301        | Aix-en-Provence | 2013B01887     | 1                | 2017-02-22 00:00:00.000 | Etablissement ouvert    | Etablissement supprimÃ© |
| 797407582 | 1301        | Aix-en-Provence | 2013B01887     | 1                | 2019-01-09 00:00:00.000 | Etablissement supprimÃ© | Etablissement supprimÃ© |

<!-- #region -->
## Règles de gestion applicables

[PO : Formules applicables]

Si nouvelle règle, ajouter ici.

L'INSEE nous informe sur le dernier status connu, a savoir Actif ou Fermé, indépendament des événements passés. Toutefois, la table de l'INPI a des informations sur l'ensemble des événements. Il faut donc informer l'ensemble de la séquence historique sur son status le plus récent.

# Charges de l'équipe

[

PO : Si des étapes particulières / des points d'attention sont attendus, être aussi explicite que possible

Spécifiquement pour l'intégration de nouvelles données dans DATUM :

*   Nombre de lignes chargées pour chaque nouvelle table
*   Poids de chaque nouvelle table
*   Durée du traitement ajouté (+ durée avant et après)

]

Il faut procéder de la manière suivante:

- récupérer la date greffe maximum sur une séquence avec le libellé évément
- Matcher avec la table sur la séquence + date greffe max ppur remplir le dernier événement

Code SQL utilisé lors de nos tests:

```
SELECT 
          ROW_NUMBER() OVER () as index_id,
          voie_type_voie.siren, 
          voie_type_voie.code_greffe, 
          voie_type_voie.nom_greffe, 
          voie_type_voie.numero_gestion, 
          voie_type_voie.id_etablissement, 
          status, 
          origin, 
          date_greffe, 
          file_timestamp, 
          libelle_evt, 
          last_libele_evt, 
          CASE WHEN last_libele_evt = 'Etablissement ouvert' THEN 'A' ELSE 'F' END AS status_admin,
          type, 
          CASE WHEN type = 'SIE' OR type = 'SEP' THEN 'true' ELSE 'false' END AS status_ets,
          "siège_pm", 
          rcs_registre, 
          adresse_ligne1, 
          adresse_ligne2, 
          adresse_ligne3, 
          adress_reconstituee_inpi,
        -- adress_nettoyee, 
        -- adresse_inpi_reconstitue, 
          adress_regex_inpi,
          adress_distance_inpi,
          numero_voie_matching, 
          voie_clean, 
          voie_matching,
          code_postal, 
          code_postal_matching, 
          ville, 
          ville_matching, 
          code_commune, 
          pays, 
          domiciliataire_nom, 
          domiciliataire_siren, 
          domiciliataire_greffe, 
          "domiciliataire_complément", 
          "siege_domicile_représentant", 
          nom_commercial, 
          enseigne, 
          "activité_ambulante", 
          "activité_saisonnière", 
          "activité_non_sédentaire", 
          "date_début_activité", 
          "activité", 
          origine_fonds, 
          origine_fonds_info, 
          type_exploitation, 
          csv_source, 
          rn 
        FROM 
          voie_type_voie 
          INNER JOIN (
            SELECT 
              convert_date.siren, 
              convert_date.code_greffe, 
              convert_date.nom_greffe, 
              convert_date.numero_gestion, 
              convert_date.id_etablissement, 
              convert_date.libelle_evt as last_libele_evt, 
              max_date_greffe 
            FROM 
              convert_date 
              INNER JOIN (
                SELECT 
                  siren, 
                  code_greffe, 
                  nom_greffe, 
                  numero_gestion, 
                  id_etablissement, 
                  MAX(
                    Coalesce(
                      try(
                        date_parse(date_greffe, '%Y-%m-%d')
                      ), 
                      try(
                        date_parse(
                          date_greffe, '%Y-%m-%d %hh:%mm:%ss.SSS'
                        )
                      ), 
                      try(
                        date_parse(
                          date_greffe, '%Y-%m-%d %hh:%mm:%ss'
                        )
                      ), 
                      try(
                        cast(date_greffe as timestamp)
                      )
                    )
                  ) as max_date_greffe 
                FROM 
                  voie_type_voie 
                GROUP BY 
                  siren, 
                  code_greffe, 
                  nom_greffe, 
                  numero_gestion, 
                  id_etablissement
              ) AS temp ON temp.siren = convert_date.siren 
              AND temp.code_greffe = convert_date.code_greffe 
              AND temp.nom_greffe = convert_date.nom_greffe 
              AND temp.numero_gestion = convert_date.numero_gestion 
              AND temp.id_etablissement = convert_date.id_etablissement 
              AND temp.max_date_greffe = convert_date.date_greffe
          ) as latest_libele ON voie_type_voie.siren = latest_libele.siren 
          AND voie_type_voie.code_greffe = latest_libele.code_greffe 
          AND voie_type_voie.nom_greffe = latest_libele.nom_greffe 
          AND voie_type_voie.numero_gestion = latest_libele.numero_gestion 
          AND voie_type_voie.id_etablissement = latest_libele.id_etablissement
      )
    ORDER BY siren, code_greffe, nom_greffe, numero_gestion, id_etablissement, date_greffe
```

# Tests d'acceptance

[PO : comment contrôler que la réalisation est conforme]

- Trouver une séquence avec:

    - Ouverture + fermeture
    - Ouverture + événement + fermeture
    - Compter le nombre distinct d'établissement ouvert/fermé 
        - Le nombre d'établissement ouvert et ou fermé
- Pour chaque séquence trouvée, vérifier le status dans le moteur de recherche de l'[INPI](https://data.inpi.fr/)

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
