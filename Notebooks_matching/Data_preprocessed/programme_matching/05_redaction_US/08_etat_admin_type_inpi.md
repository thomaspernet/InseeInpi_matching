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
# Ajouter les variables état administratif et type d’entreprise  

```
Entant que {X} je souhaite {créer une variable indiquant le status administratif et le type d'établissement} afin de {connaitre le status a date de l'établissement car l'INSEE n'informe que le dernier status connu}
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

- `status_admin`:
    - Informe du status ouvert/fermé concernant une séquence
- `status_ets`:
    - Informe du type d'établissement (SIE/PRI.SEC) concernant une séquence


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
    - `last_libele_evt`
    - `type `




### Exemple Input 1: status administratif

Dans l'exemple ci dessous, nous avons une séquence avec:

- Une ouverture + un événement
- Une ouverture + une fermeture

| siren     | code_greffe | nom_greffe | numero_gestion | id_etablissement | date_greffe             | libelle_evt                                | last_libele_evt                            |
|-----------|-------------|------------|----------------|------------------|-------------------------|--------------------------------------------|--------------------------------------------|
| 342109071 | 7501        | Paris      | 1987D01764     | 1                | 2014-01-28 00:00:00.000 | Etablissement ouvert                       | Modifications relatives à un établissement |
| 342109071 | 7501        | Paris      | 1987D01764     | 1                | 2018-04-30 00:00:00.000 | Modifications relatives à un établissement | Modifications relatives à un établissement |
| 342109071 | 7501        | Paris      | 1987D01764     | 3                | 2014-01-28 00:00:00.000 | Etablissement ouvert                       | Etablissement supprimé                     |
| 342109071 | 7501        | Paris      | 1987D01764     | 3                | 2018-04-30 00:00:00.000 | Etablissement supprimé                     | Etablissement supprimé                     |
| 342109071 | 7501        | Paris      | 1987D01764     | 4                | 2018-05-03 00:00:00.000 | Modifications relatives à un établissement | Modifications relatives à un établissement |

<!-- #region -->
### Exemple Input 2: status etablissement

Dans l'exemple ci dessous, nous avons deux séquences pour la même entreprise:

- Un siège
- Un principal


| siren     | code_greffe | nom_greffe | numero_gestion | id_etablissement | date_greffe             | type |
|-----------|-------------|------------|----------------|------------------|-------------------------|------|
| 797409612 | 9201        | Nanterre   | 2013D01713     | 1                | 2013-09-26 00:00:00.000 | SIE  |
| 797409612 | 9201        | Nanterre   | 2013D01713     | 2                | 2013-09-26 00:00:00.000 | PRI  |
<!-- #endregion -->

## Output

[PO : dans le cas de transformation de données, préciser les sorties :

*   BDD cibles
*   Tables: `inpi_etablissement_historique`
*   Champs: 
    - `status_admin`
    - `status_ets`

]

### Exemple 1: `status_admin`

| siren     | code_greffe | nom_greffe | numero_gestion | id_etablissement | date_greffe             | libelle_evt                                | last_libele_evt                            | status_admin |
|-----------|-------------|------------|----------------|------------------|-------------------------|--------------------------------------------|--------------------------------------------|--------------|
| 342109071 | 7501        | Paris      | 1987D01764     | 1                | 2014-01-28 00:00:00.000 | Etablissement ouvert                       | Modifications relatives à un établissement | A            |
| 342109071 | 7501        | Paris      | 1987D01764     | 1                | 2018-04-30 00:00:00.000 | Modifications relatives à un établissement | Modifications relatives à un établissement | A            |
| 342109071 | 7501        | Paris      | 1987D01764     | 3                | 2014-01-28 00:00:00.000 | Etablissement ouvert                       | Etablissement supprimé                     | F            |
| 342109071 | 7501        | Paris      | 1987D01764     | 3                | 2018-04-30 00:00:00.000 | Etablissement supprimé                     | Etablissement supprimé                     | F            |
| 342109071 | 7501        | Paris      | 1987D01764     | 4                | 2018-05-03 00:00:00.000 | Modifications relatives à un établissement | Modifications relatives à un établissement | A            |


### Exemple 2: `status_ets`

| siren     | code_greffe | nom_greffe | numero_gestion | id_etablissement | date_greffe             | type | status_ets |
|-----------|-------------|------------|----------------|------------------|-------------------------|------|------------|
| 797409612 | 9201        | Nanterre   | 2013D01713     | 1                | 2013-09-26 00:00:00.000 | SIE  | true       |
| 797409612 | 9201        | Nanterre   | 2013D01713     | 2                | 2013-09-26 00:00:00.000 | PRI  | false      |


## Règles de gestion applicables

[PO : Formules applicables]

Si nouvelle règle, ajouter ici.

A l'INSEE, la variable faisant référence au `status_admin` s 'appelle `etatadministratifetablissement` et est composée de deux valeurs possible, `A`, pour actif et `F` pour fermé.

L'équivalent de la variable `status_ets` à l'INSEE s'appelle `etablissementsiege` et elle est composée de deux valeurs, `true` si l'établissement est un siège, sinon `false`

# Charges de l'équipe

[

PO : Si des étapes particulières / des points d'attention sont attendus, être aussi explicite que possible

Spécifiquement pour l'intégration de nouvelles données dans DATUM :

*   Nombre de lignes chargées pour chaque nouvelle table
*   Poids de chaque nouvelle table
*   Durée du traitement ajouté (+ durée avant et après)

]

Lors de nos tests, nous avons utilisé cette query:

```
CASE WHEN last_libele_evt != 'Etablissement supprimé' THEN 'A' ELSE 'F' END AS status_admin
CASE WHEN type = 'SIE' OR type = 'SEP' THEN 'true' ELSE 'false' END AS status_ets,
```

# Tests d'acceptance

[PO : comment contrôler que la réalisation est conforme]

**Code reproduction**

```
```

- Trouver un siren avec le changement de status établissement au fil du temps. Par exemple, un principal devenu secondaire, ou bien un principal devenu siege.
- Compter le nombre de séquence distinct ayant un siège
- Compter le nombre de fois ou la variable `manque_sie_ou_pri_flag` est égale à `True` et `status_ets` est égale à `True`
    - Imprimer des exemples

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
