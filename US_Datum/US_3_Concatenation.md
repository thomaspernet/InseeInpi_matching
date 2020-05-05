[

Usage : copier-coller le texte ci-dessous (en "_mode markdown_") à la création de nouvelles US, et retenir les paragraphe applicables

Grammaire des US :

```
En tant que {X} je souhaite {concatener les bases établissements de l'INPI} afin de {pouvoir préparer la donnée en vue de la siretisation}
```

*   Y est une fonctionnalité à valeur ajoutée <-- c'est **le TITRE de l'US**, afin de garder une cohérence, commencer par un **verbe à l'infinitif**
*   Z est le bénéfice attendu de la fonctionnalité <-- à placer dans le champ consacré en bas d'US
*   X est la personne (ou le rôle) qui va en bénéficier <-- à placer dans le champ consacré en bas d'US

]

# Contexte

[PO :

*  L'objectif de cet US est de concatener les tables établissements de L'iNPI.
*  Connaissance préalable sur l'INPI/Les établissements et leur environnement.
  * Une entreprise » désigne une structure ou organisation dont le but est d’exercer une activité économique en mettant en œuvre des moyens humains, financiers et matériels adaptés.  
  * Pour mieux s’organiser et répondre à la demande, une entreprise peut créer un ou plusieurs établissements.
    * Un établissement est par définition rattaché à une entreprise.
* Un numéro SIREN est le moyen d'identifier une entreprise (le détail sera disponible dans un US futur). Un numéro SIRET est le numéro d'identification des établissements. C'est un numéro définit par l'INSEE qui n'est pas disponible a L'INPI. Toutefois, l'INPI fournit un numéro d'identification (champs `id_établissement`) unique pour chaque établissement rattaché à un SIREN. Dans la mesure ou une entreprise peut exercer dans plusieurs territoires différents, il est très probable que le numéro d'identification de l'établissement ne soit pas unique. Par exemple, le champs `id_établissement` peut contenir plusieurs `1`, pour des villes differentes. Pour différencier les établissements les uns des autres, il faut augmenter le niveau de granularité, c'est à dire prendre en compte la séquence *siren* + *code greffe* + *numero gestion* + *ID établissement*. De la, nous pouvons dégager une nouvelle règle de gestion:

## Règles de gestion

- Definition partiel
  - si csv dans le dossier Partiel, année > 2017, alors partiel, c'est a dire, modification complète du dossier due a une anomalie.
  - la date d’ingestion est indiquée dans le path, ie comme les flux

- [NEW] Une séquence est un classement chronologique pour le quadruplet suivant:

  - *siren* + *code greffe* + *numero gestion* + *ID établissement*


]

# US / ISSUES liées

[PO & DEV : s'il existe des références, les inscrire]

# Besoin

[PO : Situation attendue]

# Spécifications

## Input

[PO : dans le cas de transformation de données, préciser, les sources:

- Stock
  *   inpi_stock_etablissement
  *   inpi_partiel_etablissement ( à définir selon une règle de gestion des dates )
- Flux
  *   inpi_flux_etablissement - > Nouveau seulement ?

- Source markdown [gitlab](https://scm.saas.cagip.group.gca/PERNETTH/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/01_Athena_concatenate_ETS.md#query-pr%C3%A9paration-table)

### Champs

Ensemble des champs présents dans les stocks et flux.

A revoir les formats

"siren",
"code greffe",
"Nom_Greffe",
"numero_gestion",
"id_etablissement",
"file_timestamp",
"Libelle_Evt",
"Date_Greffe",    
"Type",
"Siège_PM",
"RCS_Registre",
"Adresse_Ligne1",
"Adresse_Ligne2",
"Adresse_Ligne3",
"Code_Postal",
"Ville",
"Code_Commune",
"Pays",
"Domiciliataire_Nom",
"Domiciliataire_Siren",
"Domiciliataire_Greffe",
"Domiciliataire_Complément",
"Siege_Domicile_Représentant",
"Nom_Commercial",
"Enseigne",
"Activité_Ambulante",
"Activité_Saisonnière",
"Activité_Non_Sédentaire",
"Date_Début_Activité",
"Activité",
"Origine_Fonds",
"Origine_Fonds_Info",
"Type_Exploitation",
"csv_source",
"origin"

]

## Output

[PO : dans le cas de transformation de données, préciser les sorties :

*   BDD cibles
*   Tables: `ets_stock_new`
*   Champs

]

## Règles de gestion applicables

[PO : Formules applicables]

- Une séquence est un classement chronologique pour le quadruplet suivant:

  - *siren* + *code greffe* + *numero gestion* + *ID établissement*

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

Example sorties dans Athena:

- [ets_initial](https://eu-west-3.console.aws.amazon.com/athena/home?region=eu-west-3#query/history/2a4fdfa2-c06f-489b-9620-9627a2f5aaa4)
- [ets_new_2017](https://eu-west-3.console.aws.amazon.com/athena/home?region=eu-west-3#query/history/bd3792aa-8fdc-4183-ad8b-c93a5de0b346)
- [ets_partiel_2018](https://eu-west-3.console.aws.amazon.com/athena/home?region=eu-west-3#query/history/dc6fa7ab-97e3-4744-8e97-45da33038762)

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
