[

Usage : copier-coller le texte ci-dessous (en "_mode markdown_") à la création de nouvelles US, et retenir les paragraphe applicables

Grammaire des US :

```
Entant que {X} je souhaite {Y} afin de {Z}
```

*   Y est une fonctionnalité à valeur ajoutée <-- c'est **le TITRE de l'US**, afin de garder une cohérence, commencer par un **verbe à l'infinitif**
*   Z est le bénéfice attendu de la fonctionnalité <-- à placer dans le champ consacré en bas d'US
*   X est la personne (ou le rôle) qui va en bénéficier <-- à placer dans le champ consacré en bas d'US

]

# Contexte

[PO :

*   Situation avant mise en oeuvre de l'US
*   Explications fonctionnelles / vocabulaire si des termes métiers sont exploités

]

# US / ISSUES liées

[PO & DEV : s'il existe des références, les inscrire]

# Besoin

[PO : Situation attendue]

# Spécifications

## Input

[PO : dans le cas de transformation de données, préciser ,les sources :

*   Applications
*   Schémas
*   Tables
*   Champs

]

## Output

[PO : dans le cas de transformation de données, préciser les sorties :

*   BDD cibles
*   Tables
*   Champs

]

## Règles de gestion applicables

[PO : Formules applicables]

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
