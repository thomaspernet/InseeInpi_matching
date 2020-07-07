# Etablissement

Dans les deux premières fiches, nous avons vu les deux grandes catégories juridiques donnant lieu a un siren, a savoir les Personnes Physiques et les Personnes Morales. Les établissements sont en un certain sens des satellites rattachés à un SIREN, avec pour numéro d’identification, le SIRET.

Dans cette fiche, nous allons définir les notions de SIREN, SIRET et bien sur d”établissement.

## Numéro SIREN

* Chaque entreprise est identifiée par un numéro Siren (Système d'identification du Répertoire des entreprises), utilisé par tous les organismes publics et les administrations en relation avec l'entreprise.
* Attribué par l'Insee lors de l'inscription de l'entreprise au Répertoire Sirene, il comporte 9 chiffres.
* Ce numéro est unique et invariable
* Il se décompose en trois groupes de trois chiffres attribués d'une manière non significative en fonction de l'ordre d'inscription de l'entreprise.
* Ex. : 231 654 987


## Numéro SIRET

* Le numéro Siret (Système d'identification du Répertoire des établissements) identifie les établissements de l'entreprise.
* Il se compose de 14 chiffres correspondant :
  *  au numéro Siren,
  *  et, au numéro NIC (numéro interne de classement), comportant 5 chiffres : les quatre premiers correspondent au numéro d'identification de l'établissement ; le cinquième chiffre est une clé
* Ex. : (numéro Siren ) 231 654 987   (numéro NIC) 12315
* Le numéro NIC identifie chaque établissement de l'entreprise
* Une entreprise est constituée d’autant d’établissements qu’il y a de lieux différents où elle exerce son activité. L’établissement est fermé quand l’activité cesse dans l’établissement concerné ou lorsque l’établissement change d’adresse.
Pour rechercher des informations sur une entreprise, il est possible de se rendre dans l’un des sites suivants:   

*  Insee
  *  http://avis-situation-sirene.insee.fr/IdentificationListeSiret.action
*  INPI/TC
  * https://data.inpi.fr/
*  Infogreffe
  *  https://www.infogreffe.fr/

## La différence entre entreprise et établissement

Le terme « entreprise » désigne une structure ou organisation dont le but est d’exercer une activité économique en mettant en œuvre des moyens humains, financiers et matériels adaptés.

La notion d’entreprise n’est pas corrélée à un statut juridique particulier et vaut aussi bien pour une entité unipersonnelle que pour une société (actionnaires multiples). Elle ne tient pas non plus compte de la valeur financière ou du volume d’activité.

La création d’une entreprise résulte de l’initiative d’une ou de plusieurs personnes qui mettent des moyens en commun pour produire des biens et/ou des services. D’une idée naît une entreprise dont l’organisation a pour objectif d’assurer sa pérennité.

Pour mieux s’organiser et répondre à la demande, une entreprise peut créer un ou plusieurs établissements.

Un établissement est par définition rattaché à une entreprise.

Plus l’entreprise est grande, plus elle comporte d’établissements, dépendants financièrement et juridiquement d’elle.


# La donnée de l'INPI

Dans cette partie, nous regroupons l'ensemble des règles de gestion détecté ou défini à date.

Il faut savoir que la donnée brute provient du FTP de l'INPI qui est décomposée en deux branches. Une première branche contenant toutes les informations sur les créations ou modifications datant d'avant mai 2017. Une deuxième branche, quant a elle, regroupe les immatriculations et modifications qui se sont produites après mai 2017.

La première branche est appelée **stock** alors que la deuxième branche s'appelle **flux**.

## Description des fichiers de stock

A l’intérieur du répertoire public > IMR_Données_Saisies > tc > stock, les fichiers de stock sont organisés en fichiers zippés par greffe dans l’arborescence par année/mois/jour de la date de transmission.

Attention : Le stock initial des données TC étant constitué à la date du 4 mai 2017, il est impératif d’intégrer de manière incrémentale toutes les données de flux mises à disposition sur serveur dès l’ouverture de la licence IMR (1ères transmissions à compter du 18 mai 2017 contenant les informations des inscriptions enregistrées à partir du 5 mai 2017).

## Stocks initiaux & partiels

La description des fichiers de stocks (stocks initiaux et partiels) est similaire à celle des fichiers de flux, décrit ci-après, avec quelques particularités :

* Le nombre de fichiers transmis pour chaque greffe est au nombre de 7,
* Ces fichiers contiennent toutes les informations relatives à l’identité de la personne morale ou physique, aux représentants, aux **établissements**, aux observations, aux actes et comptes annuels déposés, telles que générées à la date du 4 mai 2017 pour les tribunaux de commerce (personnes actives),
* La nomenclature des fichiers de stock reprend la nomenclature des fichiers de flux, avec, en guise de numéro de transmission, le numéro du stock ex. S1 (1 à n fichiers de stocks par greffe selon la volumétrie et selon la date de constitution). La numérotation est incrémentale.
*
Les stocks partiels ont une tout autre fonction, que nous décrirons plus tart. Toutefois, le stock partiel a pour objectif de corriger les erreurs ou ommissions des greffiers relatif à la création ou modification de dossier transmit précédement.

![](https://drive.google.com/uc?export=view&id=13Olhfr6CXRowaONUp8-6DJCSBdjv-MNm)

## Description des fichiers de flux

Les fichiers transmis permettent d’exploiter les données de flux en provenance des greffes des tribunaux de commerce, et plus précisément :

* Constitution du dossier d’une personne morale ou personne physique (identifiée par son siren et son n° de gestion), dans le cas d’une 1ère immatriculation,
* Mise à jour des informations disponibles sur une personne morale ou personne physique en cas de modification ou de radiation.

Dans le cas d’une immatriculation (Personne morale ou Personne physique), le dossier est composé :

* A minima, d’informations sur l’identité de la personne (ex. date d’immatriculation, inscription principale ou secondaire, dénomination, nom, prénom, forme juridique, capital, activité principale etc.)

* Complété éventuellement par des informations relatives aux :
  * Représentants
  * Etablissements (établissement principal et/ou établissements secondaires)
  * Observations (incluant les procédures collectives, mentions de radiation etc.)
  * Dépôt des comptes annuels
  * Dépôt des actes

Les fichiers sont de 2 types :

* Fichiers créés à l’immatriculation d’un dossier
* Fichiers créés suite à un événement sur un dossier (modification, radiation)

## Immatriculation/ modification et radiation

De plus, il faut bien distinguer une immatriculation d'une modification. L'immatriculation représente la création d'une entreprise alors que la modification concerne le changement d'une information ou la radiation de la société, ou fermeture d'un établissement.

### Fichiers transmis à l'immatriculation d'un dossier

- **Immatriculation**: La création d'une entreprise requiert son immatriculation au Registre du Commerce et des Sociétés (RCS). Vous pouvez effectuer cette formalité d'immatriculation de votre société en ligne ou directement auprès du greffe du Tribunal de commerce compétent.

Dans le cas d’une immatriculation, 7 types de fichiers numérotés sont transmis :

* Immatriculations de personnes morales (1) ou physiques (3)
* Informations sur les représentants (5), **établissements (8)**, observations (11), actes (12) et comptes annuels (13)

La numérotation des fichiers et la mise en place d’un numéro de transmission incrémental permettent au licencié de s’assurer de la bonne intégration de l’ensemble des fichiers.

L’intégration des données doit en effet suivre impérativement l’ordre des numéros des fichiers (num_transmission).

Le nommage des fichiers est décrit ci-dessous :

![](https://drive.google.com/uc?export=view&id=1a91xipb_eEZiSXRsmaXfIjFUSqF2Djf6)

### Fichiers transmis en cas de mise a jour d'un dossier (Evenement)

- **Modification**: Changement de gérant, de dénomination, modification relative au capital... les raisons de procéder à une rectification ou un complément des renseignements déclarés au Registre du Commerce et des Sociétés sont nombreuses.
- **Radiation**: La radiation d'un commerçant personne physique ou la radiation d'une société fait l'objet d'une déclaration auprès du greffe du Tribunal de commerce.
Le motif de la demande de radiation d'entreprise ou société peut être lié à une cessation d'activité, une dissolution, une fusion…
- **Fermeture**: L'établissement secondaire est un établissement distinct situé dans un ressort autre que celui du siège social ou de l'établissement principal. Sa fermeture entraîne une inscription modificative au RCS du lieu de sa situation. Lorsque l'établissement fermé constitue le seul établissement du ressort, la formalité correspondante entraîne une notification au greffe compétent au titre du siège social.

En cas de mise à jour d’un dossier suite à un événement (modification, radiation), les fichiers transmis ont une structure identique aux fichiers créés à l’immatriculation avec la présence de 2 champs spécifiques : la date de l’événement (Date_Greffe) et le libellé de l’événement (Libelle_Evt).

Dans ces cas, 6 types de fichiers supplémentaires, numérotés, sont transmis correspondant à :

* Evénements modifiant ou complétant les dossiers d’immatriculation des personnes morales (2) ou physiques (4)
* Evénements modifiant ou complétant les informations relatives aux représentants (6) ou aux **établissements (9)**
* Evénements supprimant des représentants (7 – Représentant partant) ou des **établissements (10 – Etablissement supprimé)**

![](https://drive.google.com/uc?export=view&id=1FVEGNqogl1NxB84BtdF4TQztCXjmtpyo)

Il existe actuellement 7 libellés d’événements possibles dont 4 pour les établissements:

- Dossier : Modifications relatives au dossier: ETB
- Etablissements : Modifications relatives à un établissement: ETB
- Etablissements : Etablissement ouvert: ETB
- Etablissements : Etablissement supprimé: ETB
- Représentants : Modifications relatives aux dirigeants
- Représentants : Nouveau dirigeant
- Représentants : Dirigeant partant

Les principes de mise à jour sont les suivants :

- Cas général : lors d’événements survenus après l’immatriculation d’un dossier, seules les données impactées par l’événement et/ou nécessaires à son identification (ex. sigle) sont transmises dans les fichiers correspondants. Dans ce contexte, la valeur (supprimée) indiquée dans certains champs permet de faire la différence entre une valeur à supprimer et une valeur non modifiée.
- Cas particuliers : dans certains cas, l’ensemble des données est retransmis pour faciliter les mises à jour et permettre la fiabilité de la base. Exemples : données d’adresse pour les établissements.
Tous les cas de figure possibles de mise à jour sont décrits ci-dessous.

Les mises à jour peuvent porter sur des modifications relatives à un établissement existant ou sur l’ouverture d’un nouvel établissement.

En plus des champs d’entête, de la date d’enregistrement au greffe (champ « Date_Greffe »), du libellé de l’événement (champ Libelle_Evt) et de l’ID (champ ID-Etablissement), on retrouve, quel que soit l’événement, les données suivantes : Type d’établissement, Adresse (Adresse lignes 1 à 3, Code postal, Ville, Code commune, Pays) lorsqu’elle existe.

-Mises à jour s’appuyant sur le renvoi systématique d’un ensemble de données (si elles existent) :
    - Modifications relatives au domiciliataire : Nom, Siren, Greffe, Complément
- Pour tous les autres types de mise à jour, la mise à jour ne porte que sur l’envoi des seules données à modifier : ex. Activité, Nom commercial, Enseigne, Exploitation, Date de début d’activité. Dans ce 2ème cas, pour distinguer les cas de suppression d’une donnée sans modification, on utilise la valeur (supprimé).

Ci dessous, le tableau récapitulatif de chaque état dans la donnée de l'INPI relatif aux établissements.

| Type evenement | Origine | Label                                      | Numérotation | Denomination fichier                                                                     | Explication                                       |
|----------------|---------|--------------------------------------------|--------------|------------------------------------------------------------------------------------------|---------------------------------------------------|
| Création       | Stock   | Etablissement ouvert                       | 8            | <code_greffe>_<num_transmission>_<AA><MM><JJ>_8_ets.csv                                  | AAMMJJ -> la date, 8 -> création                  |
| Création       | Flux    | Etablissement ouvert                       | 8            | <code_greffe>_<num_transmission>_<AA><MM><JJ>_<HH><MM><SS>_8_ets.csv                     | AAMMJJ_HHMMSS -> timestamp, 8 -> création         |
| Modification   | Flux    | Modifications relatives à un établissement | 9            | <code_greffe>_<num_transmission>_<AA><MM><JJ>_<HH><MM><SS>_9_ets_nouveau_modifie_EVT.csv | AAMMJJ_HHMMSS -> timestamp, 9 -> Modification ets |
| Suppression    | Flux    | Etablissement supprimé                     | 10           | <code_greffe>_<num_transmission>_<AA><MM><JJ>_<HH><MM><SS>_10_ets_supprime_EVT.csv       | AAMMJJ_HHMMSS -> timestamp, 10 -> Suppression ets |

Il est bon de noter que la branche **stock** ne contient pas de libellé evenement modification ou suppression

- **timestamp**: Une date avec l'année + mois + jour + heure + minute + seconde

# Règles de gestion

## Plusieurs transmissions pour le même timestamp

*  Les événements doivent impérativement suivre l'ordre d'apparition dans le csv du FTP et de la numérotation des fichiers: 8, 9 et 10 pour les établissements
  *   Pour les événements, il est possible d'avoir plusieurs informations renseignées pour une même date de transmission pour une même séquence

Par exemple, il peut arriver qu'un établissement fasse l'object d'une modification (numérotation 9) et d'une suppression (numérotation 10) lors de la même transmission (timestamp). Dans ce cas la, il faut privilégier la suppression car elle est apparue après la modification (10 > 9).

### PREUVE - 31/03/2020

```
Qu'elle est la règle a appliqué lorsqu'un événement est effectué le même jour, à la même heure pour un même établissement (même quadruplet: siren + code greffe + numéro gestion + ID établissement), mais avec des modifications pas forcément identique.
-> Il faut modifier, en respectant l’ordre des lignes, uniquement les champs où il y a une valeur.
```

### EXEMPLE

- SIREN: 818153637

## Definition séquence

*   Une séquence est un classement chronologique pour le quadruplet suivant:
    *   _siren_ + _code greffe_ + _numero gestion_ + _ID établissement_

### PREUVE - 26/03/2020

```
2/ Pour identifier un établissement, il faut bien choisir la triplette siren + numero dossier (greffer) + ID établissement ?
-> il faut choisir le quadruplet siren + code greffe + numero gestion + ID établissement
```

### EXEMPLE

## Définition partiel

* si csv dans le dossier Stock, année > 2017, alors partiel, c'est a dire, modification complète du dossier due a une anomalie.
* La date d’ingestion est indiquée dans le path, ie comme les flux

### PREUVE - 26/03/2020

```
1/ Est-ce que les csv dans le dossier Stock pour une date supérieure à 2017 peuvent être labélisés comme « partiel » rendant ainsi caduque toutes les valeurs précédentes d’un établissement ?
-> OUI
```

## Creation status partiel

- En cas de corrections majeures, la séquence annule et remplace la création et événements antérieurs. Dans ce cas, toutes les données qui ont pu être transmises antérieurement via le stock initial ou le flux doivent donc être ignorées (prendre en compte la date de transmission indiquée dans le nom des sous-répertoires du stock et des fichiers

### PREUVE - 26/03/2020

```
1/ Est-ce que les csv dans le dossier Stock pour une date supérieure à 2017 peuvent être labélisés comme « partiel » rendant ainsi caduque toutes les valeurs précédentes d’un établissement ?
-> OUI
```

### EXEMPLE

### EXEMPLE

- Siren: 539450601

## Remplissage champs manquants par séquence et date de transmission

*   Le remplissage doit se faire de la manière suivante pour la donnée brute
    *   Pour une date de transmission donnée, c'est la dernière ligne de la séquence qui doit être utilisée, remplie des valeurs manquantes extraites des lignes précédentes. Si la dernière ligne de la séquence contient un champs non vide, il ne faut pas la remplacer par la ligne précédente.

### PREUVE

```
Donc si je comprends bien, disons si il y a pour un même quadruplet 3 événements à la même date

-	Ligne 1
-	Ligne 2
-	Ligne 3

Pour avoir l’ensemble des informations, il faut que je complète la ligne 2 avec les éléments de la ligne 1, puis la ligne 3 avec les nouveaux éléments de la ligne 2.

Au final, la ligne 3 va avoir les informations de la ligne 1 & 2, et donc c’est elle qui va faire foi (ligne 1 et 2 devenant caduque)
-> Oui, il faut additionner les champs modifiés. Attention si un même champ est modifié (avec des valeurs différentes) en ligne 2, puis en ligne 3, il faudra privilégier les valeurs de la ligne 3.
```

### EXEMPLE

## Remplissage champs manquants par séquence et historique

- Pour remplir les champs manquants des événements qui n'ont pas fait l'object d'une modification lors de la transmission, il faut remplir les champs selon les valeurs historiques. Plus précisément, selon la valeurs t-1 pour une séquence donnée

### EXEMPLE

## Plusieurs créations pour une même séquence

*  Une création d'une séquence peut avoir plusieurs transmissions a des intervalles plus ou moins long
    *   Si plusieurs transmissions avec le libellé “Etablissement ouvert”, alors il faut prendre la dernière date de transmission

### PREUVE - 02/06/2020

```
Je reviens vers vous concernant les « doublons » sur les créations. En dessous, j’ai indiqué le nombre de lignes contenant des doublons sur la séquence siren,code_greffe,numero_gestion,id_etablissement. Autrement dit, il y a 454070 lignes de créations d’établissement avec 2 lignes. 261163 lignes avec 3 doublons etc. Cela impacte environ 15/20% de la donnée des créations dans le FTP.

La regle que nous avions parlé est : si doublon au niveau de la création (même séquence avec le libellé « création ») mais des dates de transmission différentes, alors prendre la plus récente.

Je n’ai pas encore vérifié si les doublons avaient des lignes identiques. Je souhaitais simplement m’assurer que la règle était la bonne, et que je faisais correctement ce retraitement.
->Oui, c’est la règle que nous appliquons.
```

### EXEMPLE

- Siren: 393826185

## Faux événements

*   Il y a certains cas ou les lignes de créations doublons sont de faux événements (mauvais envoie de la part du greffier)
    *   Si le timestamp entre la première ligne et dernière ligne est supérieures a 31 jour (exclut), il faut:
            *   Récupérer la dernière ligne, et créer une variable flag, comme pour le statut

### PREUVE - 03/06/2020

```
Je reviens vers vous de nouveau pour les doublons lors des créations. J’ai remarqué que pas tous les doublons sont identiques. Certains ont mêmes des différences significatives.

Je vous ai mis en pièce jointe un fichier Excel avec quelques siren impactés.  Pour info, les siren proviennent du FTP nouveau année 2017 et la feuille numéro 2 contient un siren dont la différence provient du ftp nouveau 2017 et 2018

Comment avez-vous traité ce genre de cas de figure ?
-> il faut toujours privilégier la dernière information reçue.

Je viens de trouver un SIREN qui comporte pas mal de problème. Voici le SIREN en question 301852026

Dans le fichier Excel appelé 301852026.xlsx (en pièce jointe), il y a un établissement créé le 01/09/2017 (lignes en jaune, onglet NEW_2017), dont toutes les lignes de l’adresse et nom_commercial sont quasiment non identique. Si je prends la dernière ligne, je n’ai plus le nom commercial, car figure dans la première ligne. De plus cet établissement a un événement. Etant donné que la dernière ligne n’est pas renseigné, je ne peux pas non plus le remplir

Quand je compile l’ensemble de la donnée (onglet Full_sample), je ne vois pas de partiel associé. Au contraire, on voit que l’établissement a eu une modification de l’adresse (ligne en rouge)

Quand je regarde le moteur de recherche de l’inpi https://data.inpi.fr/entreprises/301852026#301852026 il y a bien l’établissement mais pas d’adresse d’indiquée.

-> Il y a eu effectivement une modification d’adresse (l’évènement aurait du être « modification relative à un établissement »). Par contre, pour le nom commercial, je pense qu’il s’agit d’une erreur.
Pour preuve la formalité qui nous a été envoyée.
Si vous prenez donc la dernière ligne vous devriez être conforme à cette formalité.
Concernant l’adresse manquante sur DATAINPI, il s’agit d’une anomalie qui va être corrigée prochainement.

J’ai regardé un a un les csv du FTP avec ce siren 301852026 et le nom_commercial est bien manquant lors du dernier envoie. Le détail est dans l’excel, onglet FTP. Les lignes en rouge. (la feuille contient un filtre sur le siren en question).

Du coup, j’ai pensé a la règle a appliqué en cas d’anomalie de ce genre :

Si création contient plusieurs lignes avec des valeurs différentes, alors remplir de manière incrémentale champs vide (en prenant le n-1 de la séquence siren + code greffe + numero gestion + ID etablissement + libelle evenement = Etablissement ouvert

Comme ca, la dernière ligne est toujours celle a retourner, mais on peut remplir les champs manquants.

Est-ce que vous confirmez ?
-> Pouvez-vous m’appeler ?
- Regle refusée lors de l'appel, et l'INPI préconise de ne prendre que la dernière ligne
```

### EXEMPLE

- Siren: 829446152

## Plusieurs transmissions pour une date de greffe donnée

- Si, pour une date de greffe donnée, il y a plusieurs dates de transmission non identique, alors il faut enrichir les événements comme défini précédemment et ne récupérer que la date de transmission la plus récente. Autrement dit, il faut toujours garder une seule ligne pour chaque date de greffe, qui est la date de transmission la plus récente.

### PREUVE

```
J’ai une autre question concernant les transmissions a des dates différentes pour une même date de greffe.

On sait qu’il faut enrichir les séquences (siren/code greffe/ numéro gestion/id établissement) de manière incrémentale pour les transmissions, mais aussi en prenant les valeurs t-1 pour remplir les champs manquants.

Maintenant, il y a de nombreux cas ou la date de greffe reste la même, mais contenant plusieurs dates de transmission. Pouvons-nous définir la règle suivante

-	Si une même date de greffe a plusieurs transmissions, devons-nous prendre la dernière ligne que nous avons enrichi?
-> OUI

Par ailleurs, comment se fait-il que des transmissions pour une même date de greffe ont plusieurs mois de décalage ?
-> Nous ne savons pas, nous diffusons ce qu’infogreffe nous envoit
```

### EXEMPLE

- Siren: 833609597

## Creation flag pas siege ou principal

- Il est possible qu'un SIREN n'ai pas de siege/principal. Normalement, cela doit être corrigé par un partiel

### EXEMPLE

- Siren: 961504768
-
## Creation flag pas de création

- Il arrive que l'INPI envoie des séquences sans libellé evenement égal à "Etablissement ouvert". Lorsque cela  ce produit, l'INPI va faire la demande de partiel pour corriger le dossier.

### PREUVE

```
-  Etablissement sans création
  - Il arrive que des établissements soient supprimés (EVT) mais n'ont pas de ligne "création d'entreprise". Si cela, arrive, Infogreffe doit envoyer un partiel pour corriger. Il arrive que le greffe envoie seulement une ligne pour SEP, lorsque le Principal est fermé, le siège est toujours ouvert. Mais pas de nouvelle ligne dans la base. Le partiel devrait corriger cela.
```

### PREUVE - 15/04/2020

```
-	Il y a des Siren dont l’établissement est supprimé (EVT) mais qui n’ont pas de ligne "création d’entreprise". Est-ce normal?
-	Est-il possible d'avoir des établissements figurants a l'INSEE mais qui ne sont pas référencés à l'INPI (hors établissement ayant formulé un droit à l'oubli)
-> Pouvez-vous m’appeler, ce sera plus simple pour comprendre ?
```

### EXEMPLE

- Siren: 323398057

## Autres règles

- La variable `ville` de l'INPI n'est pas normalisée. C'est une variable libre de la créativité du greffier, qui doit être formalisée du mieux possible afin de permettre la jointure avec l'INSEE. Plusieurs règles regex ont été recensé comme la soustraction des numéros, caractères spéciaux, parenthèses, etc. Il est possible d'améliorer les règles si nécessaire
- Le code postal doit être formalisé correctement, a savoir deux longueurs possibles: zero (Null) ou cinq. Dans certains cas, le code postal se trouve dans la variable de la ville.
- La variable pays doit être formalisée, a savoir correspondre au code pays de l'INSEE. Bien que la majeure partie des valeurs soit FRANCE ou France, il convient de normaliser la variable pour récuperer les informations des pays hors France.
- Les variables de l'adresse de l'INPI ne sont pas normalisées, et ne peuvent être utilisées en l'état. Il est donc indispensable de retravailler les variables adresse pour pouvoir les comparer avec l'INSEE. Nous utilisons une règle (pattern) regex pour vérifier si les mots contenus dans l'adresse de l'INPI sont aussi contenus à l'INSEE.
- L'INSEE codifie le type de voie de la manière suivante:
    - Si le type de voie est d'une longueur inférieure ou égale à 4 caractères, le type de voie n'est pas abrégé. Ainsi, RUE ou QUAI sont écrits tels quels, alors que AVENUE est abrégée en AV.
    - La codification de l'INSEE va donc être utilisé ppur créer la variable `voie_matching`
- Pour ne pas complexifié le processus de siretisation, seule le premier numéro de voie contenu dans l'adresse nettoyée est extraite pour ensuite etre matché avec l'INSEE.

## Siren vide

### PREUVE - 06/04/2020

```
J’ai remarqué qu’il y a des lignes dans les csv qui n’ont pas de SIREN, alors que la documentation indique que c’est une valeur obligatoire.

Est-ce que les erreurs comme ça sont corrigées par la suite avec des stock partiels ?

En pièce jointe, un exemple de csv avec des siren manquant ex 0602_1310_20191221_091331_8_ets.csv

-> Ce n’est pas normal. Normalement le siren est obligatoire. Vous constaterez que le dossier de votre exemple a été renvoyé le 24/12/2019 avec, cette fois ci, le siren. Quand bien même il n’aurait pas été renvoyé, tout dossier sans siren est rejeté chez nous, il aurait donc été demandé dans un stock partiel ultérieurement.

Sur 2017-2019, seulement 228 sont dans ce cas. La liste en pièce jointe.

Comme j’ai compris, il faut utiliser les stocks partiels présents ou a venir pour avoir le siren.
-> Oui ou le flux parce qu’il peut être transmis avec le siren dans un flux ultérieur.
```

## CSV vides

### PREUVE - 08/04/2020

```
Je vous contacte à propos des csv flux de 2019. Il y a des csv vides en provenance du FTP. Je me demandais si c’était normal.
-> Oui, cela peut arriver.
```

## numérotation id établissement

### PREUVE - 23/06/2020

```
J’ai une nouvelle question concernant les établissements :

Pourquoi il y  a des établissements dont l’ID_etablissement est égal à 0 ? Devons-nous les supprimés ?

Par exemple, en pièce jointe, je vous ai joint un siren 818153637 qui a un id établissement a 0, mais sans création.  Le siège, ensuite, on a un autre établissement (1) qui devient le SEP.

Le moteur de recherche de l’INPI n’indique pas l’adresse complète donc je ne peux pas vérifier.

![](https://data.inpi.fr/entreprises/818153637#818153637)

-> C’est possible qu’il y ait des ID = 0.
En ce qui concerne votre exemple, il est ancien et je vois que nous l’avions demandé dans un stock correctif.
Pour l’adresse incomplète, il s’agit d’un bug, il sera corrigé prochainement.
```

### EXEMPLE

- Siren: 054800016

# Statistique sur les ETS

### Nombre d’observations par siren

```
SELECT COUNT(*) as value_counts, nb_siren
FROM (
   SELECT COUNT(*) as nb_siren FROM "inpi"."ets_test_filtered"
   GROUP BY siren
  )
GROUP BY nb_siren
ORDER BY nb_siren
```

Il y a 2740182 observations qui ont un seul siren

| value_counts | nb_siren |
|--------------|----------|
| 2740182      | 1        |
| 2409085      | 2        |
| 344983       | 3        |
| 342718       | 4        |
| 70760        | 5        |
| 34550        | 6        |
| 14453        | 7        |
| 6918         | 8        |
| 3852         | 9        |
| 2299         | 10       |
| 1526         | 11       |
| 1090         | 12       |
| 779          | 13       |
| 683          | 14       |
| 508          | 15       |
| 450          | 16       |
| 333          | 17       |
| 337          | 18       |
| 245          | 19       |
| 241          | 20       |
| 199          | 21       |
| 182          | 22       |
| 159          | 23       |
| 138          | 24       |
| 139          | 25       |
| 115          | 26       |
| 113          | 27       |
| 103          | 28       |
| 91           | 29       |
| 87           | 30       |
| 98           | 31       |
| 79           | 32       |
| 69           | 33       |
| 51           | 34       |
| 66           | 35       |
| 53           | 36       |
| 53           | 37       |
| 47           | 38       |
| 43           | 39       |
| 38           | 40       |
| 38           | 41       |
| 33           | 42       |
| 34           | 43       |
| 39           | 44       |
| 42           | 45       |
| 33           | 46       |
| 35           | 47       |
| 31           | 48       |
| 28           | 49       |
| 24           | 50       |
| 22           | 51       |
| 15           | 52       |
| 17           | 53       |
| 24           | 54       |
| 30           | 55       |
| 26           | 56       |
| 22           | 57       |
| 27           | 58       |
| 18           | 59       |
| 26           | 60       |
| 20           | 61       |
| 16           | 62       |
| 15           | 63       |
| 19           | 64       |
| 11           | 65       |
| 15           | 66       |
| 22           | 67       |
| 14           | 68       |
| 16           | 69       |
| 11           | 70       |
| 15           | 71       |
| 16           | 72       |
| 12           | 73       |
| 17           | 74       |
| 12           | 75       |
| 11           | 76       |
| 4            | 77       |
| 9            | 78       |
| 13           | 79       |
| 12           | 80       |
| 12           | 81       |
| 7            | 82       |
| 3            | 83       |
| 9            | 84       |
| 8            | 85       |
| 12           | 86       |
| 6            | 87       |
| 11           | 88       |
| 8            | 89       |
| 7            | 90       |
| 5            | 91       |
| 12           | 92       |
| 5            | 93       |
| 10           | 94       |
| 5            | 95       |
| 4            | 96       |
| 4            | 97       |
| 7            | 98       |
| 9            | 99       |
| 8            | 100      |
| 6            | 101      |
| 6            | 102      |
| 11           | 103      |
| 6            | 104      |
| 6            | 105      |
| 4            | 106      |
| 2            | 107      |
| 7            | 108      |
| 8            | 109      |
| 3            | 110      |
| 6            | 111      |
| 4            | 112      |
| 3            | 113      |
| 3            | 114      |
| 3            | 115      |
| 3            | 116      |
| 3            | 117      |
| 5            | 118      |
| 7            | 119      |
| 4            | 120      |
| 4            | 121      |
| 3            | 122      |
| 2            | 123      |
| 5            | 124      |
| 3            | 125      |
| 4            | 126      |
| 3            | 127      |
| 3            | 128      |
| 1            | 129      |
| 3            | 130      |
| 4            | 131      |
| 5            | 132      |
| 3            | 133      |
| 4            | 134      |
| 2            | 135      |
| 2            | 136      |
| 6            | 137      |
| 4            | 138      |
| 3            | 139      |
| 3            | 140      |
| 6            | 141      |
| 7            | 142      |
| 6            | 144      |
| 1            | 145      |
| 5            | 146      |
| 3            | 147      |
| 2            | 148      |
| 3            | 149      |
| 5            | 150      |
| 4            | 151      |
| 1            | 152      |
| 3            | 153      |
| 3            | 154      |
| 1            | 155      |
| 2            | 157      |
| 3            | 158      |
| 1            | 159      |
| 1            | 160      |
| 3            | 161      |
| 4            | 162      |
| 5            | 163      |
| 4            | 164      |
| 3            | 165      |
| 1            | 166      |
| 4            | 168      |
| 2            | 169      |
| 1            | 170      |
| 3            | 171      |
| 2            | 172      |
| 5            | 173      |
| 1            | 174      |
| 3            | 175      |
| 1            | 176      |
| 1            | 177      |
| 3            | 178      |
| 2            | 179      |
| 1            | 181      |
| 2            | 182      |
| 4            | 183      |
| 2            | 184      |
| 3            | 185      |
| 1            | 186      |
| 1            | 188      |
| 2            | 189      |
| 3            | 190      |
| 3            | 191      |
| 2            | 192      |
| 3            | 194      |
| 1            | 195      |
| 2            | 196      |
| 1            | 198      |
| 3            | 199      |
| 3            | 200      |
| 3            | 201      |
| 1            | 204      |
| 3            | 205      |
| 2            | 206      |
| 3            | 207      |
| 1            | 208      |
| 1            | 209      |
| 1            | 210      |
| 2            | 214      |
| 1            | 215      |
| 1            | 216      |
| 1            | 217      |
| 2            | 218      |
| 1            | 219      |
| 2            | 220      |
| 3            | 221      |
| 3            | 222      |
| 1            | 223      |
| 2            | 226      |
| 4            | 227      |
| 1            | 228      |
| 3            | 229      |
| 1            | 230      |
| 2            | 232      |
| 1            | 233      |
| 1            | 235      |
| 2            | 236      |
| 1            | 237      |
| 1            | 238      |
| 1            | 239      |
| 1            | 240      |
| 4            | 241      |
| 4            | 242      |
| 1            | 243      |
| 1            | 246      |
| 2            | 248      |
| 3            | 249      |
| 2            | 250      |
| 2            | 251      |
| 1            | 252      |
| 4            | 254      |
| 1            | 255      |
| 1            | 256      |
| 1            | 258      |
| 4            | 263      |
| 1            | 264      |
| 1            | 265      |
| 1            | 266      |
| 1            | 267      |
| 1            | 270      |
| 2            | 272      |
| 1            | 273      |
| 1            | 279      |
| 2            | 281      |
| 1            | 282      |
| 1            | 283      |
| 1            | 285      |
| 2            | 286      |
| 2            | 290      |
| 1            | 291      |
| 2            | 294      |
| 2            | 299      |
| 1            | 300      |
| 2            | 302      |
| 6            | 304      |
| 2            | 306      |
| 1            | 307      |
| 4            | 308      |
| 1            | 310      |
| 1            | 315      |
| 1            | 318      |
| 1            | 320      |
| 1            | 322      |
| 1            | 323      |
| 1            | 324      |
| 1            | 327      |
| 1            | 329      |
| 1            | 336      |
| 2            | 337      |
| 2            | 338      |
| 1            | 339      |
| 1            | 341      |
| 1            | 347      |
| 1            | 351      |
| 2            | 356      |
| 1            | 361      |
| 1            | 362      |
| 1            | 363      |
| 1            | 364      |
| 1            | 365      |
| 2            | 368      |
| 1            | 369      |
| 1            | 370      |
| 1            | 372      |
| 1            | 373      |
| 1            | 374      |
| 1            | 376      |
| 1            | 378      |
| 1            | 379      |
| 1            | 381      |
| 1            | 382      |
| 1            | 386      |
| 1            | 397      |
| 1            | 399      |
| 1            | 406      |
| 1            | 416      |
| 1            | 421      |
| 1            | 423      |
| 1            | 425      |
| 1            | 429      |
| 1            | 435      |
| 1            | 439      |
| 2            | 441      |
| 1            | 442      |
| 1            | 443      |
| 1            | 447      |
| 1            | 453      |
| 1            | 465      |
| 1            | 474      |
| 1            | 478      |
| 1            | 480      |
| 1            | 487      |
| 1            | 489      |
| 1            | 490      |
| 2            | 493      |
| 1            | 494      |
| 1            | 497      |
| 1            | 500      |
| 1            | 503      |
| 1            | 517      |
| 2            | 522      |
| 1            | 533      |
| 1            | 536      |
| 1            | 542      |
| 1            | 546      |
| 1            | 554      |
| 1            | 563      |
| 1            | 578      |
| 1            | 580      |
| 1            | 582      |
| 1            | 598      |
| 1            | 604      |
| 1            | 612      |
| 1            | 618      |
| 1            | 619      |
| 1            | 627      |
| 1            | 629      |
| 1            | 633      |
| 1            | 636      |
| 1            | 649      |
| 1            | 650      |
| 1            | 656      |
| 1            | 674      |
| 1            | 677      |
| 1            | 685      |
| 1            | 698      |
| 1            | 699      |
| 1            | 700      |
| 1            | 729      |
| 1            | 745      |
| 1            | 773      |
| 1            | 834      |
| 1            | 860      |
| 1            | 895      |
| 1            | 950      |
| 1            | 984      |
| 1            | 1055     |
| 1            | 1130     |
| 1            | 1152     |
| 1            | 1264     |
| 1            | 1310     |
| 1            | 1520     |
| 1            | 1662     |
| 1            | 1946     |
| 1            | 2095     |
| 1            | 2407     |
| 1            | 2529     |
| 1            | 2617     |
| 1            | 2685     |
| 1            | 3406     |
| 1            | 12556    |

###  Nombre d’observations par sequence

```
SELECT COUNT(*) as value_counts, nb_sequence
FROM (
   SELECT COUNT(*) as nb_sequence FROM "inpi"."ets_test_filtered"
   GROUP BY siren,"Nom_Greffe", "code_greffe",
      numero_gestion, id_etablissement
  )
GROUP BY nb_siren
ORDER BY nb_siren
```
Il y a 1202686 qui ont 2 séquences (2 siret)

| value_counts | nb_sequence |
|--------------|-------------|
| 8344117      | 1           |
| 1202686      | 2           |
| 102425       | 3           |
| 9746         | 4           |
| 1245         | 5           |
| 194          | 6           |
| 36           | 7           |
| 20           | 8           |
| 12           | 9           |
| 10           | 10          |
| 9            | 11          |
| 6            | 12          |
| 4            | 13          |
| 6            | 14          |
| 2            | 15          |
| 3            | 16          |
| 6            | 17          |
| 1            | 18          |
| 2            | 19          |
| 1            | 20          |
| 2            | 21          |
| 2            | 26          |
| 1            | 27          |
| 1            | 29          |
| 1            | 31          |
| 1            | 56          |


###  Nombre d’ets par siren

```
WITH table_ets AS (
SELECT COUNT(*) as nb_etb, siren
FROM(
SELECT siren,"Nom_Greffe", "code_greffe",
      numero_gestion, id_etablissement,
    COUNT(*) AS CNT
FROM "inpi"."ets_test_filtered"
GROUP BY siren,"Nom_Greffe", "code_greffe",
      numero_gestion, id_etablissement
HAVING COUNT(*) > 1
ORDER BY siren,"Nom_Greffe", "code_greffe",
      numero_gestion, id_etablissement
  )
group by siren
)
SELECT COUNT(*) as full_count, nb_etb
FROM table_ets
GROUP BY nb_etb
ORDER by nb_etb
```
Il y a 558990 entreprises avec un seul établissement. Il y a 327167 entreprises avec 2 établissements.

| full_count | nb_etb |
|------------|--------|
| 558990     | 1      |
| 327167     | 2      |
| 15377      | 3      |
| 2709       | 4      |
| 889        | 5      |
| 425        | 6      |
| 284        | 7      |
| 211        | 8      |
| 164        | 9      |
| 111        | 10     |
| 82         | 11     |
| 79         | 12     |
| 61         | 13     |
| 49         | 14     |
| 39         | 15     |
| 46         | 16     |
| 32         | 17     |
| 35         | 18     |
| 22         | 19     |
| 24         | 20     |
| 28         | 21     |
| 22         | 22     |
| 17         | 23     |
| 10         | 24     |
| 18         | 25     |
| 12         | 26     |
| 15         | 27     |
| 7          | 28     |
| 9          | 29     |
| 8          | 30     |
| 8          | 31     |
| 9          | 32     |
| 10         | 33     |
| 8          | 34     |
| 2          | 35     |
| 4          | 36     |
| 8          | 37     |
| 2          | 38     |
| 2          | 39     |
| 5          | 40     |
| 6          | 41     |
| 4          | 42     |
| 3          | 43     |
| 6          | 44     |
| 5          | 45     |
| 2          | 46     |
| 1          | 48     |
| 3          | 49     |
| 4          | 50     |
| 4          | 52     |
| 5          | 53     |
| 1          | 54     |
| 2          | 55     |
| 5          | 57     |
| 1          | 58     |
| 3          | 59     |
| 4          | 61     |
| 3          | 62     |
| 2          | 63     |
| 2          | 64     |
| 1          | 65     |
| 3          | 66     |
| 3          | 67     |
| 2          | 68     |
| 5          | 69     |
| 3          | 70     |
| 2          | 71     |
| 3          | 72     |
| 2          | 73     |
| 1          | 75     |
| 1          | 76     |
| 2          | 77     |
| 1          | 80     |
| 1          | 82     |
| 1          | 83     |
| 1          | 84     |
| 2          | 86     |
| 1          | 88     |
| 2          | 92     |
| 2          | 93     |
| 2          | 96     |
| 2          | 97     |
| 1          | 98     |
| 1          | 99     |
| 2          | 100    |
| 1          | 102    |
| 1          | 105    |
| 1          | 109    |
| 3          | 110    |
| 2          | 111    |
| 1          | 112    |
| 1          | 113    |
| 1          | 114    |
| 2          | 118    |
| 2          | 125    |
| 1          | 126    |
| 1          | 129    |
| 1          | 133    |
| 1          | 134    |
| 2          | 135    |
| 1          | 146    |
| 1          | 152    |
| 1          | 154    |
| 1          | 160    |
| 1          | 171    |
| 2          | 175    |
| 1          | 176    |
| 1          | 177    |
| 1          | 182    |
| 1          | 192    |
| 1          | 195    |
| 1          | 202    |
| 1          | 210    |
| 2          | 217    |
| 1          | 218    |
| 1          | 222    |
| 1          | 244    |
| 1          | 249    |
| 1          | 258    |
| 1          | 292    |
| 1          | 323    |
| 1          | 349    |
| 1          | 356    |
| 1          | 398    |
| 1          | 425    |
| 1          | 460    |
| 1          | 719    |
| 1          | 803    |
| 1          | 890    |
| 1          | 1925   |
