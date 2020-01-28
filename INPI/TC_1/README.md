# TITMC

## Les tribunaux d’Instance et tribunaux mixtes de commerce

Les tribunaux d’Instance et tribunaux mixtes de commerce couvrent les départements suivants :
*  Départements du Bas-Rhin, du Haut-Rhin et de la Moselle (TI)
*  Départements et régions d'outre-mer (TMC)

Apparement, changement de format en 2020

* https://github.com/etalab/rncs_worker_api_entreprise#import-des-donn%C3%A9es-des-greffes-des-tribunaux-dinstance-et-de-commerce-mixte-titmc
Import des données des Greffes des Tribunaux d'Instance et de Commerce Mixte (TITMC)
Jusqu'ici, les données des Tribunaux d'Instance et des Tribunaux Mixtes de Commerce étaient transmises au format XML, différent de celui des Tribunaux de Commerce, et malheureusement inexploitables.
Cette différence de format pour les TITMC ne devrait plus être à partir de la fin de l'année 2019, période à partir de laquelle les données de ces greffes seront normalement transmises en respectant le même protocole et le même format que les TC aujourd'hui.
Stock des données TI/TMC
A l’intérieur du répertoire IMR_Données_Saisies > titmc > stock, les fichiers sont organisés par greffe, puis par année/mois/jour (date de transmission).

Attention : Le stock initial des données TI/TMC (IMR et ACTES) étant constitué à la date du 5 mai 2018, il est impératif d’intégrer de manière incrémentale toutes les données de flux mises à disposition sur serveur à partir du 6 mai 2018 (date de transmission).

### Données de flux

Les fichiers de flux en provenance des greffes sont transmis à une fréquence quotidienne, en fonction de l’actualité des greffes.
Il est possible que certains jours, certains greffes n’envoient pas de données, ou bien que certains greffes envoient plusieurs flux de données par jour.
Les fichiers de flux sont répartis dans 2 sous répertoires en fonction de leur provenance :
*  Tribunaux de commerce : répertoire public > IMR_Données_Saisies > tc > flux
*  Tribunaux d’instance et mixtes de commerce : répertoire public > IMR_Données_Saisies > titmc > flux

A l’intérieur de ces répertoires, les fichiers sont organisés par date de transmission, puis par greffe, puis par numéro de transmission. Ils sont accompagnés de fichiers md5.

Cas spécifique des dossiers complets retransmis après correction par les greffes (stocks partiels)

Données TI/TMC : Les dossiers corrigés issus des Tribunaux d’instance et Tribunaux mixtes de commerce continuent à être transmis dans le flux, en suivant le processus de mise à jour habituel en annule et remplace cf. pages suivantes.

Format
Les formats des fichiers diffèrent en fonction des tribunaux :
*  Tribunaux de commerce (134 greffes) : fichiers au format UTF8 (stock et flux), de type CSV (séparateur point-virgule) et donc lisibles dans un tableur même si ce n’est pas la finalité. Cette réutilisation par tableurs n’est pas conseillée, des conversions inexactes pouvant être appliquées par certains tableurs (ex. : Excel – dates).
*  Tribunaux d’instance et tribunaux mixtes de commerce : fichiers au format XML (stock et flux)
DESCRIPTION DES FICHIERS DE FLUX
Chaque fichier couvre un ensemble de siren pour un greffe donné. Ces fichiers doivent être impérativement intégrés de manière incrémentale, par ordre chronologique de date de transmission.

Le contenu et l’organisation de ces répertoires sont décrits ci-après
Chaque greffe fournit pour chaque siren un dossier contenant les informations suivantes :
*  La fiche d’identité de la personne morale ou physique et l’EIRL éventuellement associée
*  Les établissements enregistrés au sein du greffe
* Les représentants de de la personne morale
* Les procédures collectives
* Les informations relatives à l’enregistrement d’actes au sein du greffe
*  Les informations relatives au dépôt de comptes annuels auprès du greffe

Un greffe ne transmet que les formalités enregistrées par une personne morale ou physique auprès de ce greffe, qu’il s’agisse d’une immatriculation, d’une modification ou d’une radiation, ainsi que les informations relatives aux établissements situés dans son ressort.
L’identifiant fonctionnel d’un dossier est constitué du couple code greffe / n° de gestion. A noter que pour un siren et un greffe donnés, il peut y avoir plusieurs n° de gestion (dans le cas par exemple d’une radiation suivie d’une ré-immatriculation, notamment suite à un changement de forme juridique avec changement de catégorie).

Les dossiers par siren sont transmis sous la forme de 3 fichiers contenant les informations suivantes :
*  RCS : Identité, Représentants, Etablissements, Procédures collectives, EIRL
* ACT : Dépôts d’actes
*  BIL : Dépôts de comptes annuels

### Structure nom des fichiers

Les fichiers de flux en provenance des greffes sont transmis à une fréquence quotidienne, en fonction de l’actualité des greffes.
Il est possible que certains jours, certains greffes n’envoient pas de données, ou bien que certains greffes envoient plusieurs flux de données par jour.
Les fichiers de flux sont répartis dans 2 sous répertoires en fonction de leur provenance :
? Tribunaux de commerce : répertoire public > IMR_Données_Saisies > tc > flux
? Tribunaux d’instance et mixtes de commerce : répertoire public > IMR_Données_Saisies > titmc > flux

A l’intérieur de ces répertoires, les fichiers sont organisés par date de transmission, puis par greffe, puis par numéro de transmission. Ils sont accompagnés de fichiers md5.

Exemple:
* 20180531220508TITMCFLUX ->20180531 2205 08
  *  date de transmission: 20180531
  * greffe:
  * numéro de transmission:

### STRUCTURE DES FICHIERS DE DONNEES IMR

La structure générale des fichiers de données est contenue dans la balise <fichier> et en cas de mise à jour de cette structure le numéro de version est incrémenté.

Les informations associées à chaque société sont structurées en rubriques :
*  <idt> … </idt> : balise associée à la rubrique « identité »
* <reps> … </reps> : balise associée à la rubrique « représentant »
* <etabs> … </etabs> : balise associée à la rubrique « établissement »
* <procs> … </procs> : balise associée à la rubrique « procédures collectives »
* <acts> … </acts> : balise associée à la rubrique « actes »
* <bils> … </bils> : balise associée à la rubrique « bilans »
