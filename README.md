# DataBase

Codes pour le matching des bases de données INPI/INSEE

Le repository contient 3 dossiers pour chaque provider:

- INPI
  - [TITMC](https://scm.saas.cagip.group.gca/PERNETTH/database/tree/master/INPI/TITMC)
  - TC
- INSEE
- Waldec

Pipeline matching

![](https://scm.saas.cagip.group.gca/PERNETTH/database/raw/master/IMAGES/Schema_matching.JPG)

 # Documentation

## INPI
## Couverture et données disponibles

### Actes et statuts

#### Couverture

Une consultation gratuite simple et rapide des données issues du registre national du commerce et des sociétés, accessible à tous !

#### Données disponibles

- Inscriptions relatives aux immatriculations, modifications et radiations, déposées auprès des greffes des tribunaux à compétence commerciale et enregistrées au registre national du commerce et des société
stock des dossiers des personnes morales et physiques actives à jour à la date du 4 mai  2017 pour les tribunaux de commerce et du 5 mai 2018 pour les tribunaux d’instance et les tribunaux mixtes de commerce (stock)
nouvelles inscriptions transmises par les greffes à compter du 5 mai 2017 (données de flux)
- Données non confidentielles saisies à partir des comptes annuels déposés auprès des greffes des **tribunaux de commerce (TC), tribunaux d’instance des départements du Bas-Rhin, du Haut-Rhin et de la Moselle (TI), tribunaux mixtes de commerce des départements et régions d'outre-mer (TMC)** et transmis à l’INPI dans le cadre de ses missions
comptes annuels déposés depuis le 1er janvier 2017 (données relatives aux bilans (actif/passif), comptes de résultat, immobilisations, amortissements et provisions
- actes et statuts des personnes morales et physiques depuis 1993 (stock)
nouveaux actes transmis par les greffes des tribunaux à compétence commerciale (données de flux)

- Les TITMC n'ont qu'une infime partie de l'ensemble des entreprises en France (environ 485110)

#### Volumétrie (estimation)

environ 6 millions d’entreprises françaises
mise à jour

 Le contenu mis à disposition par l’INPI comprend :
*  Les dossiers des données relatives aux personnes actives (morales et physiques) :
  * Un stock initial constitué à la date du 4 mai 2017 pour les données issues des Tribunaux de commerce (TC)
  * Un stock initial constitué à la date du 5 mai 2018 pour les données issues des Tribunaux d’instance et Tribunaux mixtes de commerce (TI/TMC)
* Des stocks partiels constitués des dossiers complets relivrés à la demande de l’INPI après détection d’anomalies ? Les fichiers des données contenues dans les nouvelles inscriptions (immatriculations, modifications et radiations) du Registre national du commerce et des sociétés ainsi que les informations relatives aux dépôts des actes et comptes annuels, telles que transmises par les greffes à compter du 5 mai 2017 (données de flux).
Au total, ce sont les données d’environ 5 millions de personnes actives (morales et physiques) qui sont mises à la disposition de l’ensemble des ré-utilisateurs.
Ces données sont mises à jour quotidiennement (environ 1,4 million de nouvelles inscriptions par an).
Les tribunaux représentés sont au nombre de 148 répartis comme suit (liste fournie en annexe) :
*  134 Tribunaux de commerce,
*  7 Tribunaux d’Instance des départements du Bas-Rhin, du Haut-Rhin et de la Moselle,
*  7 Tribunaux mixtes de commerce des départements et régions d'outre-mer.

### Les tribunaux d’Instance et tribunaux mixtes de commerce

Les tribunaux d’Instance et tribunaux mixtes de commerce couvrent les départements suivants :
*  Départements du Bas-Rhin, du Haut-Rhin et de la Moselle (TI)
*  Départements et régions d'outre-mer (TMC)

Apparement, changement de format en 2020

* https://github.com/etalab/rncs_worker_api_entreprise#import-des-donn%C3%A9es-des-greffes-des-tribunaux-dinstance-et-de-commerce-mixte-titmc
