# Workflow Preparation data INPI/INSEE



L'objectif du répository est de prépaper la data de l'[INPI](https://entreprise.data.gouv.fr/api_doc_rncs) en vue d'un rapprochement avec la base établissement de l'[INSEE](https://www.insee.fr/fr/metadonnees/definition/c1609).

Le workflow actuel repose sur 5 étapes séquentielles, a savoir le téléchargement de la donnée INPI, la préparation des différentes catégories (ETS, PP, PM, REP), la normalisation de la donnée entre les ETS/PP et la base établissement de l'INSEE. Finalement, un programme de siretisation pour les ETS/PP. Une vue d'ensemble de chacun des processus est expliqué dans le `README` rattaché au dossier, alors que le détail fonctionnel du/des programmes est écrit dans les notebooks.

## Tables des matières

1. [Documentation](https://github.com/thomaspernet/InseeInpi_matching/tree/master/Documentation)
   1. [IMR](https://github.com/thomaspernet/InseeInpi_matching/tree/master/Documentation/IMR)
      1. [Catalogue de donnée](https://github.com/thomaspernet/InseeInpi_matching/tree/master/Documentation/IMR#catalogue-de-donn%C3%A9es)
         - [Actes](https://github.com/thomaspernet/InseeInpi_matching/tree/master/Documentation/IMR#actes): ACTES -> Pages 39-40
         - [Comptes Annuels](https://github.com/thomaspernet/InseeInpi_matching/tree/master/Documentation/IMR#comptes-annuels):COMPTESANN -> Pages 40-41
         - [Observations](https://github.com/thomaspernet/InseeInpi_matching/tree/master/Documentation/IMR#observations):OBS -> Pages 38-39
         - [Etablissements](https://github.com/thomaspernet/InseeInpi_matching/tree/master/Documentation/IMR#etablissements):ETS -> Pages 25-29
         - [Personnes Morales](https://github.com/thomaspernet/InseeInpi_matching/tree/master/Documentation/IMR#personnes-morales):PM -> Pages 15-18
         - [Personnes Physiques](https://github.com/thomaspernet/InseeInpi_matching/tree/master/Documentation/IMR#personnes-physiques):PP -> Pages 19-24
         - [Représentants](https://github.com/thomaspernet/InseeInpi_matching/tree/master/Documentation/IMR#repr%C3%A9sentants): REP -> Pages 30-37
   2. [INSEE](https://github.com/thomaspernet/InseeInpi_matching/tree/master/Documentation/INSEE)
      1. [Données INSEE](https://github.com/thomaspernet/InseeInpi_matching/tree/master/Documentation/INSEE#donn%C3%A9es-insee)
         1. [Unite legale](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Documentation/INSEE/Description%20fichier%20StockUniteLegale.pdf)
         2. [Etablissement](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Documentation/INSEE/description-fichier-stocketablissement.pdf)
  3. Definition entreprise & Etablissements
      1. [Introduction entreprise et etablissements](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/-/blob/master/Documentation/0_introduction_entreprise_etablissement.md)
      2.
2. Integration données INSEE/INPI
   1. [Preparation INPI + INSEE](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/-/tree/master/10_sumup_preparation)
   2. [rapprochement INPI + INSEE & siretisation](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/-/tree/master/11_sumup_siretisation)
   3. [Exemple Spark Calcul Cosine](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/-/tree/master/12_spark)
