# Processus global



Le workflow actuel permet de récupérer les données sur le FTP, puis prépare la data en vue d'une ingestion dans la base de donnée ou d'une sirétisation des fichiers avant ingestion. Il y a plusieurs étapes a effectuer de manière séquentielle, qui sont les suivantes:

1. Un [notebook](https://github.com/thomaspernet/InseeInpi_matching/tree/master/Notebooks_matching/Data_preprocessed/programme_matching/00_telechargement) qui récupère la donnée du FTP 

2. 4 [notebooks](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/01_preparation/01_Athena_concatenate_ETS.ipynb) qui concatenatent la data et qui la met en forme (ETS/PP/PM/REP)

3. Un troisième [notebook](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/02_siretisation/00_Siretisation_etb.ipynb) qui normalise la donnée INPI/INSEE en vue de la siretisation (ETS/PP)

4. Un quatrième [notebook](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/02_siretisation/00_Siretisation_etb.ipynb) qui siretise les ETS/PP 

5. Un cinquième [notebook](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/03_reconstruction/00_Reconstitution.ipynb) qui reconstruit la donnée ( ETS/PP )