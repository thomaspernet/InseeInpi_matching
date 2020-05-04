# Introduction

Le dossier [00_telechargement](https://github.com/thomaspernet/InseeInpi_matching/tree/master/Notebooks_matching/Data_preprocessed/programme_matching/00_telechargement) a trois objectifs principaux:

- Récupérer la donnée de l'INPI provenant du FTP
  * Un [notebook](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/00_telechargement/00_DownloadFromFTP.ipynb) va gérer le téléchargement depuis le FTP de l'INPI.

- Déposer la donnée brute dans le S3 
  * Un [notebook](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/00_telechargement/01_SaveRAWFilesToS3.ipynb) va déposer la donnée brute dans le S3 pour conserver l'historique de données sans aucune modification.
  
- Préparer la donnée brute en y ajoutant des données de classement et de suivi
   - soit directement depuis le S3:
     - Un [notebook](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/00_telechargement/02_PrepareRawDataInS3.ipynb) va utiliser les données brutes du S3 et préparer des fichiers qu'il enregistre dans le S3 directement.
  
   - soit en local :
     - Un [notebook](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/00_telechargement/00_DownloadFilesFromS3.ipynb) pour télécharger des fichiers ou dossiers depuis le S3 en local.
     - Un [notebook](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/00_telechargement/02_PrepareRawDataLocal.ipynb) va préparer les données en local.
     - Un [notebook](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/00_telechargement/01_SavePreparedFilesToS3.ipynb) va déposer la donnée préparée dans le S3.  



Des modules utiles pour les tests et la reprise sur erreur sont également disponibles:
   - Un [notebook](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/00_telechargement/03_ConvertFileZillaXMLFileList.ipynb) permet de récupérer la liste des fichiers présents sur le FTP INPI grâce à un export FileZilla.  
   - Un [notebook](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/00_telechargement/03_CountAndCompareFileLists.ipynb) permet de vérifier la complétude des listes de fichiers en comparant ce qui est présent dans le S3 et dans le FTP INPI.
   - Un [script python](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/00_telechargement/utils.py) contenant les fonctions qui appliquent les règles de gestion pour le calcul du classement dans S3.


# 0 Téléchargement depuis le FTP INPI

Le téléchargement depuis le FTP INPI a plusieurs contraintes :
- la connexion FTP est bloquée par le proxy sur un poste standard CALF.
- il y a un quota de téléchargement de 3Go par jour.
- la connexion est coupée automatiquement par l'hôte au bout d'une centaine de fichiers récupérés via un script.
- l'arborescence dans le FTP est profonde et pas incrémentale car liée à la codification des greffes. 
Ex : 
     - /public
          - /IMR_Donnees_Saisies
              - /tc
                   - /flux
                       - /2017
                           - /08
                               - /04
                                   - /0101
                                       - /57
                                   - /0202
                                       - /60
                                   - /0203
                                       - /58
                                   - /0301
                                       - /58
                                   - ..etc


Pour télécharger les fichiers, on peut utiliser:
- un client FTP type Filezilla (le plus simple et complet, en Drag & Drop, gère la reprise sur erreur)

- le [notebook](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/00_telechargement/00_DownloadFromFTP.ipynb) générique basé sur la librairie ftputil. Attention, étant donné la coupure au bout d'une centaine de fichiers, on ne peut pas facilement télécharger l'intégralité des fichiers avec ce notebook.

  - Fonctions :
     - Télécharger un fichier depuis le FTP
     - Télécharger tous les fichiers d'un dossier spécifique depuis le FTP
     - Lister tous les fichiers du FTP / d'un dossier spécifique



- le [notebook](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/00_telechargement/00_DownloadFromFTP_specificINPI.ipynb) spécifique au FTP INPI qui parcourt une arborescence sur mesure.


# 1 Sauvegarde des fichiers sur le S3

Le [notebook](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/00_telechargement/01_SaveRAWFilesToS3.ipynb) stocke les données du FTP brutes dans le dossier [00_RawData](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/TC_1/00_RawData/?region=eu-west-3&tab=overview)
Ni les fichiers ni l'arborescence de stockage ne sont modifiés.


Fonctions utiles:
 - Calculer la nature, origine, date du fichier. Fonction ``get_file_infos``.
 - Calculer où le fichier doit être déposé dans le S3 en fonction de la nature, origine, date du fichier. Fonction ``calc_dest``
 - Préparation des fichiers: Dezipper



# 2 Modification de la donnée brute

L'objectif est de garder trace des informations d'origine du fichier si les données sont manipulées par la suite (agrégées, modifiées, déplacées ...)

Fonction ``add_source_info`` : 

On ajoute les colonnes suivantes aux csv:
 - ``csv_source`` : nom du fichier source. Exemple format: 3801_189_20180130_065752_9_ets_nouveau_modifie_EVT.csv 
 - ``file_timestamp`` = la date du fichier ou une date du fichier source (par ex date_greffe) pour les stock initial qui ont tous par défaut une date de fichier à 2017-05-24.
 - ``origin_type`` : Flux ou Stock.
 - ``origin`` : Initial ou Partiel pour Stock, NEW ou EVT pour Flux.
 - ``nature`` : ACTES, ETS, OBS, COMPTES_ANNUELS, PM, PP, REP.



Deux notebooks permettent de faire cette étape:
 - [notebook](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/00_telechargement/02_PrepareRawDataLocal.ipynb) utilisant des fichiers raw en local et ce [notebook](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/00_telechargement/01_SavePreparedFilesToS3.ipynb) pour la sauvegarde dans S3
 - ou ce [notebook](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/00_telechargement/02_PrepareRawDataInS3.ipynb) utilisant les fichiers directement sur le s3.
 
 
La donnée est stockée dans le dossier [01_donnee_source](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/TC_1/01_donnee_source/?region=eu-west-3&tab=overview)
et l'arborescence simplifiée.


# 3 Vérification de la complétude

On veut s'assurer que l'on a bien le même nombre de fichiers:
 - dans le FTP INPI
 - dans le S3 Raw
 - dans le S3 Données source

Fonctions utiles pour vérifier la complétude: 
 - lister les objets présents sur le FTP à partir d'un extract xml via ce [notebook](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/00_telechargement/03_ConvertFileZillaXMLFileList.ipynb)

 - ``count_objects_s3`` : lister tous les fichiers présents dans un dossier du S3
 - lire les listes d'objets et vérifier les différences et doublons potentiels via ce [notebook](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/00_telechargement/03_CountAndCompareFileLists.ipynb)

La liste horodatée des fichiers présents en RAW et SOURCE est disponible dans le [dossier de logs](https://s3.console.aws.amazon.com/s3/buckets/calfdata/INPI/TC_1/logs/count%2520files/?region=eu-west-3&tab=overview)
