# Class preparation Data

Le coude source est disponible [ici](https://github.com/thomaspernet/InseeInpi_matching/blob/dev_thomas/Notebooks_matching/programme_matching/inpi_insee/preparation_data.py)

## INSEE

Les données sources de l'INSEE proviennent de [Data Gouv](https://www.data.gouv.fr/fr/datasets/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret/)

- communes_insee:
    - Le fichier source pour les communes se trouvent à cette [URL](https://www.insee.fr/fr/information/3720946)
    - Le notebook pour reconstituer le csv est disponible a cette [URL](https://github.com/thomaspernet/InseeInpi_matching/blob/dev_thomas/Notebooks_matching/programme_matching/Source_intermediates.ipynb). ⚠️ Repo privé + branche
- voie:
    - Le fichier source pour les communes se trouvent à cette [URL](https://www.sirene.fr/sirene/public/variable/libelleVoieEtablissement)
    - Le notebook pour reconstituer le csv est disponible a cette [URL](https://github.com/thomaspernet/InseeInpi_matching/blob/dev_thomas/Notebooks_matching/programme_matching/Source_intermediates.ipynb). ⚠️ Repo privé + branche
- upper_word:
    - La liste des upper word (stop word capitalisé) provient de la librarie [NLTK](https://www.nltk.org/) avec un ajout manuel.
    
## INPI 

Les données de l'INPI proviennent de ses différents Notebooks:

- inpi_etb

## Normalisation du fichier INPI.

Le fichier INPI doit contenir un seul fichier gz avant d'être ingéré par le programme. Le fichier va être importé dans un format Dask, ce qui permet de paralléliser les calcules et bien sur d'éviter les problèmes de mémoire. 

La normalisation du fichier de l'INPI se fait en plusieurs étapes:

- 1) Exclusion des observations contenant des NaN pour chacune des variables candidates, à savoir:
    - Adresse_Ligne1
    - Adresse_Ligne2
    - Adresse_Ligne3
    - Code_Postal
    - Ville
    - Code_Commune
- 2) Extraction des SIREN a SIRETISER -> cela evite d'utiliser toute la base INSEE pour la sirétisation. I.e Speedup le process
- 3) Calcule du nombre de SIRET par SIREN via la fonction `nombre_siret_siren`
- 4) Normalisation de la variable commune via la fonction `clean_commune`
    - Extraction des digits dans la ville. En effet, certaines communes incluent l'arrondissement dans la variable.
    - Extraction des caractères spéciaux et espaces
    - Capitalisation du nom de la commune
    - Matching avec le fichier commune pour avoir le nom de la commune de l'INSEE.
- 5) Préparation de l'adresse via la fonction `prepare_adress`
    - Concatenation des variables `Adresse_Ligne1` + `Adresse_Ligne2` + `Adresse_Ligne3`
    - Normalisation de la variable concatenée -> Extraction des caractères speciaux, espace, digit puis capitalisation
    - Extraction de tous les stop words du fichier `upper_word`
    - Split de chaque mot restant de l'adresse 
    - Creation du regex de la forme suivante:  `MOT1$|MOT2$` 
    - Extration des digits:
        - Première variable avec le premier digit
        - Seconde variable avec une liste de digit et jointure -> DIGIT1|DIGIT2
    - Merge avec le fichier `voie` pour obtenir le type de voie de l'INSEE
    - Calcule du nombre de digit dans l'adresse
        - Si len inférieure a 2, alors NaN. C'est une variable utlisée pendant le matching des règles spéciales
    - Creation d'une variable `index` correspondant à l'index du dataframe. Indispensable
 
Le fichier est sauvegardé en format gz, et dans un table SQL
    - inpi_etb_stock_0.gz
    - inpi_origine.db
    
Un appercu de la table est disponible via cette application `App_inpi`.

## Normalisation du fichier INSEE

Pour l'étape de siretisation, les variables candidates sont les suivantes:

- 'siren',
- 'siret',
- "etablissementSiege",
- "etatAdministratifEtablissement",
- "numeroVoieEtablissement",
- "indiceRepetitionEtablissement",
- "typeVoieEtablissement",
- "libelleVoieEtablissement",
- "complementAdresseEtablissement",
- "codeCommuneEtablissement",
- "libelleCommuneEtablissement",
- "codePostalEtablissement",
- "codeCedexEtablissement",
- "libelleCedexEtablissement",
- "distributionSpecialeEtablissement",
- "libelleCommuneEtrangerEtablissement",
- "codePaysEtrangerEtablissement",
- "libellePaysEtrangerEtablissement",
- "dateCreationEtablissement"

Comme pour le fichier de l'INPI, le fichier csv est importé en Dask Dataframe. Les étapes sont les suivantes:

- 1) Filtre les SIREN à sirétiser uniquement
- 2) Filtre la date limite à l'INSEE. Cette étape sert essentiellement pour siretiser les bases de stocks. Cela évite d'utiliser des valeurs "dans le future" -> inconnu à l'INPI
- 3) Remplacement des "-" par des " " dans la variable `libelleCommuneEtablissement`
- 4) Extraction des digits en format liste de la variable `libelleVoieEtablissement`
- 5) Calcule du nombre de SIRET par SIREN
- 6) Calcule du nombre de digit dans la variable `libelleCommuneEtablissement`
    - Si len inférieure a 2, alors NaN. C'est une variable utlisée pendant le matching des règles spéciales
    
Le fichier est sauvegardé en format gz, et dans un table SQL
    - insee_2017_SIZE.gz
    - App_insee.db
    
Un appercu de la table est disponible via cette application `App_insee`.

# Algorithme Siretisation

Le code source est disponible [ici](https://github.com/thomaspernet/InseeInpi_matching/blob/dev_thomas/Notebooks_matching/programme_matching/inpi_insee/siretisation.py)

L'algorithme de SIRETISATION fonctionne avec l'aidre de trois fonctions:

- `step_one`: permet d'écarter les doublons du merge et d'appliquer les premières règles afin de connaitre l'origine de la siretisation
- `step_two_assess_test`: détermine l'origine du matching, a savoir la date, adresse, voie, numéro de voie
- `step_two_duplication`: permet de récuperer des SIRET sur les doublons émanant du merge avec l'INSEE

## Step One

```
- Test 1: doublon
        - non: Save-> `test_1['not_duplication']`
        - oui:
            - Test 2: Date equal
                - oui:
                    - Test 2 bis: doublon
                        - non: Save-> `test_2_bis['not_duplication']`
                        - oui: Save-> `test_2_bis['duplication']`
                - non:
                    - Test 3: Date sup
                        - oui:
                            - Test 2 bis: doublon
                                - non: Save-> `test_3_oui_bis['not_duplication']`
                                - oui: Save-> `test_3_oui_bis['duplication']`
                        - non: Save-> `test_3_non`
```

## step_two_assess_test

```
- Test 1: address libelle
            - Si mots dans inpi est contenu dans INSEE, True
        - Test 1 bis: address complement
            - Si mots dans inpi est contenu dans INSEE, True
        - Test 2: Date
            - dateCreationEtablissement >= Date_Début_Activité OR
            Date_Début_Activité = NaN OR (nombre SIREN a l'INSEE = 1 AND nombre
            SIREN des variables de matching = 1), True
        - Test 3: siege
            - Type = ['SEP', 'SIE'] AND siege = true, True
        - Test 4: voie
            - Type voie INPI = Type voie INSEE, True
        - Test 5: numero voie
            - Numero voie INPI = Numero voie INSEE, True
```

## step_two_duplication

```
- Si test_join_address = True:
        - Test 1: doublon:
            - Oui: append-> `df_not_duplicate`
            - Non: Pass
            - Exclue les `index` de df_duplication
            - then go next
        - Si test_address_libelle = True:
            - Test 1: doublon:
                - Oui: append-> `df_not_duplicate`
                - Non: Pass
                - Exclue les `index` de df_duplication
                - then go next
        - Si test_address_complement = True:
            - Test 1: doublon:
                - Oui: append-> `df_not_duplicate`
                - Non: Pass
                - Exclue les `index` de df_duplication
 ```
 
On peut sauvegarder le `df_not_duplicate` et le restant en tant que `special_treatment`


![](https://www.lucidchart.com/publicSegments/view/5a8cb28f-dc42-4708-babd-423962514878/image.png)
