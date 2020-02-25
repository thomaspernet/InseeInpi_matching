# Presentation Algorithme

# Class preparation Data

Le coude source est disponible [ici](https://github.com/thomaspernet/InseeInpi_matching/blob/dev_thomas/Notebooks_matching/programme_matching/inpi_insee/preparation_data.py) et le notebook pour lancer le programme est disponible [ici](https://github.com/thomaspernet/InseeInpi_matching/blob/dev_thomas/Notebooks_matching/programme_matching/Preparation_data.ipynb)

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

- [inpi_etb](https://github.com/thomaspernet/InseeInpi_matching/blob/dev_thomas/Notebooks_matching/programme_matching/Source_intermediates.ipynb)

## Normalisation du fichier INPI.

Le fichier INPI doit contenir un seul fichier gz avant d'être ingéré par le programme. Le fichier va être importé dans un format Dask, ce qui permet de paralléliser les calcules et bien sur d'éviter les problèmes de mémoire. 

La normalisation du fichier de l'INPI se fait en plusieurs étapes:

1) Exclusion des observations contenant des NaN pour chacune des variables candidates, à savoir:

    - Adresse_Ligne1
    - Adresse_Ligne2
    - Adresse_Ligne3
    - Code_Postal
    - Ville
    - Code_Commune
2) Extraction des SIREN a SIRETISER -> cela evite d'utiliser toute la base INSEE pour la sirétisation. I.e Speedup le process
3) Calcule du nombre de SIRET par SIREN via la fonction `nombre_siret_siren`
4) Normalisation de la variable commune via la fonction `clean_commune`

    - Extraction des digits dans la ville. En effet, certaines communes incluent l'arrondissement dans la variable.
    - Extraction des caractères spéciaux et espaces
    - Capitalisation du nom de la commune
    - Matching avec le fichier commune pour avoir le nom de la commune de l'INSEE.
5) Préparation de l'adresse via la fonction `prepare_adress`
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

1) Filtre les SIREN à sirétiser uniquement

2) Filtre la date limite à l'INSEE. Cette étape sert essentiellement pour siretiser les bases de stocks. Cela évite d'utiliser des valeurs "dans le future" -> inconnu à l'INPI

3) Remplacement des "-" par des " " dans la variable `libelleCommuneEtablissement`

4) Extraction des digits en format liste de la variable `libelleVoieEtablissement`

5) Calcule du nombre de SIRET par SIREN

6) Calcule du nombre de digit dans la variable `libelleCommuneEtablissement`

    - Si len inférieure a 2, alors NaN. C'est une variable utlisée pendant le matching des règles spéciales
    
Le fichier est sauvegardé en format gz, et dans un table SQL

    - insee_2017_SIZE.gz
    - App_insee.db
    
Un appercu de la table est disponible via cette application `App_insee`.

# Algorithme Siretisation

Le code source est disponible [ici](https://github.com/thomaspernet/InseeInpi_matching/blob/dev_thomas/Notebooks_matching/programme_matching/inpi_insee/siretisation.py) et le notebook pour lancer le programme est disponible [ici](https://github.com/thomaspernet/InseeInpi_matching/blob/dev_thomas/Notebooks_matching/programme_matching/Siretisation.ipynb)

L'algorithme de SIRETISATION fonctionne avec l'aidre de trois fonctions:

- `step_one`: permet d'écarter les doublons du merge et d'appliquer les premières règles afin de connaitre l'origine de la siretisation
- `step_two_assess_test`: détermine l'origine du matching, a savoir la date, adresse, voie, numéro de voie
- `step_two_duplication`: permet de récuperer des SIRET sur les doublons émanant du merge avec l'INSEE

Dans premier temps, on créer un dictionnaire avec toutes les variables de matching. Toutefois, l'algorithme va utiliser séquentiellement les variables suivantes:

```
 {'ncc', 'Code_Postal', 'Code_Commune', 'INSEE', 'digit_inpi'},
 {'ncc', 'Code_Postal', 'Code_Commune', 'INSEE'},
 {'ncc', 'Code_Postal', 'Code_Commune', 'digit_inpi'},
 {'ncc', 'Code_Postal', 'Code_Commune'},   
 {'ncc', 'Code_Postal'},
 {'ncc'},
 {'Code_Postal'},
 {'Code_Commune'}
```

Pour connaitre l'ensemble des variables de matching INSEE/INPI, veuillez vous rendre en [annexe](#annexe).

Dans la mesure ou l'algorithme fonctionne de manière séquentielle, et utilise comme input un fichier de l'INPI a siretiser. De fait, après chaque séquence, l'algorithme sauvegarde un fichier gz contenant les siren a trouver. Cette étape de sauvegarde en gz permet de loader le fichier gz en input en Dataframe Dask.

## Step One

La première étape de la séquence est l'ingestion d'un fichier gz contenant les SIREN a trouver. L'ingestion va se faire en convertissant le dataframe en Dask. L'algorithme tout d'abord utiliser la fonction `step_one` et produit deux dataframes selon si le matching avec l'INSEE a débouté sur des doublons ou non. 

Les doublons sont générés si pour un même nombre de variables de matching, il existe plusieurs possibilités à l'INSEE. Par exemple, pour un siren, ville, adressse donnée, il y a plusieurs possibilité. Cela constitue un double et il sera traiter ultérieurement, dans la mesure du possible. 

Les étapes déroulées lors du premier processus est le suivant:

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

Deux dataframe sont crées, un ne contenant pas de doublon pas les doublons et un deuxième contenant les doublon. L'algorithme va réaliser les tests sur le premier et faire d'avantage de recherche sur le second

## step_two_assess_test

Le premier dataframe ne contient pas de doublon, il est donc possible de réaliser différents tests afin de mieux déterminer
l'origine du matching. Plus précisement, si le matching a pu se faire sur la date, l'adresse, la voie, numéro de voie et le nombre unique d'index. Les règles sont définies ci-dessous.

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

Un premier fichier gz est enregistré contenant les "pure matches"

## step_two_duplication

Les second dataframe contient les doublons obtenus après le matching avec l'INSEE. L'algorithme va travailler sur différentes variables de manière séquencielle pour tenter de trouver les bons siret. Plus précisément, 3 variables qui ont été récemment créé sont utilisées:

- test_join_address -> True si la variable test_address_libelle = True (ie mot INPI trouvé dans INSEE) et test_join_address =  True
- test_address_libelle ->  True si la variable test_address_libelle = True (ie mot INPI trouvé dans INSEE)
- test_address_complement -> True si la variable test_join_address =  True

Pour chaque séquence, on réalise les tests suivants:

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



# Algorithme

Ici, on va faire l'exemple sur un fichier INPI créer au cours de la troisième séquence. 

    C:\Users\PERNETTH\AppData\Local\Continuum\anaconda3\lib\site-packages\dask\dataframe\utils.py:14: FutureWarning: pandas.util.testing is deprecated. Use the functions in the public API at pandas.testing instead.
      import pandas.util.testing as tm
    

    [########################################] | 100% Completed |  8.9s
    




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>siren</th>
      <th>Date_Début_Activité</th>
      <th>count_initial_inpi</th>
      <th>Type</th>
      <th>Pays</th>
      <th>Ville</th>
      <th>ncc</th>
      <th>Code_Postal</th>
      <th>Code_Commune</th>
      <th>Adress_new</th>
      <th>Adresse_new_clean_reg</th>
      <th>INSEE</th>
      <th>digit_inpi</th>
      <th>list_digit_inpi</th>
      <th>len_digit_address_inpi</th>
      <th>index</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>384684437</td>
      <td>1992-01-15</td>
      <td>1</td>
      <td>SEP</td>
      <td>FRANCE</td>
      <td>Trévoux</td>
      <td>TREVOUX</td>
      <td>01600</td>
      <td>01347</td>
      <td>LA GRAVIERE SAINT DIDIER DE FORMANS</td>
      <td>GRAVIERE$|SAINT$|DIDIER$|FORMANS$</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>0</td>
      <td>5</td>
    </tr>
    <tr>
      <th>1</th>
      <td>824659734</td>
      <td>2016-11-28</td>
      <td>1</td>
      <td>SEP</td>
      <td>FRANCE</td>
      <td>Miribel</td>
      <td>MIRIBEL</td>
      <td>01706</td>
      <td>01249</td>
      <td>836 ROUTE DE TRAMOYES LES ECHETS</td>
      <td>TRAMOYES$|ECHETS$</td>
      <td>RTE</td>
      <td>836</td>
      <td>NaN</td>
      <td>1</td>
      <td>10</td>
    </tr>
    <tr>
      <th>2</th>
      <td>442377040</td>
      <td>2002-07-01</td>
      <td>2</td>
      <td>PRI</td>
      <td>FRANCE</td>
      <td>Buellas</td>
      <td>BUELLAS</td>
      <td>01310</td>
      <td>NaN</td>
      <td>200 ROUTE DE TREVOUX</td>
      <td>TREVOUX$</td>
      <td>RTE</td>
      <td>200</td>
      <td>NaN</td>
      <td>1</td>
      <td>15</td>
    </tr>
    <tr>
      <th>3</th>
      <td>442377040</td>
      <td>2015-06-01</td>
      <td>2</td>
      <td>SEC</td>
      <td>FRANCE</td>
      <td>Saint-André-sur-Vieux-Jonc</td>
      <td>SAINT ANDRE SUR VIEUX JONC</td>
      <td>01960</td>
      <td>01336</td>
      <td>300 RUE DE LA MAIRIE</td>
      <td>MAIRIE$</td>
      <td>RUE</td>
      <td>300</td>
      <td>NaN</td>
      <td>1</td>
      <td>16</td>
    </tr>
    <tr>
      <th>4</th>
      <td>814101168</td>
      <td>2015-01-01</td>
      <td>1</td>
      <td>SEP</td>
      <td>FRANCE</td>
      <td>Etrez</td>
      <td>ETREZ</td>
      <td>01340</td>
      <td>01154</td>
      <td>1453 ROUTE DE FOISSIAT LIEUDIT LA SPIRE</td>
      <td>FOISSIAT$|LIEUDIT$|SPIRE$</td>
      <td>RTE</td>
      <td>1453</td>
      <td>NaN</td>
      <td>1</td>
      <td>17</td>
    </tr>
  </tbody>
</table>
</div>



Pour la première étape, on va matcher avec les variables suivante à l'INPI et à l'INSEE




    ['siren', 'Code_Postal', 'ncc', 'Code_Commune', 'digit_inpi']






    ['siren',
     'codePostalEtablissement',
     'libelleCommuneEtablissement',
     'codeCommuneEtablissement',
     'numeroVoieEtablissement']



## Step One:  Merge 2 Dataframes

Le code va produire deux dataframes, un premier sans les doublons et un autre avec les doublons. 

    [########################################] | 100% Completed |  1min  1.2s
    

### Etats sur les matchés

Il y a 434488 lignes de matchés, pour un nombre de 324413 de SIREN




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>siren</th>
      <th>Date_Début_Activité</th>
      <th>count_initial_inpi</th>
      <th>Type</th>
      <th>Pays</th>
      <th>Ville</th>
      <th>ncc</th>
      <th>Code_Postal</th>
      <th>Code_Commune</th>
      <th>Adress_new</th>
      <th>...</th>
      <th>libelleCommuneEtrangerEtablissement</th>
      <th>distributionSpecialeEtablissement</th>
      <th>codeCommuneEtablissement</th>
      <th>codeCedexEtablissement</th>
      <th>libelleCedexEtablissement</th>
      <th>codePaysEtrangerEtablissement</th>
      <th>libellePaysEtrangerEtablissement</th>
      <th>etatAdministratifEtablissement</th>
      <th>count_duplicates_</th>
      <th>origin</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>803282508</td>
      <td>2014-07-01</td>
      <td>1</td>
      <td>SEP</td>
      <td>FRANCE</td>
      <td>Saint-Martin-du-Frêne</td>
      <td>SAINT MARTIN DU FRENE</td>
      <td>01430</td>
      <td>01373</td>
      <td>60 GRANDE RUE</td>
      <td>...</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>01373</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>A</td>
      <td>1</td>
      <td>test_1_no_duplication</td>
    </tr>
    <tr>
      <th>1</th>
      <td>422812610</td>
      <td>1999-06-30</td>
      <td>2</td>
      <td>SEP</td>
      <td>FRANCE</td>
      <td>Viriat</td>
      <td>VIRIAT</td>
      <td>01440</td>
      <td>01451</td>
      <td>400 CHEMIN DE LA CRAZ</td>
      <td>...</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>01451</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>A</td>
      <td>1</td>
      <td>test_1_no_duplication</td>
    </tr>
    <tr>
      <th>2</th>
      <td>819942400</td>
      <td>2016-05-26</td>
      <td>1</td>
      <td>PRI</td>
      <td>FRANCE</td>
      <td>Belley</td>
      <td>BELLEY</td>
      <td>01300</td>
      <td>01034</td>
      <td>92 GRANDE RUE</td>
      <td>...</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>01034</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>F</td>
      <td>1</td>
      <td>test_1_no_duplication</td>
    </tr>
    <tr>
      <th>3</th>
      <td>493618235</td>
      <td>2006-12-22</td>
      <td>1</td>
      <td>SEP</td>
      <td>FRANCE</td>
      <td>PONT DE VEYLE</td>
      <td>PONT DE VEYLE</td>
      <td>01290</td>
      <td>01306</td>
      <td>50 GRANDE RUE</td>
      <td>...</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>01306</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>A</td>
      <td>1</td>
      <td>test_1_no_duplication</td>
    </tr>
    <tr>
      <th>4</th>
      <td>823065305</td>
      <td>2016-10-01</td>
      <td>2</td>
      <td>SEP</td>
      <td>FRANCE</td>
      <td>Seyssel</td>
      <td>SEYSSEL</td>
      <td>74910</td>
      <td>74269</td>
      <td>2 QUAI DU RHONE</td>
      <td>...</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>74269</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>F</td>
      <td>1</td>
      <td>test_1_no_duplication</td>
    </tr>
  </tbody>
</table>
<p>5 rows × 39 columns</p>
</div>



### Etats sur les doublons

Il y a 5424 lignes de doublons, pour un nombre de 1821 de SIREN




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>siren</th>
      <th>Date_Début_Activité</th>
      <th>count_initial_inpi</th>
      <th>Type</th>
      <th>Pays</th>
      <th>Ville</th>
      <th>ncc</th>
      <th>Code_Postal</th>
      <th>Code_Commune</th>
      <th>Adress_new</th>
      <th>...</th>
      <th>libelleCommuneEtrangerEtablissement</th>
      <th>distributionSpecialeEtablissement</th>
      <th>codeCommuneEtablissement</th>
      <th>codeCedexEtablissement</th>
      <th>libelleCedexEtablissement</th>
      <th>codePaysEtrangerEtablissement</th>
      <th>libellePaysEtrangerEtablissement</th>
      <th>etatAdministratifEtablissement</th>
      <th>count_duplicates_</th>
      <th>origin</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>3</th>
      <td>480470152</td>
      <td>2007-01-31</td>
      <td>254</td>
      <td>SEC</td>
      <td>FRANCE</td>
      <td>Brest</td>
      <td>BREST</td>
      <td>29200</td>
      <td>29019</td>
      <td>ROUTE DE GOUESNOU RD PT DE KERGARADEC ZONE D ...</td>
      <td>...</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>29019</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>A</td>
      <td>2</td>
      <td>test_2_duplication</td>
    </tr>
    <tr>
      <th>4</th>
      <td>480470152</td>
      <td>2007-01-31</td>
      <td>254</td>
      <td>SEC</td>
      <td>FRANCE</td>
      <td>Brest</td>
      <td>BREST</td>
      <td>29200</td>
      <td>29019</td>
      <td>ROUTE DE GOUESNOU RD PT DE KERGARADEC ZONE D ...</td>
      <td>...</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>29019</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>A</td>
      <td>2</td>
      <td>test_2_duplication</td>
    </tr>
    <tr>
      <th>5</th>
      <td>480470152</td>
      <td>2007-01-31</td>
      <td>254</td>
      <td>SEC</td>
      <td>FRANCE</td>
      <td>Bordeaux</td>
      <td>BORDEAUX</td>
      <td>33300</td>
      <td>33063</td>
      <td>CENTRE COMMERCIAL  AUCHAN</td>
      <td>...</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>33063</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>A</td>
      <td>2</td>
      <td>test_2_duplication</td>
    </tr>
    <tr>
      <th>6</th>
      <td>480470152</td>
      <td>2007-01-31</td>
      <td>254</td>
      <td>SEC</td>
      <td>FRANCE</td>
      <td>Bordeaux</td>
      <td>BORDEAUX</td>
      <td>33300</td>
      <td>33063</td>
      <td>CENTRE COMMERCIAL  AUCHAN</td>
      <td>...</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>33063</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>F</td>
      <td>2</td>
      <td>test_2_duplication</td>
    </tr>
    <tr>
      <th>7</th>
      <td>342122546</td>
      <td>2015-06-21</td>
      <td>52</td>
      <td>SEC</td>
      <td>France</td>
      <td>Beauvais</td>
      <td>BEAUVAIS</td>
      <td>60000</td>
      <td>60057</td>
      <td>LE BOIS QUEQUET ROUTE DE BEAUVAIS A BERNEUIL</td>
      <td>...</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>60057</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>A</td>
      <td>2</td>
      <td>test_2_duplication</td>
    </tr>
  </tbody>
</table>
<p>5 rows × 39 columns</p>
</div>



## Step 2: No duplication

Creation des tests pour connaitre la/les méthodes de matching.

Pour rappel, les tests sont les suivants:

- 'test_address_libelle',
- 'test_address_complement',
- 'test_join_address',
- 'test_date',
- 'test_1',
- 'test_siege',
- 'test_voie',
- 'test_numero'
- 'count_duplicates_final'
- len_digit_address_insee : optional

    [########################################] | 100% Completed | 37.5s
    [########################################] | 100% Completed | 37.6s
    [########################################] | 100% Completed |  0.4s
    


![png](Presentation_files/Presentation_22_0.png)



![png](Presentation_files/Presentation_23_0.png)


## Step 2 duplication

    [########################################] | 100% Completed |  0.4s
    [########################################] | 100% Completed |  0.5s
    [########################################] | 100% Completed |  0.1s
    


![png](Presentation_files/Presentation_26_0.png)



![png](Presentation_files/Presentation_27_0.png)





<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>siren</th>
      <th>Date_Début_Activité</th>
      <th>count_initial_inpi</th>
      <th>Type</th>
      <th>Pays</th>
      <th>Ville</th>
      <th>ncc</th>
      <th>Code_Postal</th>
      <th>Code_Commune</th>
      <th>Adress_new</th>
      <th>...</th>
      <th>count_siren_siret</th>
      <th>test_address_libelle</th>
      <th>test_address_complement</th>
      <th>test_join_address</th>
      <th>test_date</th>
      <th>test_1</th>
      <th>test_siege</th>
      <th>test_voie</th>
      <th>test_numero</th>
      <th>count_duplicates_final</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2</th>
      <td>480470152</td>
      <td>2007-01-31</td>
      <td>254</td>
      <td>SEC</td>
      <td>FRANCE</td>
      <td>Bordeaux</td>
      <td>BORDEAUX</td>
      <td>33300</td>
      <td>33063</td>
      <td>CENTRE COMMERCIAL  AUCHAN</td>
      <td>...</td>
      <td>NaN</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>True</td>
      <td>NaN</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>2</td>
    </tr>
    <tr>
      <th>3</th>
      <td>480470152</td>
      <td>2007-01-31</td>
      <td>254</td>
      <td>SEC</td>
      <td>FRANCE</td>
      <td>Bordeaux</td>
      <td>BORDEAUX</td>
      <td>33300</td>
      <td>33063</td>
      <td>CENTRE COMMERCIAL  AUCHAN</td>
      <td>...</td>
      <td>NaN</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>True</td>
      <td>NaN</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>2</td>
    </tr>
    <tr>
      <th>4</th>
      <td>342122546</td>
      <td>2015-06-21</td>
      <td>52</td>
      <td>SEC</td>
      <td>France</td>
      <td>Beauvais</td>
      <td>BEAUVAIS</td>
      <td>60000</td>
      <td>60057</td>
      <td>LE BOIS QUEQUET ROUTE DE BEAUVAIS A BERNEUIL</td>
      <td>...</td>
      <td>NaN</td>
      <td>True</td>
      <td>True</td>
      <td>True</td>
      <td>True</td>
      <td>NaN</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>2</td>
    </tr>
    <tr>
      <th>5</th>
      <td>342122546</td>
      <td>2015-06-21</td>
      <td>52</td>
      <td>SEC</td>
      <td>France</td>
      <td>Beauvais</td>
      <td>BEAUVAIS</td>
      <td>60000</td>
      <td>60057</td>
      <td>LE BOIS QUEQUET ROUTE DE BEAUVAIS A BERNEUIL</td>
      <td>...</td>
      <td>NaN</td>
      <td>True</td>
      <td>True</td>
      <td>True</td>
      <td>True</td>
      <td>NaN</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>2</td>
    </tr>
    <tr>
      <th>14</th>
      <td>424442762</td>
      <td>2017-03-06</td>
      <td>113</td>
      <td>SEC</td>
      <td>France</td>
      <td>Saint-Étienne-de-Lugdarès</td>
      <td>SAINT ETIENNE DE LUGDARES</td>
      <td>07590</td>
      <td>07232</td>
      <td>PLATEAU DE CHAM LONGE LE COURBIL</td>
      <td>...</td>
      <td>NaN</td>
      <td>True</td>
      <td>True</td>
      <td>True</td>
      <td>True</td>
      <td>NaN</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>3</td>
    </tr>
  </tbody>
</table>
<p>5 rows × 49 columns</p>
</div>



## Exemple de reglès spéciales 

### Recuperation via list_digit_ INPI/INSEE

On va utliser la récupération des digits dans l'adresse INPI/INPI qui est pour le moment au format liste, puis vérifier si ils correpondent dans les deux fichiers.

Attention, la technique ne marche que lorsque les digits sont dans le même ordre. Une amélioration via regex va être mise en place ultérieurement.

Dans le fichier ci dessous de traitement spéciale, il y a 1261 SIREN sans SIRET, c'est à dire, que l'algorithme n'est pas sur à 100% du SIRET.

Exemple avec le SIREN `752085324`. Dans le fichier `_special_treatment`, il y a plus de 9300 combinaisons possibles pour ce SIREN, pour 97 SIRET. Via la règles spéciale, il est possible de récupérer 86 SIRET.




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Adress_new</th>
      <th>libelleVoieEtablissement</th>
      <th>list_digit_inpi</th>
      <th>list_digit_insee</th>
      <th>siret</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>32906</th>
      <td>LIEU DIT D51 SAINT ESTEVE LOT 2</td>
      <td>D51-SAINT ESTEVE-LOT 2</td>
      <td>['51', '2']</td>
      <td>['51', '2']</td>
      <td>75208532400453</td>
    </tr>
    <tr>
      <th>33002</th>
      <td>LIEU DIT 51 SAINT ESTEVE LOT 1</td>
      <td>D51-SAINT ESTEVE-LOT 1</td>
      <td>['51', '1']</td>
      <td>['51', '1']</td>
      <td>75208532400446</td>
    </tr>
    <tr>
      <th>33101</th>
      <td>LIEU DIT D51 SAINT ESTEVE LOT 3</td>
      <td>D51-SAINT ESTEVE-LOT 3</td>
      <td>['51', '3']</td>
      <td>['51', '3']</td>
      <td>75208532400461</td>
    </tr>
    <tr>
      <th>33199</th>
      <td>LIEU DIT D51 SAINT ESTEVE LOT 4</td>
      <td>D51-SAINT ESTEVE-LOT 4</td>
      <td>['51', '4']</td>
      <td>['51', '4']</td>
      <td>75208532400479</td>
    </tr>
    <tr>
      <th>33299</th>
      <td>LIEU DIT D51 SAINT ESTEVE LOT 5</td>
      <td>D51-SAINT ESTEVE-LOT 5</td>
      <td>['51', '5']</td>
      <td>['51', '5']</td>
      <td>75208532400503</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>41722</th>
      <td>LIEU DIT RD 11 LAS TRAVESSES LOT 66</td>
      <td>RD11-LAS TRAVESSES-LOT 66</td>
      <td>['11', '66']</td>
      <td>['11', '66']</td>
      <td>75208532400347</td>
    </tr>
    <tr>
      <th>41823</th>
      <td>LIEU DIT RD 11 LAS TRAVESSES LOT 68</td>
      <td>RD11-LAS TRAVESSES-LOT 68</td>
      <td>['11', '68']</td>
      <td>['11', '68']</td>
      <td>75208532400388</td>
    </tr>
    <tr>
      <th>41922</th>
      <td>LIEU DIT RD 11 LAS TRAVESSES LOT 69</td>
      <td>RD11-LAS TRAVESSES-LOT 69</td>
      <td>['11', '69']</td>
      <td>['11', '69']</td>
      <td>75208532400404</td>
    </tr>
    <tr>
      <th>42021</th>
      <td>LIEU DIT RD 11 LAS TRAVESSES LOT 70</td>
      <td>RD11-LAS TRAVESSES-LOT 70</td>
      <td>['11', '70']</td>
      <td>['11', '70']</td>
      <td>75208532400420</td>
    </tr>
    <tr>
      <th>42112</th>
      <td>LIEU DIT RD 11 LAS TRAVESSES LOT 67</td>
      <td>RD11-LAS TRAVESSES-LOT 67</td>
      <td>['11', '67']</td>
      <td>['11', '67']</td>
      <td>75208532400362</td>
    </tr>
  </tbody>
</table>
<p>86 rows × 5 columns</p>
</div>



# annexe

```
[{'match': {'inpi': ['siren',
    'INSEE',
    'ncc',
    'Code_Postal',
    'Code_Commune',
    'digit_inpi'],
   'insee': ['siren',
    'typeVoieEtablissement',
    'libelleCommuneEtablissement',
    'codePostalEtablissement',
    'codeCommuneEtablissement',
    'numeroVoieEtablissement']}},
 {'match': {'inpi': ['siren', 'ncc', 'Code_Commune', 'Code_Postal', 'INSEE'],
   'insee': ['siren',
    'libelleCommuneEtablissement',
    'codeCommuneEtablissement',
    'codePostalEtablissement',
    'typeVoieEtablissement']}},
 {'match': {'inpi': ['siren',
    'ncc',
    'Code_Commune',
    'Code_Postal',
    'digit_inpi'],
   'insee': ['siren',
    'libelleCommuneEtablissement',
    'codeCommuneEtablissement',
    'codePostalEtablissement',
    'numeroVoieEtablissement']}},
 {'match': {'inpi': ['siren', 'ncc', 'Code_Commune', 'Code_Postal'],
   'insee': ['siren',
    'libelleCommuneEtablissement',
    'codeCommuneEtablissement',
    'codePostalEtablissement']}},
 {'match': {'inpi': ['siren', 'ncc', 'Code_Postal'],
   'insee': ['siren',
    'libelleCommuneEtablissement',
    'codePostalEtablissement']}},
 {'match': {'inpi': ['siren', 'ncc'],
   'insee': ['siren', 'libelleCommuneEtablissement']}},
 {'match': {'inpi': ['siren', 'Code_Postal'],
   'insee': ['siren', 'codePostalEtablissement']}},
 {'match': {'inpi': ['siren', 'Code_Commune'],
   'insee': ['siren', 'codeCommuneEtablissement']}}]
```

