# Algorithme Siretisation

Le code source est disponible [ici](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/inpi_insee/siretisation.py) et le notebook pour lancer le programme est disponible [ici](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/02_siretisation/00_Siretisation.ipynb)

L'algorithme de SIRETISATION fonctionne avec l'aide de trois fonctions:

- `step_one`: permet d'écarter les doublons du merge et d'appliquer les premières règles afin de connaitre l'origine de la siretisation
- `step_two_assess_test`: détermine l'origine du matching, a savoir la date, adresse, voie, numéro de voie
- `step_two_duplication`: permet de récuperer des SIRET sur les doublons émanant du merge avec l'INSEE

Dans premier temps, on crée un dictionnaire avec toutes les variables de matching. L'algorithme va utiliser séquentiellement les variables suivantes:

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

L'algorithme fonctionne de manière séquentielle, et utilise comme input un fichier de l'INPI a siretiser. De fait, après chaque séquence, l'algorithme sauvegarde un fichier gz contenant les siren a trouver. Cette étape de sauvegarde en gz permet de loader le fichier gz en input.

## Step One

La première étape de la séquence est l'ingestion d'un fichier gz contenant les SIREN a trouver. L'ingestion va se faire en convertissant le dataframe en Dask. L'algorithme tout d'abord utiliser la fonction `step_one` et produire deux dataframes selon si le matching avec l'INSEE a débouté sur des doublons ou non.

Les doublons sont générés si pour un même nombre de variables de matching, il existe plusieurs possibilités à l'INSEE. Par exemple, pour un siren, ville, adressse donnée, il y a plusieurs possibilités. Cela constitue un doublon et il sera traité ultérieurement, dans la mesure du possible.

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

Deux dataframe sont créés, un ne contenant pas de doublon et un deuxième contenant les doublons. L'algorithme va réaliser les tests sur le premier et faire d'avantage de recherche sur le second.

## step_two_assess_test

Le premier dataframe ne contient pas de doublon, il est donc possible de réaliser différents tests afin de mieux déterminer l'origine du matching. Plus précisement, si le matching a pu se faire sur la date, l'adresse, la voie, numéro de voie et le nombre unique d'index. Les règles sont définies ci-dessous.

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

Un premier fichier csv est enregistré contenant les "pure matches"

## step_two_duplication

Les seconds dataframes contiennent les doublons obtenus après le matching avec l'INSEE. L'algorithme va travailler sur différentes variables de manière séquentielle pour tenter de trouver le bons SIRET. Plus précisément, 3 variables qui ont été récemment créées sont utilisées:

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

**Vue d'ensemble du programme**

![](https://www.lucidchart.com/publicSegments/view/5a8cb28f-dc42-4708-babd-423962514878/image.png)



# Test de vérification SIRET

## Exemple de séquence

1.SIREN avec 1 seul établissement

  - https://eu-west-3.console.aws.amazon.com/athena/home?force=&region=eu-west-3#query/history/49f1b17f-1806-4a5b-b52b-70968eaaea6b

2. SIREN avec 2 établissements, communes différentes

3. SIREN avec 2 établissements, même communes, adresses différentes

4. SIREN avec 2 établissements, même communes, même adresse, activités différentes

5. SIREN avec 2 établissements, même communes, adresse proche (même rue, numéro voie différent par ex)

6. SIREN avec 4 établissements, 3 code postaux

7. SIREN avec établissement ayant changé d'adresse?

8. SIREN gestion du SEP (Siège et Principal) et de sa séparation?

9. Autres

  - 272800038 adresse quasiment identique => Unmatched

Création d'une table temporaire pour filtrer plus facilement les cas d'exemple: https://eu-west-3.console.aws.amazon.com/athena/home?force=&region=eu-west-3#query/history/21d933ee-3f78-4ad0-ae47-6afb4209efb5

Sélection des exemples:

https://eu-west-3.console.aws.amazon.com/athena/home?force=&region=eu-west-3#query/history/301ca06e-4898-45a6-8275-ded0e3ce06c8

### Vérification des Unmatched

Sélection des exemples:

https://eu-west-3.console.aws.amazon.com/athena/home?force=&region=eu-west-3#query/history/696449b8-fbaa-4eb5-a8f1-82f2c6a0363e

10. Unmatched SIREN avec un seul etablissement
11. Unmatched SIREN avec un plusieurs etablissements

=> souvent dans le cas des établissement siège et principal à la même adresse.

=> amélioration algo à apporter?

=> EX adresse exactement identique 180036147, 213500606, 210600987

=> Ex adresse proche 210600565, 271927212



## Réponse SIREN



| Source | Sequence | SIREN     | Status   |
| ------ | -------- | --------- | -------- |
| ETS    |          | 12871489  |          |
|        |          | 15451248  |          |
| ETS    | 1        | 822069092 | Ok       |
| ETS    | 1        | 533022331 | Ok       |
| ETS    | 2        | 542035795 | Ok       |
| ETS    | 2        | 542043153 | Ok       |
| ETS    | 3        | 5650031   | Ok       |
| ETS    | 3        | 5950134   | Ok       |
| ETS    | 4        | 39198189  |          |
| ETS    | 5        | 7120066   | Ok       |
| ETS    | 5        | 25680471  | Ok       |
| ETS    | 6        | 380954610 | problème |
| ETS    | 6        | 329023121 | Ok       |
| ETS    | 7        |           |          |
| ETS    | 8        | 450529524 | Ok       |
| ETS    | 9        | 59805150  | Ok       |
| ETS    | 9        | 272800038 | Ok       |
| ETS    |          | 12871489  | Ok       |
| ETS    | 9        | 272800038 | problème |
| ETS    | 9        | 504491093 | Ok       |
| ETS    | 9        | 380535534 | Ok       |
| ETS    |          | 5741509   | problème |
| ETS    | 10       | 110043015 | Ok       |
| ETS    | 10       | 273200014 | Ok       |
| ETS    | 10       | 213500606 | Ok       |
| ETS    | 11       | 180089203 | Ok       |
| ETS    | 11       | 212900187 | Ok       |
| ETS    | 11       | 270700016 | problème |
| ETS    | 11       | 277200036 | problème |



## Problèmes/vérifications

- SIREN: 380954610

  - Query ATHENA: https://eu-west-3.console.aws.amazon.com/athena/home?force=&region=eu-west-3#query/history/e82fc77f-b937-4eba-a0dc-4178d1cda764

  - Description problème

    - le SIREN est présent dans le raw INSEE
      - le SIREN n'est pas présent ni dans la table match, unmatch, ts

  - Réponse:

  - https://github.com/thomaspernet/InseeInpi_matching/blob/master/Notebooks_matching/Data_preprocessed/programme_matching/inpi_insee/preparation_data.py#L448

    - Dans le code, on exclut les SIREN qui n'ont pas d'information dans les variables de matching

    - pour le 380954610 , les variables adresses, code postal, ville etc sont vides. 
      - https://github.com/thomaspernet/InseeInpi_matching/issues/5

- Requête pour avoir tous les siren dans ce cas - 14.759 siren trouvés (table siren_lost_in_matching dans ATHENA) : 

```
/*siren_done_in_matching*/
DROP TABLE `siren_done_in_matching`;
CREATE TABLE inpi.siren_done_in_matching
WITH (
  format='PARQUET'
) AS
 select distinct siren from
 (
  (select siren from inpi.inpi_siret_initial_partiel_ets_matched) UNION ALL
  (select siren from inpi.inpi_siret_initial_partiel_ets_unmatched) UNION ALL
  (select siren from inpi.inpi_siret_initial_partiel_ets_ts)
  );
  
  
/*siren_empty_matchingFields*/
-- on exclut les SIREN qui n'ont pas d'information dans les variables de matching (adresses, code postal, ville)
DROP TABLE `siren_empty_matchingFields`;
CREATE TABLE inpi.siren_empty_matchingFields
WITH (
  format='PARQUET'
) AS
select distinct siren
FROM "inpi"."initial_partiel_evt_new_etb"
 where (
  adresse_ligne1 = '' AND 
  adresse_ligne2 = '' AND 
  code_postal = '' AND 
  ville = '' AND 
  code_commune = ''  
  )
;
  
/*siren_lost_in_matching*/
DROP TABLE `siren_lost_in_matching`;
CREATE TABLE inpi.siren_lost_in_matching
WITH (
  format='PARQUET'
) AS
select distinct siren
FROM "inpi"."initial_partiel_evt_new_ets_status_final"
where siren not in (select siren from inpi.siren_done_in_matching)
AND siren not in (select siren from inpi.siren_empty_matchingFields);

select count(*) from siren_lost_in_matching; -- 14.759

select * from siren_lost_in_matching limit 100;
```

- Nouvel exemple : SIREN:005741509
    - Query ATHENA: https://eu-west-3.console.aws.amazon.com/athena/home?force=&region=eu-west-3#query/history/baabfd0e-5241-4ab1-a253-55f3a04c9cb7

- SIREN: 272800038 

    - Query ATHENA:https://eu-west-3.console.aws.amazon.com/athena/home?force=&region=eu-west-3#query/history/2deed8ab-ee6c-45fa-86b8-c3f623e528ae

    - Description problème

        - adresse quasiment identique (une adresse où il est précisé REZ DE CHAUSSEE)
        - => Se retrouve dans Unmatched. Alors qu'il y a bien un différenciant entre les 2 adresses.
        - => Complément d'adresse à prendre en compte dans l'algorithme pour l'améliorer?

- SIREN : 277200036

    - Query ATHENA :https://eu-west-3.console.aws.amazon.com/athena/home?force=&region=eu-west-3#query/history/f9b4cce3-0349-4a06-b28e-3aa59638227d

    - Description problème

    - Il y a 2 établissements unmatched dans ce cas
        - celui au 37 RUE DE L ESTEREL => 11 établissements à la même adresse, même activité, mais enseigne1etablissement différents => le unmatch est normal. Amélioration algorithme possible pour prendre en compte enseigne1etablissement? (autre ex : 275600013)
        - par contre celui au 332 334 AVENUE BOLLEE devrait être matché avec l'établissement 00055 car il n'y a pas d'ambiguité

