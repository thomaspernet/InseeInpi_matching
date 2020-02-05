# Données INSEE

La documentation détaillée de l'INSEE est disponible ci dessous pour:

- [Unite legale](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Documentation/INSEE/Description%20fichier%20StockUniteLegale.pdf)
- [Etablissement](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Documentation/INSEE/description-fichier-stocketablissement.pdf)

## Unité légale

|    | Nom                                       | Libellé                                                                | Longueur | Type           | Ordre |
|----|-------------------------------------------|------------------------------------------------------------------------|----------|----------------|-------|
| 1  | activitePrincipaleUniteLegale             | Activité principale de l’unité légale                                  | 6        | Liste de codes | 29    |
| 2  | anneeCategorieEntreprise                  | Année de validité de la catégorie d’entreprise                         | 4        | Date           | 19    |
| 3  | anneeEffectifsUniteLegale                 | Année de validité de la tranche d’effectif salarié de l’unité légale   | 4        | Date           | 15    |
| 4  | caractereEmployeurUniteLegale             | Caractère employeur de l’unité légale                                  | 1        | Liste de codes | 33    |
| 5  | categorieEntreprise                       | Catégorie à laquelle appartient l’entreprise                           | 3        | Liste de codes | 18    |
| 6  | categorieJuridiqueUniteLegale             | Catégorie juridique de l’unité légale                                  | 4        | Liste de codes | 28    |
| 7  | dateCreationUniteLegale                   | Date de création de l'unité légale                                     | 10       | Date           | 4     |
| 8  | dateDebut                                 | Date de début d'une période d'historique d'une unité légale            | 10       | Date           | 20    |
| 9  | dateDernierTraitementUniteLegale          | Date du dernier traitement de l’unité légale dans le répertoire Sirene | 19       | Date           | 16    |
| 10 | denominationUniteLegale                   | Dénomination de l’unité légale                                         | 120      | Texte          | 24    |
| 11 | denominationUsuelle1UniteLegale           | Dénomination usuelle de l’unité légale                                 | 70       | Texte          | 25    |
| 12 | denominationUsuelle2UniteLegale           | Dénomination usuelle de l’unité légale – deuxième champ                | 70       | Texte          | 26    |
| 13 | denominationUsuelle3UniteLegale           | Dénomination usuelle de l’unité légale – troisième champ               | 70       | Texte          | 27    |
| 14 | economieSocialeSolidaireUniteLegale       | Appartenance au champ de l’économie sociale et solidaire               | 1        | Liste de codes | 32    |
| 15 | etatAdministratifUniteLegale              | État administratif de l’unité légale                                   | 1        | Liste de codes | 21    |
| 16 | identifiantAssociationUniteLegale         | Numéro au Répertoire National des Associations                         | 10       | Texte          | 13    |
| 17 | nicSiegeUniteLegale                       | Numéro interne de classement (Nic) de l’unité légale                   | 5        | Texte          | 31    |
| 18 | nombrePeriodesUniteLegale                 | Nombre de périodes de l’unité légale                                   | 2        | Numérique      | 17    |
| 19 | nomenclatureActivitePrincipaleUniteLegale | Nomenclature d’activité de la variable activitePrincipaleUniteLegale   | 8        | Liste de codes | 30    |
| 20 | nomUniteLegale                            | Nom de naissance de la personnes physique                              | 100      | Texte          | 22    |
| 21 | nomUsageUniteLegale                       | Nom d’usage de la personne physique                                    | 100      | Texte          | 23    |
| 22 | prenom1UniteLegale                        | Premier prénom déclaré pour un personne physique                       | 20       | Texte          | 7     |
| 23 | prenom2UniteLegale                        | Deuxième prénom déclaré pour un personne physique                      | 20       | Texte          | 8     |
| 24 | prenom3UniteLegale                        | Troisième prénom déclaré pour un personne physique                     | 20       | Texte          | 9     |
| 25 | prenom4UniteLegale                        | Quatrième prénom déclaré pour un personne physique                     | 20       | Texte          | 10    |
| 26 | prenomUsuelUniteLegale                    | Prénom usuel de la personne physique                                   | 20       | Texte          | 11    |
| 27 | pseudonymeUniteLegale                     | Pseudonyme de la personne physique                                     | 100      | Texte          | 12    |
| 28 | sexeUniteLegale                           | Caractère féminin ou masculin de la personne physique                  | 1        | Liste de codes | 6     |
| 29 | sigleUniteLegale                          | Sigle de l’unité légale                                                | 20       | Texte          | 5     |
| 30 | siren                                     | Numéro Siren                                                           | 9        | Texte          | 1     |
| 31 | statutDiffusionUniteLegale                | Statut de diffusion de l’unité légale                                  | 1        | Liste de codes | 2     |
| 32 | trancheEffectifsUniteLegale               | Tranche d’effectif salarié de l’unité légale                           | 2        | Liste de codes | 14    |
| 33 | unitePurgeeUniteLegale                    | Unité légale purgée                                                    | 5        | Texte          | 3     |

## Etablissement

|    | Nom                                            | Libellé                                                                              | Longueur | Type           | Ordre |
|----|------------------------------------------------|--------------------------------------------------------------------------------------|----------|----------------|-------|
| 1  | activitePrincipaleRegistreMetiersEtablissement | Activité exercée par l?artisan inscrit au registre des métiers                        | 6        | Liste de codes | 8     |
| 2  | anneeEffectifsEtablissement                    | Année de validité de la tranche d?effectif salarié de l?établissement                  | 4        | Date           | 7     |
| 3  | caractereEmployeurEtablissement                | Caractère employeur de l?établissement                                                | 1        | Liste de codes | 48    |
| 4  | codeCedex2Etablissement                        | Code cedex de l'adresse secondaire                                                    | 9        | Texte          | 36    |
| 5  | codeCedexEtablissement                         | Code cedex                                                                           | 9        | Texte          | 22    |
| 6  | codeCommune2Etablissement                      | Code commune de l?adresse secondaire                                                  | 5        | Liste de codes | 35    |
| 7  | codeCommuneEtablissement                       | Code commune de l?établissement                                                       | 5        | Liste de codes | 21    |
| 8  | codePaysEtranger2Etablissement                 | Code pays de l?adresse secondaire pour un établissement situé à l?étranger             | 5        | Liste de codes | 38    |
| 9  | codePaysEtrangerEtablissement                  | Code pays pour un établissement situé à l?étranger                                    | 5        | Liste de codes | 24    |
| 10 | codePostal2Etablissement                       | Code postal de l?adresse secondaire                                                   | 5        | Texte          | 31    |
| 11 | codePostalEtablissement                        | Code postal                                                                          | 5        | Texte          | 17    |
| 12 | complementAdresse2Etablissement                | Complément d?adresse secondaire                                                       | 38       | Texte          | 26    |
| 13 | complementAdresseEtablissement                 | Complément d?adresse                                                                  | 38       | Texte          | 12    |
| 14 | dateCreationEtablissement                      | Date de création de l?établissement                                                   | 10       | Date           | 5     |
| 15 | dateDebut                                      | Date de début d'une période d'historique d'un établissement                          | 10       | Date           | 40    |
| 16 | dateDernierTraitementEtablissement             | Date du dernier traitement de l?établissement dans le répertoire Sirene               | 19       | Date           | 9     |
| 17 | denominationUsuelleEtablissement               | Dénomination usuelle de l?établissement                                               | 100      | Texte          | 45    |
| 18 | distributionSpeciale2Etablissement             | Distribution spéciale de l?adresse secondaire de l?établissement                       | 26       | Texte          | 34    |
| 19 | distributionSpecialeEtablissement              | Distribution spéciale de l?établissement                                              | 26       | Texte          | 20    |
| 20 | enseigne1Etablissement                         | Première ligne d?enseigne de l?établissement                                           | 50       | Texte          | 42    |
| 21 | enseigne2Etablissement                         | Deuxième ligne d?enseigne de l?établissement                                           | 50       | Texte          | 43    |
| 22 | enseigne3Etablissement                         | Troisième ligne d?enseigne de l?établissement                                          | 50       | Texte          | 44    |
| 23 | etablissementSiege                             | Qualité de siège ou non de l?établissement                                            | 5        | Texte          | 10    |
| 24 | etatAdministratifEtablissement                 | État administratif de l?établissement                                                 | 1        | Liste de codes | 41    |
| 25 | indiceRepetition2Etablissement                 | Indice de répétition dans la voie pour l?adresse secondaire                           | 1        | Texte          | 28    |
| 26 | indiceRepetitionEtablissement                  | Indice de répétition dans la voie                                                    | 1        | Texte          | 14    |
| 27 | libelleCedex2Etablissement                     | Libellé du code cedex de l?adresse secondaire                                         | 100      | Texte          | 37    |
| 28 | libelleCedexEtablissement                      | Libellé du code cedex                                                                | 100      | Texte          | 23    |
| 29 | libelleCommune2Etablissement                   | Libellé de la commune de l?adresse secondaire                                         | 100      | Texte          | 32    |
| 30 | libelleCommuneEtablissement                    | Libellé de la commune                                                                | 100      | Texte          | 18    |
| 31 | libelleCommuneEtranger2Etablissement           | Libellé de la commune de l?adresse secondaire pour un établissement situé à l?étranger | 100      | Texte          | 33    |
| 32 | libelleCommuneEtrangerEtablissement            | Libellé de la commune pour un établissement situé à l?étranger                        | 100      | Texte          | 19    |
| 33 | libellePaysEtranger2Etablissement              | Libellé du pays de l?adresse secondaire pour un établissement situé à l?étranger       | 100      | Texte          | 39    |
| 34 | libellePaysEtrangerEtablissement               | Libellé du pays pour un établissement situé à l?étranger                              | 100      | Texte          | 25    |
| 35 | libelleVoie2Etablissement                      | Libellé de voie de l?adresse secondaire                                               | 100      | Texte          | 30    |
| 36 | libelleVoieEtablissement                       | Libellé de voie                                                                      | 100      | Texte          | 16    |
| 37 | nic                                            | Numéro interne de classement de l'établissement                                      | 5        | Texte          | 2     |
| 38 | nombrePeriodesEtablissement                    | Nombre de périodes de l?établissement                                                 | 2        | Numérique      | 11    |
| 39 | nomenclatureActivitePrincipaleEtablissement    | Nomenclature d?activité de la variable activitePrincipaleEtablissement                | 8        | Liste de codes | 47    |
| 40 | numeroVoie2Etablissement                       | Numéro de la voie de l?adresse secondaire                                             | 4        | Numérique      | 27    |
| 41 | numeroVoieEtablissement                        | Numéro de voie                                                                       | 4        | Numérique      | 13    |
| 42 | siren                                          | Numéro Siren                                                                         | 9        | Texte          | 1     |
| 43 | siret                                          | Numéro Siret                                                                         | 14       | Texte          | 3     |
| 44 | statutDiffusionEtablissement                   | Statut de diffusion de l?établissement                                                | 1        | Liste de codes | 4     |
| 45 | trancheEffectifsEtablissement                  | Tranche d?effectif salarié de l?établissement                                          | 2        | Liste de codes | 6     |
| 46 | typeVoie2Etablissement                         | Type de voie de l?adresse secondaire                                                  | 4        | Liste de codes | 29    |
| 47 | typeVoieEtablissement                          | Type de voie                                                                         | 4        | Liste de codes | 15    |


