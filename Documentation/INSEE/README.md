# Donn�es INSEE

La documentation d�taill�e de l'INSEE est disponible ci dessous pour:

- [Unite legale](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Documentation/INSEE/Description%20fichier%20StockUniteLegale.pdf)
- [Etablissement](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Documentation/INSEE/description-fichier-stocketablissement.pdf)

## Unit� l�gale

|    | Nom                                       | Libell�                                                                | Longueur | Type           | Ordre |
|----|-------------------------------------------|------------------------------------------------------------------------|----------|----------------|-------|
| 1  | activitePrincipaleUniteLegale             | Activit� principale de l�unit� l�gale                                  | 6        | Liste de codes | 29    |
| 2  | anneeCategorieEntreprise                  | Ann�e de validit� de la cat�gorie d�entreprise                         | 4        | Date           | 19    |
| 3  | anneeEffectifsUniteLegale                 | Ann�e de validit� de la tranche d�effectif salari� de l�unit� l�gale   | 4        | Date           | 15    |
| 4  | caractereEmployeurUniteLegale             | Caract�re employeur de l�unit� l�gale                                  | 1        | Liste de codes | 33    |
| 5  | categorieEntreprise                       | Cat�gorie � laquelle appartient l�entreprise                           | 3        | Liste de codes | 18    |
| 6  | categorieJuridiqueUniteLegale             | Cat�gorie juridique de l�unit� l�gale                                  | 4        | Liste de codes | 28    |
| 7  | dateCreationUniteLegale                   | Date de cr�ation de l'unit� l�gale                                     | 10       | Date           | 4     |
| 8  | dateDebut                                 | Date de d�but d'une p�riode d'historique d'une unit� l�gale            | 10       | Date           | 20    |
| 9  | dateDernierTraitementUniteLegale          | Date du dernier traitement de l�unit� l�gale dans le r�pertoire Sirene | 19       | Date           | 16    |
| 10 | denominationUniteLegale                   | D�nomination de l�unit� l�gale                                         | 120      | Texte          | 24    |
| 11 | denominationUsuelle1UniteLegale           | D�nomination usuelle de l�unit� l�gale                                 | 70       | Texte          | 25    |
| 12 | denominationUsuelle2UniteLegale           | D�nomination usuelle de l�unit� l�gale � deuxi�me champ                | 70       | Texte          | 26    |
| 13 | denominationUsuelle3UniteLegale           | D�nomination usuelle de l�unit� l�gale � troisi�me champ               | 70       | Texte          | 27    |
| 14 | economieSocialeSolidaireUniteLegale       | Appartenance au champ de l��conomie sociale et solidaire               | 1        | Liste de codes | 32    |
| 15 | etatAdministratifUniteLegale              | �tat administratif de l�unit� l�gale                                   | 1        | Liste de codes | 21    |
| 16 | identifiantAssociationUniteLegale         | Num�ro au R�pertoire National des Associations                         | 10       | Texte          | 13    |
| 17 | nicSiegeUniteLegale                       | Num�ro interne de classement (Nic) de l�unit� l�gale                   | 5        | Texte          | 31    |
| 18 | nombrePeriodesUniteLegale                 | Nombre de p�riodes de l�unit� l�gale                                   | 2        | Num�rique      | 17    |
| 19 | nomenclatureActivitePrincipaleUniteLegale | Nomenclature d�activit� de la variable activitePrincipaleUniteLegale   | 8        | Liste de codes | 30    |
| 20 | nomUniteLegale                            | Nom de naissance de la personnes physique                              | 100      | Texte          | 22    |
| 21 | nomUsageUniteLegale                       | Nom d�usage de la personne physique                                    | 100      | Texte          | 23    |
| 22 | prenom1UniteLegale                        | Premier pr�nom d�clar� pour un personne physique                       | 20       | Texte          | 7     |
| 23 | prenom2UniteLegale                        | Deuxi�me pr�nom d�clar� pour un personne physique                      | 20       | Texte          | 8     |
| 24 | prenom3UniteLegale                        | Troisi�me pr�nom d�clar� pour un personne physique                     | 20       | Texte          | 9     |
| 25 | prenom4UniteLegale                        | Quatri�me pr�nom d�clar� pour un personne physique                     | 20       | Texte          | 10    |
| 26 | prenomUsuelUniteLegale                    | Pr�nom usuel de la personne physique                                   | 20       | Texte          | 11    |
| 27 | pseudonymeUniteLegale                     | Pseudonyme de la personne physique                                     | 100      | Texte          | 12    |
| 28 | sexeUniteLegale                           | Caract�re f�minin ou masculin de la personne physique                  | 1        | Liste de codes | 6     |
| 29 | sigleUniteLegale                          | Sigle de l�unit� l�gale                                                | 20       | Texte          | 5     |
| 30 | siren                                     | Num�ro Siren                                                           | 9        | Texte          | 1     |
| 31 | statutDiffusionUniteLegale                | Statut de diffusion de l�unit� l�gale                                  | 1        | Liste de codes | 2     |
| 32 | trancheEffectifsUniteLegale               | Tranche d�effectif salari� de l�unit� l�gale                           | 2        | Liste de codes | 14    |
| 33 | unitePurgeeUniteLegale                    | Unit� l�gale purg�e                                                    | 5        | Texte          | 3     |

## Etablissement

|    | Nom                                            | Libell�                                                                              | Longueur | Type           | Ordre |
|----|------------------------------------------------|--------------------------------------------------------------------------------------|----------|----------------|-------|
| 1  | activitePrincipaleRegistreMetiersEtablissement | Activit� exerc�e par l?artisan inscrit au registre des m�tiers                        | 6        | Liste de codes | 8     |
| 2  | anneeEffectifsEtablissement                    | Ann�e de validit� de la tranche d?effectif salari� de l?�tablissement                  | 4        | Date           | 7     |
| 3  | caractereEmployeurEtablissement                | Caract�re employeur de l?�tablissement                                                | 1        | Liste de codes | 48    |
| 4  | codeCedex2Etablissement                        | Code cedex de l'adresse secondaire                                                    | 9        | Texte          | 36    |
| 5  | codeCedexEtablissement                         | Code cedex                                                                           | 9        | Texte          | 22    |
| 6  | codeCommune2Etablissement                      | Code commune de l?adresse secondaire                                                  | 5        | Liste de codes | 35    |
| 7  | codeCommuneEtablissement                       | Code commune de l?�tablissement                                                       | 5        | Liste de codes | 21    |
| 8  | codePaysEtranger2Etablissement                 | Code pays de l?adresse secondaire pour un �tablissement situ� � l?�tranger             | 5        | Liste de codes | 38    |
| 9  | codePaysEtrangerEtablissement                  | Code pays pour un �tablissement situ� � l?�tranger                                    | 5        | Liste de codes | 24    |
| 10 | codePostal2Etablissement                       | Code postal de l?adresse secondaire                                                   | 5        | Texte          | 31    |
| 11 | codePostalEtablissement                        | Code postal                                                                          | 5        | Texte          | 17    |
| 12 | complementAdresse2Etablissement                | Compl�ment d?adresse secondaire                                                       | 38       | Texte          | 26    |
| 13 | complementAdresseEtablissement                 | Compl�ment d?adresse                                                                  | 38       | Texte          | 12    |
| 14 | dateCreationEtablissement                      | Date de cr�ation de l?�tablissement                                                   | 10       | Date           | 5     |
| 15 | dateDebut                                      | Date de d�but d'une p�riode d'historique d'un �tablissement                          | 10       | Date           | 40    |
| 16 | dateDernierTraitementEtablissement             | Date du dernier traitement de l?�tablissement dans le r�pertoire Sirene               | 19       | Date           | 9     |
| 17 | denominationUsuelleEtablissement               | D�nomination usuelle de l?�tablissement                                               | 100      | Texte          | 45    |
| 18 | distributionSpeciale2Etablissement             | Distribution sp�ciale de l?adresse secondaire de l?�tablissement                       | 26       | Texte          | 34    |
| 19 | distributionSpecialeEtablissement              | Distribution sp�ciale de l?�tablissement                                              | 26       | Texte          | 20    |
| 20 | enseigne1Etablissement                         | Premi�re ligne d?enseigne de l?�tablissement                                           | 50       | Texte          | 42    |
| 21 | enseigne2Etablissement                         | Deuxi�me ligne d?enseigne de l?�tablissement                                           | 50       | Texte          | 43    |
| 22 | enseigne3Etablissement                         | Troisi�me ligne d?enseigne de l?�tablissement                                          | 50       | Texte          | 44    |
| 23 | etablissementSiege                             | Qualit� de si�ge ou non de l?�tablissement                                            | 5        | Texte          | 10    |
| 24 | etatAdministratifEtablissement                 | �tat administratif de l?�tablissement                                                 | 1        | Liste de codes | 41    |
| 25 | indiceRepetition2Etablissement                 | Indice de r�p�tition dans la voie pour l?adresse secondaire                           | 1        | Texte          | 28    |
| 26 | indiceRepetitionEtablissement                  | Indice de r�p�tition dans la voie                                                    | 1        | Texte          | 14    |
| 27 | libelleCedex2Etablissement                     | Libell� du code cedex de l?adresse secondaire                                         | 100      | Texte          | 37    |
| 28 | libelleCedexEtablissement                      | Libell� du code cedex                                                                | 100      | Texte          | 23    |
| 29 | libelleCommune2Etablissement                   | Libell� de la commune de l?adresse secondaire                                         | 100      | Texte          | 32    |
| 30 | libelleCommuneEtablissement                    | Libell� de la commune                                                                | 100      | Texte          | 18    |
| 31 | libelleCommuneEtranger2Etablissement           | Libell� de la commune de l?adresse secondaire pour un �tablissement situ� � l?�tranger | 100      | Texte          | 33    |
| 32 | libelleCommuneEtrangerEtablissement            | Libell� de la commune pour un �tablissement situ� � l?�tranger                        | 100      | Texte          | 19    |
| 33 | libellePaysEtranger2Etablissement              | Libell� du pays de l?adresse secondaire pour un �tablissement situ� � l?�tranger       | 100      | Texte          | 39    |
| 34 | libellePaysEtrangerEtablissement               | Libell� du pays pour un �tablissement situ� � l?�tranger                              | 100      | Texte          | 25    |
| 35 | libelleVoie2Etablissement                      | Libell� de voie de l?adresse secondaire                                               | 100      | Texte          | 30    |
| 36 | libelleVoieEtablissement                       | Libell� de voie                                                                      | 100      | Texte          | 16    |
| 37 | nic                                            | Num�ro interne de classement de l'�tablissement                                      | 5        | Texte          | 2     |
| 38 | nombrePeriodesEtablissement                    | Nombre de p�riodes de l?�tablissement                                                 | 2        | Num�rique      | 11    |
| 39 | nomenclatureActivitePrincipaleEtablissement    | Nomenclature d?activit� de la variable activitePrincipaleEtablissement                | 8        | Liste de codes | 47    |
| 40 | numeroVoie2Etablissement                       | Num�ro de la voie de l?adresse secondaire                                             | 4        | Num�rique      | 27    |
| 41 | numeroVoieEtablissement                        | Num�ro de voie                                                                       | 4        | Num�rique      | 13    |
| 42 | siren                                          | Num�ro Siren                                                                         | 9        | Texte          | 1     |
| 43 | siret                                          | Num�ro Siret                                                                         | 14       | Texte          | 3     |
| 44 | statutDiffusionEtablissement                   | Statut de diffusion de l?�tablissement                                                | 1        | Liste de codes | 4     |
| 45 | trancheEffectifsEtablissement                  | Tranche d?effectif salari� de l?�tablissement                                          | 2        | Liste de codes | 6     |
| 46 | typeVoie2Etablissement                         | Type de voie de l?adresse secondaire                                                  | 4        | Liste de codes | 29    |
| 47 | typeVoieEtablissement                          | Type de voie                                                                         | 4        | Liste de codes | 15    |


