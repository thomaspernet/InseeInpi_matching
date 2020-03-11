# Donn�es INSEE

La d�finition de l'ensemble des termes se trouvent [ici](https://www.sirene.fr/sirene/public/static/definitions)

La documentation d�taill�e de l'INSEE est disponible ci dessous pour:

- [Unite legale](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Documentation/INSEE/Description%20fichier%20StockUniteLegale.pdf)
- [Etablissement](https://github.com/thomaspernet/InseeInpi_matching/blob/master/Documentation/INSEE/description-fichier-stocketablissement.pdf)

L'INSEE fournit une documentation exhaustive pour les bases de donn�es UL et �tablissements. Toutes les informations sont regroup�es dans cette [adresse](https://www.sirene.fr/sirene/public/static/liste-variables#). Nous avons regroup� les informations dans les tableaux ci dessous.

## Unit� l�gale

| Nom                                       | Libell�                                                                | url                                                                                    | Longueur | Type           | Ordre |
|-------------------------------------------|------------------------------------------------------------------------|----------------------------------------------------------------------------------------|----------|----------------|-------|
| activitePrincipaleUniteLegale             | Activit� principale de l�unit� l�gale                                  | https://www.sirene.fr/sirene/public/variable/activitePrincipaleUniteLegale             | 6        | Liste de codes | 29    |
| anneeCategorieEntreprise                  | Ann�e de validit� de la cat�gorie d�entreprise                         | https://www.sirene.fr/sirene/public/variable/anneeCategorieEntreprise                  | 4        | Date           | 19    |
| anneeEffectifsUniteLegale                 | Ann�e de validit� de la tranche d�effectif salari� de l�unit� l�gale   | https://www.sirene.fr/sirene/public/variable/anneeEffectifsUniteLegale                 | 4        | Date           | 15    |
| caractereEmployeurUniteLegale             | Caract�re employeur de l�unit� l�gale                                  | https://www.sirene.fr/sirene/public/variable/caractereEmployeurUniteLegale             | 1        | Liste de codes | 33    |
| categorieEntreprise                       | Cat�gorie � laquelle appartient l�entreprise                           | https://www.sirene.fr/sirene/public/variable/categorieEntreprise                       | 3        | Liste de codes | 18    |
| categorieJuridiqueUniteLegale             | Cat�gorie juridique de l�unit� l�gale                                  | https://www.sirene.fr/sirene/public/variable/categorieJuridiqueUniteLegale             | 4        | Liste de codes | 28    |
| dateCreationUniteLegale                   | Date de cr�ation de l'unit� l�gale                                     | https://www.sirene.fr/sirene/public/variable/dateCreationUniteLegale                   | 10       | Date           | 4     |
| dateDebut                                 | Date de d�but d'une p�riode d'historique d'une unit� l�gale            | https://www.sirene.fr/sirene/public/variable/dateDebut                                 | 10       | Date           | 20    |
| dateDernierTraitementUniteLegale          | Date du dernier traitement de l�unit� l�gale dans le r�pertoire Sirene | https://www.sirene.fr/sirene/public/variable/dateDernierTraitementUniteLegale          | 19       | Date           | 16    |
| denominationUniteLegale                   | D�nomination de l�unit� l�gale                                         | https://www.sirene.fr/sirene/public/variable/denominationUniteLegale                   | 120      | Texte          | 24    |
| denominationUsuelle1UniteLegale           | D�nomination usuelle de l�unit� l�gale                                 | https://www.sirene.fr/sirene/public/variable/denominationUsuelle1UniteLegale           | 70       | Texte          | 25    |
| denominationUsuelle2UniteLegale           | D�nomination usuelle de l�unit� l�gale � deuxi�me champ                | https://www.sirene.fr/sirene/public/variable/denominationUsuelle2UniteLegale           | 70       | Texte          | 26    |
| denominationUsuelle3UniteLegale           | D�nomination usuelle de l�unit� l�gale � troisi�me champ               | https://www.sirene.fr/sirene/public/variable/denominationUsuelle3UniteLegale           | 70       | Texte          | 27    |
| economieSocialeSolidaireUniteLegale       | Appartenance au champ de l��conomie sociale et solidaire               | https://www.sirene.fr/sirene/public/variable/economieSocialeSolidaireUniteLegale       | 1        | Liste de codes | 32    |
| etatAdministratifUniteLegale              | �tat administratif de l�unit� l�gale                                   | https://www.sirene.fr/sirene/public/variable/etatAdministratifUniteLegale              | 1        | Liste de codes | 21    |
| identifiantAssociationUniteLegale         | Num�ro au R�pertoire National des Associations                         | https://www.sirene.fr/sirene/public/variable/identifiantAssociationUniteLegale         | 10       | Texte          | 13    |
| nicSiegeUniteLegale                       | Num�ro interne de classement (Nic) de l�unit� l�gale                   | https://www.sirene.fr/sirene/public/variable/nicSiegeUniteLegale                       | 5        | Texte          | 31    |
| nombrePeriodesUniteLegale                 | Nombre de p�riodes de l�unit� l�gale                                   | https://www.sirene.fr/sirene/public/variable/nombrePeriodesUniteLegale                 | 2        | Num�rique      | 17    |
| nomenclatureActivitePrincipaleUniteLegale | Nomenclature d�activit� de la variable activitePrincipaleUniteLegale   | https://www.sirene.fr/sirene/public/variable/nomenclatureActivitePrincipaleUniteLegale | 8        | Liste de codes | 30    |
| nomUniteLegale                            | Nom de naissance de la personnes physique                              | https://www.sirene.fr/sirene/public/variable/nomUniteLegale                            | 100      | Texte          | 22    |
| nomUsageUniteLegale                       | Nom d�usage de la personne physique                                    | https://www.sirene.fr/sirene/public/variable/nomUsageUniteLegale                       | 100      | Texte          | 23    |
| prenom1UniteLegale                        | Premier pr�nom d�clar� pour un personne physique                       | https://www.sirene.fr/sirene/public/variable/prenom1UniteLegale                        | 20       | Texte          | 7     |
| prenom2UniteLegale                        | Deuxi�me pr�nom d�clar� pour un personne physique                      | https://www.sirene.fr/sirene/public/variable/prenom2UniteLegale                        | 20       | Texte          | 8     |
| prenom3UniteLegale                        | Troisi�me pr�nom d�clar� pour un personne physique                     | https://www.sirene.fr/sirene/public/variable/prenom3UniteLegale                        | 20       | Texte          | 9     |
| prenom4UniteLegale                        | Quatri�me pr�nom d�clar� pour un personne physique                     | https://www.sirene.fr/sirene/public/variable/prenom4UniteLegale                        | 20       | Texte          | 10    |
| prenomUsuelUniteLegale                    | Pr�nom usuel de la personne physique                                   | https://www.sirene.fr/sirene/public/variable/prenomUsuelUniteLegale                    | 20       | Texte          | 11    |
| pseudonymeUniteLegale                     | Pseudonyme de la personne physique                                     | https://www.sirene.fr/sirene/public/variable/pseudonymeUniteLegale                     | 100      | Texte          | 12    |
| sexeUniteLegale                           | Caract�re f�minin ou masculin de la personne physique                  | https://www.sirene.fr/sirene/public/variable/sexeUniteLegale                           | 1        | Liste de codes | 6     |
| sigleUniteLegale                          | Sigle de l�unit� l�gale                                                | https://www.sirene.fr/sirene/public/variable/sigleUniteLegale                          | 20       | Texte          | 5     |
| siren                                     | Num�ro Siren                                                           | https://www.sirene.fr/sirene/public/variable/siren                                     | 9        | Texte          | 1     |
| statutDiffusionUniteLegale                | Statut de diffusion de l�unit� l�gale                                  | https://www.sirene.fr/sirene/public/variable/statutDiffusionUniteLegale                | 1        | Liste de codes | 2     |
| trancheEffectifsUniteLegale               | Tranche d�effectif salari� de l�unit� l�gale                           | https://www.sirene.fr/sirene/public/variable/trancheEffectifsUniteLegale               | 2        | Liste de codes | 14    |
| unitePurgeeUniteLegale                    | Unit� l�gale purg�e                                                    | https://www.sirene.fr/sirene/public/variable/unitePurgeeUniteLegale                    | 5        | Texte          | 3     |


## Etablissement

| Nom                                            | Libell�                                                                              | url                                                                                         | complement                                          | Longueur | Type           | Ordre |
|------------------------------------------------|--------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------|-----------------------------------------------------|----------|----------------|-------|
| activitePrincipaleEtablissement                | Activit� principale de l'�tablissement pendant la p�riode                            | https://www.sirene.fr/sirene/public/variable/activitePrincipaleEtablissement                |                                                     | 6        | Liste de codes | 46    |
| activitePrincipaleRegistreMetiersEtablissement | Activit� exerc�e par l?artisan inscrit au registre des m�tiers                        | https://www.sirene.fr/sirene/public/variable/activitePrincipaleRegistreMetiersEtablissement |                                                     | 6        | Liste de codes | 8     |
| anneeEffectifsEtablissement                    | Ann�e de validit� de la tranche d?effectif salari� de l?�tablissement                  | https://www.sirene.fr/sirene/public/variable/anneeEffectifsEtablissement                    |                                                     | 4        | Date           | 7     |
| caractereEmployeurEtablissement                | Caract�re employeur de l?�tablissement                                                | https://www.sirene.fr/sirene/public/variable/caractereEmployeurEtablissement                |                                                     | 1        | Liste de codes | 48    |
| codeCedex2Etablissement                        | Code cedex de l?adresse secondaire                                                    | https://www.sirene.fr/sirene/public/variable/codeCedex2Etablissement                        |                                                     | 9        | Texte          | 36    |
| codeCedexEtablissement                         | Code cedex                                                                           | https://www.sirene.fr/sirene/public/variable/codeCedexEtablissement                         |                                                     | 9        | Texte          | 22    |
| codeCommune2Etablissement                      | Code commune de l?adresse secondaire                                                  | https://www.sirene.fr/sirene/public/variable/codeCommune2Etablissement                      |                                                     | 5        | Liste de codes | 35    |
| codeCommuneEtablissement                       | Code commune de l?�tablissement                                                       | https://www.sirene.fr/sirene/public/variable/codeCommuneEtablissement                       |                                                     | 5        | Liste de codes | 21    |
| codePaysEtranger2Etablissement                 | Code pays de l?adresse secondaire pour un �tablissement situ� � l?�tranger             | https://www.sirene.fr/sirene/public/variable/codePaysEtranger2Etablissement                 |                                                     | 5        | Liste de codes | 38    |
| codePaysEtrangerEtablissement                  | Code pays pour un �tablissement situ� � l?�tranger                                    | https://www.sirene.fr/sirene/public/variable/codePaysEtrangerEtablissement                  |                                                     | 5        | Liste de codes | 24    |
| codePostal2Etablissement                       | Code postal de l?adresse secondaire                                                   | https://www.sirene.fr/sirene/public/variable/codePostal2Etablissement                       |                                                     | 5        | Texte          | 31    |
| codePostalEtablissement                        | Code postal                                                                          | https://www.sirene.fr/sirene/public/variable/codePostalEtablissement                        |                                                     | 5        | Texte          | 17    |
| complementAdresse2Etablissement                | Compl�ment d?adresse secondaire                                                       | https://www.sirene.fr/sirene/public/variable/complementAdresse2Etablissement                |                                                     | 38       | Texte          | 26    |
| complementAdresseEtablissement                 | Compl�ment d?adresse                                                                  | https://www.sirene.fr/sirene/public/variable/complementAdresseEtablissement                 |                                                     | 38       | Texte          | 12    |
| dateCreationEtablissement                      | Date de cr�ation de l?�tablissement                                                   | https://www.sirene.fr/sirene/public/variable/dateCreationEtablissement                      |                                                     | 10       | Date           | 5     |
| dateDebut                                      | Date de d�but d'une p�riode d'historique d'un �tablissement                          | https://www.sirene.fr/sirene/public/variable/dateDebut                                      |                                                     | 10       | Date           | 40    |
| dateDernierTraitementEtablissement             | Date du dernier traitement de l?�tablissement dans le r�pertoire Sirene               | https://www.sirene.fr/sirene/public/variable/dateDernierTraitementEtablissement             |                                                     | 19       | Date           | 9     |
| denominationUsuelleEtablissement               | D�nomination usuelle de l?�tablissement                                               | https://www.sirene.fr/sirene/public/variable/denominationUsuelleEtablissement               |                                                     | 100      | Texte          | 45    |
| distributionSpeciale2Etablissement             | Distribution sp�ciale de l?adresse secondaire de l?�tablissement                       | https://www.sirene.fr/sirene/public/variable/distributionSpeciale2Etablissement             |                                                     | 26       | Texte          | 34    |
| distributionSpecialeEtablissement              | Distribution sp�ciale de l?�tablissement                                              | https://www.sirene.fr/sirene/public/variable/distributionSpecialeEtablissement              |                                                     | 26       | Texte          | 20    |
| enseigne1Etablissement                         | Premi�re ligne d?enseigne de l?�tablissement                                           | https://www.sirene.fr/sirene/public/variable/enseigne1Etablissement                         |                                                     | 50       | Texte          | 42    |
| enseigne2Etablissement                         | Deuxi�me ligne d?enseigne de l?�tablissement                                           | https://www.sirene.fr/sirene/public/variable/enseigne2Etablissement                         |                                                     | 50       | Texte          | 43    |
| enseigne3Etablissement                         | Troisi�me ligne d?enseigne de l?�tablissement                                          | https://www.sirene.fr/sirene/public/variable/enseigne3Etablissement                         |                                                     | 50       | Texte          | 44    |
| etablissementSiege                             | Qualit� de si�ge ou non de l?�tablissement                                            | https://www.sirene.fr/sirene/public/variable/etablissementSiege                             |                                                     | 5        | Texte          | 10    |
| etatAdministratifEtablissement                 | �tat administratif de l?�tablissement                                                 | https://www.sirene.fr/sirene/public/variable/etatAdministratifEtablissement                 |                                                     | 1        | Liste de codes | 41    |
| indiceRepetition2Etablissement                 | Indice de r�p�tition dans la voie pour l?adresse secondaire                           | https://www.sirene.fr/sirene/public/variable/indiceRepetition2Etablissement                 | https://www.sirene.fr/sirene/public/variable/indrep | 1        | Texte          | 28    |
| indiceRepetitionEtablissement                  | Indice de r�p�tition dans la voie                                                    | https://www.sirene.fr/sirene/public/variable/indiceRepetitionEtablissement                  | https://www.sirene.fr/sirene/public/variable/indrep | 1        | Texte          | 14    |
| libelleCedex2Etablissement                     | Libell� du code cedex de l?adresse secondaire                                         | https://www.sirene.fr/sirene/public/variable/libelleCedex2Etablissement                     |                                                     | 100      | Texte          | 37    |
| libelleCedexEtablissement                      | Libell� du code cedex                                                                | https://www.sirene.fr/sirene/public/variable/libelleCedexEtablissement                      |                                                     | 100      | Texte          | 23    |
| libelleCommune2Etablissement                   | Libell� de la commune de l?adresse secondaire                                         | https://www.sirene.fr/sirene/public/variable/libelleCommune2Etablissement                   |                                                     | 100      | Texte          | 32    |
| libelleCommuneEtablissement                    | Libell� de la commune                                                                | https://www.sirene.fr/sirene/public/variable/libelleCommuneEtablissement                    |                                                     | 100      | Texte          | 18    |
| libelleCommuneEtranger2Etablissement           | Libell� de la commune de l?adresse secondaire pour un �tablissement situ� � l?�tranger | https://www.sirene.fr/sirene/public/variable/libelleCommuneEtranger2Etablissement           |                                                     | 100      | Texte          | 33    |
| libelleCommuneEtrangerEtablissement            | Libell� de la commune pour un �tablissement situ� � l?�tranger                        | https://www.sirene.fr/sirene/public/variable/libelleCommuneEtrangerEtablissement            |                                                     | 100      | Texte          | 19    |
| libellePaysEtranger2Etablissement              | Libell� du pays de l?adresse secondaire pour un �tablissement situ� � l?�tranger       | https://www.sirene.fr/sirene/public/variable/libellePaysEtranger2Etablissement              |                                                     | 100      | Texte          | 39    |
| libellePaysEtrangerEtablissement               | Libell� du pays pour un �tablissement situ� � l?�tranger                              | https://www.sirene.fr/sirene/public/variable/libellePaysEtrangerEtablissement               |                                                     | 100      | Texte          | 25    |
| libelleVoie2Etablissement                      | Libell� de voie de l?adresse secondaire                                               | https://www.sirene.fr/sirene/public/variable/libelleVoie2Etablissement                      |                                                     | 100      | Texte          | 30    |
| libelleVoieEtablissement                       | Libell� de voie                                                                      | https://www.sirene.fr/sirene/public/variable/libelleVoieEtablissement                       |                                                     | 100      | Texte          | 16    |
| nic                                            | Num�ro interne de classement de l'�tablissement                                      | https://www.sirene.fr/sirene/public/variable/nic                                            |                                                     | 5        | Texte          | 2     |
| nombrePeriodesEtablissement                    | Nombre de p�riodes de l?�tablissement                                                 | https://www.sirene.fr/sirene/public/variable/nombrePeriodesEtablissement                    |                                                     | 2        | Num�rique      | 11    |
| nomenclatureActivitePrincipaleEtablissement    | Nomenclature d?activit� de la variable activitePrincipaleEtablissement                | https://www.sirene.fr/sirene/public/variable/nomenclatureActivitePrincipaleEtablissement    |                                                     | 8        | Liste de codes | 47    |
| numeroVoie2Etablissement                       | Num�ro de la voie de l?adresse secondaire                                             | https://www.sirene.fr/sirene/public/variable/numeroVoie2Etablissement                       |                                                     | 4        | Num�rique      | 27    |
| numeroVoieEtablissement                        | Num�ro de voie                                                                       | https://www.sirene.fr/sirene/public/variable/numeroVoieEtablissement                        |                                                     | 4        | Num�rique      | 13    |
| siren                                          | Num�ro Siren                                                                         | https://www.sirene.fr/sirene/public/variable/siren                                          |                                                     | 9        | Texte          | 1     |
| siret                                          | Num�ro Siret                                                                         | https://www.sirene.fr/sirene/public/variable/siret                                          |                                                     | 14       | Texte          | 3     |
| statutDiffusionEtablissement                   | Statut de diffusion de l?�tablissement                                                | https://www.sirene.fr/sirene/public/variable/statutDiffusionEtablissement                   |                                                     | 1        | Liste de codes | 4     |
| trancheEffectifsEtablissement                  | Tranche d?effectif salari� de l?�tablissement                                          | https://www.sirene.fr/sirene/public/variable/trancheEffectifsEtablissement                  |                                                     | 2        | Liste de codes | 6     |
| typeVoie2Etablissement                         | Type de voie de l?adresse secondaire                                                  | https://www.sirene.fr/sirene/public/variable/typeVoie2Etablissement                         |                                                     | 4        | Liste de codes | 29    |
| typeVoieEtablissement                          | Type de voie                                                                         | https://www.sirene.fr/sirene/public/variable/typeVoieEtablissement                          |                                                     | 4        | Liste de codes | 15    |