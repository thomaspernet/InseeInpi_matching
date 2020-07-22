# Vérification SIREN/Regle US Datum

Dans cette section, nous allons indiquer tous les siren qui ont fait l'objet d'un flag. Cela va servir a se constituer une bibliothèque de siren qui vont ensuite etre transformer en point d'attention. L'idée principal est de détécter des problèmes sur les règles de gestion actuels ou bien des anomalies dans la donnée de l'INPI, voir de l'INSEE. Ce travail de construction de la bibliothèque de règle de gestion va faciliter le travail a venir des data analyste/scientist car il y aura a disposition, un ensemble de point d'attention dans lequel il faudra faire attention.

Pour indiquer un siren, il faut utiliser le template suivant:

- Titre point attention: Indiquer un titre si le siren permet de déboucher sur un point d'attention ou une règle de gestion. Exemple de titre: `Plusieurs PRI par SIREN`
* Base de donnée
    * INPI: Utiliser le site https://data.inpi.fr/ et indiquer le siren. Exemple -> https://data.inpi.fr/entreprises/400571824#400571824
    * INSEE: Utliser le site http://avis-situation-sirene.insee.fr/jsp/avis-formulaire.jsp et indiquer l'établissement en question. Exemple -> http://avis-situation-sirene.insee.fr/ListeSiretToEtab.action?form.siren=317253433&form.nic=00054

## SIREN XX

* Metadata
  * Resolu: OUI/NON
  * Sequence:
    * code_greffe :
    * numero_gestion :
    * id_etablisement :
  * US: [XX](https://tree.taiga.io/project/olivierlubet-air/us/)
  * Description problème
    *
  * Base de donnée
      * INPI:
      * INSEE:
      * Datum:
        * Base de donnée: ``
        * Table: ``
```
SELECT *
FROM
WHERE siren = ""
```
  * Réponse:
    *  Titre point attention -> Si fait l'object
    * Decrire la réponse


## SIREN 055502868

* Metadata
  * Resolu: OUI
  * Sequence:
    * code_greffe : 6901
    * numero_gestion : 1976B00579
    * id_etablisement : 2
  * US: [2746](https://tree.taiga.io/project/olivierlubet-air/us/2746)
  * Description problème
    * les doublons retrouvés après filtre et les partiel ayant un statut à IGNORE.
  * Base de donnée
      * INPI: https://data.inpi.fr/entreprises/055502868#055502868
      * INSEE: http://avis-situation-sirene.insee.fr/ListeSiretToEtab.action?form.siren=055502868&form.nic=00031
      * Datum:
        * Base de donnée: ``
        * Table: ``
```
SELECT *
FROM
WHERE siren = ""
```
  * Réponse:
    *  Titre point attention -> Si fait l'object
    * La query a bien ignorer les partiels précédents. Aucun problème
    * Toutefois, l’établissement est désigné en tant que secondaire à l’INSEE, mais principal à l’INPI
    * Le siren n’a pas de siège dans le moteur de recherche de l’INPI, mais dans notre base il y en a un, qui correspond a la même adresse que le principal, de plus le siège a l’INSEE correspond a un secondaire à l’INPI

## SIREN 306140039

  * Metadata
    * Resolu: OUI/NON
    * Sequence:
      * code_greffe :
      * numero_gestion :
      * id_etablisement :
    * US: [2746](https://tree.taiga.io/project/olivierlubet-air/us/2746)
    * Description problème
      *   * les doublons retrouvés après filtre et les partiel ayant un statut à IGNORE.
    * Base de donnée
        * INPI: https://data.inpi.fr/entreprises/306140039#306140039
        * INSEE:
        * Datum:
          * Base de donnée: ``
          * Table: ``
  ```
  SELECT *
  FROM
  WHERE siren = ""
  ```
  * Réponse:
      *  Titre point attention -> Si fait l'object
      * La query a bien ignorer les partiels précédents. Aucun problème
