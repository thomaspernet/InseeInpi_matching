

- [NEW] Siren sans Siège ou Principal
  - Il est possible qu'un SIREN n'ai pas de siege/principal. Normalement, cela doit etre corrigé par un partiel
- [NEW] Etablissement sans création
  - Il arrive que des établissements soient supprimés (EVT) mais n'ont pas de ligne "création d'entreprise". Si cela, arrive, Infogreffe doit envoyer un partiel pour corriger. Il arrive que le greffe envoie seulement une ligne pour SEP, lorsque le Principal est fermé, le siège est toujours ouvert. Mais pas de nouvelle ligne dans la base. Le partiel devrait corriger cela.


Deux autres variables sont a créer afin de renseigner si le siren n'a pas de siège ou principal et si l'établissement n'a pas de création.

4. Créer la variable sie_ou_pri_flag
5. Créer la variable pas_creation_flag


### Exemple input `sie_ou_pri_flag`

Siren **961504768**
    *  [INPI](https://data.inpi.fr/entreprises/961504768#961504768)
    *  [961504768](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/US_Datum/Data_example/US_2464/siren_exemple.xlsx)
        *   Feuille 961504768

### Exemple input `pas_creation_flag`

Siren **378943187/379090376/323398057**
    *  [INPI](https://data.inpi.fr/entreprises/961504768#961504768)
    *  [sans_creation](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/US_Datum/Data_example/US_2464/siren_exemple.xlsx)
        *   Feuille sans_creation


        ### Exemple output `sie_ou_pri_flag`

        Siren **961504768**
        *  [INPI](https://data.inpi.fr/entreprises/961504768#961504768)
        *  [961504768](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/US_Datum/Data_example/US_2464/siren_exemple.xlsx)
            *   Feuille 961504768

        ### Exemple output `pas_creation_flag`

        Siren **378943187/379090376/323398057**
        *  [INPI](https://data.inpi.fr/entreprises/961504768#961504768)
        *  [sans_creation](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/US_Datum/Data_example/US_2464/siren_exemple.xlsx)
            *   Feuille sans_creation


            ## Créer la variable `sie_ou_pri_flag`

            Chaque SIREN doit disposer d'un établissement principal ou d'un siège. Pour connaitre la nature de l'établissement, il faut utiliser la variable `type`. Cette variable possède 4 possibilités:

            1.	SEC
            2.	PRI
            3.	SIE
            4.	SEP

            Si un SIREN ne possède pas de PRI, SIE ou SEP alors cela veut dire qu'il n'a pas de siège ou de principal. Il faut donc l'indiquer via la variable `sie_ou_pri_flag`:

            Si le SIREN n'a pas de siège ou principal alors TRUE, sinon False

            Il y a environ 37722 SIREN sans Siege ou Principal. [Snippet](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/snippets/49)

            Siren **005520242**
            *  [INPI](https://data.inpi.fr/entreprises/961504768#961504768)
            *  [961504768](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/US_Datum/Data_example/US_2464/siren_exemple.xlsx)
                *   Feuille 961504768

            ## Créer la variable `pas_creation_flag`

            Normalement, chaque établissement doit posséder une ligne avec un libellé égale a "Etablissement ouvert". La variable `libelle_evt` comprend quatre valeurs, dont `Etablissement ouvert` qui est notre target.

            1.	Modifications relatives au dossier
            2.	Etablissement supprimé
            3.	Etablissement ouvert
            4.	Modifications relatives à un établissement

            Dans l'idéal, il faudrait détecter les séquences qui n'ont pas de création et l'indiquer via la variable `pas_creation_flag`:

            Si une séquence n'a pas de création alors TRUE, sinon False

            Il y a environ 53430 etablissements sans création [Snippet](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/snippets/50)

            Siren **378943187/379090376/323398057**
            - INPI
              - [378943187](https://data.inpi.fr/entreprises/378943187#378943187)
              - [379090376](https://data.inpi.fr/entreprises/379090376#379090376)
              - [323398057](https://data.inpi.fr/entreprises/323398057#323398057)
            - [sans_creation](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/blob/master/US_Datum/Data_example/US_2464/siren_exemple.xlsx)
                *   Feuille sans_creation


                - Compter le nombre de SIREN sans Siege ou Principal
- Compter le nombre d'établissements sans création
