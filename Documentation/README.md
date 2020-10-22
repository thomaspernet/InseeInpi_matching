# Entreprise et etablissement

# Table des matières

- [Entreprise et etablissement](#entreprise-et-etablissement)
- [Table des matières](#table-des-matires)
- [Introduction entreprises et etablissements](#introduction-entreprises-et-etablissements)
- [Le concept d’entreprise et d’établissement](#le-concept-dentreprise-et-dtablissement)
  - [Entreprise](#entreprise)
  - [Etablissement](#etablissement)
  - [Différence entre entreprise et établissement](#diffrence-entre-entreprise-et-tablissement)
- [Identification des entreprises et établissements par l’administration française](#identification-des-entreprises-et-tablissements-par-ladministration-franaise)
  - [SIREN](#siren)
  - [SIRET](#siret)
- [Les différents types d’établissements](#les-diffrents-types-dtablissements)
  - [Etablissement siège](#etablissement-sige)
  - [Etablissement principal](#etablissement-principal)
  - [Etablissement secondaire](#etablissement-secondaire)
- [Fournisseurs de données de l’administration française: INSEE & INPI](#fournisseurs-de-donnes-de-ladministration-franaise-insee--inpi)
- [Les unités légales](#les-units-lgales)
  - [Etablissements](#etablissements)
- [Les formes juridiques](#les-formes-juridiques)
- [La suppression et la radiation](#la-suppression-et-la-radiation)
- [La donnée de l’INPI](#la-donne-de-linpi)
  - [Tribunaux De Commerce](#tribunaux-de-commerce)
- [Relation INSEE-INPI](#relation-insee-inpi)
  - [name: python3](#name-python3)
- [Personnes Physiques](#personnes-physiques)
- [Personne physique : une définition juridique](#personne-physique--une-dfinition-juridique)
  - [Définition d’une personne physique](#dfinition-dune-personne-physique)
  - [Les formes juridiques existantes : synthèse et conséquences fiscales](#les-formes-juridiques-existantes--synthse-et-consquences-fiscales)

# Introduction entreprises et etablissements

Le projet de rapprochement de la donnée disponible dans l’Open Data requière un minimum de connaissances sur certains concepts économiques et juridiques. En effet, l’objectif principal de noter projet est de joindre les informations économiques misent a disposition de la part de l’INSEE avec celle des tribunaux de commerce, qui diffuse l’information juridique.

Les concepts d’entreprise, d’établissements mais aussi de statut juridique est central dans le projet. Dans une première partie, nous allons expliquer, de manière non exhaustive, chacun des concepts, avant d’expliquer le contenu et la transformation des bases de données des tribunaux de commerce.

# Le concept d’entreprise et d’établissement

## Entreprise

Le concept d’entreprise mais surtout d’établissement est central dans le projet de rapprochement des bases de données statistiques de l’INSEE, a celles des tribunaux de commerce.

Une entreprise est une organisation ou une unité institutionnelle, mue par un projet décliné en stratégie, en politiques et en plans d'action, dont le but est de produire et de fournir des biens ou des services à destination d'un ensemble de clients ou d'usagers, en réalisant un équilibre de ses comptes de charges et de produits.

Pour ce faire, une entreprise fait appel, mobilise et consomme des ressources (matérielles, humaines, financières, immatérielles et informationnelles) ce qui la conduit à devoir coordonner des fonctions (fonction d'achat, fonction commerciale, fonction informatique). Elle exerce son activité dans le cadre d'un contexte précis auquel elle doit s'adapter : un environnement plus ou moins concurrentiel, une filière technico-économique caractérisée par un état de l'art, un cadre socio-culturel et réglementaire spécifique. Elle peut se donner comme objectif de dégager un certain niveau de rentabilité, plus ou moins élevé.

L’INSEE va rattacher l’entreprise a une unité légale. L'unité légale est une entité juridique de droit public ou privé. Cette entité juridique peut être :
* une personne morale, dont l'existence est reconnue par la loi indépendamment des personnes ou des institutions qui la possèdent ou qui en sont membres ;
* une personne physique, qui, en tant qu'indépendant, peut exercer une activité économique.
Elle est obligatoirement déclarée aux administrations compétentes (Greffes des tribunaux, Sécurité sociale, DGI...) pour exister. L'existence d'une telle unité dépend du choix des propriétaires ou de ses créateurs (pour des raisons organisationnelles, juridiques ou fiscales).

Il faut faire attention à la terminologie des mots. La définition de l'unité légale ne doit pas être confondue avec celle de l'entreprise, considérée comme unité statistique.


## Etablissement

L'établissement est une unité de production géographiquement individualisée, mais juridiquement dépendante de l'unité légale. Il produit des biens ou des services : ce peut être une usine, une boulangerie, un magasin de vêtements, un des hôtels d'une chaîne hôtelière, la « boutique » d'un réparateur de matériel informatique...

L'établissement, unité de production, constitue le niveau le mieux adapté à une approche géographique de l'économie.

La population des établissements est relativement stable dans le temps et est moins affectée par les mouvements de restructuration juridique et financière que celle des entreprises.

## Différence entre entreprise et établissement

Le terme « entreprise » désigne une structure ou organisation dont le but est d’exercer une activité économique en mettant en œuvre des moyens humains, financiers et matériels adaptés.

La notion d’entreprise n’est pas corrélée à un statut juridique particulier et vaut aussi bien pour une entité unipersonnelle que pour une société (actionnaires multiples). Elle ne tient pas non plus compte de la valeur financière ou du volume d’activité.

La création d’une entreprise résulte de l’initiative d’une ou de plusieurs personnes qui mettent des moyens en commun pour produire des biens et/ou des services. D’une idée naît une entreprise dont l’organisation a pour objectif d’assurer sa pérennité.

Pour mieux s’organiser et répondre à la demande, une entreprise peut créer un ou plusieurs établissements.

Un établissement est par définition rattaché à une entreprise.

Plus l’entreprise est grande, plus elle comporte d’établissements, dépendants financièrement et juridiquement d’elle.

# Identification des entreprises et établissements par l’administration française

Dans la partie précédente, nous avons fourni une définition non-exhaustive des concepts d’entreprise et d’établissement. L’administration française centralise un bon nombre d’information sur les entreprises en vue de faire des études statistiques, mais aussi permettre de faire un suivi. Les informations sur les entreprises françaises sont fournis principalement par l’INSEE, 'Institut national de la statistique et des études économiques et par l’INPI, Institut national de la propriété industrielle.

Chaque entreprise se voit attribuer un numéro d’identification unique à 9 chiffres, appelé le SIREN, alors que les établissements sont identifiés grâce à un SIRET, qui lui contient 14 chiffres.

## SIREN

Chaque entreprise est identifiée par un numéro Siren (Système d'identification du Répertoire des entreprises), utilisé par tous les organismes publics et les administrations en relation avec l'entreprise.

Le siren est attribué par l'Insee lors de l'inscription de l'entreprise au répertoire Sirene, il comporte 9 chiffres.
* Ce numéro est unique et invariable
* Il se décompose en trois groupes de trois chiffres attribués d'une manière non significative en fonction de l'ordre d'inscription de l'entreprise.
* Ex. : 231 654 987

Par exemple, le SIREN de l’entreprise CALF est le 692 029 457.

Le numéro SIREN ne changera jamais pour une entreprise. Il sera toujours rendu public sauf si une demande de droit à l’oubli a été formulé.

## SIRET

Le numéro Siret (Système d'identification du Répertoire des établissements) identifie les établissements de l'entreprise. Il se compose de 14 chiffres correspondant :
  *  au numéro Siren,
  *  et, au numéro NIC (numéro interne de classement), comportant 5 chiffres : les quatre premiers correspondent au numéro d'identification de l'établissement ; le cinquième chiffre est une clé

Par exemple, le numéro Siren  231 654 987  suivi du numéro NIC 12315 compose le Siret
* Le numéro NIC identifie chaque établissement de l'entreprise

Pour continuer notre exemple avec CALF, le siren est le 692 029 457, et le siret 692 029 457 01126, situé au 12 Place des Etats Unis - 92120 Montrouge. L’entreprise CALF possède plusieurs établissements. Pour ne citer que les trois premiers:

* 01431 → 101 AV LE CORBUSIER
* 01423 → 14 RUE LOUIS TARDY
* 01415 → 53 RUE DE L ETANG
Source:  [INSEE](http://avis-situation-sirene.insee.fr/IdentificationListeSiret.action)

Une entreprise est constituée d’autant d’établissements qu’il y a de lieux différents où elle exerce son activité. L’établissement est fermé quand l’activité cesse dans l’établissement concerné ou lorsque l’établissement change d’adresse.

Il est indispensable de comprendre que l’attribution du siret se fait avec l’adresse de l’établissement. Une entreprise n’a, en soi, pas d’adresse car elle exerce, via ses établissements, sur un territoire donnée. Néanmoins, un établissement est rattaché à une adresse. L’entreprise peut posséder autant d’établissements qu’elle le souhaite, dans la mesure de la profitabilité de ses derniers. L’entreprise possédera autant de siret quelle a d’établissements. Dès lors qu’un établissement déménage, un nouveau siret sera attribué. L’établissement localisé à la première adresse va être fermé administrativement, et un nouvel établissement sera ouvert.

Pour rechercher des informations sur une entreprise, il est possible de se rendre dans l’un des sites suivants:   
*  Insee
  *  http://avis-situation-sirene.insee.fr/IdentificationListeSiret.action
*  INPI/TC
  * https://data.inpi.fr/
*  Infogreffe
  *  https://www.infogreffe.fr/


# Les différents types d’établissements

Comme énoncé en introduction, une entreprise possède sont caractère légale via l’unité légale. L’unité légale a pour adresse le siège de l’établissement. Il existe deux autres types d’établissement: principal et secondaire.

## Etablissement siège

Le siège social d’une société est tout simplement son “domicile juridique”, son "adresse administrative". En pratique, plusieurs options sont disponibles pour fixer le siège social d'une société : chez un des dirigeants, dans un local commercial, dans un centre d'affaires, etc.

Il est obligatoirement fixé dans les statuts. Autrement dit, c'est l'adresse "officielle" qui figurera sur l’[extrait Kbis](https://www.legalstart.fr/fiches-pratiques/creer-sa-societe/extrait-kbis/) et qui devra être mentionnée sur toutes les factures et courriers. C'est souvent à cette adresse que l'Assemblée Générale des associés (en SAS ou en SARL) se regroupe pour prendre les décisions importantes. Au delà de la signification même du siège social, le choix n'est pas anodin car plusieurs conséquences juridiques en découlent : le siège social détermine par exemple la nationalité de l'entreprise ou encore le tribunal compétent en cas de litige (pour certains contentieux).

Une société n'a qu'un seul siège social, mais elle peut avoir plusieurs "établissements" ou "lieux d'exploitation", concepts qui ont une signification différente. Le siège est le lieu de direction de la société mais son activité peut tout à fait s’exercer ailleurs. Il est d'ailleurs assez fréquent que le siège soit en réalité une simple boîte aux lettres.

Le siège social est important car il détermine la nationalité de la société. Ainsi, une société ayant son siège en France sera considérée comme française, et se verra appliquer le droit français. Il détermine également le tribunal territorialement compétent

## Etablissement principal
L'établissement est entendu comme un lieu d'exploitation commerciale, et donc rattaché à un fonds de commerce ou à une activité, contrairement au siège social.

Le lieu de l'établissement principal est dans la majorité des cas le même que celui du siège social pour les sociétés. Mais ce n'est en rien une obligation, il peut être situé à une adresse différente, que ce soit ou non dans le même ressort du greffe.

## Etablissement secondaire

L'article R.123-40 le définit comme "tout établissement permanent, distinct du siège social ou de l'établissement principal et dirigé par la personne tenue à l'immatriculation, un préposé ou une personne ayant le pouvoir de lier des rapports juridiques avec les tiers".

Lors de l'ouverture d'un premier établissement dans le ressort d'un tribunal où il n'est pas immatriculé à titre principal (donc un établissement secondaire), le dirigeant procède à l'inscription au greffe du ressort de l'établissement secondaire (dans le délai d'1 mois avant ou après cette ouverture en vertu de l'article R.123-41), ce greffe dit "secondaire" avertira le greffe dit "principal", qu'un établissement a été ouvert dans son ressort.

Si un second établissement est ouvert dans le même greffe "secondaire" (il y a donc au moins 2 établissements secondaires dans ce même ressort), le dirigeant procède pareillement à l'inscription de ce nouvel établissement, mais le greffe "principal" ne sera cette fois pas informé

En résumé, les notions juridiques d’établissement siège, principal et secondaire sont très importantes. Elles permettent de distinguer le caractère juridique d’une entreprise et donc d’établir l’objectif de l’établissement. Par exemple, prendre en considération la localisation d’un siège pour faire une analyse économique n’est pas forcément pertinent car l’activité économique de l’entreprise se trouve sur le principal (si différent du siège) et dans les établissements secondaires.

# Fournisseurs de données de l’administration française: INSEE & INPI

Il y a deux grands fournisseurs de données en France, a savoir l’INSEE et l’INPI.

L’Institut national de la statistique et des études économiques est chargé de la production, de l'analyse et de la publication des statistiques officielles en France : comptabilité nationale annuelle et trimestrielle, évaluation de la démographie nationale, du taux de chômage, etc. Il constitue une direction générale du ministère chargé des finances. Il dispose d’une indépendance de fait vis-à-vis du gouvernement, désormais garantie en droit par la loi. L’INSEE est en charge de la gestion du répertoire SIRENE, Système Informatique pour le Répertoire des ENtreprises et des Etablissements.

Depuis la loi Macron de 2015, l’INPI est en charge de transmettre gratuitement les données fournies par le greffe via le RNCS, le registre national du commerce et des sociétés correspond à la centralisation des données des RCS réalisée par l’INPI. La loi Macron a modernisé le rôle de l’Institut national de la propriété industrielle (INPI) en indiquant qu’il « assure la diffusion et la mise à la disposition gratuite du public, à des fins de réutilisation, des informations techniques, commerciales et financières qui sont contenues dans le registre national du commerce et des sociétés et dans les instruments centralisés de publicité légale » Pour plus d’information sur la loi Macron, veuillez lire [Comment Infogreffe a gardé la main sur les données légales des entreprises](https://www.lemonde.fr/les-decodeurs/article/2018/06/22/comment-infogreffe-a-garde-la-main-sur-les-donnees-legales-des-entreprises_5319408_4355770.html), [Ce que contient (désormais) la loi Macron](https://www.lemonde.fr/les-decodeurs/article/2015/08/06/ce-que-contient-desormais-la-loi-macron_4714255_4355770.html) ou [etat et registre du commerce](https://blogavocat.fr/space/michel.benichou/content/etat-et-registre-du-commerce_).

Les données fournies par l’INSEE et par l’INPI sont donc complémentaires, bien que redondante dans certains cas. L’INSEE étant le gestionnaire du répertoire SIRENE met à disposition l’ensemble des informations quelle reçoit avec comme dénominateur commun, le siren et le siret. L’INPI est tributaire de ce qu’infogreffe lui envoie. Etant donné la situation conflictuelle entre ses deux entités administratives, Infogreffe a décidé de mettre à disposition que le numéro de siren. Plus précisément, les bases de données de l’INPI n’ont pas de numéro siret.

En résumé, l’INSEE est en charge de la gestion du répertoire SIRENE, qui contient des informations administratives et économique des entreprises. L’INPI a pour mission la mise a disposition des informations du Registre national du commerce et des sociétés (RNCS), à savoir  la date de création, d’immatriculation, la forme juridique, le capital social, la dénomination sociale, le nom commercial, le sigle, l’activité principale, les représentants et l’adresse des établissements.

# Les unités légales

L’INSEE a la mission de collecter et de mettre a disposition des données du répertoire SIRENE. En détail, l’Insee est chargé d’identifier :

* les entrepreneurs individuels exerçant de manière indépendante une profession non salariée (exemple : un commerçant, un médecin),
* les personnes morales de droit privé ou de droit public soumises au droit commercial,
* les institutions et services de l’État et les collectivités territoriales, ainsi que tous leurs établissements,
* les associations dans certains cas.

Sont donc inscrits au répertoire tous les entrepreneurs individuels ou les personnes morales :

* immatriculés au Registre du Commerce et des Sociétés,
* immatriculés au Répertoire des Métiers,
* employant du personnel salarié (à l’exception des particuliers employeurs),
* soumis à des obligations fiscales,
* bénéficiaires de transferts financiers publics.

Toutes les mises à jour d’entreprises et d’établissements (créations, modifications, cessations) enregistrés dans Sirene proviennent des informations déclaratives des entreprises auprès des Centres de Formalités des Entreprises (CFE).

Il a été établi dans la partie dédiée aux types d’établissements que l’unité légale fournie les informations sur le siège. Les informations inhérentes au siège sont forcément les mêmes pour tous les autres établissements de l’entreprise. Dès lors, l’INSEE a distingué deux grands ensembles de données:

* Les unités légales
* Les établissements

Les fichiers relatifs aux unités légales fournissent des informations plus exhaustives sur le siège, alors que les fichiers correspondants aux entreprises sont plus spécifiques. Il y a des informations dans les unités légales qui se chevauchent avec les établissements car le siège d’une entreprise se trouve forcément dans les unités légales. Il est bon de souligner que tous les siren ne sont pas dans les unités légales. Seul les siren enregistrés au RNCS apparaissent dans les unités légales. En effet, il existe des catégories d’entreprises qui n’ont pas l’obligation de s’inscrire au RNCS, c’est le cas par exemple des auto-entrepreneurs.

Les fichiers de l’INSEE sont disponibles tous les premiers du mois.

Au sein de la base de donnée des unités légales, il y a deux tables, a savoir la table des données historisées et la table contenant le stock.
* le fichier stock des entreprises (ensemble des entreprises actives et cessées dans leur état courant au répertoire),
* le fichier stock des valeurs historisées des entreprises (pour toutes les entreprises, ensemble des valeurs de certaines variables historisées dans le répertoire Sirene)

### Variables historisées au niveau de l'unité légale

Sirene conserve tout l‘historique des variables dans les cas suivants :

*  les informations figurent dans le code de commerce comme, par exemple, la dénomination ;
* les informations sont utiles au sens de l’utilisation statistique comme, par exemple, le caractère employeur ou non de l’unité légale.

Quand une variable est historisée au niveau de l’unité légale, si son pendant existe au niveau de l’établissement, il est également historisé. C’est ainsi qu’on dispose de :

* l’historique du statut employeur d’une unité légale donnée ;
* l’historique du statut employeur de chacun des établissements qui dépendent de cette unité légale.

L'historisation des variables du répertoire Sirene a été mise en oeuvre à partir de 2005.

Les variables historisées au niveau de l'unité légale sont les suivantes :

* La dénomination pour les personnes morales ;
*  Le nom de naissance pour les personnes physiques ;
* Le nom d’usage pour les personnes physiques ;
* La dénomination usuelle ;
* La catégorie juridique ;
* L'état administratif ;
* Le Nic du siège ;
* L'activité principale ;
* Le caractère employeur ;
* L’appartenance au champ de l’économie sociale et solidaire (ESS).

Il est important de noter que l’unité légale fournie de l’information sur le siège uniquement. La table unité légale est le point d’entré pour cartographier les sociétés. Toutes les entreprises possédant un SIREN doivent obligatoirement être présentes dans cette table. Les informations complémentaire dans cette table sont a rapprocher avec le siège de l’entreprise uniquement.

Les données des unités légales sont disponibles sur le site de l’INSEE, a cette adresse, [base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret](https://www.data.gouv.fr/fr/datasets/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret/).

Les champs et leur définition sont présents dans le Gitlab, [Documentation/INSEE#Unité légales](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/tree/master/Documentation/INSEE#unit%C3%A9-l%C3%A9gale)

L’INPI n’a pas de table identique aux unités légales.

## Etablissements

Du coté de l’INSEE, les bases de stocks issues du Répertoire des entreprises et des établissements (REE/Sirene) regroupent les entreprises et les établissements actifs en France métropolitaine et dans les Dom au 31 décembre (ceci à partir du millésime 2015). Pour les millésimes précédents, les bases sont observées au 1er janvier. Les données au 31 décembre de l'année n sont donc disponibles en n+2.

Ces bases portent sur deux champs économiques distincts : le champ marchand non agricole et le champ complémentaire au champ marchand non agricole, pour des données définitives au 31/12/2016.

* le fichier stock des établissements (ensemble des établissements actifs et fermés dans leur état courant au répertoire),
* le fichier stock des valeurs historisées des établissements (pour tous les établissements, ensemble des valeurs de certaines variables historisées dans le répertoire Sirene),

### Variables historisées au niveau de l'établissement

Les variables historisées au niveau de l'établissement sont les suivantes :

* L'enseigne ;
* La dénomination usuelle ;
* L'activité principale de l'établissement ;
* L'état administratif ;
* Le caractère employeur de l'établissement.

Les données des unités légales sont disponibles sur le site de l’INSEE, a cette adresse, base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret.

Les champs et leur définition sont présents dans le Gitlab, Documentation/INSEE#etablissement

Les données des établissements dans la base de données de l’INPI disponible depuis le FTP. A la différence de l’INSEE, l’INPI envoie de manière quotidienne des CSV qui contiennent les ajouts ou modification d’information relative aux établissements. Pour trouver les CSV faisant référence aux établissements, il faut récupérer ceux qui ont un suffixe égal à “ETS”. Le détail sur la façon dont l’INPI transfert la donnée sera évoquée dans une prochaine partie.

Pour récupérer les données du FTP, il faut un compte. La création d’un compte est gratuite, mais nécessite plusieurs jours d’attente.

Les champs et leur définition sont présents dans le Gitlab, IMR#etablissements.

# Les formes juridiques

![](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/-/blob/master/IMAGES/00_formes_juridique.jpeg)

# La suppression et la radiation

* [Tout ce que vous devez savoir sur la radiation de société](https://www.captaincontrat.com/articles-gestion-entreprise/radiation-societe) by [[Maxime]]
  * Metadata
    * Type source:: #article
    * Method:: #unstructured
    * Topic:: [[INPI]], #entreprise, #etablissement, #radiation
    * Summary::
  * Highlights:
    * L’immatriculation au Registre du Commerce et des Sociétés (RCS), ou du Répertoire des Métiers (RM, pour les artisans), marque le début de la vie d’une entreprise
      * Cela constitue le début de l’existence de la personne morale ainsi que le début de son activité commerciale.
      * À l’inverse, c’est la [radiation d’une entreprise au RCS](https://dillinger.io/fiche/dissolution-liquidation/) qui marque la fin de vie de cette dernière
      * La radiation correspond à la dernière étape d’une fermeture d’entreprise
      * Elle n’arrive qu’après la [dissolution](https://dillinger.io/articles-dissolution-liquidation/dissolution-et-liquidation-societe) (qui est l’acte qui prononce l’extinction de la société) et la liquidation de la société (qui est une conséquence comptable de la dissolution).
      * La radiation, quant à elle, est une étape charnière pour la fermeture d’une entreprise puisqu’elle supprime l’immatriculation de la société au RCS
      * c’est par la radiation que l’on peut alors parler de la suppression totale de l’entreprise
    *  Les différents cas de radiation
      * La radiation d’une entreprise met un terme juridique et définitif de celle-ci
      * il existe deux types de radiation
        * La radiation issue d’une procédure normale
        * La radiation d’office
      *  La procédure normale de radiation
      *  La radiation d’office
        * La radiation d’office est issue d’une décision administrative qui entraîne d’office la dissolution de la société.
        * l’administration impose la radiation dans certains cas :
        * le décès de la personne immatriculée
        * Si la personne physique détenant l’entreprise immatriculée meurt, cela doit être déclaré au RCS
        * Il est possible de déclarer le maintien provisoire de la société pendant un délai d’un an, renouvelable une fois. Au-delà de ce délai, la radiation d’office sera effectuée par le greffier
        * si la personne assujettie décède sans avoir notifié la présence d’un héritier au RCS, alors le greffier procédera à la radiation d’office
        * La cessation d’activité
          * Il s’agit du cas où la personne immatriculée mentionne au RCS la [cessation de son activité](https://dillinger.io/freelance/cessation-activite-eurl).
          * Après un délai de trois ans à compter de la mention de cessation d’activité au RCS, le greffier rappelle à la personne assujettie les obligations déclaratives, relatives à la dissolution, qu’il n’a pas respectée
        * La dissolution
          * Une société est radiée d’office après un délai de trois ans à partir de la notification de la dissolution de cette dernière
          * La radiation d’office prononcée par décision de justice
          * Dans ce cas, la radiation est due à une cause judiciaire ou contentieuse
          * En effet, l’administration, ou n’importe quel particulier, peut mettre en demeure une société si ceux-ci ont connaissance d’un acte ou événement ayant pour conséquence la radiation de l’entreprise
          * De plus, il existe d’autres raisons de radiation d’office décidée par le ministère public. Comme, notamment, dans les cas de
            * interdiction de gérer : la radiation est d’office lorsqu’un commerçant est frappé d’une interdiction d’exercer une activité commerciale en vertu d’une décision judiciaire passée en force de chose jugée ou d’une décision administrative exécutoire
            * clôture issue d’une procédure de faillite
            * cessation totale d’activité d’une personne morale
    *  La procédure de radiation
      * La procédure de radiation est la même pour toutes les formes de société.
      * la société doit avoir cessé son activité commerciale
      * Une fois la cessation d’activité effective, vous avez un délai de 30 jours afin de la déclarer auprès du Centre de Formalités des Entreprises (CFE) ou du greffe du tribunal de commerce
    *  Que se passe-t-il après la radiation d’une société ?
      * La conséquence principale de la radiation est la suppression de l’immatriculation de la société au RCS
      * l’entreprise perd son identité commerciale, elle n’existe donc plus
      * il est possible que des contentieux subsistent entre la société et des personnes physiques ou morales
      * Il existe principalement deux types de conflits
      * la société est débitrice
        * le créancier devra se retourner vers un mandataire
        * . Lors de la liquidation, c’est alors le liquidateur qui prend la gestion de la société liquidatrice
      * la société est créancière
        * c’est un associé qui pourra mettre en demeure son débiteur.
        * ATTENTION : la radiation d’une société n’est effective qu’à partir de la publication au RCS de la clôture de sa liquidation. Avant ce moment-là, l’entreprise garde sa personnalité juridique.
* [Entreprise radiée : définition, modalités et effets - Ooreka](https://creation-entreprise.ooreka.fr/astuce/voir/612323/entreprise-radiee) by [[Unknown Author]]
  * Metadata
    * Type source:: #article
    * Method:: #unstructured
    * Topic:: [[INPI]], #entreprise, #etablissement, #radiation
    * Summary::
  * Highlights:
    *  Radiation d’une entreprise : procédure
      * En 2018, 120 418 sociétés ont été radiées, dont 39 880 pour la seule région Île de France
      * La radiation d’un commerçant personne physique ou la radiation d’une société doit obligatoirement faire l’objet d’une déclaration auprès du greffe du tribunal de commerce
      * Le motif de la demande de radiation d’entreprise ou société peut être lié à une cessation d’activité, une dissolution, une fusion, etc
      * selon la jurisprudence une société conserve sa personnalité morale, même après la clôture de la liquidation, tant qu’elle a des créances ou des dettes
      * Pour les personnes physiques, la radiation de l’immatriculation principale est gratuite, et les frais de notification liés à la radiation d’une immatriculation secondaire s’élèvent à 33,79 €.
      * Pour les personnes morales, la radiation de l’immatriculation principale est gratuite (en cas de dépôt d’acte, le coût est de 14,35 €) et les frais de notification liés à la radiation d’une immatriculation secondaire s’élèvent à 46,48 € (tarifs TTC en 2019
    *  Entreprise radiée : radiation d’une personne phys
      * La radiation d’un commerçant personne physique est déclarée
      * effectuer au greffe du tribunal de commerce dans le mois qui précède ou qui suit la cessation totale d’activité dans le ressort ([article R. 123-51 du Code de commerce](https://www.legifrance.gouv.fr/affichCodeArticle.do?idArticle=LEGIARTI000006256459&cidTexte=LEGITEXT000005634379)). Cette demand
      * [Entrepreneur individuel](https://creation-entreprise.ooreka.fr/comprendre/entreprise-individuelle) ne peut conserver des établissements secondaires après avoir déclaré la cessation d’activité de son établissement principal.
    * Tarif Fermeture
      * https://www.infogreffe.com/formalites-entreprise/tarifs-des-formalites.html
* [Déclaration de radiation d’activité ou d’entreprise - Infogreffe](https://www.infogreffe.fr/radiation/declaration-radiation-entreprise.html) by [[Unknown Author]]
  * Metadata
    * Type source:: #article
    * Method:: #unstructured
    * Topic:: [[INPI]], #entreprise, #etablissement, #radiation
    * Summary::
  * Highlights:
    * La radiation du Registre du Commerce et des Sociétés d’un commerçant personne physique est gratuite
    * Mais si l’entreprise individuelle a un (ou plusieurs) établissement(s) immatriculé(s) au RCS dont le ressort territorial est celui d’un autre tribunal : le montant exigé pour la notification de la radiation est 8.45 euros (à multiplier par le nombre d’immatriculations secondaires).
* [Comment fermer un établissement secondaire?](https://www.legalstart.fr/fiches-pratiques/siege-social/fermeture-etablissement-secondaire/) by [[Meriadeg Mallard]]
  * Metadata
    * Type source:: #article
    * Method:: #unstructured
    * Topic:: [[INPI]], #entreprise, #etablissement, #fermeture
    * Summary::
  * Highlights:
    * Pour certaines raisons économiques ou stratégiques, les dirigeants d’une société peuvent décider de la fermeture d’un**établissement secondaire.**
    *  Comment effectuer une fermeture d’[établissement secondaire](https://dillinger.io/fiches-pratiques/siege-social/ouverture-etablissement-secondaire/)?
      * L’existence de l’établissement est inscrite au RCS étant donné que celui-ci a sa propre immatriculation
      * Pour fermer un établissement secondaire il va donc falloir procéder à une inscription modificative au RCS
      * Les formalités relatives à la fermeture d’un établissement secondaires peuvent être effectuées indifféremment auprès du greffe du tribunal de commerce ou auprès du CFE
      * La procédure de fermeture d’un établissement secondaire ne varie pas selon la forme sociale de la société.
      * Pour réaliser la fermeture d’un établissement secondaire, un dossier doit être déposé indifféremment, au Greffe du tribunal de commerce du ressort de l’établissement, ou au [Centre de Formalité des Entreprises](https://dillinger.io/fiches-pratiques/demarches-creation/centre-de-formalites-des-entreprises/) (CFE) du ressort de l’établissement

# La donnée de l’INPI

Le contenu mis à disposition par l’INPI comprend :
*  Les dossiers des données relatives aux personnes actives (morales et physiques) :
  * Un stock initial constitué à la date du 4 mai 2017 pour les données issues des Tribunaux de commerce (TC)
  * Un stock initial constitué à la date du 5 mai 2018 pour les données issues des Tribunaux d’instance et Tribunaux mixtes de commerce (TI/TMC)
* Des stocks partiels constitués des dossiers complets relivrés à la demande de l’INPI après détection d’anomalies . Les fichiers des données contenues dans les nouvelles inscriptions (immatriculations, modifications et radiations) du Registre national du commerce et des sociétés ainsi que les informations relatives aux dépôts des actes et comptes annuels, telles que transmises par les greffes à compter du 5 mai 2017 (données de flux).
Au total, ce sont les données d’environ 5 millions de personnes actives (morales et physiques) qui sont mises à la disposition de l’ensemble des ré-utilisateurs.
Ces données sont mises à jour quotidiennement (environ 1,4 million de nouvelles inscriptions par an).
Les tribunaux représentés sont au nombre de 148 répartis comme suit (liste fournie en annexe) :
*  134 Tribunaux de commerce,
*  7 Tribunaux d’Instance des départements du Bas-Rhin, du Haut-Rhin et de la Moselle,
*  7 Tribunaux mixtes de commerce des départements et régions d'outre-mer.


## Tribunaux De Commerce

L’INPI met a disposition la donnée via un FTP. Le FTP contient les données des TC et des TI/TMC. Dans notre projet, nous nous focalisons uniquement sur les TC. Les données du FTP sont séparées selon deux branches, une branche dite de stock et une branche de flux. Au sein de ses deux branches, il y a plusieurs types d’information, a savoir le statut juridique ou l’établissement, mais aussi les actes et observations.

Plus précisément, il y a 7 sources de données dans les tribunaux de commerce. Chacune des sources fait référence aux caractéristiques des entreprises (statut juridique et établissements). Nous avons indiqué le schéma dans le Gitlab, et mis à disposition de l’utilisateur via le lien URL affiché
* PM
  * https://scm.saas.cagip.group.gca/PERNETTH/InseeInpi_matching/tree/master/Documentation/IMR#personnes-morales
* PP
  * https://scm.saas.cagip.group.gca/PERNETTH/InseeInpi_matching/tree/master/Documentation/IMR#personnes-physiques
* Rep
  * https://scm.saas.cagip.group.gca/PERNETTH/InseeInpi_matching/tree/master/Documentation/IMR#repr%C3%A9sentants
* ETS
  * https://scm.saas.cagip.group.gca/PERNETTH/InseeInpi_matching/tree/master/Documentation/IMR#etablissements
* Obs
  * https://scm.saas.cagip.group.gca/PERNETTH/InseeInpi_matching/tree/master/Documentation/IMR#observations
* Actes
  * https://scm.saas.cagip.group.gca/PERNETTH/InseeInpi_matching/tree/master/Documentation/IMR#actes
* Comptes annuels
  * https://scm.saas.cagip.group.gca/PERNETTH/InseeInpi_matching/tree/master/Documentation/IMR#comptes-annuels
Un récapitulatif des variables selon la présence dans les tables est disponible dans le Gitlab, https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/tree/master/Documentation/IMR#r%C3%A9capitulatif-ensemble-des-variables-selon-origine

Pour notre projet de siretisation, nous allons avoir besoin des CSV relatif aux établissements, qui se trouve dans la branche des stocks et des flux. L’INPI recoit des transmissions de dossier de la part d’Infogreffe. Afin de pour tracer le dossier, l’INPI indique 2 informations indispensables, attaché au greffe:
* code greffe (nom greffe)
* Numéro de gestion

et deux informations relatives à l’établissement
* siren
* id etablissement

Effectivement, le numéro de siret n’est pas présent dans les données de l’INPI. Dès lors, ce dernier crée un numéro d’établissement. Ainsi, pour distinguer les différents établissements présents dans une entreprise, il faut utiliser les 4 informations fournis par l’INPI, à savoir, le code greffe, le numéro de gestion, le siren et l’identification d’établissement. Pour le moment, nous distinguerons un établissement au sens de l’INPI et celui de l’INSEE. Un établissement au sens de l’INSEE est assez facile à distinguer, il est référencé par son SIREN-SIRET alors que l’établissement au sens de l’INPI est catégorisé selon le quadruplet code greffe-numéro gestion-siren-id etablissement. Nous verrons plus tard pourquoi nous distinguons les établissements ayant pour origine l’INPI et l’INSEE.

La branche des stocks contient deux types d’information. Premièrement, elle rassemble toutes les entreprises créées avant le 5 mai 2017. L’ensemble de ce CSV s’appelle stock initial. Deuxièmement, elle contient des CSV correcteur de mauvaise transmission de dossier de la part du greffe. Autrement dit, le greffe peut envoyer à l’INPI des dossiers qui contiennent des anomalies ou des erreurs. L’INPI en informe infogreffe, qui en retour, va transmettre une correction à l’INPI. L’INPI va mettre a disposition des CSV correcteurs dans la branches des stocks, que nous appellerons par la suite, stock partiel.

La branche des flux va contenir toutes les informations relatives à la création d’établissement, modification ou suppression. l’INPI assigne les labels suivants dans la variable libelle_evt , selon le type d’événement:
* Création d’établissement: Etablissement ouvert
* Modification d’information relative a un établissement: Modifications relatives à un établissement
* Fermeture d’établissement: Etablissement supprimé

Les CSV de l’INPI ont une typologie très normée:

* fichier de stock/partiel:
 <code_greffe>_<num_transmission>_<AA><MM><JJ>_8_ets.csv
* Fichier de flux - Création
 <code_greffe>_<num_transmission>_<AA><MM><JJ>_<HH><MM><SS>_8_ets.csv
* Fichier de flux - modification
<code_greffe>_<num_transmission>_<AA><MM><JJ>_<HH><MM><SS>_9_ets_nouveau_modifie_EVT.csv
* Fichier de flux - suppression
<code_greffe>_<num_transmission>_<AA><MM><JJ>_<HH><MM><SS>_10_ets_supprime_EVT.csv

En commun, le code greffe et le numéro de transmission et la date de transmission. La date de transmission pour le stock initial a uniquement la valeur 20170504, alors que les stock partiels ont une valeur égale à la date de transmission. Les csv dans les flux ont une information supplémentaire, à savoir l’heure de transmission. Cette information est importante pour plusieurs raisons. En effet, l’INPI recoit les informations des greffes à  des heures de la journée différente d’une transmission à l’autre. Par exemple, le greffe peut envoyer un ensemble de dossier à 8h30 aujourd’hui et transmettre un autre ensemble à 15h15 le lendemain.

Chacune des transmissions peut contenir des dossiers identiques ou différents, ce qui complexifie un peu la tache. Sans raison particulière, l’INPI peut recevoir les informations d’un même dossier étaler sur plusieurs jours, mois ou année. Ainsi, l’INPI recommande d’ingérer la donnée de manière incrémentale, et de prioriser la dernière transmission, aux précédentes. Ce dernier point est très important car l’ensemble de la partie relative au filtrage et enrichissement de la donnée de l’INPI va être fondée dessus.

# Relation INSEE-INPI

Il y a deux grands fournisseurs de données concernant les entreprises en France:
* INSEE
* INPI

L’INSEE se charge de toute la partie donnée d’entreprise et est rattaché au ministère de l’économie et des finances. L’INPI est rattaché au ministère des finances (et du premier ministre). L’INPI s’occupe de toute la partie juridique.

Le type de chambre de commerce va dépendre du type d’activité de l’entreprise, et suivant sa taille ou son status, l’entreprise se doit de s’inscrire à l’INPI. C’est le cas par exemple des sociétés commerciale (SARL/SA/EURL) qui sont rattachées à la CCI et de fait doivent s’inscrire à l’INPI. Les professions libérales ou micro-entreprises n’ont pas besoin de s’inscrire à l’INPI à moins que la personne soit un agent commercial.

![](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/-/raw/master/IMAGES/02_nouveau_schema_institution.PNG)

Le point d’entré de la cartographie des entreprises en France se fait via les unités légales. En effet, cette dernière a un caractère juridique qui fourni la preuve d’immatriculation de l’entreprise. Ensuite, pour connaitre les informations sur le choix juridique de l’entité (Personne Morale ou Personne Physique), il faut se concentrer sur les tables des PP et des PM fourni par l’INPI. Chacune des deux tables tables comportent le siren avec un ensemble d’information spécifique à la personne morale et/ou physique.

Comme  nous le savons déjà, chaque entreprise peut posséder un ou plusieurs établissement(s). C’est le rôle de l’INSEE de fournir un numéro NIC, qui va permettre de constituer le SIRET. Le siret est tout simplement un identifiant unique permettant de géolocaliser une entreprise. Le SIRET n’est pas présent à l’INPI. Toutefois, avec quelques règles de gestion, il est possible de rapprocher les deux tables, à savoir établissement INPI et établissement INSEE.

Nous savons aussi que le SIRET de l’unité légale est celui du siège. Lors du rapprochement des différentes tables, il est possible de rassembler et recouper la plupart des informations entre elles pour reconstituer la cartographie de l’entreprise.

![](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/-/raw/master/IMAGES/03_relation_inpi_insee.png)

Sources:
* https://fr.wikipedia.org/wiki/Entreprise
* https://fr.wikipedia.org/wiki/Institut_national_de_la_statistique_et_des_%C3%A9tudes_%C3%A9conomiques
* https://www.insee.fr/fr/metadonnees/definition/c1044
* https://www.insee.fr/fr/metadonnees/definition/c1496
* https://www.legalstart.fr/fiches-pratiques/siege-social/

---
jupyter:
  jupytext:
    formats: ipynb,md
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.2'
      jupytext_version: 1.4.0
  kernelspec:
    display_name: Python 3
    language: python
    name: python3
---

<!-- #region -->
# Personnes Physiques

# Personne physique : une définition juridique

## Définition d’une personne physique

### Personne physique définition juridique

* Les personnes sont prévues dans le Code civil au Livre I – Des personnes. Selon le vocabulaire de l’association Capitant la personne est «l’être qui jouit de la personnalité juridique», c’est-à-dire de « l’aptitude à être titulaire de droits et assujetti à des obligations» (Vocabulaire Capitant, V° Personne).
* Au sein des personnes on distingue ensuite deux catégories: les personnes physiques et les personnes morales
*  Les personnes physiques sont des êtres constitués de chair et de sang alors que les personnes morales sont des regroupements autour d’intérêts communs.

### Si une entreprise n'est pas une société, elle n'est pas une personne morale mais une personne physique

* C'est l'entrepreneur qui possède la personnalité juridique de l'entreprise et non l'entreprise elle-même
* Aucune séparation n'est faite entre le patrimoine de l'entreprise et celui de l'associé
* Le patrimoine de l'associé se trouve entièrement engagé

### Statut juridique d’une personne physique

*  Dès sa naissance, une personne est reconnue, instituée: elle est inscrite dans une généalogie, elle a un nom, prénom, une nationalité et un domicile (c’est l’identification de la personne physique).
* La personne est dès lors protégée: elle dispose de droits relatifs à la protection de son image, de son honneur, de sa vie privée, de sa dignité. On parle alors des droits de la personnalité.
* Elle est également assujettie à des obligations, en premier lieu respecter les droits des autres personnes. Elle doit également respecter les engagements qu’elle prend, par exemple honorer un contrat qu’elle a conclu


### Personne physique et entrepreneur

* Une personne physique qui veut créer une entreprise doit-elle forcément lui attribuer la personnalité morale ? Y a-t-il toujours une opposition : société ou personne physique ?
* Plusieurs possibilités s’ouvrent aux personnes physiques qui souhaitent lancer une affaire personnelle

### Les entreprises individuelles qui n’impliquent pas la personnalité morale
https://www.mr-entreprise.fr/wp-content/uploads/2020/02/schema-choisir-statut-juridique-leblogdudirigeant.png

### L’entreprise individuelle

* Commençons par l’entreprise individuelle (« EI »). L’entreprise individuelle est-elle une personne physique ou morale ?
* L’EI est réservée aux personnes physiques: une personne morale ne peut créer une entreprise individuelle.
* De plus, l’entrepreneur personne physique exerce en son nom propre et non au travers d’une personne morale
*  Cela a notamment pour conséquence que l’entrepreneur est assujetti à l’impôt sur le revenu et que son patrimoine privé et son patrimoine personnel sont confondus
* Il faut noter que l’EI est très populaire en France : ce statut comporte de nombreux avantages et sa créationest facilitée
* La personne physique qui crée une EI doit enfin l’immatriculer
* Comme mentionné, un des désavantages de l’EI est toutefois la confusion des patrimoines. L’entrepreneur ne peut protéger son patrimoine personnel de ses créanciers professionnels
  * La loi Macron de 2015 a toutefois atténué cet aspect : elle protège aujourd’hui d’office votre résidence principale et il est possible de recourir à une déclaration d’insaisissabilité pour protéger les biens mentionnés dans la déclaration

### L’entreprise individuelle à responsabilité limitée

* L’EIRL est prévue aux articles L. 526-6 et suivants du Code de commerce. Comme dans l’EI, la création d’une EIRL est facilitée : une simple déclaration suffit.
  *  l’EIRL permet par exemple d’opter pour l’impôt sur les sociétés.
    * Comme pour l’entreprise individuelle, l’EIRL n’a pas la personnalité morale
    * Par contre, au contraire de l’EI, l’entrepreneur aura dès la création de l’EIRL un patrimoine privé et un patrimoine professionnel distinct, spécifique à son activité
    *  L’EIRL permet en effet de créer un patrimoine personnel d’affectation: l’entrepreneur met ainsi à l’abri son patrimoine personnel de ses créanciers professionnels
    *  L’entrepreneur en EIRL devra également tenir une comptabilité commerciale autonome

### Personne physique et auto-entrepreneur

* Une personne physique peut décider de créer une micro-entreprise pour commencer une activité (par exemple lancer une start-up).
*  L’entrepreneur individuel peut également décider d’opter pour le **régime de la micro-entreprise** s’il remplit certaines conditions liées à des seuils
* Il ne faut pas franchir les **seuils de chiffre d’affaires** suivants
  * 70 000€ pour une activité de prestations de service
  * 170 000 € pour une activité d’achat-vente
* Les avantages sont que les obligations comptables sont simplifiées, et que, au niveau des charges sociales, le montant des cotisations dépend de la nature de l’activité.
*  En cas de dépassement de ces plafonds, il faudra par contre passer du statut de micro-entrepreneur à celui de l’entreprise individuelle
* A noter, le statut de micro entrepreneur a remplacé celui de l’auto entrepreneur en 2016
* La micro entreprise comme l’entreprise individuelle n’a pas la personnalité morale
* La raison sociale, qui désigne le nom de la personne morale (comme une sorte de nom de famille), correspond pour les micros entreprises au nom de famille de la personne physique

### Les sociétés unipersonnelles qui impliquent la personnalité morale

* Il existe différents types de sociétés unipersonnelles (à associé unique) comme par exemple, la SASU (société par actions simplifiée à associé unique) ou l’EURL (entreprise unipersonnelle à responsabilité limitée).
* La création et la gestion de ces sociétés impliquent toutefois plus de formalités
  *  Par exemple la rédaction de statuts, la publication de la création de la société, l’immatriculation au RCS, etc, ces sociétés doivent être immatriculées au Registre du commerce et des sociétés
            Elles ont donc la personnalité morale

## Les formes juridiques existantes : synthèse et conséquences fiscales

### La catégorie des entreprises individuelles

*  l’Entreprise Individuelle (EI) ou l’Entreprise Individuelle à Responsabilité Limité (EIRL). La particularité de ces formes juridiques étant que leur patrimoine se confond avec celui du dirigeant. Les dettes de l’entreprise sont les dettes du dirigeant. Il est toutefois possible d’affecter une partie du patrimoine à l’entreprise afin de limiter les risques.
* un régime fiscal et social ultra simplifié : l’ autoentreprise qui est maintenant appelée microentreprise
*  l’Entreprise Individuelle (EI) ou l’Entreprise Individuelle à Responsabilité Limité (EIRL). La particularité de ces formes juridiques étant que leur patrimoine se confond avec celui du dirigeant. Les dettes de l’entreprise sont les dettes du dirigeant. Il est toutefois possible d’affecter une partie du patrimoine à l’entreprise afin de limiter les risques
https://www.leblogdudirigeant.com/wp-content/uploads/2015/10/Choisir-son-statut-juridique-les-Entreprises-Individuelles-EI-.jpg

### Relation avec les données de L'INPI

L’INPI fournit une table dédiée aux personnes physiques. Dans cette table, nous pouvons trouver l’ensemble de l’historique pour chaque personne ayant fait une demande d’immatriculation.

### Schéma de donnée

| Champ                         |  Nom                                           |  PP                                                                                                                                                                        |  PP_EVT                                                                                                                                                                                                                                                        |
|-------------------------------|------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Code Greffe                   |  Code Greffe                                   |  Valeur obligatoire                                                                                                                                                        |  Valeur obligatoire                                                                                                                                                                                                                                            |
| Nom_Greffe                    |  Nom greffe                                    |  Valeur obligatoire                                                                                                                                                        |  Valeur obligatoire                                                                                                                                                                                                                                            |
| Numero_Gestion                |  Numéro de gestion                             |  Valeur obligatoire                                                                                                                                                        |  Valeur obligatoire                                                                                                                                                                                                                                            |
| Siren                         |  Siren                                         |  Valeur obligatoire                                                                                                                                                        |  Valeur obligatoire                                                                                                                                                                                                                                            |
| Type_Inscription              |  Type d’inscription (P ou S)                   |  Valeur obligatoire                                                                                                                                                        |  Valeur toujours renseignée.                                                                                                                                                                                                                                   |
| Date_Immatriculation          |  Date d’immatriculation                        |  Valeur renseignée de manière aléatoire (en fonction des greffes)                                                                                                          |  Valeur renseignée en cas de modification, sinon vide                                                                                                                                                                                                          |
| Date_1re_Immatriculation      |  Date 1ere immatriculation                     |  Valeur renseignée de manière aléatoire (en fonction des greffes)                                                                                                          |  Valeur renseignée en cas de modification, sinon vide.                                                                                                                                                                                                         |
| Date_Radiation                |  Date de radiation                             |  Toujours vide                                                                                                                                                             |  Valeur renseignée en cas de modification ou de radiation, sinon vide.En cas de déradiation, la valeur indiquée est (Déradiation).                                                                                                                             |
| Date_Transfert                |  Date de transfert                             |  Valeur renseignée en cas de transfert ou de modification, sinon vide.(date d’effet)                                                                                       |  Toujours vide                                                                                                                                                                                                                                                 |
| Sans_Activité                 |  Flag « sans activité »                        |  La valeur est Oui ou vide                                                                                                                                                 |  Valeur renseignée en cas de modification, sinon vide.                                                                                                                                                                                                         |
| Date_Debut_Activité           |  Date début d’activité (si renseignée)         |  Valeur renseignée de manière aléatoire (en fonction des greffes).Jamais renseignée lorsque le flag « sans activité » = oui                                                |  Valeur renseignée en cas de modification, sinon vide.                                                                                                                                                                                                         |
| Date_Début_1re_Activité       |  Date début première activité (si renseignée)  |  Valeur renseignée de manière aléatoire (en fonction des greffes)                                                                                                          |  Valeur renseignée en cas de modification, sinon vide.                                                                                                                                                                                                         |
| Date_Cessation_Activité       |  Date cessation activité (si renseignée)       |  Valeur renseignée si existe.                                                                                                                                              |  Valeur renseignée en cas de modification, sinon vide.Si renseignée, supprimer le libellé du champ « activité principale »                                                                                                                                     |
| Nom_Patronymique              |  Nom patronymique                              |  Valeur obligatoire                                                                                                                                                        |  Valeur renseignée en cas de modification, sinon vide.                                                                                                                                                                                                         |
| Nom_Usage                     |  Nom usage                                     |  Valeur renseignée si existe.                                                                                                                                              |  Valeur renseignée en cas de modification, sinon vide.                                                                                                                                                                                                         |
| Pseudonyme                    |  Pseudonyme                                    |  Valeur renseignée si existe.                                                                                                                                              |  Valeur renseignée en cas de modification, sinon vide.                                                                                                                                                                                                         |
| Prénoms                       |  Prénoms                                       |  Attention : Parfois le prénom est renseigné dans la balise « Nom_Usage » ou « Nom_Patronymique »                                                                          |  Valeur renseignée en cas de modification, sinon vide.Attention : Parfois le prénom est renseigné dans la balise « Nom_Usage » ou « Nom_Patronymique »                                                                                                         |
| Date_Naissance                |  Date naissance                                |  Valeur renseignée si existe.                                                                                                                                              |  Valeur renseignée en cas de modification, sinon vide.                                                                                                                                                                                                         |
| Ville_Naissance               |  Ville naissance                               |  Valeur renseignée si existe                                                                                                                                               |  Valeur renseignée en cas de modification, sinon vide.                                                                                                                                                                                                         |
| Pays_Naissance                |  Pays naissance                                |  Valeur renseignée si existe                                                                                                                                               |  Valeur renseignée en cas de modification, sinon vide.                                                                                                                                                                                                         |
| Nationalité                   |  Nationalité                                   |  Valeur renseignée si existe                                                                                                                                               |  Valeur renseignée en cas de modification, sinon vide.                                                                                                                                                                                                         |
| Adresse_Ligne1                |  Ligne 1 – Adresse                             |  Selon les greffes, l’adresse (n°+ voie) sera présente soit en ligne1 adresse, soit en ligne2 adresse.Toutes les lignes d’adresse ne sont pas nécessairement renseignées.  |  Toutes les valeurs existantes sont renvoyées (adresse etc.) en cas de mise à jour de l’une ou l’autre des valeurs.                                                                                                                                            |
| Adresse_Ligne2                |  Ligne 2 – Adresse                             |  Selon les greffes, l’adresse (n°+ voie) sera présente soit en ligne1 adresse, soit en ligne2 adresse.Toutes les lignes d’adresse ne sont pas nécessairement renseignées.  |  Toutes les valeurs existantes sont renvoyées (adresse etc.) en cas de mise à jour de l’une ou l’autre des valeurs.                                                                                                                                            |
| Adresse_Ligne3                |  Ligne 3 – Adresse                             |  Selon les greffes, l’adresse (n°+ voie) sera présente soit en ligne1 adresse, soit en ligne2 adresse.Toutes les lignes d’adresse ne sont pas nécessairement renseignées.  |  Toutes les valeurs existantes sont renvoyées (adresse etc.) en cas de mise à jour de l’une ou l’autre des valeurs.                                                                                                                                            |
| Code_Postal                   |  Code postal                                   |  Selon les greffes, l’adresse (n°+ voie) sera présente soit en ligne1 adresse, soit en ligne2 adresse.Toutes les lignes d’adresse ne sont pas nécessairement renseignées.  |  Toutes les valeurs existantes sont renvoyées (adresse etc.) en cas de mise à jour de l’une ou l’autre des valeurs.                                                                                                                                            |
| Ville                         |  Ville                                         |  Selon les greffes, l’adresse (n°+ voie) sera présente soit en ligne1 adresse, soit en ligne2 adresse.Toutes les lignes d’adresse ne sont pas nécessairement renseignées.  |  Toutes les valeurs existantes sont renvoyées (adresse etc.) en cas de mise à jour de l’une ou l’autre des valeurs.                                                                                                                                            |
| Code_Commune                  |  Code commune                                  |  Valeur renseignée si existe.                                                                                                                                              |  Valeur renseignée en cas de modification, sinon vide.                                                                                                                                                                                                         |
| Pays                          |  Pays                                          |  Valeur renseignée si existe.                                                                                                                                              |  Valeur renseignée en cas de modification, sinon vide.                                                                                                                                                                                                         |
| Activité_Forain               |  Flag « Forain »                               |  Valeur toujours renseignée                                                                                                                                                |  Valeur renseignée en cas de modification, sinon vide.                                                                                                                                                                                                         |
| DAP                           |  Flag « DAP »                                  |  Valeur toujours renseignée                                                                                                                                                |  Valeur renseignée en cas de modification, sinon vide.                                                                                                                                                                                                         |
| DAP_Dénomination              |  Dénomination (DAP)                            |  Valeur renseignée si flag « DAP » = OUI                                                                                                                                   |  Valeur renseignée en cas de modification, sinon vide.                                                                                                                                                                                                         |
| DAP_Objet                     |  Objet (DAP)                                   |  Valeur renseignée si flag « DAP » = OUI                                                                                                                                   |  Valeur renseignée en cas de modification, sinon vide.                                                                                                                                                                                                         |
| DAP_Date_Clôture              |  Date de clôture (DAP)                         |  Valeur renseignée si flag « DAP » = OUI                                                                                                                                   |  Valeur renseignée en cas de modification, sinon vide.                                                                                                                                                                                                         |
| DAP_Adresse_Ligne1            |  Adresse (DAP)                                 |  Valeur renseignée si flag « DAP » = OUI                                                                                                                                   |  Valeur renseignée en cas de modification, sinon vide.                                                                                                                                                                                                         |
| DAP_Adresse_Ligne2            |  Adresse (DAP)                                 |  Valeur renseignée si flag « DAP » = OUI                                                                                                                                   |  Valeur renseignée en cas de modification, sinon vide.                                                                                                                                                                                                         |
| DAP_Adresse_Ligne3            |  Adresse (DAP)                                 |  Valeur renseignée si flag « DAP » = OUI                                                                                                                                   |  Valeur renseignée en cas de modification, sinon vide.                                                                                                                                                                                                         |
| DAP_Code_Postal               |  Adresse (DAP)                                 |  Valeur renseignée si flag « DAP » = OUI                                                                                                                                   |  Valeur renseignée en cas de modification, sinon vide.                                                                                                                                                                                                         |
| DAP_Ville                     |  Adresse (DAP)                                 |  Valeur renseignée si flag « DAP » = OUI                                                                                                                                   |  Valeur renseignée en cas de modification, sinon vide.                                                                                                                                                                                                         |
| DAP_Code_Commune              |  Adresse (DAP)                                 |  Valeur renseignée si flag « DAP » = OUI                                                                                                                                   |  Valeur renseignée en cas de modification, sinon vide.                                                                                                                                                                                                         |
| DAP_Pays                      |  Adresse (DAP)                                 |  Valeur renseignée si flag « DAP » = OUI                                                                                                                                   |  Valeur renseignée en cas de modification, sinon vide.                                                                                                                                                                                                         |
| EIRL                          |  Flag « EIRL »                                 |  Valeur toujours renseignée                                                                                                                                                |  Valeur renseignée en cas de modification, sinon vide.                                                                                                                                                                                                         |
| Auto-entrepreneur             |  Flag « Auto-entrepreneur »                    |  Valeur renseignée de manière aléatoire                                                                                                                                    |  Valeur renseignée de manière aléatoire                                                                                                                                                                                                                        |
| Conjoint_Collab_Nom_Patronym  |  Nom patronymique (conjoint collab.)           |  Valeur renseignée si existe.                                                                                                                                              |  Toutes les valeurs existantes sont renseignées (valeurs relatives au conjoint collaborateur) en cas de mise à jour de l’une ou l’autre des valeurs.Dans le cas de la suppression, chaque champ prend la valeur « supprimé » et la date de fin est renseignée. |
| Conjoint_Collab_Nom_Usage     |  Nom usage (conjoint collab.)                  |  Valeur renseignée si existe.                                                                                                                                              |  Toutes les valeurs existantes sont renseignées (valeurs relatives au conjoint collaborateur) en cas de mise à jour de l’une ou l’autre des valeurs.Dans le cas de la suppression, chaque champ prend la valeur « supprimé » et la date de fin est renseignée. |
| Conjoint_Collab_Pseudo        |  Pseudonyme (conjoint collab.)                 |  Valeur renseignée si existe.                                                                                                                                              |  Toutes les valeurs existantes sont renseignées (valeurs relatives au conjoint collaborateur) en cas de mise à jour de l’une ou l’autre des valeurs.Dans le cas de la suppression, chaque champ prend la valeur « supprimé » et la date de fin est renseignée. |
| Conjoint_Collab_Prénoms       |  Prénoms (conjoint collab.)                    |  Valeur renseignée si existe.                                                                                                                                              |  Toutes les valeurs existantes sont renseignées (valeurs relatives au conjoint collaborateur) en cas de mise à jour de l’une ou l’autre des valeurs.Dans le cas de la suppression, chaque champ prend la valeur « supprimé » et la date de fin est renseignée. |
| Conjoint_Collab_Date_Fin      |  Date fin (conjoint collab.)                   |  Valeur renseignée si existe.                                                                                                                                              |  Toutes les valeurs existantes sont renseignées (valeurs relatives au conjoint collaborateur) en cas de mise à jour de l’une ou l’autre des valeurs.Dans le cas de la suppression, chaque champ prend la valeur « supprimé » et la date de fin est renseignée. |
| Date_Greffe                   |  Date de l’événement                           |  Obligatoire                                                                                                                                                               |  Obligatoire                                                                                                                                                                                                                                                   |
| Libelle_Evt                   |  Libellé de l’évenement                        |  Obligatoire                                                                                                                                                               |  Obligatoire                                                                                                                                                                                                                                                   |

Les information ci-dessus sont propres à  la personne physique ou au conjoint. Lorsque nous allons regrouper l’information avec l’INSEE, nous pourrons rattacher un SIRET à la personne morale qui en l’occurrence peut posséder plusieurs adresses. En effet, une personne morale qui déménage se voit attribuer un nouveau siren, mais encore une personne morale peut avoir plusieurs établissements simultanément.

## Cas pratique

Mr machin siren 123456789 a trois structures. Une structure de conseil, et deux boutiques. trois adresses différentes, 3 siret différents, 2 activités différentes mais un siren commun

### Questions

* Une personne physique a t-elle un siren unique?
  * Oui
*  Le nom commercial d'une personne physique est il le nom de famille?
  * Oui
* Si le PP a trois boutiques, le nom commercial est forcément le nom de famille?
  * Oui c'est la difficulté des personnes physiques le SIREN attribué sur M MACHIN est à vie il aura toujours le siren 123456789 :
            avec le NIC 0001 pour la partie conseil  donc siret 123456789 0001 établissement principal
            avec ses deux boutiques de vêtement  - même activité pas exemple  une à PARIS l'autre à TOULOUSE
* une jeune fille ouvre une structure en pp, nom commercial = son nom de famille. Elle se marie, le nom commercial change automatiquement, ou elle peut grder son nom de jeune fille?
  * Dans la mesure où en effet la PP à généralement comme raison sociale (INSEE) ou dénomination sociale (INIP) son nom et prénom, la jeune marié peut en faire la demande si elle le souhaite. Mais pas obligatoire car professionnellement, surtout si elle à pignon sur rue, elle est plus connu avec son nom de jeune fille (donc plutôt très rare). Masi rien ne l'empêche de changer si elle veut (couts en plus à prévoir et si le nom de son marie est plus vendeur)  
  *  C'est plutôt dans le cas d'un divorce où cela est plus fréquent. Le mari peut demander à se qu'elle ne porte plus son nom si elle c'est inscrite avec son nom marital ... Le juge peut exiger ou pas la conservation du nom marital si jugé impactant pour l'exercice de sa profession
*  La personne physique doit gérer tous ses établissements dans la même adresse (celle du siège)?
  * un siège comme je te l'ai dit est le lieu où notamment le tribunal de compétence devra être appelé en cas de litige. Ce siège peut être sur un établissement secondaire ou un établissement principal (à 95% le cas).
  * Pour les personnes Physique la notion de siège n'a pas d'importance car  en cas de litige c'est la personnes physique qui est assignée, qu'elle est ouverte un magasin à Paris ou à Marseille peu importe où elle se trouve c'est la personnes physique qui sera condamnée au tribunal de Marseille si le litige à lieux à Marseille.
  * Pour une Société il faut localisé le tribunal de compétence donc si le litige à lieu à Toulouse et que le siège est à Paris le plaignant devra saisir le tribunal de Paris.
*  Si une PP a plusieurs adresses dans la table, toutes actives, c'est que la personne a plusieurs établissements d'ouverts avec au moins un qui est le siège et un autre le principal (si différent du siège)
  *   oui mais l'établissement principal (il ne peut y en avoir qu'un) fait office de siège (même si cette notion de siège n'a pas d'importance pour les PP).
  *  il ne peut y en avoir qu'un si et seulement si c'est une activité identique bien sure
  * la pp peut ainsi avoir un siège/principal et plusieurs établissements secondaires partageants ou non les mêmes métiers
* si la pp a deux activités distinctes, il y a deux principals?
            Oui

### Echange avec INPI

Le contact de l'INPI est le suivant: Flament Lionel <lflament@inpi.fr>

* Forme juridique
  * L'insee a une variable categorieJuridiqueUniteLegale avec 260  possibilités (légèrement plus que la doc)
  * l'inpi il y a une variable forme_juridique qui contient 389 possibilités:
    * documentation: 261
    * FTP: 389
  *
Email INPI

```
J’ai ingéré l’ensemble des personnes morales du FTP jusqu’à fin 2019. Une variable a attiré mon attention, ‘forme_juridique’.
En récupérant les formes juridiques et leur code dans la documentation page 64, j’ai voulu faire correspondre le libellé et le code dans la base personne morale.
La documentation indique 261 formes juridiques, alors que les csv du FTP en ont 389. Je vous ai mis en pièce jointe l’Excel avec les match et non match (left_only = forme juridique documentation, right_only = FTP)
Ma question est la suivante :
Que faire des formes juridiques qui ne sont pas dans la documentation, et vice versa. En effet, l’INSEE a elle aussi 260 forme juridique.
```

Réponse:
```
Bonjour,
C’est effectivement un problème. Les greffes des tribunaux de commerce nous envoient les formes juridiques sous format libellé qui semble libre ; contrairement aux Tribunaux d’Instance et mixtes de commerce pour qui la forme juridique est codée selon la table de l’INSEE.
Pour info, nous avons mis en annexe de la documentation technique le tableau des formes juridiques afin de décoder les codes forme juridique des TI/TMC.
Cordialement
```

# Règles de gestion

# La donnée de l'INPI

Dans cette partie, nous regroupons l'ensemble des règles de gestion détecté ou défini à date.

Il faut savoir que la donnée brute provient du FTP de l'INPI qui est décomposé en deux branches. Une première branche contenant toutes les informations sur les créations ou modifications datant d'avant mai 2017. Une deuxième branche, quant a elle, regroupe les immatriculations et modifications qui se sont produites après mai 2017.

La première branche est appelée **stock** alors que la deuxième branche s'appelle **flux**.

## Description des fichiers de stock

A l’intérieur du répertoire public > IMR_Données_Saisies > tc > stock, les fichiers de stock sont organisés en fichiers zippés par greffe dans l’arborescence par année/mois/jour de la date de transmission.

Attention : Le stock initial des données TC étant constitué à la date du 4 mai 2017, il est impératif d’intégrer de manière incrémentale toutes les données de flux mises à disposition sur serveur dès l’ouverture de la licence IMR (1ères transmissions à compter du 18 mai 2017 contenant les informations des inscriptions enregistrées à partir du 5 mai 2017).

* Des stocks partiels constitués des dossiers complets relivrés à la demande de l’INPI après détection d’anomalies ?
*  Les fichiers des données contenues dans les nouvelles inscriptions (immatriculations, modifications et radiations) du Registre national du commerce et des sociétés ainsi que les informations relatives aux dépôts des actes et comptes annuels, telles que transmises par les greffes à compter du 5 mai 2017 (données de flux).

## Stocks initiaux & partiels

La description des fichiers de stocks (stocks initiaux et partiels) est similaire à celle des fichiers de flux cf. ci-après, avec quelques particularités :
* Le nombre de fichiers transmis pour chaque greffe est au nombre de 7,
* Ces fichiers contiennent toutes les informations relatives à l’identité de la personne morale ou physique, aux représentants, aux établissements, aux observations, aux actes et comptes annuels déposés, telles que générées à la date du 4 mai 2017 pour les tribunaux de commerce (personnes actives),
* La nomenclature des fichiers de stock reprend la nomenclature des fichiers de flux, avec, en guise de numéro de transmission, le numéro du stock ex. S1 (1 à n fichiers de stocks par greffe selon la volumétrie et selon la date de constitution). La numérotation est incrémentale.

![](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/-/raw/master/IMAGES/04_template_nom_csv_inpi.PNG)

## Description des fichiers de flux

Les fichiers transmis permettent d’exploiter les données de flux en provenance des greffes des tribunaux de commerce, et plus précisément :
* Constitution du dossier d’une personne morale ou personne physique (identifiée par son siren et son n° de gestion), dans le cas d’une 1ère immatriculation,
* Mise à jour des informations disponibles sur une personne morale ou personne physique en cas de modification ou de radiation.

Dans le cas d’une immatriculation (Personne morale ou Personne physique), le dossier est composé :
* A minima, d’informations sur l’identité de la personne (ex. date d’immatriculation, inscription principale ou secondaire, dénomination, nom, prénom, forme juridique, capital, activité principale etc.)
* Complété éventuellement par des informations relatives aux :
  * Représentants
  * Etablissements (établissement principal et/ou établissements secondaires)
  * Observations (incluant les procédures collectives, mentions de radiation etc.)
  * Dépôt des comptes annuels
  * Dépôt des actes
Les fichiers sont de 2 types :
* Fichiers créés à l’immatriculation d’un dossier
* Fichiers créés suite à un événement sur un dossier (modification, radiation)

## Immatriculation/ modification et radiation

De plus, il faut bien distinguer une immatriculation d'une modification. L'immatriculation représente la création d'une entreprise alors que la modification concerne le changement d'une information ou la radiation de la société, ou fermeture d'un établissement.

### Fichiers transmis à l'immatriculation d'un dossier

- **Immatriculation**: La création d'une entreprise requiert son immatriculation au Registre du Commerce et des Sociétés (RCS). Vous pouvez effectuer cette formalité d'immatriculation de votre société en ligne ou directement auprès du greffe du Tribunal de commerce compétent.

Dans le cas d’une immatriculation, 7 types de fichiers numérotés sont transmis :
* Immatriculations de personnes morales (1) ou physiques (3)
* Informations sur les représentants (5), **établissements (8)**, observations (11), actes (12) et comptes annuels (13)

La numérotation des fichiers et la mise en place d’un numéro de transmission incrémental permettent au licencié de s’assurer de la bonne intégration de l’ensemble des fichiers.

L’intégration des données doit en effet suivre impérativement l’ordre des numéros des fichiers (num_transmission).
Le nommage des fichiers est décrit ci-dessous :

![](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/-/raw/master/IMAGES/05_template_nom_csv_inpi.PNG)

### Fichiers transmis en cas de mise a jour d'un dossier (Evenement)

- **Modification**: Changement de gérant, de dénomination, modification relative au capital... les raisons de procéder à une rectification ou un complément des renseignements déclarés au Registre du Commerce et des Sociétés sont nombreuses.
- **Radiation**: La radiation d'un commerçant personne physique ou la radiation d'une société fait l'objet d'une déclaration auprès du greffe du Tribunal de commerce.
Le motif de la demande de radiation d'entreprise ou société peut être lié à une cessation d'activité, une dissolution, une fusion…
- **Fermeture**: L'établissement secondaire est un établissement distinct situé dans un ressort autre que celui du siège social ou de l'établissement principal. Sa fermeture entraîne une inscription modificative au RCS du lieu de sa situation. Lorsque l'établissement fermé constitue le seul établissement du ressort, la formalité correspondante entraîne une notification au greffe compétent au titre du siège social.

En cas de mise à jour d’un dossier suite à un événement (modification, radiation), les fichiers transmis ont une structure identique aux fichiers créés à l’immatriculation avec la présence de 2 champs spécifiques : la date de l’événement (Date_Greffe) et le libellé de l’événement (Libelle_Evt).

Dans ces cas, 6 types de fichiers supplémentaires, numérotés, sont transmis correspondant à :
* Evénements modifiant ou complétant les dossiers d’immatriculation des personnes morales (2) ou physiques (4)
* Evénements modifiant ou complétant les informations relatives aux représentants (6) ou aux **établissements (9)**
* Evénements supprimant des représentants (7 – Représentant partant) ou des **établissements (10 – Etablissement supprimé)**

![](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/-/raw/master/IMAGES/06_template_nom_csv_inpi.PNG)

Ci dessous, le tableau récapitulatif de chaque état dans la donnée de l'INPI relatif aux établissements.

- **timestamp**: Une date avec l'année + mois + jour + heure + minute + seconde

# Règles de gestion

## Plusieurs transmissions pour le même timestamp

*  Les événements doivent impérativement suivre l'ordre d'apparition dans le csv du FTP et de la numérotation des fichiers: 8, 9 et 10 pour les établissements
  *   Pour les événements, il est possible d'avoir plusieurs informations renseignées pour une même date de transmission pour une même séquence

Par exemple, il peut arriver qu'un établissement fasse l'object d'une modification (numérotation 9) et d'une suppression (numérotation 10) lors de la même transmission (timestamp). Dans ces cas la, il faut privilégier la suppression car est apparue après la modification (10 > 9).

PREUVE
31/03/2020

Qu'elle est la règle a appliqué lorsqu'un événement est effectué le même jour, à la même heure pour un même établissement (même quadruplet: siren + code greffe + numéro gestion + ID établissement), mais avec des modifications pas forcément identique.
-> Il faut modifier, en respectant l’ordre des lignes, uniquement les champs où il y a une valeur.

EXEMPLE

## Définition partiel

* si csv dans le dossier Stock, année > 2017, alors partiel, c'est a dire, modification complète du dossier due a une anomalie.
* La date d’ingestion est indiquée dans le path, ie comme les flux

PREUVE
26/03/2020

1/ Est-ce que les csv dans le dossier Stock pour une date supérieure à 2017 peuvent être labélisés comme « partiel » rendant ainsi caduque toutes les valeurs précédentes d’un établissement ?
-> OUI

EXEMPLE

## Definition séquence

*   Une séquence est un classement chronologique pour le quadruplet suivant:
    *   _siren_ + _code greffe_ + _numero gestion_ + _ID établissement_

PREUVE
26/03/2020

2/ Pour identifier un établissement, il faut bien choisir la triplette siren + numero dossier (greffer) + ID établissement ?
-> il faut choisir le quadruplet siren + code greffe + numero gestion + ID établissement

EXEMPLE

## Remplissage des valeurs incrémentale

*   Le remplissage doit se faire de la manière suivante pour la donnée brute
        *   Pour une date de transmission donnée, c'est la dernière ligne de la séquence qui doit être utilisée, remplie des valeurs manquantes extraites des lignes précédentes. Si la dernière ligne de la séquence contient un champs non vide, il ne faut pas la remplacer par la ligne précédente.

PREUVE

Donc si je comprends bien, disons si il y a pour un même quadruplet 3 événements à la même date

-	Ligne 1
-	Ligne 2
-	Ligne 3

Pour avoir l’ensemble des informations, il faut que je complète la ligne 2 avec les éléments de la ligne 1, puis la ligne 3 avec les nouveaux éléments de la ligne 2.

Au final, la ligne 3 va avoir les informations de la ligne 1 & 2, et donc c’est elle qui va faire foi (ligne 1 et 2 devenant caduque)
-> Oui, il faut additionner les champs modifiés. Attention si un même champ est modifié (avec des valeurs différentes) en ligne 2, puis en ligne 3, il faudra privilégier les valeurs de la ligne 3.

EXEMPLE

## Creation status partiel

- En cas de corrections majeures, la séquence annule et remplace la création et événements antérieurs. Dans ce cas, toutes les données qui ont pu être transmises antérieurement via le stock initial ou le flux doivent donc être ignorées (prendre en compte la date de transmission indiquée dans le nom des sous-répertoires du stock et des fichiers

PREUVE
26/03/2020 OK
1/ Est-ce que les csv dans le dossier Stock pour une date supérieure à 2017 peuvent être labélisés comme « partiel » rendant ainsi caduque toutes les valeurs précédentes d’un établissement ?
-> OUI

EXEMPLE

## Plusieurs créations pour une même séquence

*  Une création d'une séquence peut avoir plusieurs transmission a des intervalles plus ou moins long
    *   Si plusieurs transmissions avec le libellé “création établissement” ou “création", alors il faut prendre la dernière date de transmission

PREUVE
02/06/2020

Je reviens vers vous concernant les « doublons » sur les créations. En dessous, j’ai indiqué le nombre de lignes contenant des doublons sur la séquence siren,code_greffe,numero_gestion,id_etablissement. Autrement dit, il y a 454070 lignes de créations d’établissement avec 2 lignes. 261163 lignes avec 3 doublons etc. Cela impacte environ 15/20% de la donnée des créations dans le FTP.

La regle que nous avions parlé est : si doublon au niveau de la création (même séquence avec le libellé « création ») mais des dates de transmission différentes, alors prendre la plus récente.

Je n’ai pas encore vérifié si les doublons avaient des lignes identiques. Je souhaitais simplement m’assurer que la règle était la bonne, et que je faisais correctement ce retraitement.
->Oui, c’est la règle que nous appliquons.

## Plusieurs transmissions pour une date de greffe donnée

- Si, pour une date de greffe donnée, il y a plusieurs dates de transmission non identique, alors il faut enrichir les événements comme défini précédemment et ne récupérer que la date de transmission la plus récente. Autrement dit, il faut toujours garder une seule ligne pour chaque date de greffe, qui est la date de transmission la plus récente.

PREUVE
J’ai une autre question concernant les transmissions a des dates différentes pour une même date de greffe.

On sait qu’il faut enrichir les séquences (siren/code greffe/ numéro gestion/id établissement) de manière incrémentale pour les transmissions, mais aussi en prenant les valeurs t-1 pour remplir les champs manquants.

Maintenant, il y a de nombreux cas ou la date de greffe reste la même, mais contenant plusieurs dates de transmission. Pouvons-nous définir la règle suivante

-	Si une même date de greffe a plusieurs transmissions, devons-nous prendre la dernière ligne que nous avons enrichi?
-> OUI

Par ailleurs, comment se fait-il que des transmissions pour une même date de greffe ont plusieurs mois de décalage ?
-> Nous ne savons pas, nous diffusons ce qu’infogreffe nous envoit

EXEMPLE

## Faux événements

*   Il y a certains cas ou les lignes de créations doublons sont de faux événements (mauvais envoie de la part du greffier)
    *   Si le timestamp entre la première ligne et dernière ligne est supérieures a 31 jour (exclut), il faut:
            *   Récupération de la dernière ligne, et créer une variable flag, comme pour le statut

PREUVE

03/06/2020

Je reviens vers vous de nouveau pour les doublons lors des créations. J’ai remarqué que pas tous les doublons sont identiques. Certains ont mêmes des différences significatives.

Je vous ai mis en pièce jointe un fichier Excel avec quelques siren impactés.  Pour info, les siren proviennent du FTP nouveau année 2017 et la feuille numéro 2 contient un siren dont la différence provient du ftp nouveau 2017 et 2018

Comment avez-vous traité ce genre de cas de figure ?
-> il faut toujours privilégier la dernière information reçue.

Je viens de trouver un SIREN qui comporte pas mal de problème. Voici le SIREN en question 301852026

Dans le fichier Excel appelé 301852026.xlsx (en pièce jointe), il y a un établissement créé le 01/09/2017 (lignes en jaune, onglet NEW_2017), dont toutes les lignes de l’adresse et nom_commercial sont quasiment non identique. Si je prends la dernière ligne, je n’ai plus le nom commercial, car figure dans la première ligne. De plus cet établissement a un événement. Etant donné que la dernière ligne n’est pas renseigné, je ne peux pas non plus le remplir

Quand je compile l’ensemble de la donnée (onglet Full_sample), je ne vois pas de partiel associé. Au contraire, on voit que l’établissement a eu une modification de l’adresse (ligne en rouge)

Quand je regarde le moteur de recherche de l’inpi https://data.inpi.fr/entreprises/301852026#301852026 il y a bien l’établissement mais pas d’adresse d’indiquée.

-> Il y a eu effectivement une modification d’adresse (l’évènement aurait du être « modification relative à un établissement »). Par contre, pour le nom commercial, je pense qu’il s’agit d’une erreur.
Pour preuve la formalité qui nous a été envoyée.
Si vous prenez donc la dernière ligne vous devriez être conforme à cette formalité.
Concernant l’adresse manquante sur DATAINPI, il s’agit d’une anomalie qui va être corrigée prochainement.

J’ai regardé un a un les csv du FTP avec ce siren 301852026 et le nom_commercial est bien manquant lors du dernier envoie. Le détail est dans l’excel, onglet FTP. Les lignes en rouge. (la feuille contient un filtre sur le siren en question).

Du coup, j’ai pensé a la règle a appliqué en cas d’anomalie de ce genre :

Si création contient plusieurs lignes avec des valeurs différentes, alors remplir de manière incrémentale champs vide (en prenant le n-1 de la séquence siren + code greffe + numero gestion + ID etablissement + libelle evenement = Etablissement ouvert

Comme ca, la dernière ligne est toujours celle a retourner, mais on peut remplir les champs manquants.

Est-ce que vous confirmez ?

-> Pouvez-vous m’appeler ?

EXEMPLE
- Validation de la règle lors de l'appel


## Creation flag pas siege ou principal

-  Siren sans Siège ou Principal
  - Il est possible qu'un SIREN n'ai pas de siege/principal. Normalement, cela doit être corrigé par un partiel

## Creation flag pas de création

PREUVE


-  Etablissement sans création
  - Il arrive que des établissements soient supprimés (EVT) mais n'ont pas de ligne "création d'entreprise". Si cela, arrive, Infogreffe doit envoyer un partiel pour corriger. Il arrive que le greffe envoie seulement une ligne pour SEP, lorsque le Principal est fermé, le siège est toujours ouvert. Mais pas de nouvelle ligne dans la base. Le partiel devrait corriger cela.

PREUVE
15/04/2020
-	Il y a des Siren dont l’établissement est supprimé (EVT) mais qui n’ont pas de ligne "création d’entreprise". Est-ce normal?
-	Est-il possible d'avoir des établissements figurants a l'INSEE mais qui ne sont pas référencés à l'INPI (hors établissement ayant formulé un droit à l'oubli)
-> Pouvez-vous m’appeler, ce sera plus simple pour comprendre ?

EXEMPLE

## Autres règles

- La variable `ville` de l'INPI n'est pas normalisée. C'est une variable libre de la créativité du greffier, qui doit être formalisée du mieux possible afin de permettre la jointure avec l'INSEE. Plusieurs règles regex ont été recensé comme la soustraction des numéros, caractères spéciaux, parenthèses, etc. Il est possible d'améliorer les règles si nécessaire
- Le code postal doit être formalisé correctement, a savoir deux longueurs possibles: zero (Null) ou cinq. Dans certains cas, le code postal se trouve dans la variable de la ville.
- La variable pays doit être formalisée, a savoir correspondre au code pays de l'INSEE. Bien que la majeure partie des valeurs soit FRANCE ou France, il convient de normaliser la variable pour récuperer les informations des pays hors France.
- Les variables de l'adresse de l'INPI ne sont pas normalisées, et ne peuvent être utilisées en l'état. Il est donc indispensable de retravailler les variables adresse pour pouvoir les comparer avec l'INSEE. Nous utilisons une règle (pattern) regex pour vérifier si les mots contenus dans l'adresse de l'INPI sont aussi contenus à l'INSEE.
- [NEW] L'INSEE codifie le type de voie de la manière suivante:
    - Si le type de voie est d'une longueur inférieure ou égale à 4 caractères, le type de voie n'est pas abrégé. Ainsi, RUE ou QUAI sont écrits tels quels, alors que AVENUE est abrégée en AV.
    - La codification de l'INSEE va donc être utilisé ppur créer la variable `voie_matching`
- [NEW] Pour ne pas complexifié le processus de siretisation, seule le premier numéro de voie contenu dans l'adresse nettoyée est extraite pour ensuite etre matché avec l'INSEE.

## Siren vide

06/04/2020

J’ai remarqué qu’il y a des lignes dans les csv qui n’ont pas de SIREN, alors que la documentation indique que c’est une valeur obligatoire.

Est-ce que les erreurs comme ça sont corrigées par la suite avec des stock partiels ?

En pièce jointe, un exemple de csv avec des siren manquant ex 0602_1310_20191221_091331_8_ets.csv

-> Ce n’est pas normal. Normalement le siren est obligatoire. Vous constaterez que le dossier de votre exemple a été renvoyé le 24/12/2019 avec, cette fois ci, le siren. Quand bien même il n’aurait pas été renvoyé, tout dossier sans siren est rejeté chez nous, il aurait donc été demandé dans un stock partiel ultérieurement.

Sur 2017-2019, seulement 228 sont dans ce cas. La liste en pièce jointe.

Comme j’ai compris, il faut utiliser les stocks partiels présents ou a venir pour avoir le siren.
-> Oui ou le flux parce qu’il peut être transmis avec le siren dans un flux ultérieur.


## CSV vides

08/04/2020
Je vous contacte à propos des csv flux de 2019. Il y a des csv vides en provenance du FTP. Je me demandais si c’était normal.
-> Oui, cela peut arriver.

## numérotation id établissement

23/06/2020
J’ai une nouvelle question concernant les établissements :

Pourquoi il y  a des établissements dont l’ID_etablissement est égal à 0 ? Devons-nous les supprimés ?

Par exemple, en pièce jointe, je vous ai joint un siren 818153637 qui a un id établissement a 0, mais sans création.  Le siège, ensuite, on a un autre établissement (1) qui devient le SEP.

Le moteur de recherche de l’INPI n’indique pas l’adresse complète donc je ne peux pas vérifier.

https://data.inpi.fr/entreprises/818153637#818153637

-> C’est possible qu’il y ait des ID = 0.
En ce qui concerne votre exemple, il est ancien et je vois que nous l’avions demandé dans un stock correctif.
Pour l’adresse incomplète, il s’agit d’un bug, il sera corrigé prochainement.

### Email INPI

Thomas
-> INPI

 26/03/2020 OK
1/ Est-ce que les csv dans le dossier Stock pour une date supérieure à 2017 peuvent être labélisés comme « partiel » rendant ainsi caduque toutes les valeurs précédentes d’un établissement ?
-> OUI

2/ Pour identifier un établissement, il faut bien choisir la triplette siren + numero dossier (greffer) + ID établissement ?
-> il faut choisir le quadruplet siren + code greffe + numero gestion + ID établissement

31/03/2020 OK

Qu'elle est la règle a appliqué lorsqu'un événement est effectué le même jour, à la même heure pour un même établissement (même quadruplet: siren + code greffe + numéro gestion + ID établissement), mais avec des modifications pas forcément identique.
-> Il faut modifier, en respectant l’ordre des lignes, uniquement les champs où il y a une valeur.

Donc si je comprends bien, disons si il y a pour un même quadruplet 3 événements à la même date

-	Ligne 1
-	Ligne 2
-	Ligne 3

Pour avoir l’ensemble des informations, il faut que je complète la ligne 2 avec les éléments de la ligne 1, puis la ligne 3 avec les nouveaux éléments de la ligne 2.

Au final, la ligne 3 va avoir les informations de la ligne 1 & 2, et donc c’est elle qui va faire foi (ligne 1 et 2 devenant caduque)
-> Oui, il faut additionner les champs modifiés. Attention si un même champ est modifié (avec des valeurs différentes) en ligne 2, puis en ligne 3, il faudra privilégier les valeurs de la ligne 3.

06/04/2020 OK

J’ai remarqué qu’il y a des lignes dans les csv qui n’ont pas de SIREN, alors que la documentation indique que c’est une valeur obligatoire.

Est-ce que les erreurs comme ça sont corrigées par la suite avec des stock partiels ?

En pièce jointe, un exemple de csv avec des siren manquant ex 0602_1310_20191221_091331_8_ets.csv

-> Ce n’est pas normal. Normalement le siren est obligatoire. Vous constaterez que le dossier de votre exemple a été renvoyé le 24/12/2019 avec, cette fois ci, le siren. Quand bien même il n’aurait pas été renvoyé, tout dossier sans siren est rejeté chez nous, il aurait donc été demandé dans un stock partiel ultérieurement.

Sur 2017-2019, seulement 228 sont dans ce cas. La liste en pièce jointe.

Comme j’ai compris, il faut utiliser les stocks partiels présents ou a venir pour avoir le siren.
-> Oui ou le flux parce qu’il peut être transmis avec le siren dans un flux ultérieur.

08/04/2020 OK
Je vous contacte à propos des csv flux de 2019. Il y a des csv vides en provenance du FTP. Je me demandais si c’était normal.
-> Oui, cela peut arriver.

15/04/2020 OK
o	Il y a des Siren dont l’établissement est supprimé (EVT) mais qui n’ont pas de ligne "création d’entreprise". Est-ce normal?
o	Est-il possible d'avoir des établissements figurants a l'INSEE mais qui ne sont pas référencés à l'INPI (hors établissement ayant formulé un droit à l'oubli)
-> Pouvez-vous m’appeler, ce sera plus simple pour comprendre ?

25/05/2020
J’ai ingéré l’ensemble des personnes morales du FTP jusqu’à fin 2019. Une variable a attiré mon attention, ‘forme_juridique’.

En récupérant les formes juridiques et leur code dans la documentation page 64, j’ai voulu faire correspondre le libellé et le code dans la base personne morale.

La documentation indique 261 formes juridiques, alors que les csv du FTP en ont 389. Je vous ai mis en pièce jointe l’Excel avec les match et non match (left_only = forme juridique documentation, right_only = FTP)

Ma question est la suivante :

Que faire des formes juridiques qui ne sont pas dans la documentation, et vice versa. En effet, l’INSEE a elle aussi 260 forme juridique.
-> C’est effectivement un problème. Les greffes des tribunaux de commerce nous envoient les formes juridiques sous format libellé qui semble libre ; contrairement aux Tribunaux d’Instance et mixtes de commerce pour qui la forme juridique est codée selon la table de l’INSEE.
Pour info, nous avons mis en annexe de la documentation technique le tableau des formes juridiques afin de décoder les codes forme juridique des TI/TMC.

Merci pour votre réponse. J’ai utilisé la table en annexe, qui est identique en tout point à celle de l’INSEE.

Je n’ai pas encore regardé le nombre d’observations qui ne rentre pas dans cette table. Si le nombre est trop important, nous allons devoir faire une table de correspondance intermédiaire. Par exemple, la forme Société agricole forestière n’a pas de correspondance dans le tableau en annexe. De fait, il n’y a pas de numéro. Avoir une table de correspondance avec un niveau de granularité plus fin serait très appréciée.

Auriez-vous par tout hasard ce genre de table de correspondance (pour les 129 manquants-389-260)? Si non, nous allons en faire une nous-même.

-> Désolé, nous ne disposons de ce type de table. Nous diffusons ce que les greffes nous communiquent.

26/05/2020
J’ai regardé les siren dans la table des personnes morales qui étaient présents plus d’une fois, et le siren suivant a attiré mon attention : 843902552

https://data.inpi.fr/entreprises/843902552#843902552

Le siren 843902552 apparait 4 fois dans la table établissement, 2 fois en 2018 et 2 fois en 2019. Il y a deux établissements différents. Toutefois, ils sont indiqués en tant que « création ». Comment cela est-il possible, ça n’aurait pas dû être un partiel pour 2019 ?

Lorsque ce genre de situation arrive, que faut-il faire ?

Maintenant, ce même siren est présent 3 fois dans la table personne morale. 2 fois pour une création 2018 + 2019 et une fois en événement.

Là encore, comment est-ce possible d’avoir deux créations pour un même siren dans les personnes morales ? La forme juridique est rattachée aux  établissements ? Si oui, alors comment savoir l’établissement impacté par un événement. Ex pour le siren  843902552  pm ligne événement

Je vous ai mis en pièce jointe, les lignes correspondantes a ce siren pour ets et pm
-> Pouvez-vous m’appeler que je vous explique ce sera plus simple ?

02/06/2020 OK
Je reviens vers vous concernant les « doublons » sur les créations. En dessous, j’ai indiqué le nombre de lignes contenant des doublons sur la séquence siren,code_greffe,numero_gestion,id_etablissement. Autrement dit, il y a 454070 lignes de créations d’établissement avec 2 lignes. 261163 lignes avec 3 doublons etc. Cela impacte environ 15/20% de la donnée des créations dans le FTP.

La regle que nous avions parlé est : si doublon au niveau de la création (même séquence avec le libellé « création ») mais des dates de transmission différentes, alors prendre la plus récente.

Je n’ai pas encore vérifié si les doublons avaient des lignes identiques. Je souhaitais simplement m’assurer que la règle était la bonne, et que je faisais correctement ce retraitement.
->Oui, c’est la règle que nous appliquons.

03/06/2020 OK
Je reviens vers vous de nouveau pour les doublons lors des créations. J’ai remarqué que pas tous les doublons sont identiques. Certains ont mêmes des différences significatives.

Je vous ai mis en pièce jointe un fichier Excel avec quelques siren impactés.  Pour info, les siren proviennent du FTP nouveau année 2017 et la feuille numéro 2 contient un siren dont la différence provient du ftp nouveau 2017 et 2018

Comment avez-vous traité ce genre de cas de figure ?
-> il faut toujours privilégier la dernière information reçue.

Je viens de trouver un SIREN qui comporte pas mal de problème. Voici le SIREN en question 301852026

Dans le fichier Excel appelé 301852026.xlsx (en pièce jointe), il y a un établissement créé le 01/09/2017 (lignes en jaune, onglet NEW_2017), dont toutes les lignes de l’adresse et nom_commercial sont quasiment non identique. Si je prends la dernière ligne, je n’ai plus le nom commercial, car figure dans la première ligne. De plus cet établissement a un événement. Etant donné que la dernière ligne n’est pas renseigné, je ne peux pas non plus le remplir

Quand je compile l’ensemble de la donnée (ongle Full_sample), je ne vois pas de partiel associé. Au contraire, on voit que l’établissement a eu une modification de l’adresse (ligne en rouge)

Quand je regarde le moteur de recherche de l’inpi https://data.inpi.fr/entreprises/301852026#301852026 il y a bien l’établissement mais pas d’adresse d’indiquée.

-> Il y a eu effectivement une modification d’adresse (l’évènement aurait du être « modification relative à un établissement »). Par contre, pour le nom commercial, je pense qu’il s’agit d’une erreur.
Pour preuve la formalité qui nous a été envoyée.
Si vous prenez donc la dernière ligne vous devriez être conforme à cette formalité.
Concernant l’adresse manquante sur DATAINPI, il s’agit d’une anomalie qui va être corrigée prochainement.

J’ai regardé un a un les csv du FTP avec ce siren 301852026 et le nom_commercial est bien manquant lors du dernier envoie. Le détail est dans l’excel, onglet FTP. Les lignes en rouge. (la feuille contient un filtre sur le siren en question).

Du coup, j’ai pensé a la règle a appliqué en cas d’anomalie de ce genre :

Si création contient plusieurs lignes avec des valeurs différentes, alors remplir de manière incrémentale champs vide (en prenant le n-1 de la séquence siren + code greffe + numero gestion + ID etablissement + libelle evenement = Etablissement ouvert

Comme ca, la dernière ligne est toujours celle a retourner, mais on peut remplir les champs manquants.

Est-ce que vous confirmez ?

-> Pouvez-vous m’appeler ?

17/06/2020
Je suis actuellement en train de comprendre la relation entre les Unités légales (Personne Physique/Morale) et les établissements.

J’ai compris qu’une entreprise (au sens statistique) est soit une personne physique soit une personne morale. Une entreprise peut avoir plusieurs établissements.

J’arrive très bien à saisir qu’une entreprise (personne morale) peut avoir plusieurs établissements. Toutefois, une personne physique, quant à elle, ne peut avoir plusieurs établissements ? En somme, la personne physique ne peut avoir d’établissement autre que le siège/principal car ne peut posséder qu’une seule adresse.

A vrai dire, une personne morale a une relation 1:n avec les établissements, alors que la personne physique ne peut avoir qu’une relation 1 :1 avec les établissements. Le 1 :1 étant le dernier établissement dans la base ets.

Au cours de la vie d’une personne physique, cette dernière peut avoir plusieurs établissements en cas de déménagement, mais seulement un seul actif ?

Est-ce que m’a compréhension est correcte ?

-> Non, une personne physique peut avoir plusieurs établissements (cf cerfa en pièce jointe cadre 10).

Donc comme je le comprends la pp peut ainsi avoir un siège/principal et plusieurs établissements secondaires partageants ou non les mêmes métiers
-> Oui

23/06/2020
J’ai une nouvelle question concernant les établissements :

Pourquoi il y  a des établissements dont l’ID_etablissement est égal à 0 ? Devons-nous les supprimés ?

Par exemple, en pièce jointe, je vous ai joint un siren 818153637 qui a un id établissement a 0, mais sans création.  Le siège, ensuite, on a un autre établissement (1) qui devient le SEP.

Le moteur de recherche de l’INPI n’indique pas l’adresse complète donc je ne peux pas vérifier.

https://data.inpi.fr/entreprises/818153637#818153637

-> C’est possible qu’il y ait des ID = 0.
En ce qui concerne votre exemple, il est ancien et je vois que nous l’avions demandé dans un stock correctif.
Pour l’adresse incomplète, il s’agit d’un bug, il sera corrigé prochainement.

23/06/2020 OK
J’ai une autre question concernant les transmissions a des dates différentes pour une même date de greffe.

On sait qu’il faut enrichir les séquences (siren/code greffe/ numéro gestion/id établissement) de manière incrémentale pour les transmissions, mais aussi en prenant les valeurs t-1 pour remplir les champs manquants.

Maintenant, il y a de nombreux cas ou la date de greffe reste la même, mais contenant plusieurs dates de transmission. Pouvons-nous définir la règle suivante

-	Si une même date de greffe a plusieurs transmissions, devons-nous prendre la dernière ligne que nous avons enrichi?
-> OUI

Par ailleurs, comment se fait-il que des transmissions pour une même date de greffe ont plusieurs mois de décalage ?
-> Nous ne savons pas, nous diffusons ce qu’infogreffe nous envoit

En pièce jointe, un exemple avec le siren 300392057, ou potentiellement, on pourrait garder la ligne 8. A noter que le fichier excel est pour les pp, mais identique pour les etb.

24/06/2020
Je viens de tomber sur le siren suivant : 300186947 qui contient une valeur « impossible » dans le champs « date_radiation » Le greffier a marqué « déradiation », a défaut de mettre une date

Comment traitez vous ce genre de cas de figure, a savoir les valeurs qui ne respectent pas les champs et les déradiations
En pièce jointe, le csv source

-> C’est tout à fait normal, c’est indiqué dans la doc technique :



Dans ce cas, il faut supprimer la date de radiation que vous avez en base.

01/07/2020
Dans le cadre de la documentation des règles de gestion et compréhension de la table établissement, je me suis penché sur le libellé des evt.

Dans la documentation page  29 et 42, il est indiqué que le libellé d’evt pour la table établissement est Etablissement supprimé ou Modifications relatives à un établissement.

Comment se fait-il que nous avons aussi le libellé Modifications relatives au dossier.
Dans quelle mesure nous pouvons avoir ce libellé ?


AAMMJJ_HHMMSS -> timestamp, 9 -> Modification ets

# Points attention

Dans cette section, nous allons indiquer tous les siren qui ont fait l'objet d'un flag. Cela va servir a se constituer une bibliothèque de siren qui vont ensuite être transformer en point d'attention. L'idée principal est de détecter des problèmes sur les règles de gestion actuels ou bien des anomalies dans la donnée de l'INPI, voir de l'INSEE. Ce travail de construction de la bibliothèque de règle de gestion va faciliter le travail a venir des data analyste/scientist car il y aura a disposition, un ensemble de point d'attention dans lequel il faudra faire attention.

# Problèmes dans la data

Pour indiquer un siren, il faut utiliser le template suivant:

* Titre point attention: Indiquer un titre si le siren permet de déboucher sur un point d'attention ou une règle de gestion. Exemple de titre: Plusieurs PRI par SIREN
*  Base de donnée
  * INPI: Utiliser le site https://data.inpi.fr/ et indiquer le siren. Exemple -> https://data.inpi.fr/entreprises/400571824#400571824
  * INSEE: Utliser le site http://avis-situation-sirene.insee.fr/jsp/avis-formulaire.jsp et indiquer l'établissement en question. Exemple -> http://avis-situation-sirene.insee.fr/ListeSiretToEtab.action?form.siren=317253433&form.nic=00054

# Etablissement avec plusieurs principaux

## Description problème

Il y a des établissements qui ont plusieurs principaux qui ont été créé le même jour

### Exemples

* Metadata
  *  Resolu: NON
  * Sequence:
    * siren: 302666110
    * code_greffe : 3102
    * numero_gestion : 1978B00486
    * id_etablisement : 3 / 4  
  * US: [XX](https://tree.taiga.io/project/olivierlubet-air/us/)
* Base de donnée
  *  INPI: https://data.inpi.fr/entreprises/400571824#400571824
  * INSEE: http://avis-situation-sirene.insee.fr/ListeSiretToEtab.action?form.siren=302666110&form.nic=00036
  *  Datum:
  * Base de donnée: inpi
  *  Table: ets_final_sql
  * Query AWS

## Description problème

* L’entreprise a deux principaux et un siège
* Un seul établissement à l’INSEE

# Etablissements avec plusieurs ID mais même ETS

## Description problème

Il y plusieurs id établissements lorsque l’établissement est à la fois siège et principal et que le greffier a créé plusieurs lignes. Cela crée artificiellement plusieurs établissements. De plus, cela créer un problème avec une des règles de gestion (Etablissements) qui est de ne garder que la dernière ligne pour une date de greffe donnée mais avec plusieurs dates de transmission. On fait la récupération par séquence, de fait, cela traite les sièges et principaux différemment → comme donné par l’INPI.

### Exemples

* Exemples
* Metadata
  *  Resolu: NON
  * Sequence:
    * siren: 317253433
    * code_greffe : 2701 / 7501 / 7801
    * numero_gestion : 2018D00043 /  1988D02121  / 1980D00011
    * id_etablisement : 0/1/2/3/10
  * US: [XX](https://tree.taiga.io/project/olivierlubet-air/us/)
* Base de donnée
  *  INPI: https://data.inpi.fr/entreprises/317253433#317253433
  * INSEE: http://avis-situation-sirene.insee.fr/ListeSiretToEtab.action?form.siren=317253433&form.nic=00054
  *  Datum:
  * Base de donnée: inpi
  *  Table: ets_final_sql
  * Query AWS
* Description problème
*  L’entreprise a 3 sièges
  * Il y a deux sièges à la même adresse
    * le moteur de recherche de l’INPI indique 2 sièges et 2 principal
  * Il y a un autre siège à une adresse différente
  * Il y a un secondaire qui n’est pas présent à l’INSEE
  * Le siège à l’INSEE est à une adresse différente de l’INPI
  * Aucune information sur les radiations à l’INPI

# SIE et PRI deux lignes pas toutes remplies

## Description problème

On sait qu’un établissement peut être un siège et un principal à la fois. Lorsque cela arrive, il est donc indispensable que tous les champs soient remplis. Toutefois, il arrive dans certains cas ou les champs ne sont pas entièrement remplis
### Exemples

* Metadata
  *  Resolu: OUI/NON
  * Sequence:
    * siren:  437864820
    * code_greffe : 7301
    * numero_gestion : 2001D00111
    * id_etablisement : 0 / 10
  * US: [XX](https://tree.taiga.io/project/olivierlubet-air/us/)
* Base de donnée
  *  INPI: https://data.inpi.fr/entreprises/437864820#437864820
  * INSEE: http://avis-situation-sirene.insee.fr/ListeSiretToEtab.action?form.siren=437864820&form.nic=00018
  *  Datum:
  * Base de donnée: inpi
  *  Table: ets_final_sql
  * Query
    * INPI
    * INSEE

## Description problème

* Prenons l’exemple du siren suivant 437864820 , lors de la création le greffier a indiqué deux lignes, une pour le siège, et une autre pour le principal. Il aurait donc du remplir à l’identique chaque champ, mais cela n’a pas été fait. De plus, l’INPI indique comme nous l’avons déjà mis en avant, deux établissements (alors que ce n’est qu’un seul).
  * Comment faire dans ces cas la pour remplir les informations manquantes.
* Le siège est indiqué comme radié à l’INPI dans le moteur de recherche
* L’établissement est fermé à l’INSEE


# Manque information Principal INSEE

## Description problème

 Est ce que la variable etablissementSiege à l’INSEE fait référence au siège uniquement ou au siège et principal (OUI). En effet, l’INSEE, dans son moteur de recherche précise Etablissement siège ou établissement principal  mais ne mentionne pas dans les champs si c’est l’un ou l’autre, uniquement siège.

 Il peut arriver qu’à l’INPI, il n’y a qu’une seule ligne → 1 séquence, mais correspond a plusieurs lignes a l’INSEE. C’est le cas lorsque l’adresse est la même. Toutefois, il est très difficile de distinguer lequel est le bon dans la mesure ou l’information contenue dans la variable etablissementSiege et type n’est pas assez précise.  On pourrait regarder si la base contient des informations sur les événements, mais lorsqu’il s’agit d’un principal, il est très difficile de pouvoir faire correspondre avec exactitude le SIREN-SIRE

### Exemples

* Metadata
  *  Resolu: OUI/NON
  * Sequence:
    * siren: 385025655
    * code_greffe : 3402
    * numero_gestion : 1994A00130
    * id_etablisement : 1 / 2 / 3
  * US: [XX](https://tree.taiga.io/project/olivierlubet-air/us/)
* Base de donnée
  *  INPI:
  * INSEE:
  *  Datum:
  * Base de donnée: inpi
  *  Table: ets_final_sql
  * Query:
    * INPI
    * INSEE

## Description problème

*  le siren 385025655 n’a pas de siège dans la base INPI. Le principal est situé au 2 rue de la Cave. L’INSEE nous informe qu’a cette adresse, il y a eu un établissement fermé, puis un autre établissement a cette même adresse qui est le siège ou principal. A l’INPI, aucune fermeture pour cette adresse.

# Pas de radiation dans FTP

## Description problème

Il y a des établissements dont nous n’avons pas d’information sur les radiations

### Exemples

* Metadata
  *  Resolu: OUI/NON
  * Sequence:
    * siren: 819247461
    * code_greffe : 5001
    * numero_gestion : 2018B00319
    * id_etablisement : 1
  * US: [XX](https://tree.taiga.io/project/olivierlubet-air/us/)
* Base de donnée
  * INPI: https://data.inpi.fr/entreprises/819247461#819247461
  * INSEE:
    * SIE ou PRI: http://avis-situation-sirene.insee.fr/ListeSiretToEtab.action?form.siren=819247461&form.nic=00026
    * SEC: http://avis-situation-sirene.insee.fr/ListeSiretToEtab.action?form.siren=819247461&form.nic=00018
  *  Datum:
  * Base de donnée: inpi
  *  Table: ets_final_sql
  * Query AWS

## Description problème

*  Lors de la sirétisation, le siren 819247461 a été matché avec le bon siret mais l’établissement est référencé en tant que SEC à l’INPI alors que l’INSEE n’indique jamais de secondaire pour cet établissement. Le moteur de recherche de l’INPI non plus. Le FTP a bien la valeur SEC pour le csv 5001_429_20190101_090709_8_ets.csv . Toutefois, le SEC a été convertit en SEP via des événements suivants. Le problème, c’est que l’établissement domicilié au 25 Bis rue de l'Abreuvoir ne semble pas avoir d’information sur ça radiation

# Un etablissement  INPI plusieurs ETS INSEE

## Description problème

### Exemples

* Metadata
  *  Resolu: OUI/NON
  * Sequence:
    * siren:
    * code_greffe :
    * numero_gestion :
    * id_etablisement :
  * US: [XX](https://tree.taiga.io/project/olivierlubet-air/us/)
* Base de donnée
  *  INPI:
  * INSEE:
  *  Datum:
  * Base de donnée: inpi
  *  Table: ets_final_sql
  * Query AWS

## Description problème

* Le siren 385025655 n’a pas de siège dans la base INPI. Le principal est situé au 2 rue de la Cave. L’INSEE nous informe qu’a cette adresse, il y a eu un établissement fermé, puis un autre établissement a cette même adresse qui est le siège ou principal. A l’INPI, aucune fermeture pour cette adresse.
* A l’INSEE, a l’adresse  2 rue de la Cave, il y a un établissement non siège fermé, et un siège actif. Toutefois, l’INSEE indique Adresse de l'entreprise et étab. princ. pour ce siren 38502565500056
* Le moteur de recherche de l’inpi n’indique plus les mêmes informations
* L’établissement Du clos n’est pas présent à l’INPI

# La dateCreationEtablissement INSEE différent du moteur de recherche

## Description problème

Il est possible que la date de dateCreationEtablissement de l’INSEE ne corresponde pas aux informations fournit par le site internet
### Exemples

* Metadata
  *  Resolu: NON
  * Sequence:
    * siren: 437865439
    * code_greffe : 2701
    * numero_gestion : 2019A00260
    * id_etablisement : 1
  * US: [XX](https://tree.taiga.io/project/olivierlubet-air/us/)
* Base de donnée
  *  INPI: https://data.inpi.fr/entreprises/437865439#437865439
  * INSEE: http://avis-situation-sirene.insee.fr/ListeSiretToEtab.action?form.siren=437865439&form.nic=00032
  *  Datum:
  * Base de donnée: inpi
  *  Table: ets_final_sql
  * Query
    * INPI
    * INSEE

## Description problème

* Le siren 437865439 a un établissement (SIE) domicilié au 54 A RUE SAINT VINCENT , actif depuis le 04/11/2019 hors, la donnée de l’INSEE indique une date de création datant du  2012-01-01
* A l’INPI, date de création 2019-11-04
* L’INPI ne possède pas tous les établissements du siren

# SIE ou PRI à l’INPI mais SEC à l’INSEE

## Description problème

Il y a, dans de nombreuses fois, une différence de type d’établissement entre l’INPI et l’INSEE. En effet, il y a des établissements à l’INPI qui sont classifiés en tant que SIE, SEP ou PRI alors que l’INSEE classifie en tant que secondaire.

### Exemples

* Metadata
  *  Resolu: OUI/NON
  * Sequence:
    * siren: 389425760
    * code_greffe :5906
    * numero_gestion : 1992B50168
    * id_etablisement : 1
  * US: [XX](https://tree.taiga.io/project/olivierlubet-air/us/)
* Base de donnée
  *  INPI:  https://data.inpi.fr/entreprises/389425760#389425760
  * INSEE: http://avis-situation-sirene.insee.fr/ListeSiretToEtab.action?form.siren=389425760&form.nic=00011
  *  Datum:
  * Base de donnée: inpi
  *  Table: ets_final_sql
  * Query
    * INPI
    * INSEE

## Description problème

* Le siren 389425760 est un SEP à l’INPI, mais un secondaire à l’INSEE. De plus, cet établissement est fermé à la fois à l’INSEE et à l’INPI. A l’INPI seul, le principal est fermé mais pas l’INPI. Aucune trace du nouveau siège à l’INPI.
* Les informations ont disparu dans le moteur de recherche de l’INPI

# Evenements sur ETS non créé

## Description problème

 Il y a des établissements ayant fait l’objet d’une modification alors que l’établissement n’a pas été créé. C’est le cas lorsque le greffe se trompe dans l’identifiant de l’établissement. En effet, une séquence (siren, code greffe, numéro de gestion, id établissement) doit être unique au cours de la vie de l’établissement. Néanmoins, le greffe semble créer de nouveaux id établissement lors d’un événement sans raison sous-jacente.

### Exemples

* Metadata
  *  Resolu: NON
  * Sequence:
    * siren: 322303769
    * code_greffe :501
    * numero_gestion :1981D00105
    * id_etablisement :0/1/2
  * US: [XX](https://tree.taiga.io/project/olivierlubet-air/us/)
* Base de donnée
  *  INPI: https://data.inpi.fr/entreprises/322303769#322303769
  * INSEE: http://avis-situation-sirene.insee.fr/ListeSiretToEtab.action?form.siren=322303769&form.nic=00010
  *  Datum:
  * Base de donnée: inpi
  *  Table: ets_final_sql
  * Query
    * INPI
    * INSEE

## Description problème

*  Ce siren contient plusieurs problèmes:
  * Du coté de l’INSEE:
    * Le moteur de recherche de l’INSEE indique un seul établissement à l’adresse 1105 AV PIERRE BERNARD-REYMOND
    * La donnée brute de l’INSEE indique un seul établissement à l’adresse QUAI MONTREDUIT
    * Il y a une divergence dans la donnée de l’INSEE en elle même
  * Du coté de l’INPI
    * Il y a une création d’établissement en initial à l’adresse QUARTIER MONTREDUIT avec deux lignes, un SIE et un PRI
    * Il y a eu une modification de l’adresse et du statut sur la séquence 322303769, 501, 1981D00105, 1. PRI devenu SIE
    * Il y a eu une modification d’une séquence sans création → 322303769, 501, 1981D00105, 2. L’ID établissement 2 n’existe pas. La modification aurait impacté le PRI.
    * L’adresse  QUARTIER MONTREDUIT est inexistante dans le moteur de recherche de l’INPI
    * Vérification du csv d’origine effectué → information affichée dans notre bdd identique
