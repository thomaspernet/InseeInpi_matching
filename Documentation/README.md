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

##
Etablissement principal
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

![](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/-/blob/master/IMAGES/03_nouveau_schema_institution.PNG)

Le point d’entré de la cartographie des entreprises en France se fait via les unités légales. En effet, cette dernière a un caractère juridique qui fourni la preuve d’immatriculation de l’entreprise. Ensuite, pour connaitre les informations sur le choix juridique de l’entité (Personne Morale ou Personne Physique), il faut se concentrer sur les tables des PP et des PM fourni par l’INPI. Chacune des deux tables tables comportent le siren avec un ensemble d’information spécifique à la personne morale et/ou physique.

Comme  nous le savons déjà, chaque entreprise peut posséder un ou plusieurs établissement(s). C’est le rôle de l’INSEE de fournir un numéro NIC, qui va permettre de constituer le SIRET. Le siret est tout simplement un identifiant unique permettant de géolocaliser une entreprise. Le SIRET n’est pas présent à l’INPI. Toutefois, avec quelques règles de gestion, il est possible de rapprocher les deux tables, à savoir établissement INPI et établissement INSEE.

Nous savons aussi que le SIRET de l’unité légale est celui du siège. Lors du rapprochement des différentes tables, il est possible de rassembler et recouper la plupart des informations entre elles pour reconstituer la cartographie de l’entreprise.

![](https://scm.saas.cagip.group.gca/PERNETTH/inseeinpi_matching/-/blob/master/IMAGES/01_relation_inpi_insee.png)

Sources:
* https://fr.wikipedia.org/wiki/Entreprise
* https://fr.wikipedia.org/wiki/Institut_national_de_la_statistique_et_des_%C3%A9tudes_%C3%A9conomiques
* https://www.insee.fr/fr/metadonnees/definition/c1044
* https://www.insee.fr/fr/metadonnees/definition/c1496
* https://www.legalstart.fr/fiches-pratiques/siege-social/


<!-- #endregion -->
