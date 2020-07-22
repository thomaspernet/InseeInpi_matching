# Compter le nombre de siret par siren [INSEE] 

```
Entant que {X} je souhaite {compter le nombre d’établissement par entreprise, a l'instant t, à l'INSEE} afin de {pouvoir récupérer les siren amibgus n'ayant qu'un seul établissement}
```

**Metadatab**

- Taiga:
    - Numero US: [2955](https://tree.taiga.io/project/olivierlubet-air/us/2955)
- Gitlab
    - Notebook: []()
    - Markdown: []()
    - Data:
        - []()
        - 

# Contexte

Nous savons qu'une entreprise peut être composée d'un ou plusieurs établissements. Un établissement à l'INPI est normalement référencé par la séquence _siren_ + _code greffe_ + _numero gestion_ + _ID établissement_. Dans certains cas, nous pouvons déduire le siret si l'entreprise n'a qu'un seul établissement. Cette déduction peut être utile lorsque le siège et le principal sont à la même adresse, mais que l'INPI crée deux établissements différents.  

Nous savons aussi que l'INSEE donne un snapshot sur l'état de son stock à l'instant t, donc le nombre de siret indique véritablement le nombre d'établissement actif ou fermé rattaché à une entreprise.

Workflow US (via stock)

![workflow](https://www.lucidchart.com/publicSegments/view/d9e4494d-bfaf-4d0e-9e0f-53011cda7eb9/image.png)

# US / ISSUES liées

[PO & DEV : s'il existe des références, les inscrire]

# Besoin

Dans cette US, le besoin est le suivant:

- `count_initial_insee` 
     - Compte du nombre de siret (établissement) par siren (entreprise).



# Spécifications

### Origine information (si applicable) 

- Metadata:
    - Type
    - Source
    - Summary
    
## Input

[PO : dans le cas de transformation de données, préciser ,les sources :

*   Applications
*   Schémas
*   Tables: `inpi_etablissement_historique`
*   CSV: 
*   Champs: 
    - `siren` 



### Exemple Input 1

Exemple d'entreprises avec plusieurs siret (établissements)

| siren     | siret          |
|-----------|----------------|
| 000325175 | 00032517500016 |
| 000325175 | 00032517500024 |
| 000325175 | 00032517500057 |
| 000325175 | 00032517500065 |
| 000325175 | 00032517500040 |
| 000325175 | 00032517500032 |
| 001807254 | 00180725400022 |
| 001807254 | 00180725400014 |
| 005420021 | 00542002100015 |
| 005420021 | 00542002100049 |

## Output

[PO : dans le cas de transformation de données, préciser les sorties :

*   BDD cibles
*   Tables: `inpi_etablissement_historique`
*   Champs: 

]

Exemple du compte d'entreprises avec plusieurs établissements. L'entreprise `000325175` a 6 établissements actifs ou fermés

| siren     | siret          | count_initial_insee |
|-----------|----------------|---------------------|
| 000325175 | 00032517500024 | 6                   |
| 000325175 | 00032517500057 | 6                   |
| 000325175 | 00032517500065 | 6                   |
| 000325175 | 00032517500032 | 6                   |
| 000325175 | 00032517500040 | 6                   |
| 000325175 | 00032517500016 | 6                   |
| 001807254 | 00180725400022 | 2                   |
| 001807254 | 00180725400014 | 2                   |
| 005420021 | 00542002100015 | 5                   |
| 005420021 | 00542002100049 | 5                   |

## Règles de gestion applicables

[PO : Formules applicables]

Si nouvelle règle, ajouter ici.

# Charges de l'équipe

[

PO : Si des étapes particulières / des points d'attention sont attendus, être aussi explicite que possible

Spécifiquement pour l'intégration de nouvelles données dans DATUM :

*   Nombre de lignes chargées pour chaque nouvelle table
*   Poids de chaque nouvelle table
*   Durée du traitement ajouté (+ durée avant et après)

]

Code SQL utilisé lors de nos tests

``` 
query = """
SELECT 
  count_initial_insee,
  concat_adress.siren,
siret, 
dateCreationEtablissement,
         etablissementSiege,
         etatAdministratifEtablissement,
         codePostalEtablissement,
         codeCommuneEtablissement,
         libelleCommuneEtablissement,
  ville_matching,
  libelleVoieEtablissement,
         complementAdresseEtablissement,
         numeroVoieEtablissement,
         indiceRepetitionEtablissement,
         typeVoieEtablissement,
  adress_reconstituee_insee,
         enseigne1Etablissement,
         enseigne2Etablissement,
         enseigne3Etablissement
FROM concat_adress
LEFT JOIN (
  SELECT siren, COUNT(siren) as count_initial_insee
  FROM concat_adress
  GROUP BY siren
  ) as count_siren
ON concat_adress.siren = count_siren.siren
"""

```

# Tests d'acceptance

[PO : comment contrôler que la réalisation est conforme]

- Vérifier que le nombre d'observation est le même que l'US [2954](https://tree.taiga.io/project/olivierlubet-air/us/2954)
- Compter le nombre d'établissements par entreprise

| count_initial_insee | count    |
|---------------------|----------|
| 1                   | 16157828 |
| 2                   | 6300154  |
| 3                   | 2907591  |
| 4                   | 1404216  |
| 5                   | 703885   |
| 6                   | 367200   |
| 7                   | 202461   |
| 8                   | 122208   |
| 9                   | 80253    |
| 10                  | 56090    |
| 11                  | 42614    |
| 12                  | 35472    |
| 13                  | 30212    |
| 14                  | 25564    |
| 15                  | 22530    |
| 16                  | 19472    |
| 17                  | 17374    |
| 18                  | 15750    |
| 19                  | 15390    |
| 20                  | 14100    |
| 21                  | 12999    |
| 22                  | 11726    |
| 23                  | 10580    |
| 24                  | 11088    |
| 25                  | 9725     |
| 26                  | 10738    |
| 27                  | 9558     |
| 28                  | 9324     |
| 29                  | 9686     |
| 30                  | 8610     |
| 31                  | 8711     |
| 32                  | 7008     |
| 33                  | 7425     |
| 34                  | 7616     |
| 35                  | 6685     |
| 36                  | 7128     |
| 37                  | 5809     |
| 38                  | 6346     |
| 39                  | 5577     |
| 40                  | 6360     |
| 41                  | 5494     |
| 42                  | 6090     |
| 43                  | 5461     |
| 44                  | 5412     |
| 45                  | 4725     |
| 46                  | 4462     |
| 47                  | 4747     |
| 48                  | 4944     |
| 49                  | 4459     |
| 50                  | 4650     |
| 51                  | 4539     |
| 52                  | 5148     |
| 53                  | 3975     |
| 54                  | 3726     |
| 55                  | 5060     |
| 56                  | 3752     |
| 57                  | 3876     |
| 58                  | 4524     |
| 59                  | 3776     |
| 60                  | 3900     |
| 61                  | 3782     |
| 62                  | 3348     |
| 63                  | 4095     |
| 64                  | 2944     |
| 65                  | 3705     |
| 66                  | 3630     |
| 67                  | 2345     |
| 68                  | 2244     |
| 69                  | 3657     |
| 70                  | 3360     |
| 71                  | 3124     |
| 72                  | 3528     |
| 73                  | 3139     |
| 74                  | 2812     |
| 75                  | 3150     |
| 76                  | 3040     |
| 77                  | 2464     |
| 78                  | 2340     |
| 79                  | 2686     |
| 80                  | 2400     |
| 81                  | 2754     |
| 82                  | 2542     |
| 83                  | 2573     |
| 84                  | 2856     |
| 85                  | 1955     |
| 86                  | 2408     |
| 87                  | 2610     |
| 88                  | 2464     |
| 89                  | 1869     |
| 90                  | 1620     |
| 91                  | 1911     |
| 92                  | 2116     |
| 93                  | 2418     |
| 94                  | 1316     |
| 95                  | 2185     |
| 96                  | 1152     |
| 97                  | 1649     |
| 98                  | 2548     |
| 99                  | 1683     |
| 100                 | 2100     |
| 101                 | 1717     |
| 102                 | 2244     |
| 103                 | 1133     |
| 104                 | 1560     |
| 105                 | 1575     |
| 106                 | 1590     |
| 107                 | 1819     |
| 108                 | 1836     |
| 109                 | 1635     |
| 110                 | 1320     |
| 111                 | 1776     |
| 112                 | 1008     |
| 113                 | 1695     |
| 114                 | 1824     |
| 115                 | 920      |
| 116                 | 2436     |
| 117                 | 1170     |
| 118                 | 2596     |
| 119                 | 1666     |
| 120                 | 1920     |
| 121                 | 1573     |
| 122                 | 2318     |
| 123                 | 1476     |
| 124                 | 1736     |
| 125                 | 1125     |
| 126                 | 1260     |
| 127                 | 1778     |
| 128                 | 1152     |
| 129                 | 1290     |
| 130                 | 1820     |
| 131                 | 786      |
| 132                 | 1320     |
| 133                 | 1463     |
| 134                 | 1340     |
| 135                 | 1215     |
| 136                 | 1088     |
| 137                 | 1096     |
| 138                 | 2346     |
| 139                 | 1529     |
| 140                 | 980      |
| 141                 | 846      |
| 142                 | 426      |
| 143                 | 1430     |
| 144                 | 720      |
| 145                 | 1595     |
| 146                 | 2044     |
| 147                 | 2352     |
| 148                 | 1036     |
| 149                 | 1341     |
| 150                 | 900      |
| 151                 | 604      |
| 152                 | 1216     |
| 153                 | 1071     |
| 154                 | 2310     |
| 155                 | 465      |
| 156                 | 1092     |
| 157                 | 785      |
| 158                 | 1106     |
| 159                 | 1749     |
| 160                 | 1120     |
| 161                 | 1449     |
| 162                 | 486      |
| 163                 | 1141     |
| 164                 | 492      |
| 165                 | 990      |
| 166                 | 332      |
| 167                 | 835      |
| 168                 | 840      |
| 169                 | 845      |
| 170                 | 680      |
| 171                 | 684      |
| 172                 | 2064     |
| 173                 | 1557     |
| 174                 | 348      |
| 175                 | 1225     |
| 176                 | 1232     |
| 177                 | 354      |
| 178                 | 1246     |
| 179                 | 537      |
| 180                 | 720      |
| 181                 | 543      |
| 182                 | 1092     |
| 183                 | 183      |
| 184                 | 920      |
| 185                 | 185      |
| 186                 | 1488     |
| 187                 | 748      |
| 188                 | 376      |
| 189                 | 189      |
| 190                 | 950      |
| 191                 | 1719     |
| 192                 | 1152     |
| 193                 | 772      |
| 194                 | 776      |
| 195                 | 975      |
| 196                 | 1176     |
| 197                 | 197      |
| 198                 | 990      |
| 199                 | 398      |
| 200                 | 800      |
| 201                 | 1206     |
| 202                 | 1010     |
| 203                 | 609      |
| 204                 | 816      |
| 205                 | 205      |
| 206                 | 824      |
| 207                 | 828      |
| 208                 | 832      |
| 209                 | 1672     |
| 210                 | 630      |
| 211                 | 1055     |
| 212                 | 1060     |
| 213                 | 639      |
| 214                 | 1284     |
| 215                 | 1290     |
| 216                 | 648      |
| 217                 | 868      |
| 218                 | 1090     |
| 219                 | 657      |
| 220                 | 880      |
| 221                 | 221      |
| 222                 | 1332     |
| 223                 | 446      |
| 224                 | 448      |
| 225                 | 450      |
| 226                 | 452      |
| 227                 | 1135     |
| 228                 | 456      |
| 229                 | 687      |
| 230                 | 230      |
| 231                 | 462      |
| 232                 | 928      |
| 233                 | 466      |
| 234                 | 468      |
| 235                 | 940      |
| 236                 | 708      |
| 237                 | 474      |
| 238                 | 238      |
| 239                 | 1195     |
| 240                 | 240      |
| 241                 | 482      |
| 242                 | 968      |
| 243                 | 486      |
| 244                 | 244      |
| 245                 | 490      |
| 246                 | 984      |
| 247                 | 1235     |
| 248                 | 248      |
| 249                 | 498      |
| 250                 | 750      |
| 251                 | 502      |
| 252                 | 504      |
| 253                 | 506      |
| 254                 | 254      |
| 255                 | 1020     |
| 256                 | 256      |
| 258                 | 774      |
| 259                 | 1036     |
| 260                 | 780      |
| 261                 | 261      |
| 262                 | 524      |
| 263                 | 1052     |
| 264                 | 264      |
| 265                 | 530      |
| 269                 | 807      |
| 270                 | 810      |
| 271                 | 813      |
| 272                 | 1088     |
| 273                 | 1365     |
| 274                 | 548      |
| 275                 | 550      |
| 276                 | 1104     |
| 277                 | 277      |
| 278                 | 278      |
| 279                 | 837      |
| 280                 | 280      |
| 281                 | 562      |
| 282                 | 564      |
| 283                 | 283      |
| 284                 | 1420     |
| 285                 | 570      |
| 286                 | 286      |
| 287                 | 1435     |
| 288                 | 576      |
| 289                 | 289      |
| 290                 | 1450     |
| 291                 | 291      |
| 293                 | 879      |
| 294                 | 588      |
| 295                 | 295      |
| 296                 | 888      |
| 297                 | 594      |
| 298                 | 596      |
| 299                 | 299      |
| 301                 | 602      |
| 302                 | 604      |
| 303                 | 606      |
| 304                 | 912      |
| 305                 | 1830     |
| 306                 | 612      |
| 307                 | 614      |
| 308                 | 924      |
| 309                 | 309      |
| 310                 | 310      |
| 311                 | 311      |
| 312                 | 936      |
| 313                 | 939      |
| 314                 | 628      |
| 315                 | 630      |
| 316                 | 948      |
| 317                 | 317      |
| 318                 | 318      |
| 319                 | 319      |
| 320                 | 1280     |
| 321                 | 1284     |
| 322                 | 322      |
| 323                 | 323      |
| 324                 | 972      |
| 325                 | 1300     |
| 326                 | 978      |
| 327                 | 327      |
| 328                 | 328      |
| 329                 | 1316     |
| 330                 | 330      |
| 332                 | 996      |
| 333                 | 666      |
| 334                 | 668      |
| 335                 | 335      |
| 337                 | 337      |
| 338                 | 338      |
| 339                 | 339      |
| 340                 | 340      |
| 343                 | 686      |
| 344                 | 344      |
| 346                 | 346      |
| 347                 | 694      |
| 348                 | 696      |
| 349                 | 349      |
| 350                 | 700      |
| 351                 | 351      |
| 352                 | 352      |
| 358                 | 716      |
| 360                 | 720      |
| 361                 | 722      |
| 362                 | 724      |
| 363                 | 363      |
| 364                 | 728      |
| 365                 | 730      |
| 366                 | 366      |
| 367                 | 367      |
| 369                 | 369      |
| 370                 | 370      |
| 371                 | 371      |
| 372                 | 372      |
| 373                 | 373      |
| 374                 | 374      |
| 376                 | 1128     |
| 377                 | 1131     |
| 378                 | 378      |
| 379                 | 379      |
| 380                 | 380      |
| 382                 | 764      |
| 385                 | 385      |
| 386                 | 772      |
| 387                 | 774      |
| 388                 | 388      |
| 389                 | 389      |
| 390                 | 390      |
| 391                 | 391      |
| 394                 | 394      |
| 396                 | 792      |
| 397                 | 397      |
| 398                 | 398      |
| 400                 | 800      |
| 401                 | 401      |
| 402                 | 402      |
| 405                 | 405      |
| 406                 | 812      |
| 411                 | 411      |
| 412                 | 824      |
| 414                 | 414      |
| 416                 | 416      |
| 421                 | 421      |
| 422                 | 422      |
| 427                 | 854      |
| 428                 | 428      |
| 430                 | 430      |
| 433                 | 866      |
| 436                 | 436      |
| 437                 | 874      |
| 440                 | 1320     |
| 441                 | 1323     |
| 444                 | 444      |
| 445                 | 445      |
| 446                 | 446      |
| 449                 | 898      |
| 450                 | 450      |
| 451                 | 902      |
| 452                 | 452      |
| 457                 | 457      |
| 458                 | 458      |
| 459                 | 459      |
| 463                 | 463      |
| 464                 | 464      |
| 466                 | 466      |
| 467                 | 934      |
| 472                 | 472      |
| 474                 | 474      |
| 475                 | 475      |
| 480                 | 480      |
| 482                 | 482      |
| 487                 | 487      |
| 492                 | 492      |
| 495                 | 495      |
| 496                 | 496      |
| 497                 | 497      |
| 498                 | 498      |
| 503                 | 503      |
| 505                 | 505      |
| 506                 | 506      |
| 508                 | 508      |
| 510                 | 510      |
| 512                 | 512      |
| 517                 | 517      |
| 518                 | 518      |
| 521                 | 521      |
| 526                 | 526      |
| 527                 | 1054     |
| 528                 | 528      |
| 533                 | 533      |
| 546                 | 546      |
| 551                 | 551      |
| 553                 | 1106     |
| 555                 | 555      |
| 564                 | 564      |
| 565                 | 565      |
| 568                 | 568      |
| 569                 | 569      |
| 577                 | 577      |
| 589                 | 589      |
| 594                 | 594      |
| 604                 | 604      |
| 609                 | 609      |
| 610                 | 610      |
| 613                 | 613      |
| 616                 | 616      |
| 622                 | 622      |
| 626                 | 626      |
| 629                 | 629      |
| 631                 | 631      |
| 633                 | 1266     |
| 634                 | 634      |
| 643                 | 643      |
| 648                 | 648      |
| 656                 | 656      |
| 658                 | 658      |
| 665                 | 665      |
| 670                 | 670      |
| 671                 | 671      |
| 676                 | 676      |
| 679                 | 679      |
| 681                 | 681      |
| 685                 | 685      |
| 687                 | 687      |
| 690                 | 1380     |
| 704                 | 704      |
| 708                 | 708      |
| 712                 | 1424     |
| 717                 | 717      |
| 723                 | 1446     |
| 736                 | 736      |
| 744                 | 744      |
| 751                 | 751      |
| 754                 | 754      |
| 758                 | 758      |
| 768                 | 768      |
| 773                 | 773      |
| 794                 | 794      |
| 812                 | 812      |
| 816                 | 816      |
| 830                 | 830      |
| 832                 | 832      |
| 878                 | 878      |
| 903                 | 903      |
| 916                 | 916      |
| 933                 | 933      |
| 937                 | 937      |
| 950                 | 950      |
| 955                 | 955      |
| 957                 | 957      |
| 958                 | 958      |
| 965                 | 965      |
| 966                 | 966      |
| 969                 | 969      |
| 971                 | 971      |
| 986                 | 986      |
| 990                 | 990      |
| 1020                | 1020     |
| 1025                | 1025     |
| 1061                | 1061     |
| 1066                | 1066     |
| 1071                | 1071     |
| 1075                | 1075     |
| 1084                | 1084     |
| 1086                | 1086     |
| 1099                | 1099     |
| 1118                | 1118     |
| 1162                | 1162     |
| 1206                | 1206     |
| 1210                | 2420     |
| 1219                | 1219     |
| 1254                | 1254     |
| 1280                | 1280     |
| 1283                | 1283     |
| 1298                | 1298     |
| 1300                | 1300     |
| 1328                | 1328     |
| 1378                | 1378     |
| 1384                | 1384     |
| 1386                | 2772     |
| 1419                | 1419     |
| 1436                | 1436     |
| 1473                | 1473     |
| 1481                | 1481     |
| 1565                | 1565     |
| 1633                | 1633     |
| 1634                | 1634     |
| 1709                | 1709     |
| 1745                | 1745     |
| 1796                | 1796     |
| 1844                | 1844     |
| 1904                | 1904     |
| 2025                | 2025     |
| 2149                | 2149     |
| 2241                | 2241     |
| 2346                | 2346     |
| 2367                | 2367     |
| 2407                | 2407     |
| 2590                | 2590     |
| 2611                | 2611     |
| 2689                | 2689     |
| 2789                | 2789     |
| 2816                | 2816     |
| 2828                | 2828     |
| 2840                | 2840     |
| 2948                | 2948     |
| 2968                | 2968     |
| 3154                | 3154     |
| 3261                | 3261     |
| 3263                | 3263     |
| 3387                | 3387     |
| 3707                | 3707     |
| 4353                | 4353     |
| 4396                | 4396     |
| 5859                | 5859     |
| 6360                | 6360     |
| 9069                | 9069     |
| 9139                | 9139     |
| 12545               | 12545    |

**Code reproduction**

```
```

# CONCEPTION

Conception réalisée par ............. et ..................

[DEV :

Important :

*   Ce chapitre doit impérativement être complété **avant de basculer l'US à 'développement en cours'**
*   La conception doit systématiquement être **faite à deux**
*   Il ne doit **pas y avoir de code dans ce chapitre**
*   Tout au long du développement, ce chapitre doit être enrichi
*   Le nom du binôme ayant participé à la conception doit être précisé dans l'US

Contenu :

*   Décrire les traitements nouveaux / modifiés : emplacement des fichiers (liens vers GIT), mise en avant des évolutions fortes, impacts dans la chaîne d'exploitation
*   Points d'attention spécifiques : notamment sur les règles de gestion et leur mise en oeuvre technique

]

# Evolution de la documentation

[DEV :

*   Identifier les champs enrichis dans le dictionnaire de données
*   Identifier les impacts dans les documents pérennes DTA, DEXP, Consignes de supervision
*   Identifier les impacts dans les documents de MEP (FI)

]

# Tests réalisés

[DEV : préciser les tests réalisés pour contrôler le bon fonctionnement, et les résultats obtenus]

# Tests automatiques mis en oeuvre

[DEV : préciser les TA et expliciter leur fonctionnement]

# Démonstration

[DEV : suivant le cas, publier sur le sharepoint et mettre un lien ici soit :

*   Capture d'écran
*   Vidéo publiée

]

# Creation markdown

    jupyter nbconvert --no-input --to markdown 12_nombre_ets_insee.ipynb
    Report Available at this adress:
     /Users/thomas/Google Drive/Projects/GitHub/Repositories/InseeInpi_matching/Notebooks_matching/Data_preprocessed/programme_matching/05_redaction_US/US_md/12_nombre_ets_insee.md

