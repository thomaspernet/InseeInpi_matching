# Processus de siretisation

Le graphique ci dessous indique les étapes a réalisé pour faire le rapprochement entre l'INPI et l'INSEE.

Nous rappelons que cette nouvelle version de la siretisation indique la ligne la plus probable selon les informations fournies par l'INSEE. Nous avons établi une matrice de règle, et regardons, pour un index donné, la ligne dans la matrice qui indique la meilleure possibilité.


![](https://app.lucidchart.com/publicSegments/view/dc9a6210-77ac-4358-96c9-ab5a714cfac1/image.png)

Nous pouvons dégager trois grands axes. Dans un premier temps, il faut merger les deux tables, INSEE-INPI et créer un numéro de ligne. Dans un second temps, nous allons créer plusieurs tables avec les variables qui vont permettre de dégager les règles, cette a dire vérifier la cohérence des informations entre l'INSEE et l'INPI. Dans chacune des tables, nous allons avoir une variable commune, a savoir le numéro de ligne.

Dans le dernier axe, nous allons rapprocher l'ensemble des tables créées précédements avec la table mergée initialement. Ensuite, nous allons créer les règles, et récupérer la règle minimum selon l'index (un index avec plusieurs siret a plusieurs lignes, mais le même index).
