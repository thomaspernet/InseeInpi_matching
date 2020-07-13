import json, os, re, sqlite3
from dask.diagnostics import ProgressBar
from dask.multiprocessing import get
import dask.dataframe as dd
import pandas as pd
import numpy as np
#from nltk.corpus import stopwords
from sqlalchemy import create_engine
pbar = ProgressBar()
pbar.register()

class preparation:
    def __init__(self, parameters = None):
        """
        Parametrisation du programme de siretisation des fichiers de l'INPI.
        La parametrisation sert principalement a définir les paths pour
        récupérer les fichiers nécéssaires pour le préparer les bases de
        données INPI/INSEE

        Args:
        - parameters: Dictionary, les "keys" sont les suivantes:
            - communes_insee: Path pour localiser le fichier des communes de
            France
            - upper_word: Path pour localiser les mots a enlever dans la
            préparation de l'adresse
            - voie: Path pour localiser les types de voies indéxés par l'INSEE
            - insee: Path pour localiser le fichier de l'INSEE. Format csv
            - inpi_etb: Path pour localiser le fichier de l'INPI, etablissement.
            Format csv
            - date_end: Indique une date de fin pour filtrer le fichier insee
            format: "YYYY-MM-DD"

        """
        self.communes = parameters['communes_insee']
        self.upper_word = pd.read_csv(parameters['upper_word']
        ).iloc[:,0].to_list()
        self.voie = pd.read_csv(parameters['voie'])
        self.insee = parameters['insee']
        self.inpi_etb = parameters['inpi_etb']
        self.filename = os.path.split(parameters['inpi_etb'])[1][:-4]
        self.date_end = parameters['date_end']

    def save_sql(self,df,  db,table,  query):
        """
        SAuvegarde un dataframe vers un serveur SQL

        Args:
        - df: Pandas DataFrame
        - db: String: Path ou doit etre sauvegardé la dataframe
        - table: String, Nom de la table
        - query: String: SQL query

        """

        conn = sqlite3.connect(db)
        c = conn.cursor()
        c.execute(query)
        conn.commit()

        if 'index' in df.columns:
            df = df.drop(columns = 'index')
        df.to_sql(table, conn, if_exists='replace', index = False)



    def import_dask(self, file, usecols = None, dtype=None):
        """
        Import un fichier gzip ou csv en format Dask

        Args:
        - file: String, Path pour localiser le fichier, incluant le nom et
        l'extension
        - usecols: List: les noms des colonnes a importer. Par defaut, None
        - dtype: Dictionary: La clé indique le nom de la variable, la valeur
        indique le type de la variable

        Return:
        Dask Pandas DataFrame
        """
        extension = os.path.splitext(file)[1]
        if usecols == None:
            low_memory = False
        else:
            low_memory = True
        if extension == '.gz':
            dd_df = dd.read_csv(file, usecols = usecols, dtype = dtype,
        blocksize=None,compression='gzip', low_memory = low_memory)
        else:
            dd_df = dd.read_csv(file, usecols = usecols, dtype = dtype,
        blocksize=None, low_memory = low_memory)

        return dd_df

    def nombre_siret_siren(self, df_dd, origin = 'INSEE'):
        """
        Calcule le nombre de SIRET par siren

        Args:
        - df_dd: Dask dataframe contenant la variable siren
        - origin: String: Explicite si le Dataframe provient de l'INSEE ou de
        l'INPI. Choix possible: INSEE ou INPI

        Return:
        Un dataFrame Pandas avec une variable count

        """

        # Test if in
        list_option = ["INSEE", "INPI"]

        if origin not in list_option:
            return print(
            "Veuillez utiliser l'un des mots clés suivants {} \n \
        pour l'argument origin".format(list_option)
        )

        if origin == 'INSEE':
            var_name = 'count_initial_insee'
        else:
            var_name = 'count_initial_inpi'

        df_dd_count = df_dd.merge(
        (df_dd
        .groupby('siren')['siren']
        .count()
        .rename(var_name)
        .reset_index()))

        return df_dd_count

    def clean_commune(self, df_inpi):
        """
        Prepare les fichiers de l'INPI -> Nettoie la variable Ville dans l'INPI
        pour correspondre au fichier communes_insee.

        Le nettoyage utilise le regex pour:
        - Enlever les mots suivants:
            CEDEX, cedex, Cedex, ,COMMUNE DE ,COMMUNE DE,commune de ,commune de,
                Commune de ,Commune de
        - Enlève les caractères spéciaux
        - Enlève les espaces
        - Enlèves les digits

        Returns:
        - Un dataframe avec les nouvelles variables suivantes

            """
        regex = "CEDEX|cedex|Cedex|\([^)]*\)|/\s\s+/" \
            "|^\d+\s|\s\d+\s|\s\d+$|\d+|\.|\--|COMMUNE DE |" \
            "COMMUNE DE|commune de |commune de|Commune de |Commune de |\s$"
            ### Voir si on peut faire avec Dask
        communes = pd.read_csv(self.communes, low_memory=False)

        df_inpi['test'] =df_inpi['ville'].str.extract(r'(\d+)')
        df_inpi['ville_clean'] = df_inpi['ville'].str.replace(
            regex,'')
        df_inpi['ville_clean'] = df_inpi['ville_clean'].str.replace(
            '\s$|\s^','')

            ### arrondissement
        df_inpi['ville2'] = np.where(
            np.logical_and(
             ~df_inpi['test'].isin([np.nan]),
            df_inpi['test'].str.len() <=2
        )
       ,
        df_inpi['ville_clean'] + '' + df_inpi['test'].astype(str),
        df_inpi['ville_clean']
    )

        df_inpi['ville_upper'] = df_inpi['ville_clean'].str.upper()

        df_inpi = df_inpi.merge(communes,
                             left_on='ville2',
                             right_on='possibilite',
                             how='left',
                             indicator=True)

        df_inpi = pd.concat([
        df_inpi.loc[lambda x: x['_merge'].isin(['both'])],
        (df_inpi
         .loc[lambda x: x['_merge'].isin(['left_only'])]
         .drop(columns=['ncc', 'possibilite', '_merge'])
         .merge(communes,
                left_on='ville_upper',
                right_on='possibilite',
                how='left',
                indicator=True)
         )
    ])

        df_inpi = pd.concat([
        df_inpi.loc[lambda x: x['_merge'].isin(['both'])],
        (df_inpi
         .loc[lambda x: x['_merge'].isin(['left_only'])]
         .drop(columns=['ncc', 'possibilite', '_merge'])
         .assign(
             noaccent=lambda x: x['ville_clean'].str.normalize('NFKD')
             .str.encode('ascii', errors='ignore')
             .str.decode('utf-8'))
         ).merge(communes,
                 left_on='noaccent',
                 right_on='possibilite',
                 how='left',
                 indicator=True)])

        df_inpi = df_inpi.drop(columns = ['test', 'ville_clean',
         'ville2', 'ville_upper','possibilite', '_merge', 'noaccent'])

        return df_inpi

    def create_split_adress(self,x):
        """
        Découpe l'adresse en une liste de mots. L'input de la fonction est
        généralement une adresse au préalable normalisée. Exemple:
        Adresse : "JONQUILLES JAUNES BORD MER" -> [JONQUILLES,JAUNES,BORD,MER]

        Args:
        - x: Une serie contenant l'adresse. De préférence, une serie avec une
        adresse normalisée

        Return:
        Une liste
        """
        split_ = x.str.split().to_list()
        return  split_

    def create_regex_adress(self, x):
        """
        Regroupe les mots de l'adresse ensemble avec comme séparateur "|" et
        le signe $ en fin de mot pour indiquer qu'il ne faut parser que le mot
        en question et pas ce qu'il y a après.

        Args:
        - x: column conntenant l'adresse dans un dataFrame pandas

        Returns:
        un String concatenés des mots de la colonne
        """
        try:
            split_ = [i + "$" for i in x]
            reg = '|'.join(split_)
        except:
            reg = np.nan
        return  reg

    def len_digit_address(self, x):
        """
        Compte le nombre de digits contenu dans la variable adresse.
        Si l'adresse est vide, ie pas renseignée par l'INPI ou l'INSEE alors
        impossible de calculer le len, donc retourne un nan

        Args:
        - x: Une serie contenant l'adresse. De préférence, une serie avec une
        adresse normalisée

        Note: On peut faire tout le test dans la fonction, mais on souhaite
        garder le nombre de digit dans l'adresse pour plus de clarté dans les
        logs on ne peut pas cacluler le len sur une valeur manquante, a l'insee
        il y a des lignes sans adresse

        Return:
        Un integer ou un NaN via numpy
        """
        try:
            return len(x)
        except:
            np.nan

    def prepare_adress(self, df):
        """
        Créer deux colonnes nétoyées de l'adresse a partir d'un dataframe INPI.
        La première variable va nétoyer l'adresse en enlevant les valeurs comme
        route, avenue qui ne sont pas indiquées dans l'INSEE (variables de
        matching)n netoie les accents, digits, etc. La deuxième variable va
        concatener l'adresse en vue d'un parsing regex

        Args:
        - df: Pandas DataFrame

        Returns:
        DataFrame Pandas nétoyé avec les variables adresses nétoyées
        """
        temp_adresse = (df
        .assign(

        adress_new = lambda x:
            x['adresse_ligne1'].fillna('') + ' '+\
            x['adresse_ligne2'].fillna('') + ' '+\
            x['adresse_ligne3'].fillna(''),
        adresse_new_clean=lambda x: x['adress_new'].str.normalize(
                'NFKD')
            .str.encode('ascii', errors='ignore')
            .str.decode('utf-8')
            .str.replace('[^\w\s]|\d+', ' ')
            .str.upper(),
        )
        .assign(
        adresse_new_clean = lambda x: x['adresse_new_clean'].apply(
        lambda x:' '.join([word for word in str(x).split() if word not in
        (self.upper_word)])),
        adress_new = lambda x: x['adress_new'].str.normalize(
            'NFKD')
        .str.encode('ascii', errors='ignore')
        .str.decode('utf-8')
        .str.replace('[^\w\s]', ' ')
        .str.upper(),
        adresse_new_clean_split=lambda x:
        self.create_split_adress(x['adresse_new_clean']
        ),
        adresse_new_clean_reg = lambda x:
        x['adresse_new_clean_split'].apply(lambda x:
        self.create_regex_adress(x)),
        ### Peut etre a modifier
        digit_inpi = lambda x: x['adress_new'].str.extract(r'(\d+)'),
        list_digit_inpi = lambda x:x['adress_new'].str.findall(r"(\d+)"),
        #special_digit = lambda x:x['Adress_new'].str.findall(r"(\d+)").apply(
        #lambda x:'&'.join([i for i in x])),
        possibilite = lambda x:
        x['adress_new'].str.extract(r'(' + '|'.join(
        self.voie['possibilite'].to_list()) +')'),

        )
        .drop(columns = ['adresse_new_clean','adresse_new_clean_split'])
        .merge(self.voie, how = 'left')
        )

        ### List multiple digits in address -> process to improve
        temp_adresse['len_digit_address_inpi'] = \
        temp_adresse['list_digit_inpi'].apply(lambda x:
        self.len_digit_address(x))

        temp_adresse.loc[
        temp_adresse['len_digit_address_inpi'] >= 2,
        'list_digit_inpi'
        ] =  temp_adresse['list_digit_inpi']

        temp_adresse.loc[
        temp_adresse['len_digit_address_inpi'] < 2,
        'list_digit_inpi'
        ] = np.nan

        temp_adresse['len_digit_address_inpi'] = \
        temp_adresse['len_digit_address_inpi'].fillna(0)

        return temp_adresse

    def normalize_inpi(self,origin = 'Initial', save_gz = True):
        """
        Prepare le fichier gz en vue de la siretisation
        La fonction prend le fichier d'origine de l'INPII définit dans
        l'instantiation de la classe. Ensuite, Ensuite, la fonction Enlève
        toutes les lignes dont les variables de matching ont uniquement des nan
        . Une liste de SIREN est
        aussi renseignée afin de ne contenir que les SIREN a recuperer.
        Cela permet de gagner en mémoire lors de la siretisation_inpi

        Args:
        - siren_inpi: List: liste de siren a retrouver

        Return:
        - Trois fichiers sont sauvegardé:
            - Un dataframe pandas filtré et avec une nouvelle variable,
            le count de SIRET par SIREN et la nromalisation des communes
            - Une liste avec les SIREN a filtrer dans l'INSEE
            - Le dataframe sauvegardé dans une base SQL
        """
        regex = r"pp"
        matches = re.search(regex, self.inpi_etb)
        if matches:
            dtype={
                'siren': 'object',
                'code_greffe': 'object',
        'type': 'object',
        'code_commune': 'object',
        'code_postal': 'object',
        'code_postal_matching': 'object',
        'adresse_ligne1': 'object',
        'adresse_ligne2': 'object',
        'adresse_ligne3': 'object',
        'ville': 'object',
        'pays': 'object',
                'auto_entrepreneur': 'object',
       'conjoint_collab_pseudo': 'object',
       'dap_adresse_ligne1': 'object',
       'dap_adresse_ligne2': 'object',
       'dap_adresse_ligne3': 'object',
       'dap_code_commune': 'object',
       'dap_date_cloture': 'object',
       'dap_denomination': 'object',
       'dap_objet': 'object',
       'dap_pays': 'object',
       'dap_ville': 'object',
       'sans_activite': 'object',
            'file_timestamp': 'object',
        'date_greffe': 'object',
            'date_début_activité':'object'}

            reindex = ["siren",
"code_greffe",
"nom_greffe",
"numero_gestion",
"origin",
"file_timestamp",
"date_greffe",
"libelle_evt",
"type",
"type_inscription",
 "date_immatriculation",
 "date_1re_immatriculation",
 "date_radiation",
 "date_transfert",
 "sans_activite",
 "date_début_activité",
 "date_debut_1re_activite",
 "date_cessation_activite",
 "nom_patronymique",
 "nom_usage",
 "pseudonyme",
 "prenoms",
 "date_naissance",
 "ville_naissance",
 "pays_naissance",
 "nationalité",
 "adresse_ligne1",
 "adresse_ligne2",
 "adresse_ligne3",
 "adress_new",
 "adresse_new_clean_reg",
 "possibilite",
 "INSEE",
 "digit_inpi",
 "list_digit_inpi",
 "len_digit_address_inpi",
 "code_postal_matching",
 "ville",
                "ncc",
 "code_commune",
                'count_initial_inpi',
 "pays",
 "activite_forain",
 "eirl",
 "auto_entrepreneur",
 "dap",
 "dap_denomination",
 "dap_objet",
 "dap_date_cloture",
 "dap_adresse_ligne1",
 "dap_adresse_ligne2",
 "dap_adresse_ligne3",
 "dap_code_postal",
 "dap_ville",
 "dap_code_commune",
 "dap_pays",
 "conjoint_collab_nom_patronym",
 "conjoint_collab_nom_usage",
 "conjoint_collab_pseudo",
 "conjoint_collab_prenoms",
 "conjoint_collab_date_fin",
 "max_partiel",
 "csv_source"]

        else:
            dtype={
        'siren': 'object',
        'type': 'object',
        'code_commune': 'object',
        'code_postal': 'object',
        'code_postal_matching': 'object',
        'code_greffe': 'object',
        'adresse_ligne1': 'object',
        'adresse_ligne2': 'object',
        'adresse_ligne3': 'object',
        'ville': 'object',
        'pays': 'object',
        'domiciliataire_greffe': 'object',
        'domiciliataire_nom': 'object',
        'domiciliataire_siren': 'object',
        'date_début_activité':'object',
        'siege_domicile_representant': 'object',
        'domiciliataire_complement': 'object',
        'id_etablissement': 'object', ### problème avec certains fichiers
        'file_timestamp': 'object',
        'date_greffe': 'object',
            'date_début_activité':'object'
                         }
            reindex =[
"siren",
"code_greffe",
"nom_greffe",
"numero_gestion",
"id_etablissement",
"status",
"origin",
"file_timestamp",
"date_greffe",
"libelle_evt",
"type",
"siege_pm",
"rcs_registre",
"adresse_ligne1",
"adresse_ligne2",
"adresse_ligne3",
"adress_new",
"adresse_new_clean_reg",
"possibilite",
"INSEE",
"digit_inpi",
"list_digit_inpi",
"len_digit_address_inpi",
"code_postal_matching",
"ville",
"ncc",
"code_commune",
"pays",
"domiciliataire_nom",
"domiciliataire_siren",
'count_initial_inpi',
"domiciliataire_greffe",
"domiciliataire_complement",
"Siege_domicile_representant",
"nom_commercial",
"enseigne",
"activite_ambulante",
"activite_saisonniere",
"activite_Non_Sedentaire",
"date_début_activité",
"activite",
"origine_fonds",
"origine_fonds_info",
"type_exploitation",
"csv_source"
]

        dd_df_inpi = self.import_dask(
            file = self.inpi_etb,
            usecols = None,
            dtype=dtype)

        #dd_df_inpi = dd_df_inpi.rename(columns={"Siren": "siren"}).compute()
        dd_df_inpi = dd_df_inpi.loc[dd_df_inpi['origin'].isin(origin)].compute()
        #dd_df_inpi = dd_df_inpi.compute()
        #dd_df_inpi.columns = map(str.lower, dd_df_inpi.columns)

        siren_fullna = dd_df_inpi.loc[
                      (dd_df_inpi['adresse_ligne1'].isin([np.nan]))
                     & (dd_df_inpi['adresse_ligne2'].isin([np.nan]))
                     & (dd_df_inpi['adresse_ligne3'].isin([np.nan]))
                     & (dd_df_inpi['code_postal_matching'].isin([np.nan]))
                     & (dd_df_inpi['ville'].isin([np.nan]))
                     & (dd_df_inpi['code_commune'].isin([np.nan]))
                     ]['siren']

        siren_inpi = dd_df_inpi['siren'].drop_duplicates()

        subset_inpi = dd_df_inpi.loc[
                                 (~dd_df_inpi['siren'].isin(
                                 siren_fullna.to_list()))
                                 ]

        subset_inpi = self.nombre_siret_siren(df_dd = subset_inpi,
             origin = 'INPI')

        subset_inpi_cleaned = self.clean_commune(df_inpi = subset_inpi)

        subset_inpi_cleaned = self.prepare_adress(df =
        subset_inpi_cleaned)

        subset_inpi_cleaned = (subset_inpi_cleaned
        .reindex(columns  = reindex)
        .assign(index = lambda x:
        x.index))

        if save_gz:
            size_ = subset_inpi.shape[0]
            print('Saving {} observations'.format(size_))

            ### Creer un nouveau dossier pour accueillir csv: input/INPI
            directory = "".join(origin)
            parent_dir = 'data/input/INPI/'
            path = os.path.join(parent_dir, directory)
            os.mkdir(path)

            name = "inpi_{0}_{1}_{2}.csv".format(
            self.filename,
            directory,
            0
            )
            location = os.path.join(path, name)

            subset_inpi_cleaned.to_csv(location,index = False)

            ### Creer un nouveau dossier pour accueillir csv: input/SIREN_INPI
            size_ = len(siren_inpi)
            print('Saving {} observations'.format(size_))
            parent_dir = 'data/input/SIREN_INPI/'
            path = os.path.join(parent_dir, directory)
            os.mkdir(path)

            name = "inpi_SIREN_{0}_{1}.csv".format(
            self.filename,
            directory
            )

            location = os.path.join(path, name)
            print("""
            Copier/coller le path\n
            {}\n
            pour la fonction INSEE
            """.format(location))

            siren_inpi.to_csv(location, index = False)


    def normalize_insee(self, siren_inpi_gz, save_gz= True, save_sql = False):
        """
        Prepare le fichier gz en vue de la siretisation
        La fonction prend le fichier d'origine de l'INSEE définit dans
        l'instantiation de la classe. Ensuite, la date de création de l'
        etablissement permet de filtrer la base. Une liste de SIREN est
        aussi renseignée afin de ne contenir que les SIREN a recuperer.
        Cela permet de gagner en mémoire lors de la siretisation_inpi

        Args:
        - siren_inpi: strig: Indique le nom du fichier gz contenant les
        siret a filter ->liste de siren a retrouver

        Return:

        Un dataframe pandas filtré et avec une nouvelle variable, le count de
        SIRET par SIREN

        Return:
        - Deux fichiers sont sauvegardé:
            - Un dataframe pandas filtré et avec une nouvelle variable,
            le count de SIRET par SIREN
            - Le dataframe sauvegardé dans une base SQL

        """

        usecols=[
            'siren',
            'siret',
             "etablissementSiege",
             "etatAdministratifEtablissement",
             "numeroVoieEtablissement",
             "indiceRepetitionEtablissement",
             "typeVoieEtablissement",
             "libelleVoieEtablissement",
             "complementAdresseEtablissement",
             "codeCommuneEtablissement",
             "libelleCommuneEtablissement",
              "codePostalEtablissement",
              "codeCedexEtablissement",
              "libelleCedexEtablissement",
              "distributionSpecialeEtablissement",
              "libelleCommuneEtrangerEtablissement",
              "codePaysEtrangerEtablissement",
               "libellePaysEtrangerEtablissement",
               "dateCreationEtablissement"
                                   ]

        dtype={
            'siren': 'object',
            'siret': 'object',
            "etablissementSiege": "object",
            "etatAdministratifEtablissement": "object",
            "numeroVoieEtablissement": 'object',
            "indiceRepetitionEtablissement": 'object',
            "typeVoieEtablissement": 'object',
            "libelleVoieEtablissement": 'object',
            "complementAdresseEtablissement": 'object',
            "codeCommuneEtablissement": 'object',
            "libelleCommuneEtablissement": 'object',
            "codePostalEtablissement": 'object',
            "codeCedexEtablissement": 'object',
            "libelleCedexEtablissement": 'object',
            "distributionSpecialeEtablissement": 'object',
            "libelleCommuneEtrangerEtablissement": 'object',
            "codePaysEtrangerEtablissement": 'object',
            "libellePaysEtrangerEtablissement": 'object',
            "dateCreationEtablissement":'object'
                                 }

        reindex = [
    'siren','siret','dateCreationEtablissement','count_initial_insee',
    'etablissementSiege','complementAdresseEtablissement',
    'numeroVoieEtablissement','indiceRepetitionEtablissement',
    'typeVoieEtablissement','libelleVoieEtablissement',
    'len_digit_address_insee','list_digit_insee','codePostalEtablissement',
    'libelleCommuneEtablissement','libelleCommuneEtrangerEtablissement',
    'distributionSpecialeEtablissement','codeCommuneEtablissement',
    'codeCedexEtablissement','libelleCedexEtablissement',
    'codePaysEtrangerEtablissement','libellePaysEtrangerEtablissement',
    'etatAdministratifEtablissement',
 ]

        dd_df_insee = self.import_dask(file = self.insee,
            usecols = usecols,
            dtype=dtype)

        siren_inpi = pd.read_csv(siren_inpi_gz,
                                 dtype = {'siren':'object'})['siren'].to_list()

        subset_insee = (dd_df_insee
        .loc[dd_df_insee['siren'].isin(siren_inpi)]
        #.loc[dd_df_insee['dateCreationEtablissement'] <= self.date_end]
        .assign(
            libelleCommuneEtablissement = lambda x:
            x['libelleCommuneEtablissement'].str.replace('-', ' '),
             list_digit_insee = lambda x:
             x['libelleVoieEtablissement'].str.findall(r"(\d+)")
               )
               )

        subset_insee = self.nombre_siret_siren(df_dd = subset_insee,
             origin = 'INSEE').compute()

        ### List multiple digit in address

        subset_insee['len_digit_address_insee'] = \
        subset_insee['list_digit_insee'].apply(lambda x:
        self.len_digit_address(x))

        subset_insee.loc[
        subset_insee['len_digit_address_insee'] >= 2,
        'list_digit_insee'
        ] =  subset_insee['list_digit_insee']

        subset_insee.loc[
        subset_insee['len_digit_address_insee'] < 2,
        'list_digit_insee'
        ] = np.nan

        #subset_insee = subset_insee.drop(columns = 'temp_len')
        subset_insee['len_digit_address_insee'] = \
        subset_insee['len_digit_address_insee'].fillna(0)

        subset_insee = subset_insee.reindex(columns = reindex)

        if save_gz:
            size_ = subset_insee.shape[0]
            print('Saving {} observations'.format(size_))

            regex = r"[^_]+$"

            parent_dir = 'data/input/INSEE/'
            matches = re.search(regex, siren_inpi_gz)
            directory = os.path.splitext(matches.group())[0]

            path = os.path.join(parent_dir, directory)
            os.mkdir(path)

            name = "insee_{0}_{1}.csv".format(
            size_,
            directory
            )

            location = os.path.join(path, name)

            (subset_insee
            .assign(index = lambda x:
            x.index)
            .to_csv(location,
                    index = False)
            )
