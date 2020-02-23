import json, os, re, sqlite3
from dask.diagnostics import ProgressBar
from dask.multiprocessing import get
import dask.dataframe as dd
import pandas as pd
import numpy as np
from nltk.corpus import stopwords
from sqlalchemy import create_engine
pbar = ProgressBar()
pbar.register()



class preparation:
    def __init__(self, parameters = None):
        """
        Parametrisation du programme de siretisation des fichiers de l'INPI

        Args:
        - parameters: Dictionary, les "keys" sont les suivantes:
            - communes_insee: Path pour localiser le fichier des communes de
            France
            - insee: Path pour localiser le fichier de l'INSEE. Format csv
            - inpi_etb: Path pour localiser le fichier de l'INPI, etablissement.
            Format gz
            - date_end: Indique une date de fin pour filtrer le fichier insee
            format: "YYYY-MM-DD"

        """
        self.communes = parameters['communes_insee']
        self.insee = parameters['insee']
        self.inpi_etb = parameters['inpi_etb']
        self.date_end = parameters['date_end']
        self.upper_word = pd.read_csv(parameters['upper_word']
        ).iloc[:,0].to_list()
        self.voie = pd.read_csv(parameters['voie'])

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

        Returns:
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

        Returns: Un dataframe avec les nouvelles variables suivantes:
        -

            """
        regex = "CEDEX|cedex|Cedex|\([^)]*\)|/\s\s+/" \
            "|^\d+\s|\s\d+\s|\s\d+$|\d+|\.|\--|COMMUNE DE |" \
            "COMMUNE DE|commune de |commune de|Commune de |Commune de |\s$"
            ### Voir si on peut faire avec Dask
        communes = pd.read_csv(self.communes, low_memory=False)

        df_inpi['test'] =df_inpi['Ville'].str.extract(r'(\d+)')
        df_inpi['Ville_clean'] = df_inpi['Ville'].str.replace(
            regex,'')
        df_inpi['Ville_clean'] = df_inpi['Ville_clean'].str.replace(
            '\s$|\s^','')

            ### arrondissement
        df_inpi['ville2'] = np.where(
            np.logical_and(
             ~df_inpi['test'].isin([np.nan]),
            df_inpi['test'].str.len() <=2
        )
       ,
        df_inpi['Ville_clean'] + '' + df_inpi['test'].astype(str),
        df_inpi['Ville_clean']
    )

        df_inpi['Ville_upper'] = df_inpi['Ville_clean'].str.upper()

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
                left_on='Ville_upper',
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
             noaccent=lambda x: x['Ville_clean'].str.normalize('NFKD')
             .str.encode('ascii', errors='ignore')
             .str.decode('utf-8'))
         ).merge(communes,
                 left_on='noaccent',
                 right_on='possibilite',
                 how='left',
                 indicator=True)])

        df_inpi = df_inpi.drop(columns = ['test', 'Ville_clean',
         'ville2', 'Ville_upper','possibilite', '_merge', 'noaccent'])

        return df_inpi

    def create_split_adress(self,x):
        """

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
        On peut faire tout le test dans la fonciton, mais on souhaite garder
        le nombre de digit dans l'adresse pour plus de clarté dans les logs
        on ne peut pas cacluler le len sur une valeur manquante, a linsee
        il y a des lignes sans adresse
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

        Adress_new = lambda x:
            x['Adresse_Ligne1'].fillna('') + ' '+\
            x['Adresse_Ligne2'].fillna('') + ' '+\
            x['Adresse_Ligne3'].fillna(''),
            Adresse_new_clean=lambda x: x['Adress_new'].str.normalize(
                'NFKD')
            .str.encode('ascii', errors='ignore')
            .str.decode('utf-8')
            .str.replace('[^\w\s]|\d+', ' ')
            .str.upper(),
        )
        .assign(
        Adresse_new_clean = lambda x: x['Adresse_new_clean'].apply(
        lambda x:' '.join([word for word in str(x).split() if word not in
        (self.upper_word)])),
        Adress_new = lambda x: x['Adress_new'].str.normalize(
            'NFKD')
        .str.encode('ascii', errors='ignore')
        .str.decode('utf-8')
        .str.replace('[^\w\s]', ' ')
        .str.upper(),
        Adresse_new_clean_split=lambda x:
        self.create_split_adress(x['Adresse_new_clean']
        ),
        Adresse_new_clean_reg = lambda x:
        x['Adresse_new_clean_split'].apply(lambda x:self.create_regex_adress(x)),
        ### Peut etre a modifier
        digit_inpi = lambda x: x['Adress_new'].str.extract(r'(\d+)'),
        list_digit_inpi = lambda x:x['Adress_new'].str.findall(r"(\d+)"),
        #special_digit = lambda x:x['Adress_new'].str.findall(r"(\d+)").apply(
        #lambda x:'&'.join([i for i in x])),
        possibilite = lambda x:
        x['Adress_new'].str.extract(r'(' + '|'.join(
        self.voie['possibilite'].to_list()) +')'),

        )
        .drop(columns = ['Adresse_new_clean','Adresse_new_clean_split'])
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

    def normalize_inpi(self, save_gz = True, save_sql =False):
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

        dtype={
        'Siren': 'object',
        'Type': 'object',
        'Code_Postal': 'object',
        'Code_Commune': 'object',
        'Adresse_Ligne1': 'object',
        'Adresse_Ligne2': 'object',
        'Adresse_Ligne3': 'object',
        'Ville': 'object',
        'Pays': 'object',
        'Domiciliataire_Greffe': 'object',
        'Domiciliataire_Siren': 'object',
        'Date_Début_Activité':'object'
                         }

        reindex = [
        'Code Greffe', 'Nom_Greffe','Numero_Gestion','RCS_Registre',
        'Date_Greffe','Libelle_Evt','ID_Etablissement','siren',
        'Nom_Commercial','Enseigne','Date_Début_Activité',
        'Domiciliataire_Nom','Domiciliataire_Siren','count_initial_inpi',
        'Domiciliataire_Greffe','Domiciliataire_Complément','Type','Siège_PM',
        'Activité','Origine_Fonds','Origine_Fonds_Info','Type_Exploitation',
        'Pays','Ville','ncc','Code_Postal','Code_Commune',
        'Adresse_Ligne1','Adresse_Ligne2','Adresse_Ligne3','Adress_new',
        'Adresse_new_clean_reg','possibilite','INSEE','digit_inpi',
        'list_digit_inpi','len_digit_address_inpi',
        'Siege_Domicile_Représentant','Activité_Ambulante',
        'Activité_Saisonnière','Activité_Non_Sédentaire']

        dd_df_inpi = self.import_dask(file = self.inpi_etb,
            usecols = None,
            dtype=dtype)

        dd_df_inpi = dd_df_inpi.rename(columns={"Siren": "siren"}).compute()

        siren_fullna = dd_df_inpi.loc[
                      (dd_df_inpi['Adresse_Ligne1'].isin([np.nan]))
                     & (dd_df_inpi['Adresse_Ligne2'].isin([np.nan]))
                     & (dd_df_inpi['Adresse_Ligne3'].isin([np.nan]))
                     & (dd_df_inpi['Code_Postal'].isin([np.nan]))
                     & (dd_df_inpi['Ville'].isin([np.nan]))
                     & (dd_df_inpi['Code_Commune'].isin([np.nan]))
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
            (subset_inpi_cleaned
            .to_csv(r'data\input\INPI\inpi_etb_stock_{}.gz'.format(0
            ),
            compression='gzip', index = False))

            size_ = len(siren_inpi)
            print('Saving {} observations'.format(size_))
            siren_inpi.to_csv(
            r'data\input\SIREN_INPI\inpi_etb_stock_{}.gz'.format(
            0
            ),
            compression='gzip', index = False)
        if save_sql:
            print('Creating SQL database')
            query = "CREATE TABLE INPI \
(Code Greffe, Nom_Greffe,Numero_Gestion,RCS_Registre, \
 Date_Greffe,Libelle_Evt,ID_Etablissement,siren, Nom_Commercial, \
 Enseigne,Date_Début_Activité, Domiciliataire_Nom,Domiciliataire_Siren,\
 Domiciliataire_Greffe,Domiciliataire_Complément,Type,Siège_PM,\
 Activité,Origine_Fonds,Origine_Fonds_Info,Type_Exploitation,\
 Adresse_Ligne1,Adresse_Ligne2,Adresse_Ligne3,INSEE,digit_inpi,\
 Adress_new,Adresse_new_clean_reg, Code_Postal,\
 Ville,ncc,possibilite,Code_Commune,Pays,Siege_Domicile_Représentant,\
 count_initial_inpi,len_digit_address_inpi,\
 Activité_Ambulante,Activité_Saisonnière,Activité_Non_Sédentaire)"
            try:
                os.remove(r'App\SQL\inpi_origine.db')
                self.save_sql(
                df = subset_inpi_cleaned.drop(columns = ['list_digit_inpi',
                'index']),
                db = r'App\SQL\inpi_origine.db',
                table = 'INPI',
                query =query)
            except:
                self.save_sql(
                df = subset_inpi_cleaned.drop(columns = ['list_digit_inpi',
                'index']),
                db = r'App\SQL\inpi_origine.db',
                table = 'INPI',
                query =query)

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
        compression = 'gzip', dtype = {'siren':'object'})['siren'].to_list()

        subset_insee = (dd_df_insee
        .loc[dd_df_insee['siren'].isin(siren_inpi)]
        .loc[dd_df_insee['dateCreationEtablissement'] <= self.date_end]
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
            (subset_insee
            .assign(index = lambda x:
            x.index)
            .to_csv('data\input\insee_2017_{}.gz'.format(size_
            ),
            compression='gzip', index = False))

        if save_sql:

            print('Creating SQL database')
            query = "CREATE TABLE INSEE (\
            siren,siret,dateCreationEtablissement,count_initial_insee,\
         etablissementSiege,complementAdresseEtablissement,\
         numeroVoieEtablissement,indiceRepetitionEtablissement,\
         typeVoieEtablissement,libelleVoieEtablissement,\
         len_digit_address_insee,codePostalEtablissement,\
         libelleCommuneEtablissement,libelleCommuneEtrangerEtablissement,\
         distributionSpecialeEtablissement,codeCommuneEtablissement,\
         codeCedexEtablissement,libelleCedexEtablissement,\
         codePaysEtrangerEtablissement,libellePaysEtrangerEtablissement,\
         etatAdministratifEtablissement)"
            try:
                os.remove(r'App\SQL\App_insee.db')
                self.save_sql(
                df = subset_insee.drop(columns = ['list_digit_insee']),
                db = r'App\SQL\App_insee.db',
                table = 'INSEE',
                query =query)
            except:
                self.save_sql(
                df = subset_insee.drop(columns = ['list_digit_insee']),
                db = r'App\SQL\App_insee.db',
                table = 'INSEE',
                query =query)
