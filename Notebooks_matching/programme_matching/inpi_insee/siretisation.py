import json, os, re
from dask.diagnostics import ProgressBar
from dask.multiprocessing import get
import dask.dataframe as dd
import pandas as pd
import numpy as np
from nltk.corpus import stopwords
pbar = ProgressBar()
pbar.register()

class siretisation_inpi:
    def __init__(self, parameters = None):
        """
        Parametrisation du programme de siretisation des fichiers de l'INPI

        Args:
        - parameters: Dictionary, les "keys" sont les suivantes:
            - communes_insee: Path pour localiser le fichier des communes de
            France
            - insee: Path pour localiser le fichier de l'INSEE. Format gz
            - inpi_etb: Path pour localiser le fichier de l'INPI, etablissement.
            Format gz

        """
        insee_col = ['siren',
 'siret',
 'dateCreationEtablissement',
 'complementAdresseEtablissement',
 'numeroVoieEtablissement',
 'indiceRepetitionEtablissement',
 'typeVoieEtablissement',
 'libelleVoieEtablissement',
 'codePostalEtablissement',
 'libelleCommuneEtablissement',
 'libelleCommuneEtrangerEtablissement',
 'distributionSpecialeEtablissement',
 'codeCommuneEtablissement',
 'codeCedexEtablissement',
 'libelleCedexEtablissement',
 'codePaysEtrangerEtablissement',
 'libellePaysEtrangerEtablissement',
 'count_initial_insee']

        inpi_col =['siren',
        'index',
 'Type',
 'Adresse_Ligne1',
 'Adresse_Ligne2',
 'Adresse_Ligne3',
 'Code_Postal',
 'Ville',
 'Code_Commune',
 'Pays',
 'count_initial_inpi',
 'ncc']
        insee_dtype = {
    'siren': 'object',
    'siret': 'object',
    'dateCreationEtablissement': 'object',
    'complementAdresseEtablissement': 'object',
    'numeroVoieEtablissement': 'object',
    'indiceRepetitionEtablissement': 'object',
    'typeVoieEtablissement': 'object',
    'libelleVoieEtablissement': 'object',
    'codePostalEtablissement': 'object',
    'libelleCommuneEtablissement': 'object',
    'libelleCommuneEtrangerEtablissement': 'object',
    'distributionSpecialeEtablissement': 'object',
    'codeCommuneEtablissement': 'object',
    'codeCedexEtablissement': 'object',
    'libelleCedexEtablissement': 'object',
    'codePaysEtrangerEtablissement': 'object',
    'libellePaysEtrangerEtablissement': 'object',
    'count_initial_insee': 'int'
}

        inpi_dtype = {
    'siren': 'object',
    'index': 'object',
 'Type': 'object',
 'Adresse_Ligne1': 'object',
 'Adresse_Ligne2': 'object',
 'Adresse_Ligne3': 'object',
 'Code_Postal': 'object',
 'Ville': 'object',
 'Code_Commune': 'object',
 'Pays': 'object',
 'count_initial_inpi': 'int',
 'ncc': 'object',
}

        list_inpi = [
        'siren',
             'siret',
        'index',
             'Type',
             'Adresse_Ligne1',
             'Adresse_Ligne2',
             'Adresse_Ligne3',
             'Code_Postal',
             'Ville',
             'Code_Commune',
             'Pays',
             'ncc',
             '_merge']
        self.communes = parameters['communes_insee']
        self.insee = parameters['insee']
        self.insee_col = insee_col
        self.insee_dtype = insee_dtype
        self.inpi_etb = parameters['inpi_etb']
        self.inpi_col = inpi_col
        self.inpi_dtype = inpi_dtype
        self.list_inpi = list_inpi
        self.upper_word = pd.read_csv(parameters['upper_word']
        ).iloc[:,0].to_list()

    def import_dask(self, file, usecols = None, dtype=None):
        """
        """
        dd_df = dd.read_csv(file, usecols = usecols, dtype = dtype,
        blocksize=None,compression='gzip')

        return dd_df

    def log_detail(self, df_, option = 'left_only'):
        """
        option -> right_only ou left_only
        """
        log_ = {

    'total_match':[int(df_['_merge'].value_counts()['both']),
                   float(df_['_merge'].value_counts()['both']/df_.shape[0])
                  ],
    'total_unmatch':[int(df_['_merge'].value_counts()[option]),
                   float(df_['_merge'].value_counts()[option]/df_.shape[0])
                  ],
    'details_unmatch': {
        'Code_Postal':int(df_.loc[lambda x: x['_merge'].isin([option])].isna().sum()[['Code_Postal']][0]),
        'Code_Commune':int(df_.loc[lambda x: x['_merge'].isin([option])].isna().sum()[['Code_Commune']][0]),
    }
}
        return log_

    def match_unmatch(self, df_inpi_initial, df_inpi_mergeboth,
     step = '1_unique_siren',
                  to_csv = True):
        """
        """

        merge_ = (
        df_inpi_mergeboth
        .merge(df_inpi_initial,
               how='right',
               indicator=True)
    )

        match_ = merge_.loc[lambda x:
                       x['_merge'].isin(['both'])].drop(columns = '_merge')

        unmatch_ = merge_.loc[lambda x:
                       ~x['_merge'].isin(['both'])].drop(columns = ['_merge',
                                                                    'siret'])


        if to_csv:
            name_match = 'data/output/match_{}_{}.gz'.format(step, match_.shape[0])
            name_unmatch = 'data/input/unmatched/unmatch_{}_{}.gz'.format(
            step, unmatch_.shape[0])
            match_.to_csv(name_match, index = False, compression='gzip',)
            unmatch_.to_csv(name_unmatch, index = False,compression='gzip')

        return unmatch_

    def create_split_adress(self,x):
        """
        """
        split_ = x.str.split().to_list()
        return  split_


    def create_regex_adress(self, x):
        """
        """
        try:
            split_ = [i + "$" for i in x]
            reg = '|'.join(split_)
        except:
            reg = np.nan
        return  reg

    def find_regex(self,regex, test_str, siret):
        """
        """
        try:
            matches = re.search(regex, test_str)
            if matches:
                return siret
            else:
                return np.nan
        except:
            return np.nan

    def index_marks(self, nrows, chunk_size):

        df_ = range(1 * chunk_size, (nrows // chunk_size + 1) *
        chunk_size, chunk_size)

        return df_

    def split(self,dfm, chunk_size):
        """
        """

        indices = self.index_marks(dfm.shape[0], chunk_size)
        return np.split(dfm, indices)



    def prepare_adress(self, df):
        """
        """
        temp_adresse = df.compute().assign(

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
        temp_adresse['Adresse_new_clean'] = (temp_adresse['Adresse_new_clean']
                                            .apply(lambda x:
                                                   ' '.join([word for word in
                                                             str(x).split() if
                                                             word not in
                                                             (self.upper_word)]))
                                            )

        temp_adresse = temp_adresse.assign(
        Adresse_new_clean_split=lambda x:
        self.create_split_adress(x['Adresse_new_clean'])
    )

        temp_adresse['Adresse_new_clean_reg'] = \
        temp_adresse['Adresse_new_clean_split'].apply(lambda x:
                                                     self.create_regex_adress(x))

        temp_adresse = temp_adresse.drop(columns = ['Adresse_new_clean',
                                                'Adresse_new_clean_split'])

        chunks = self.split(temp_adresse, 60000)
        try:
            for i in range(0, 15):
                chunks[i].to_csv(
            r'Data\input\unmatched\chunk\chunk_{}.gz'.format(i),
                index = False, compression ='gzip')
        except:
            pass

    def clean_commune(self):
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
        regex = "CEDEX|cedex|Cedex|\([^)]*\)|/\s\s+/|^\d+\s|\s\d+\s|\s\d+$|\d+|\.|\--|COMMUNE DE |COMMUNE DE|commune de |commune de|Commune de |Commune de |\s$"
        ### Voir si on peut faire avec Dask
        test_adress = pd.read_csv(self.inpi_etb, low_memory=False)
        communes = pd.read_csv(self.communes, low_memory=False)

        test_adress['test'] =test_adress['Ville'].str.extract(r'(\d+)')
        test_adress['Ville_clean'] = test_adress['Ville'].str.replace(regex,'')
        test_adress['Ville_clean'] = test_adress['Ville_clean'].str.replace('\s$|\s^',
                                                                    '')

        ### arrondissement
        test_adress['ville2'] = np.where(
    np.logical_and(
         ~test_adress['test'].isin([np.nan]),
        test_adress['test'].str.len() <=2
    )
   ,
    test_adress['Ville_clean'] + '' + test_adress['test'].astype(str),
    test_adress['Ville_clean']
)

        test_adress['Ville_upper'] = test_adress['Ville_clean'].str.upper()

        test_adress = test_adress.merge(communes,
                         left_on='ville2',
                         right_on='possibilite',
                         how='left',
                         indicator=True)

        test_adress = pd.concat([
    test_adress.loc[lambda x: x['_merge'].isin(['both'])],
    (test_adress
     .loc[lambda x: x['_merge'].isin(['left_only'])]
     .drop(columns=['ncc', 'possibilite', '_merge'])
     .merge(communes,
            left_on='Ville_upper',
            right_on='possibilite',
            how='left',
            indicator=True)
     )
])

        test_adress = pd.concat([
    test_adress.loc[lambda x: x['_merge'].isin(['both'])],
    (test_adress
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


        ### Creation log
        log_commune = {

    'total_match':[int(test_adress['_merge'].value_counts()['both']),
                   float(test_adress['_merge'].value_counts()['both']/test_adress.shape[0])
                  ],
    'total_unmatch':[int(test_adress['_merge'].value_counts()['left_only']),
                   float(test_adress['_merge'].value_counts()['left_only']/test_adress.shape[0])
                  ],
    'details_unmatch': {
        'Code_Postal':int(test_adress.loc[lambda x: x['_merge'].isin(['left_only'])].isna().sum()[['Code_Postal']][0]),
        'Code_Commune':int(test_adress.loc[lambda x: x['_merge'].isin(['left_only'])].isna().sum()[['Code_Commune']][0]),
    }
}
        with open(r'data\logs\commune.json', 'w') as outfile:
                json.dump(log_commune, outfile)
        test_adress = test_adress.drop(columns = ['test', 'Ville_clean', 'ville2', 'Ville_upper',
        'possibilite', '_merge', 'noaccent'])
        return test_adress

    def match_unique_etb(self, df_ets):
        """


        """
        insee = self.import_dask(file = self.insee,
        usecols = self.insee_col, dtype= self.insee_dtype)

        inpi = self.import_dask(file = df_ets,
        usecols = self.inpi_col, dtype=self.inpi_dtype)

        m1_unique = (
        insee.loc[insee['count_initial_insee'].isin([1])]
        .merge(inpi.loc[inpi['count_initial_inpi'].isin([1])],
               how='right', indicator=True)
    )

        unmatched = self.match_unmatch(
        df_inpi_initial=inpi.compute(),
        df_inpi_mergeboth=(m1_unique.compute()
                       .reindex(columns=self.list_inpi)
                       .loc[lambda x:
                            x['_merge'].isin(['both'])]
                       .drop(columns=['_merge'])),
        step='1_unique_siren',
        to_csv=True)

        log_ = self.log_detail(m1_unique, option = 'right_only')
        with open(r'data\logs\1_unique_siren.json', 'w') as outfile:
                json.dump(log_, outfile)

        return unmatched

    def merge_siren_candidat(self,
    df_input, regex_go = False, matching_voie =False,
    option=['ncc', 'libelleCommuneEtablissement']):
        """
        option list can only be one of these:
        - ['ncc', 'libelleCommuneEtablissement']
        - ['ncc', 'libelleCommuneEtablissement']
        - ['ncc', 'libelleCommuneEtablissement']
        """
        insee = self.import_dask(file=self.insee,
                        usecols=self.insee_col, dtype=self.insee_dtype)

        if '_merge' in df_input.columns:
            df_input = (df_input
                    .drop(columns=['siret',
                                   'numeroVoieEtablissement',
                                   'libelleVoieEtablissement',
                                   'codePostalEtablissement',
                                   'libelleCommuneEtablissement',
                                   'codeCommuneEtablissement',
                                   '_merge']))
        if 'count_insee' in df_input.columns:
            df_input = (df_input
                    .drop(columns=['count_insee']))


        insee = insee.merge(
        (insee
         .groupby('siren')['siren']
         .count()
         .rename('count_insee')
         .reset_index())
    )

        temp = df_input.merge(insee,
                          how='left',
                          left_on=['siren', option[0]],
                          right_on=['siren',  option[1]],
                          indicator=True,
                          suffixes=['_insee', '_inpi'])

        to_check = temp[temp['_merge'].isin(['both'])]
        nomatch = temp[~temp['_merge'].isin(['both'])]

        if regex_go:
            to_check['siret_test1'] = to_check.map_partitions(
            lambda df:
                df.apply(lambda x:
                    self.find_regex(
                     x['Adresse_new_clean_reg'],
                     x['libelleVoieEtablissement'],
                     x['siret']), axis=1)
    )
            to_check = to_check.dropna(subset=['siret_test1']).compute()
            group_option = 'Adress_new'
            if matching_voie:
                to_check['digit_inpi'] = \
                to_check['Adress_new'].str.extract(r'(\d+)')

                to_check['test'] = np.where(
                to_check['digit_inpi'] ==
                to_check['numeroVoieEtablissement'],
                True, False)

                to_check = to_check[to_check['test'].isin([True])]
                group_option = 'numeroVoieEtablissement'
        else:
            group_option = option[1]
            to_check = to_check.compute()

    # calcul le nombre cas de figure 2 -> très conservative

        test_match = (to_check
                  .merge(
                      (to_check
                       .groupby(['siren', group_option])['siren']
                       .count()
                       .rename('count_inpi')
                       .reset_index()
                       )
                  )
                  )

        if matching_voie:
            true_match = (test_match
                  .loc[lambda x:
                       (x['count_inpi'] == 1)
                      |
                      (x['count_insee'].isin([1])
                      & ~x['count_inpi'].isin([1]))]
                  .reindex(columns=self.list_inpi))
        else:
            true_match = (test_match
                  .loc[lambda x:x['count_inpi'] == 1]
                  .reindex(columns=self.list_inpi))

        dic_ = {
        'true_match': true_match,
        'unmatch': nomatch
    }

        return dic_
