import json, os, re
from dask.diagnostics import ProgressBar
from dask.multiprocessing import get
import dask.dataframe as dd
import pandas as pd
import numpy as np
from nltk.corpus import stopwords
pbar = ProgressBar()
pbar.register()


 ################
class siretisation_inpi:
    def __init__(self, parameters = None):
        """
        Parametrisation du programme de siretisation des fichiers de l'INPI

        Args:
        - parameters: Dictionary, les "keys" sont les suivantes:
            - insee: Path pour localiser le fichier de l'INSEE. Format gz
            - inpi_etb: Path pour localiser le fichier de l'INPI, etablissement.
            Format gz

        """
        self.insee = parameters['insee']
        self.inpi_etb = parameters['inpi_etb']
        #self.insee_col = insee_col
        #self.insee_dtype = insee_dtype

        #self.inpi_col = inpi_col
        #self.inpi_dtype = inpi_dtype
        #self.list_inpi = list_inpi

    def import_dask(self, file, usecols = None, dtype=None):
        """
        Import un fichier gzip ou csv en format Dask

        Deja dans preparation data

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


    def merge_siren_candidat(self,
    df_input, regex_go = False, matching_voie =False,relax_regex = False,
    siege_etat=False, option=['ncc', 'libelleCommuneEtablissement'],
    var_adress_insee = 'libelleVoieEtablissement'):
        """
        option list can only be one of these:
        - ['ncc', 'libelleCommuneEtablissement']
        - ['Code_Postal', 'codePostalEtablissement']
        - ['Code_Commune', 'codeCommuneEtablissement']
        var_adress_insee: libelleVoieEtablissement ou
        complementAdresseEtablissement.exemple siren  322385949
        """
        insee_col = ['siren',
         'siret',
         'dateCreationEtablissement',
         "etablissementSiege",
         "etatAdministratifEtablissement",
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

        insee_dtype = {
             'siren': 'object',
             'siret': 'object',
             "etablissementSiege": "object",
             "etatAdministratifEtablissement": "object",
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


        insee = self.import_dask(
        file=self.insee,
        usecols=insee_col,
        dtype=insee_dtype)

        if '_merge' in df_input.columns:
            try:
                df_input = (df_input
                    .drop(columns=['siret',
                                   'numeroVoieEtablissement',
                                   'libelleVoieEtablissement',
                                   'codePostalEtablissement',
                                   'libelleCommuneEtablissement',
                                   'codeCommuneEtablissement',
                                   'complementAdresseEtablissement',
                                   'count_insee',
                                   '_merge']))
            except:
                df_input = (df_input
                    .drop(columns=['siret',
                                   'numeroVoieEtablissement',
                                   'libelleVoieEtablissement',
                                   'codePostalEtablissement',
                                   'libelleCommuneEtablissement',
                                   'codeCommuneEtablissement',
                                   'complementAdresseEtablissement',
                                   '_merge']))

        if siege_etat:
            ### On retire les etb fermées et les etb non principaux
            insee = insee.loc[
            (insee['etablissementSiege'].isin(['true']))
            & (insee['etatAdministratifEtablissement'].isin(['A']))
            ]

            df_input = df_input.loc[df_input['Type'].isin(['PRI', 'SEP'])]
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

        return to_check

        if regex_go:
            if relax_regex:

                to_check['Adresse_new_clean_reg'] = \
                to_check['Adresse_new_clean_reg'].str.replace('$', '')


            to_check['siret_test1'] = to_check.map_partitions(
            lambda df:
                df.apply(lambda x:
                    self.find_regex(
                     x['Adresse_new_clean_reg'],
                     x[var_adress_insee],
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
