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

    def import_dask(self, file, usecols = None, dtype=None, parse_dates = False):
        """
        Import un fichier gzip ou csv en format Dask

        Deja dans preparation data

        Args:
        - file: String, Path pour localiser le fichier, incluant le nom et
        l'extension
        - usecols: List: les noms des colonnes a importer. Par defaut, None
        - dtype: Dictionary: La clé indique le nom de la variable, la valeur
        indique le type de la variable
        - parse_dates: bool or list of int or names or list of lists or dict,
         default False
        """
        extension = os.path.splitext(file)[1]
        if usecols == None:
            low_memory = False
        else:
            low_memory = True
        if extension == '.gz':
            dd_df = dd.read_csv(file, usecols = usecols, dtype = dtype,
        blocksize=None,compression='gzip', low_memory = low_memory,
        parse_dates = parse_dates)
        else:
            dd_df = dd.read_csv(file, usecols = usecols, dtype = dtype,
        blocksize=None, low_memory = low_memory,parse_dates = parse_dates)

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

    def find_regex(self,regex, test_str):
        """
        """
        try:
            matches = re.search(regex, test_str)
            if matches:
                return True
            else:
                return False
        except:
            return False

    def index_marks(self, nrows, chunk_size):

        df_ = range(1 * chunk_size, (nrows // chunk_size + 1) *
        chunk_size, chunk_size)

        return df_

    def split(self,dfm, chunk_size):
        """
        """

        indices = self.index_marks(dfm.shape[0], chunk_size)
        return np.split(dfm, indices)

    def split_duplication(self, df):
        """
        """
        if 'count_duplicates_' in df.columns:
            df = df.drop(columns = 'count_duplicates_')

        df = df.merge(
            (df
                .groupby('index')['index']
                .count()
                .rename('count_duplicates_')
                .reset_index()
                )
                )
        try:
            df = df.compute()
        except:
            pass

        dic_ = {
            'not_duplication':df[df['count_duplicates_'].isin([1])],
            'duplication' : df[~df['count_duplicates_'].isin([1])],
            'report_dup':df[
            ~df['count_duplicates_'].isin([1])
            ]['count_duplicates_'].value_counts()
            }

        return dic_

    def create_test(self,left_on, right_on,df_input):
        """
        Le calcul DAsk se fait dans la focntion split_duplication
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
             #'dateCreationEtablissement': 'object',
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
        dtype=insee_dtype,
        parse_dates = ['dateCreationEtablissement'])

        temp = df_input.merge(insee,
                          how='left',
                          left_on=left_on,
                          right_on= right_on,
                          indicator=True,
                          suffixes=['_insee', '_inpi'])

        to_check = temp[temp['_merge'].isin(['both'])].drop(columns= '_merge')
        nomatch = temp[~temp['_merge'].isin(['both'])].drop(columns= '_merge')

        ### Solution temporaire
        to_check["Date_Début_Activité"] = \
        to_check["Date_Début_Activité"].map_partitions(
        pd.to_datetime,
        format='%Y/%m/%d',
        errors = 'coerce',
        meta = ('datetime64[ns]')
        )

        test_1 = self.split_duplication(df = to_check)
        #df_no_duplication = pd.DataFrame()
        #df_duplication = pd.DataFrame()
        ### On aggège les "no_duplications" et "duplication"


        # Test 1: doublon -> non
        #test_1['not_duplication'].to_csv(
        #r"data\input\TESTS\test1_nodup_{}.gz".format(
        #test_1['not_duplication'].shape[0]), compression ="gzip")

        test_1['not_duplication'] = test_1['not_duplication'].assign(
        origin = 'test_1_no_duplication'
        )


        ## Test 2: Date equal -> oui
        test_2_oui = test_1['duplication'][
        (test_1['duplication']['Date_Début_Activité'] ==
                     test_1['duplication']['dateCreationEtablissement'])
                     ]
        ### Test 2: Date equal -> oui, Test 2 bis: doublon
        test_2_bis = self.split_duplication(df = test_2_oui)

        #### Test 2: Date equal -> oui, Test 2 bis: doublon: non
        #test_2_bis['not_duplication'].to_csv(
        #r"data\input\TESTS\test2_nodup_{}.gz".format(
        #test_2_bis['not_duplication'].shape[0]), compression ="gzip")
        test_2_bis['not_duplication'] = test_2_bis['not_duplication'].assign(
        origin = 'test_2_no_duplication'
        )

        #### Test 2: Date equal -> oui, Test 2 bis: doublon: oui
        #test_2_bis['duplication'].to_csv(
        #r"data\input\TESTS\test2_dup_{}.gz".format(
        #test_2_bis['duplication'].shape[0]), compression ="gzip")

        test_2_bis['duplication'] = test_2_bis['duplication'].assign(
        origin = 'test_2_duplication'
        )

        ## Test 2: Date equal -> non
        ### Test 2: Date equal -> non -> test 3: Date sup -> oui
        test_3_oui = test_1['duplication'].loc[
        (test_1['duplication']['dateCreationEtablissement'] >
        test_1['duplication']['Date_Début_Activité'])
        & (~test_1['duplication']['index'].isin(test_2_oui['index'].to_list()))
        ]

        ##### Test 2: Date equal -> non -> test 3: Date sup -> oui
        ##### Test 3 bis: doublon:
        test_3_oui_bis = self.split_duplication(df = test_3_oui)

        ###### Test 3 bis: doublon: non
        #test_3_oui_bis['not_duplication'].to_csv(
        #r"data\input\TESTS\test3_nodup_{}.gz".format(
        #test_3_oui_bis['not_duplication'].shape[0]), compression ="gzip")
        test_3_oui_bis['not_duplication'] = \
        test_3_oui_bis['not_duplication'].assign(
         origin = 'test_3_no_duplication'
         )

        ###### Test 3 bis: doublon:oui
        #test_3_oui_bis['duplication'].to_csv(
        #r"data\input\TESTS\test3_nodup_{}.gz".format(
        #test_3_oui_bis['duplication'].shape[0]), compression ="gzip")
        test_3_oui_bis['duplication'] = \
        test_3_oui_bis['duplication'].assign(
         origin = 'test_3_duplication'
         )

        ### Append to dataframes
        df_no_duplication = pd.concat([
        test_1['not_duplication'],
        test_2_bis['not_duplication'],
        test_3_oui_bis['not_duplication']
        ], axis = 0)

        df_duplication = pd.concat([
        test_2_bis['duplication'],
        test_3_oui_bis['duplication']
        ], axis =0)

        ###### Test 3: Date equal -> non -> test 3: Date sup -> non
        test_3_non = test_1['duplication'].loc[
        (~test_1['duplication']['index'].isin(
        test_2_oui['index'].to_list()+
        test_3_oui['index'].to_list()
        )
        )
        ]

        test_3_non.to_csv(
        r"data\input\TESTS\test3_specialtreat_{}.gz".format(
        test_3_non.shape[0]
        ), compression ="gzip")

        ### SAve in case of
        df_no_duplication.to_csv(
        r"data\input\TESTS\df_no_duplication_{}.gz".format(
        df_no_duplication.shape[0]
        ), compression ="gzip")

        df_duplication.to_csv(
        r"data\input\TESTS\df_duplication_{}.gz".format(
        df_duplication.shape[0]
        ), compression ="gzip")

        return (df_no_duplication, df_duplication)

    def step_two_assess_test(self, df):
        """
        """
        ## Calcul nb siren/siret
        df_ = (df
        .merge(
        (df
        .groupby([
        'siren','ncc',
        'Code_Postal','Code_Commune',
        'INSEE','digit_inpi'])['siren']
             .count()
             .rename('count_siren_siret')
             .reset_index()
             ),how = 'left'
             )
             )

        ## Test 1: address
        df_ = dd.from_pandas(df_, npartitions=10)
        df_['test_address_libelle'] = df_.map_partitions(
            lambda df:
                df.apply(lambda x:
                    self.find_regex(
                     x['Adresse_new_clean_reg'],
                     x['libelleVoieEtablissement']), axis=1)
                     ).compute()

        df_['test_address_complement'] = df_.map_partitions(
            lambda df:
                df.apply(lambda x:
                    self.find_regex(
                     x['Adresse_new_clean_reg'],
                     x['complementAdresseEtablissement']), axis=1)
                     ).compute()

        df_ = df_.compute()

        ## test join Adress
        df_.loc[
        (df_['test_address_libelle'] == True)
        &(df_['test_address_complement'] == True),
        'test_join_address'] = True

        df_.loc[
        (df_['test_join_address'] != True),
        'test_join_address'] = False

        ## Test 2: Date
        df_.loc[
        (df_['dateCreationEtablissement'] >=
        df_['Date_Début_Activité'])
        | (df_['Date_Début_Activité'].isin([np.nan]))
        | (df_['count_siren_siret'].isin([1])
        & df_['count_initial_insee'].isin([1])),
        'test_date'] = True

        df_.loc[df_['test_date'].isin([np.nan]),'test_1'] = False

        ## Test 3: siege
        df_['test_siege'] = np.where(
        np.logical_and(
        df_['Type'].isin(['SEP', 'SIE']),
        df_['etablissementSiege'].isin(['true'])
        ),
        True, False
        )

        ## Test 4: voie
        df_.loc[
        df_['INSEE'] == df_['typeVoieEtablissement'],
        'test_voie'
        ] = True

        df_.loc[
        df_['INSEE'] != df_['typeVoieEtablissement'],
        'test_voie'
        ] = False

        ## Test 5: numero voie
        df_.loc[
        df_['digit_inpi'] == df_['numeroVoieEtablissement'],
        'test_numero'
        ] = True

        df_.loc[
        df_['digit_inpi'] != df_['numeroVoieEtablissement'],
        'test_numero'
        ] = False

        ## Final test: count unique index
        df_ = df_.merge(
        (df_
        .groupby('index')['index']
        .count()
        .rename('count_duplicates_final')
        .reset_index()
        )
        )

        return df_

    def step_two_duplication(self, df_duplication):
        """
        """
        duplicates_ = self.step_two_assess_test(df = df_duplication)
        df_not_duplicate = pd.DataFrame()
        copy_duplicate = duplicates_.copy()

        for i in ['test_join_address','test_address_libelle',
         'test_address_complement']:
         ### split duplication
            test_1 = self.split_duplication(
            copy_duplicate[
            copy_duplicate[i].isin([True])]
    )

            ### append unique
            df_not_duplicate = (
            df_not_duplicate
            .append(test_1['not_duplication']
            .assign(test = i)
            )
            )

            copy_duplicate = (copy_duplicate
                   .loc[~copy_duplicate['index'].isin(
                       pd.concat([
                           test_1['duplication'],
                           test_1['not_duplication']
                       ], axis = 0)['index']
                       .drop_duplicates())])

        (df_not_duplicate
        .to_csv(r'data\output\not_duplicate_{}.gz'.format(
        df_not_duplicate.shape[0]),
        compression = 'gzip'))

        # Special treatment
        sp = (duplicates_[
        ~duplicates_['index']
        .isin(df_not_duplicate['index'])])

        (sp.to_csv(r'data\output\special_treatment_{}.gz'.format(
        sp.shape[0]),
        compression = 'gzip'))

    def merge_siren_candidat(self,left_on, right_on,
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
             #'dateCreationEtablissement': 'object',
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
        dtype=insee_dtype,
        parse_dates = ['dateCreationEtablissement'])

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
                          left_on=left_on,
                          right_on= right_on,
                          indicator=True,
                          suffixes=['_insee', '_inpi'])

        to_check = temp[temp['_merge'].isin(['both'])]
        nomatch = temp[~temp['_merge'].isin(['both'])]

        ### Prepare les tests


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
