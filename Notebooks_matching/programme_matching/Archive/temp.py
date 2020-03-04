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

                   chunks = self.split(temp_adresse, 60000)
                   try:
                       for i in range(0, 15):
                           chunks[i].to_csv(
                       r'Data\input\unmatched\chunk\chunk_{}.gz'.format(i),
                           index = False, compression ='gzip')
                   except:
                       pass


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
