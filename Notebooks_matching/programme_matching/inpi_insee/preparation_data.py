import json, os, re
from dask.diagnostics import ProgressBar
from dask.multiprocessing import get
import dask.dataframe as dd
import pandas as pd
import numpy as np
from nltk.corpus import stopwords
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

        """
        self.insee = parameters['insee']
        self.date_end = parameters['date_end']

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
        if == '.gz':
            dd_df = dd.read_csv(file, usecols = usecols, dtype = dtype,
        blocksize=None,compression='gzip')
        else:
            dd_df = dd.read_csv(file, usecols = usecols, dtype = dtype,
        blocksize=None)

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

    def normalize_insee(self):
            """
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
            "libellePaysEtrangerEtablissement": 'object'
                                 }

            dd_df_insee = self.import_dask(file = self.insee,
            usecols = usecols,
            dtype=dtype)

            subset_insee = (dd_df_insee
            .loc[data_insee_['dateCreationEtablissement'] <= self.date_end]
            .assign(
            libelleCommuneEtablissement = lambda x:
            x['libelleCommuneEtablissement'].str.replace('-', ' ')
                )
               )

            subset_insee = nombre_siret_siren(df_dd = subset_insee,
             origin = 'INSEE')
