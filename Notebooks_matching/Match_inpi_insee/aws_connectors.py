import pandas as pd
import re, os, glob, boto3, json, datetime
from sagemaker import get_execution_role
import dask.dataframe as dd

class aws_instantiate:
    def __init__(self, instance_aws, bucket = None):
        """
        instance_aws: Instance name:
            format -> 'https://calfdata.s3.eu-west-3.amazonaws.com'
        bucket: Bucket name
        """
        self.instance_aws = instance_aws
        self.bucket = bucket
        self.s3 = boto3.resource('s3')

    def uploadFileBucket(self,pathfile):
        """
        Download file from S3 bucket

        args:
        pathfile: path where the filename is located in S3,
        including filename
        ex: INPI/TC_1/Stock/dtypes_stock.json
        """

        regex = r"([^\/]+$)"
        matches = re.search(regex, pathfile)
        self.s3.meta.client.download_file(
            self.bucket,
            pathfile,
            matches.group()
        )

    def uploadBacthFileBucket(self,pathfiles):
        """
        Download file from S3 bucket

        args:
        pathfile: list path where the filename is located in S3,
        including filename
        ex: INPI/TC_1/Stock/dtypes_stock.json
        """

        regex = r"([^\/]+$)"
        for file in pathfiles:
            matches = re.search(regex, file)
            self.s3.meta.client.download_file(
                self.bucket,
                file,
                matches.group()
        )
            print('File {} downloaded'.format(file))

    def load_df_dd(self, path_file, dtypes = None, usecols = None):
        """
        Upload file to Sagemaker as a Pandas dataframe
        using Dask for lazy computation

        args:
        pathfile: list path where the filename is located in S3,
        including filename
        ex: INSEE/Stock/ETS/StockEtablissement_utf8.csv
        dtypes: Type name or dict of column -> type, optional
        usecols: Return a subset of the columns.
        If list-like, all elements must either be positional
        (i.e. integer indices into the document columns)
        or strings that correspond to column names provided either
        by the user in names or inferred from the document header row(s).

        returns:
        Pandas Dataframe

        """
        ### file extension

        filename, file_extension = os.path.splitext(path_file)


        pathtofile = "{}/{}".format(self.instance_aws,path_file)

        if file_extension ==  '.csv':
            df_dask= dd.read_csv(pathtofile,
                                 usecols = usecols,
                                 dtype = dtypes,
                                 blocksize=None,
                                 low_memory=False
                            )
        else:
            df_dask= dd.read_csv(pathtofile,
                                 usecols = usecols,
                                 dtype = dtypes,
                                 blocksize=None,
                                 compression='gzip',
                                 low_memory=False
                            )

        return df_dask