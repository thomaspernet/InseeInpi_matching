import pandas as pd
import re, os, glob, boto3, json, datetime, logging, io
#from sagemaker import get_execution_role
import dask.dataframe as dd
#import pymongo as pm
from botocore.exceptions import ClientError
from pyathena import connect

class aws_instantiate():
    def __init__(self, credential):
        #self.client = pm.MongoClient()
        self.credential = credential
        #self.bucket = 'creditsafeoptimum'
        #self.folder_bucket = 'data'

    def load_credential(self):
        """
        """
        filename, file_extension = os.path.splitext(self.credential)
        if file_extension== '.csv':
            load_cred = pd.read_csv(self.credential)

            key = load_cred.iloc[0, 1]
            secret_ = load_cred.iloc[1, 1]
        
        else:
            with open(self.credential) as json_file:
                data = json.load(json_file)
                
                key = data['aws_access_key_id']
                secret_ = data['aws_secret_access_key']

        return key, secret_

    def client_boto(self, option = 'client'):
        """
        """

        key, secret_ = self.load_credential()

        if option == 'client':
            client_ = boto3.client(
                's3',
                aws_access_key_id=key,
                aws_secret_access_key=secret_,
                region_name = 'eu-west-3'
                )
        elif option == 'resource':
            client_ = boto3.resource(
                's3',
                aws_access_key_id=key,
                aws_secret_access_key=secret_,
                region_name = 'eu-west-3'
                )

        elif option == 'athena':
            client_ = boto3.client(
                'athena',
                aws_access_key_id=key,
                aws_secret_access_key=secret_,
                region_name = 'eu-west-3'
                )
        return client_

    def download_file(self, bucket, file_name):
        """
        """
        #if subfolder is None:
        paths3 = '{}/{}'.format(bucket, file_name)

        client_boto = self.client_boto()

    # Upload the file
        try:
            response = client_boto.download_file(
                self.bucket,paths3, file_name)
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def upload_file(self, bucket, file_name, subfolder=None):
        """Upload a file to an S3 bucket

        :param file_name: File to upload
        :param bucket: Bucket to upload to
        :param object_name: S3 object name. If not specified then file_name is used
        :return: True if file was uploaded, else False
        """

        # If S3 object_name was not specified, use file_name
        if subfolder is None:
            subfolder = file_name

        client_boto = self.client_boto()

    # Upload the file
        try:
            response = client_boto.upload_file(
                file_name, bucket, subfolder)
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def create_folder(self, directory_name):
        """
        """
        try:
            self.client_boto().put_object(Bucket=self.bucket,
              Key=(directory_name))
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def copy_object_s3(self, filename, destination):
        """
        filename -> include subfolder+filaneme
        ex: 'data/MR01_R_20200103.gz'
        destination: -> include subfolder+filaneme
        ex: 'data_sql/MR01_R_20200103.gz'
        """

        copy_source = {
        'Bucket': self.bucket,
        'Key': filename
 }
        try:
            self.client_boto(option = 'resource').meta.client.copy(
            copy_source,
             self.bucket,
              destination)
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def run_query(self, query, database, s3_output):
        """

        s3_output -> 'output_sql'
        """


        full_s3_output = 's3://{0}/{1}/'.format(self.bucket, s3_output)

        client = self.client_boto('athena')
        response = client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': database
                },
            ResultConfiguration={
            'OutputLocation': full_s3_output,
            }
        )
        print('Execution ID: ' + response['QueryExecutionId'])
        return response

    def remove_all_bucket(self, bucket, path_remove):
        """
        """
        try:
            my_bucket = self.client_boto(option = 'ressource').Bucket(
                bucket)
            for item in my_bucket.objects.filter(Prefix=path_remove):
                item.delete()
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def query_to_df(self, query, s3_output):
        """

        s3_output -> 'output_sql'
        """


        s3_output = 's3://{}/{}/'.format(self.bucket, s3_output)

        key, secret_ = self.load_credential()

        conn = connect(aws_access_key_id=key,
               aws_secret_access_key=secret_,
               s3_staging_dir=s3_output,
               region_name='eu-west-3')

        df = pd.read_sql(query, conn)

        return df

    def load_df_dd(self, bucket, path_file, dtypes = None, usecols = None,
                   pandas = True, cloud = False):
        """
        Upload file to Sagemaker as a Pandas dataframe
        using Dask for lazy computation
        args:
        pathfile: list path where the filename is located in S3,
        including filename
        ex: path_file = 'data/LUCM01_R_20200103.gz'
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
        if cloud:
            data_location = 's3://{}/{}'.format(bucket, path_file)
        else:
            data_location = path_file

        #pathtofile = "{}/{}".format(self.instance_aws,path_file)

        if file_extension ==  '.csv':
            if pandas:
                df_dask= pd.read_csv(data_location,
                                     usecols = usecols,
                                     dtype = dtypes,
                                     blocksize=None,
                                     low_memory=False
                                )
            else:
                df_dask= dd.read_csv(data_location,
                                 usecols = usecols,
                                 dtype = dtypes,
                                 blocksize=None,
                                 low_memory=False
                            )
        else:
            if pandas:
                df_dask= pd.read_csv(data_location,
                                 usecols = usecols,
                                 dtype = dtypes,
                                 blocksize=None,
                                 compression='gzip',
                                 low_memory=False
                            )
            else:
                df_dask= dd.read_csv(data_location,
                                 usecols = usecols,
                                 dtype = dtypes,
                                 blocksize=None,
                                 compression='gzip',
                                 low_memory=False
                            )
        return df_dask
    
    def read_df_from_s3(self, bucket, key):
        """
        key is the key in S3
        """
        obj = self.client_boto(option = 'resource').Object(bucket, key)
        
        body = obj.get()['Body'].read()
        
        df_ = pd.read_csv(
            io.BytesIO(body),
            sep = ';',
            error_bad_lines=False)
    
        return df_

