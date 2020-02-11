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
            
    def url_instance_bucket(self, path_file):
        """
        """
        pathtofile = "{}/{}".format(self.instance_aws,path_file)
        
        return pathtofile

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
    
    def load_insee(self, option ='etb', for_matching = True):
        """
        Load INSEE data from S3 bucket
        
        args:
        option: string: etb or ul
        for_matching: Boolean. If True, import only variables 
        candidates for matchibng
        
        return 
        pandas dataframe
        
        """
        path_file = "INSEE/Stock/ETS/StockEtablissement_utf8.csv"
        pathtofile = "{}/{}".format(self.instance_aws,path_file)
        dtype={'codeCommune2Etablissement': 'object',
       'codeCommuneEtablissement': 'object',
       'codePostalEtablissement': 'float64',
       'complementAdresse2Etablissement': 'object',
       'enseigne2Etablissement': 'object',
       'enseigne3Etablissement': 'object',
       'indiceRepetition2Etablissement': 'object',
       'libelleCommuneEtrangerEtablissement': 'object',
       'libellePaysEtrangerEtablissement': 'object',
       'numeroVoieEtablissement': 'object'}
        
        if for_matching:
            usecols = ['siren', 'siret']
        
            df_dask= dd.read_csv(pathtofile,
                                 usecols = usecols,
                                 dtype = dtype,
                                 blocksize=None,
                                 low_memory=False
                            )
        else:
            df_dask= dd.read_csv(pathtofile,
                                 #usecols = usecols,
                                 dtype = dtype,
                                 blocksize=None,
                                 low_memory=False
                            )
        
        return df_dask

    def save_to_s3(self, file_name, file_path):
        """
        Save file from S3 bucket

        args:
        
            file_name : file name ex: dtypes_stock.json
        
            file_path: full path where the file should be saved in S3,
            including filename
            ex: INPI/TC_1/Stock/dtypes_stock.json
        
        Return: Nothing
        """
        
        #Upload a file to an S3 bucket

        self.s3.meta.client.upload_file(
            file_name,
            self.bucket,
            file_path
        )
        
        # remove file from workspace
        os.remove(file_name)

    def gen_matching_stats_json(self, source, nature, origin, source_path, save_to_path, data_merged, save):
        """
        Generate json including matching stats :
    
        json_ = {
            'nature': , 
            'origin': ,
            'path': ,
            "details": {
                'total_rows_origin':,
                'total_match': ,
                'total_unmatched_left':,
                'total_unmatched_right':,
                },
            }

        args:
            source: source of the match data (ex: 'insee')
            nature: inpi_nature, 
            origin: inpi_origin,
            source_path: path of the source data,
            save_to_path : relative path where the file should be saved,
            data_merged: dataframe with merge result,
            save: should the file be saved to s3 (yes/no)
        
        Return: String content for json
        """
        
        # define save_to paths
        json_filename = "{}_{}_{}.{}".format(source,nature,origin,'json') 
        json_absolute_filepath =  "{}/{}".format(save_to_path,json_filename)
        
        # prepare json file
        
        merge_result = data_merged.groupby('_merge')['_merge'].count()
    
        json_ = {
            'nature': nature, 
            'origin': origin,
            'path': source_path,
            "details": {
                'total_rows_origin':str(len(data_merged)),
                'total_match': str(merge_result.both),
                'total_unmatched_left':str(merge_result.left_only),
                'total_unmatched_right':str(merge_result.right_only),
                },
            }

        with open(json_filename,
                    'w') as outfile:
            json.dump(json_, outfile)
            
          
        # save json to s3
        if save == 'yes':
            self.save_to_s3(file_name=json_filename,file_path=json_absolute_filepath)
            
        return json_
    
    def gen_full_matching(self, source, nature, origin, source_path, save_to_path, data_merged, save):
            """
            Generates a dictionnary with :
            - json with matching stats
            - matched dataframe
            - unmatched dataframe

            args:
                source: source of the match data (ex: 'insee')
                nature: inpi_nature, 
                origin: inpi_origin,
                source_path: path of the source data,
                save_to_path : relative path where the file should be saved,
                data_merged: dataframe with merge result,
                save: should the files be saved to s3 (yes/no)
        
            Return: Dictionnary
            """
        
            # generate matching stats json and save to s3
            json_ = self.gen_matching_stats_json(source = source, 
                nature = nature, 
                origin = origin, 
                source_path = source_path, 
                save_to_path = save_to_path,
                data_merged = data_merged,
                save=save)

            # generate matched data gz and save to s3
            matched_df = self.gen_matching(source = source, 
                nature = nature, 
                origin = origin, 
                save_to_path = save_to_path,
                data_merged = data_merged,
                match_type = 'both',
                save=save)
        
            # generate unmatched data gz and save to s3
            unmatched_df = self.gen_matching(source = source, 
                nature = nature, 
                origin = origin, 
                save_to_path = save_to_path,
                data_merged = data_merged,
                match_type = 'right_only',
                save=save)       
        
            dict_ = {
                'matching_json': json_,
                'matched_df': matched_df,
                'unmatched_df': unmatched_df
            }
        
            return dict_
    
    def gen_matching(self, source, nature, origin, save_to_path, data_merged, match_type, save):
            """
            Generates a gz file containing (un)matched results, and saves to s3

            args:
                source: source of the match data (ex: 'insee')
                nature: inpi_nature, 
                origin: inpi_origin,
                save_to_path : relative path where the file should be saved,
                data_merged: dataframe with merge result,
                match_type: 'both'/'right_only'/'left_only',
                save: should the files be saved to s3 (yes/no)
        
            Return: name of generated (un)matched file
            """
            
            # define paths
            path_suffix = 'matche' if match_type == 'both' else 'non_matche'
            save_to_full_path = "{}/{}".format(save_to_path,path_suffix)
            gz_filename = "{}_{}_{}_{}.{}".format(source,nature,origin,path_suffix,'gz')
            save_to_full_filename = "{}/{}".format(save_to_full_path,gz_filename)
        
            # prepare gz file
            gz_filename = "{}_{}_{}_{}.{}".format(source,nature,origin,path_suffix,'gz')
            data_matched = data_merged[data_merged['_merge'] == match_type]
            gz_output = data_matched.to_csv(gz_filename,
                index = False,
                compression='gzip')
        
            # save gz to s3
            if save == 'yes':
                self.save_to_s3(file_name=gz_filename,
                              file_path=save_to_full_filename)
        
            return gz_filename