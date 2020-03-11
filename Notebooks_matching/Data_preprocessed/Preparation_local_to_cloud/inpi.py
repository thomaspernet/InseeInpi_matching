import pandas as pd
import re, os, glob, boto3, json, datetime
#from sagemaker import get_execution_role

class inpiStockFlux:
    def __init__(self, instance_aws, bucket):
        """
        instance_aws: Instance name:
            format -> 'https://calfdata.s3.eu-west-3.amazonaws.com'
        bucket: Bucket name
        """
        self.instance_aws = instance_aws
        self.bucket = bucket
        self.s3 = boto3.resource('s3')

    def myconverter(self,o):
        """
        Convert datetime to json serializable format
        """
        if isinstance(o, datetime.datetime):
            return o.__str__()

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

        #return data_location

    def appendInpiFlux(self,
                       option_extract,
                       dtype,
                       parse_date,
                       new = True,
                       year = 2017,
                       return_frame = True,
                       to_s3 = True):

        """
        Append all csv files in a folder to a Pandas DataFrame and save the
        output in S3: INPI/TC_1/Stock_processed from S3 bucket:
        /INPI/TC_1/Flux/


        format output-> gz
        - Option + filename +stock +gz:
            - 2017_NEW_PP.gz

        Args:
        option_extract: String, from list ['ACTES', 'COMPTES_ANNUELS','ETS',
                      'OBS', 'PM', 'PP','REP']
        dtype: variables type, use 'str', for the data and pd.Int64Dtype()
        for integer. If possible
        parse_date: A list with the variables to convert into dates
        new: Boolean, if true, then go NEW folder, else EVT
        year: integer. Locate year folder, 2017,2018,2019,2020

        Return:
            Pandas DataFrame
        """

        # Test if in
        list_option = ["ACTES", "COMPTES_ANNUELS", "ETS",
                      "OBS", "PM", "PP", "REP"]

        if option_extract not in list_option:
            return print(
            "Veuillez utiliser l'un des mots clés suivants {} \n \
        pour l'argument origin".format(list_option)
        )

        my_bucket = self.s3.Bucket(self.bucket)

        list_mois =  ["01", "02", "03", "04", "05", "06",
                     "07","08", "09", "10", "11", "12"]

        for month in list_mois:
            if new:
                subfilter = "INPI/TC_1/Flux/{0}/{1}/{2}/NEW".format(str(year),
                                                                   month,
                                                                   option_extract)
                new_option = 'NEW'
            else:
                subfilter = "INPI/TC_1/Flux/{0}/{1}/{2}/EVT".format(str(year),
                                                                month,
                                                                option_extract)
                new_option = 'EVT'

            df_output = pd.DataFrame()
            list_csv_failed = []
            #regex = r"[^\/]+(?=\.[^\/.]*$)"
            for object_summary in my_bucket.objects.filter(Prefix=subfilter):
                if object_summary.key.endswith(".csv"):
                    filename = '{}/{}'.format(
                    self.instance_aws,
                    object_summary.key)

                    ### log if failed save:
                    try:
                        df_temp = pd.read_csv(filename, sep=";",
                                      dtype=dtype,
                                      parse_dates=parse_date)
                        df_output = df_output.append(df_temp, sort=False)
                    except:
                        list_csv_failed.append(filename)

        ### generate log
        log_ = {
            'filename': 'name_stored_data',
            'variables': list(df_output),
            'size': df_output.shape[0],
            'dtype':df_output.dtypes.apply(lambda x: x.name).to_dict(),
            'created_at':datetime.datetime.now(),
            'failed_append':list_csv_failed
        }

        ### save data to S3
        #matches = re.search(regex, object_summary.key)
        name_stored_data = '{0}_{1}_{2}'.format(
            year,
            new_option,
            option_extract)

        df_output.to_csv('{}.gz'.format(name_stored_data),
            index = False,
            compression='gzip')

        with open('{}.json'.format(name_stored_data),
                  'w', encoding='utf8') as outfile:
            json.dump(log_, outfile, default = self.myconverter,ensure_ascii=False)

        ### store log
        if to_s3:
            role = get_execution_role()
            self.s3.meta.client.upload_file('{}.json'.format(name_stored_data),
                           self.bucket,
                           'INPI/TC_1/Stock_processed/{}.json'.format(
                               name_stored_data)
                          )

            self.s3.meta.client.upload_file('{}.gz'.format(name_stored_data),
                           self.bucket,
                           'INPI/TC_1/Stock_processed/{}.gz'.format(
                               name_stored_data)
                          )



        os.remove('{}.gz'.format(name_stored_data))
        os.remove('{}.json'.format(name_stored_data))

        if return_frame:
            return df_output

        return subfilter



    def appendInpiStock(self, option_extract,
                        dtype, parse_date,
                        partiel = False,
                        return_frame = True):
        """
        Append all csv files in a folder to a Pandas DataFrame and save the
        output in S3: INPI/TC_1/Stock/Stock_processed

        format output-> gz
        - Option + filename +stock +gz:
            - initial_PP.gz

        Args:
        option_extract: String, from list ['ACTES', 'COMPTES_ANNUELS','ETS',
                      'OBS', 'PM', 'PP','REP']
        dtype: variables type, use 'str', for the data and pd.Int64Dtype()
        for integer. If possible
        parse_date: A list with the variables to convert into dates
        partiel: Boolean, if true, then go partiel folder, else stock

        Return:
            Pandas DataFrame
        """
        # Test if in
        list_option = ["ACTES", "COMPTES_ANNUELS", "ETS",
                      "OBS", "PM", "PP", "REP"]

        role = get_execution_role()

        if option_extract not in list_option:
            return print(
            "Veuillez utiliser l'un des mots clés suivants {} \n \
        pour l'argument origin".format(list_option)
        )

        my_bucket = self.s3.Bucket(self.bucket)
        if partiel:
            subfilter = "INPI/TC_1/Stock/Stock_partiel/{}".format(option_extract)
            stock_ = 'partiel'
        else:
            subfilter = "INPI/TC_1/Stock/Stock_initial/{}".format(option_extract)
            stock_ = 'initial'

        df_output = pd.DataFrame()
        #regex = r"[^\/]+(?=\.[^\/.]*$)"
        for object_summary in my_bucket.objects.filter(Prefix=subfilter):
            if object_summary.key.endswith(".csv"):
                filename = '{}/{}'.format(
                    self.instance_aws,
                    object_summary.key)

                df_temp = pd.read_csv(filename, sep=";",
                                      dtype=dtype,
                                      parse_dates=parse_date)
                df_output = df_output.append(df_temp)

        ### generate log
        log_ = {
            'filename': 'name_stored_data',
            'variables': list(df_output),
            'size': df_output.shape[0],
            'dtype':df_output.dtypes.apply(lambda x: x.name).to_dict(),
            'created_at':datetime.datetime.now()
        }

        ### save data to S3
        #matches = re.search(regex, object_summary.key)
        name_stored_data = '{}_{}'.format(
            stock_,
            option_extract)

        df_output.to_csv('{}.gz'.format(name_stored_data),
            index = False,
            compression='gzip')

        with open('{}.json'.format(name_stored_data),
                  'w', encoding='utf8') as outfile:
            json.dump(log_, outfile, default = self.myconverter,ensure_ascii=False)

        ### store log

        self.s3.meta.client.upload_file('{}.json'.format(name_stored_data),
                           self.bucket,
                           'INPI/TC_1/Stock_processed/{}.json'.format(
                               name_stored_data)
                          )

        ### store gz
        self.s3.meta.client.upload_file('{}.gz'.format(name_stored_data),
                           self.bucket,
                           'INPI/TC_1/Stock_processed/{}.gz'.format(
                               name_stored_data)
                          )



        os.remove('{}.gz'.format(name_stored_data))
        os.remove('{}.json'.format(name_stored_data))

        if return_frame:
            return df_output
