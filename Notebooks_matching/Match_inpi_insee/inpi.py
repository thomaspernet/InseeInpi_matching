import pandas as pd
import glob
import re
from sagemaker import get_execution_role
import boto3

class inpiStock:
    def __init__(self, instance_aws, bucket):
        self.instance_aws = instance_aws
        self.bucket = bucket

    def appendInpiStock(self, option_extract,dtype, parse_date, partiel = False):
        """
        Append all csv files in a folder to a Pandas DataFrame

        Args:
        Flux: Boolean: True to parse flux, else Stock
        origin: String, from list ['ACTES', 'COMPTES_ANNUELS','ETS',
                      'OBS', 'PM', 'PP','REP']
        EVT: Boolean: True for EVT / False

        dtype: variables type, use 'str', for the data and pd.Int64Dtype()
        for integer. If possible
        parse_date: A list with the variables to convert into dates
        path: Path to find the csv
        
        
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

        s3 = boto3.resource('s3')
        my_bucket = s3.Bucket(self.bucket)
        if partiel:
            subfilter = "INPI/TC_1/Stock/Stock_partiel/{}".format(option_extract)
        else:
            subfilter = "INPI/TC_1/Stock/Stock_initial/{}".format(option_extract)
        
        df_output = pd.DataFrame()
        for object_summary in my_bucket.objects.filter(Prefix=subfilter):
            if object_summary.key.endswith(".csv"):
                filename = '{}/{}'.format(
                    self.instance_aws,
                    #self.bucket,
                    object_summary.key)

                df_temp = pd.read_csv(filename, sep=";",
                                      dtype=dtype,
                                      parse_dates=parse_date)
                df_output = df_output.append(df_temp)
                
        return df_output

        


    


#for i,file in enumerate(glob.glob('{}\*.csv'.format(path_pp))):
#    try:
#        df_ = pd.read_csv(file, sep = ";",
#                          dtype = dtype,
#                          parse_dates  =parse_dates)
#        df_open = pd.concat([df_open],
#        ignore_index=True)
#    except Exception as e:
#        print(file, e)
