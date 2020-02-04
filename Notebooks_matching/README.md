# Class `ws_connectors`


# Class `inpi`

inpiStockFlux
 	"""
        instance_aws: Instance name:
            format -> 'https://calfdata.s3.eu-west-3.amazonaws.com'
        bucket: Bucket name
        """

uploadFileBucket(self,pathfile):
        """
        Download file from S3 bucket
        
        args:
        pathfile: path where the filename is located in S3,
        including filename
        ex: INPI/TC_1/Stock/dtypes_stock.json
        """

uploadBacthFileBucket(self,pathfiles):
        """
        Download file from S3 bucket
        
        args:
        pathfile: list path where the filename is located in S3,
        including filename
        ex: INPI/TC_1/Stock/dtypes_stock.json
        """

appendInpiFlux(self,
                       option_extract,
                       dtype,
                       parse_date,
                       new = True,
                       year = 2017,
                       return_frame = True):
        
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