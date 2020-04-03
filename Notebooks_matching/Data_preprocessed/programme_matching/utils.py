import os
import pandas as pd
import datetime
from datetime import datetime

def get_file_infos(filename):
    
    """
    Computes information that are in the filename to classify its content.
    cf INPI files documentation here : https://coda.io/d/CreditAgricole_dCtnoqIftTn/INSEE-INPI_suTDp

    args:
         - filename: Any filename
         ex : '0101_S1_20170504_11_obs.csv'

    returns:
        A list of informations : (nature_,origin_,suborigin_,year_,timestamp_).

    """

    f_s = filename.split('_')

    nature_=f_s[-1].split('.')[0]
    suborigin_ = None

    date_=f_s[2]
    year_=date_[0:4]
    month_=date_[4:6]
    day_=date_[6:9]
    
    if filename.endswith('EVT.csv'):
        nature_='ETS'
        suborigin_ = 'EVT'

    time_= f_s[3]    
    if f_s[1].startswith('S'):
        origin_='Stock'
        time_='0000'
        if str(year_) == '2017':
            suborigin_ = 'Initial'
        else:
            suborigin_ = 'Partiel'
    else :
        origin_='Flux'


    if suborigin_ is None:
            suborigin_ ='NEW'

    datetime_str=date_ + '_' + time_
    timestamp_=datetime.strptime(datetime_str,"%Y%m%d_%H%M%S")

    if nature_ == 'annuels':
        nature_ = 'comptes_annuels'
    nature_=nature_.upper()

    return (nature_,origin_,suborigin_,year_,timestamp_)

    
def calc_dest(filename):
    """
    Computes path where the file should be saved in S3 given its filename.

    args:
         - filename: Any filename
         ex : '0101_1_20170512_112544_10_ets_supprime_EVT.csv'

    returns:
        dest_path in S3 (no / at start)
        ex : 'INPI/TC_1/01_donnee_source/Flux/2017/ETS/EVT'

    """     

    (nature_,origin_,suborigin_,year_,timestamp_) = get_file_infos(filename)
    if origin_ == 'Flux':
        root_path='INPI/TC_1/01_donnee_source/Flux'
        dest_path = "{}/{}/{}/{}".format(root_path,year_,nature_,suborigin_)
    else:
        root_path='INPI/TC_1/01_donnee_source/Stock/Stock'
        dest_path = "{}_{}/{}/{}".format(root_path,suborigin_,year_,nature_)

    return dest_path


def add_source_info(filename,source_path,dest_path = None):
    """
    Add file information inside csv columns to make it available in case of further files agregating operations.

    args:
         - filename: Any filename
         ex : '0101_S1_20170504_11_obs.csv'
         - source_path : location of the input file.
         ex: 'data/dl'
         - dest_path (optional): location of the result file. By default, same as source_path.
         ex: 'data/prep'

    returns:
        dest_full_path : location and name of the result file.
        ex: 'data/prep/0101_S1_20170504_11_obs.csv'

    """
    source_full_path="{}/{}".format(source_path,filename)
    if dest_path:
        if not os.path.isdir(dest_path):
             os.mkdir(dest_path)
    else:
        dest_path = source_path
    dest_full_path="{}/{}".format(dest_path,filename)
    
    try:
        df=pd.read_csv(source_full_path, sep=';', header=0, dtype='object')
        (nature_,origin_,suborigin_,year_,timestamp_) = get_file_infos(filename)
        df['csv_source']=filename
        df['file_timestamp']=timestamp_
        df['nature']=nature_
        df['type']=origin_
        df['origin']=suborigin_

        df.to_csv(dest_full_path, index=False)
    except Exception as e:
        print(e)
        return False
    
    return dest_full_path