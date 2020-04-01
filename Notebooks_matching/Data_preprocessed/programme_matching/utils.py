import os
import pandas as pd
import datetime
from datetime import datetime
def get_file_infos(filename):

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
        (nature_,origin_,suborigin_,year_,timestamp_) = get_file_infos(filename)
        if origin_ == 'Flux':
            root_path='/INPI/TC_1/01_donnee_source/Flux'
            dest_path = "{}/{}/{}/{}".format(root_path,year_,nature_,suborigin_)
        else:
            root_path='/INPI/TC_1/01_donnee_source/Stock/Stock'
            dest_path = "{}_{}/{}/{}".format(root_path,suborigin_,year_,nature_)

        return dest_path

    
def add_source_info(filename,source_path,dest_path = None):

    source_full_path="{}/{}".format(source_path,filename)
    if dest_path:
        if not os.path.isdir(dest_path):
             os.mkdir(dest_path)
    else:
        dest_path = source_path
    dest_full_path="{}/{}".format(dest_path,filename)
    
    try:
        df=pd.read_csv(source_full_path, sep=';', header=0)
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
    
def unzip(zipfilename,path):
        full_path = os.path.join(path, zip_filename)
        with zipfile.ZipFile(full_path, 'r') as zip_ref:
            zip_ref.extractall(dest_path)