import pandas as pd
import glob
import re

def appendData(
    dtype, parse_date, path, flux=False,
    origin="PP",EVT=False,
    list_unique=False
):
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
    if origin not in ["ACTES", "COMPTES_ANNUELS", "ETS",
                      "OBS", "PM", "PP", "REP"]:
        print(
            "Veuillez utiliser l'un des mots clés suivants {} \n \
        pour l'argument origin"
        )
        exit()

    matches_evt = re.search("Stock", path)
    if matches_evt:
        pathcsv = "{}\{}".format(path, origin)
    else:
        if EVT:
            pathcsv = "{}\{}\{}".format(path, origin, "EVT")
        else:
            pathcsv = "{}\{}\{}".format(path, origin, "NEW")

    return pathcsv
    df_ = pd.concat(
        [
            pd.read_csv(file, sep=";", dtype=dtype, parse_dates=parse_date)
            for file in glob.glob("{}\*.csv".format(pathcsv))
        ],
        ignore_index=True,
    )

    if list_unique:
        df_unique = []
        for c in list(df_pp):
            unique_ = df_pp[c].unique().tolist()

            df_ = {"col": c, "values": unique_}
            df_unique.append(df_)
        json.dumps(df_unique, indent=4, sort_keys=True, default=str)

    return df_


#for i,file in enumerate(glob.glob('{}\*.csv'.format(path_pp))):
#    try:
#        df_ = pd.read_csv(file, sep = ";",
#                          dtype = dtype,
#                          parse_dates  =parse_dates)
#        df_open = pd.concat([df_open],
#        ignore_index=True)
#    except Exception as e:
#        print(file, e)
