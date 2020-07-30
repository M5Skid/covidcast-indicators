## Needed to process the geo files to get from xls file to a simpler csv.
## pip install xlrd

## Author: James Sharpnack @jsharpna


import pandas as pd
from os import path
import json


INPUT_DIR = "."
OUTPUT_DIR = "../../delphi_utils/data"
MSA_FILE = "03_20_MSAs.xls"
FIPS_MSA_OUT_FILE = "fips_msa_cross.csv"
ZIP_CODE_FILE = "02_20_uszips.csv"
ZIP_FIPS_OUT_FILE = "zip_fips_cross.csv"
STATE_OUT_FILE = "state_codes.csv"
FIPS_HRR_FILE = "transfipsToHRR.csv"
FIPS_HRR_OUT_FILE = "fips_hrr_cross.csv"
ZIP_FIPS_FILE_2 = "ZIP_COUNTY_032020.xlsx"
ZIP_HSA_HRR_FILE = "ZipHsaHrr18.csv"
ZIP_HSA_OUT_FILE = "zip_hsa_cross.csv"
ZIP_HRR_OUT_FILE = "zip_hrr_cross.csv"
JHU_FIPS_FILE = "UID_ISO_FIPS_LookUp_Table.csv"
JHU_FIPS_OUT_FILE = "jhu_uid_fips_cross.csv"
FIPS_ZIP_FILE = "COUNTY_ZIP_032020.xlsx"
FIPS_ZIP_OUT_FILE = "fips_zip_cross.csv"

def convert_fips(x):
    """Ensure fips is a string of length 5."""
    return str(x).zfill(5)

def convert_fips_to_stcode(fips_ser):
    """convert fips string Series to state code string Series"""
    return fips_ser.str[:2]

def wei_iter(df):
    """
    Generator that runs through fips/zip dataframe and spits out dictionary
    """
    for _, row in df.iterrows():
        wei_js = row['county_weights']
        wei_js = json.loads(wei_js.replace("'", '"'))
        wei_sum = sum(wei_js.values())
        if (wei_sum == 0) and (len(wei_js)==1):
            yield {'zip': convert_fips(row['zip']),
                   'fips': convert_fips(row['fips']),
                   'weight': 1.}
        else:
            for fips, wei in wei_js.items():
                yield {'zip': convert_fips(row['zip']),
                       'fips': convert_fips(fips),
                       'weight': wei / wei_sum}

def proc_zip_fips():
    """ ZIP FIPS Cross """
    zip_df = pd.read_csv(ZIP_CODE_FILE)
    zip_reduced = pd.DataFrame(a for a in wei_iter(zip_df))
    zip_reduced.to_csv(path.join(OUTPUT_DIR,ZIP_FIPS_OUT_FILE), index=False)

    zip_df['st_code'] = convert_fips_to_stcode(zip_df['fips'].astype(str).str.zfill(5))
    state_df = zip_df[['st_code','state_id','state_name']].drop_duplicates()
    assert state_df.shape[0] == 52, "More than 52 states?"
    state_df.to_csv(path.join(OUTPUT_DIR,STATE_OUT_FILE), index=False)
    return True

def proc_fips_msa():
    """ FIPS MSA Cross """
    msa_cols = {'CBSA Code': int,
                'Metropolitan/Micropolitan Statistical Area': str,
                'FIPS State Code': str,
                'FIPS County Code': str}
    msa_df = pd.read_excel(MSA_FILE, skiprows=2, skipfooter=4, usecols=msa_cols.keys(), dtype=msa_cols)
    metro_bool = msa_df['Metropolitan/Micropolitan Statistical Area'] == 'Metropolitan Statistical Area'
    msa_df = msa_df[metro_bool]
    msa_df['fips'] = msa_df['FIPS State Code'].str.cat(msa_df['FIPS County Code']).astype(int)
    msa_df.rename(columns={'CBSA Code':'msa'},inplace=True)
    msa_df = msa_df[['fips','msa']]
    msa_df.to_csv(path.join(OUTPUT_DIR,FIPS_MSA_OUT_FILE), index=False)
    return True

def proc_fips_hrr():
    """ FIPS HRR Cross """
    hrr_df = pd.read_csv(FIPS_HRR_FILE)
    hrr_df = pd.melt(hrr_df,
                id_vars=['fips'],
                value_vars=hrr_df.columns[3:],
                var_name='hrr',
                value_name='weight')
    hrr_df = hrr_df[hrr_df['weight'] != 0.]
    hrr_df.to_csv(path.join(OUTPUT_DIR,FIPS_HRR_OUT_FILE), index=False)
    return True

def proc_zip_hsa_hrr():
    zip_df = pd.read_csv(ZIP_HSA_HRR_FILE)
    hsa_df = zip_df[['zipcode18','hsanum']].\
        rename(columns={'zipcode18':'zip', 'hsanum':'hsa'})
    hrr_df = zip_df[['zipcode18','hrrnum']].\
        rename(columns={'zipcode18':'zip', 'hrrnum':'hrr'})
    hsa_df.to_csv(path.join(OUTPUT_DIR, ZIP_HSA_OUT_FILE), index=False)
    hrr_df.to_csv(path.join(OUTPUT_DIR, ZIP_HRR_OUT_FILE), index=False)
    return True

def proc_jhu_uid_to_fips():
    hand_additions = pd.DataFrame([
        {'jhu_uid': 84070002, 'fips': 25007, 'weight': 0.5},
        {'jhu_uid': 84070002, 'fips': 25019, 'weight': 0.5},
        {'jhu_uid': 84070003, 'fips': 29095, 'weight': 0.25},
        {'jhu_uid': 84070003, 'fips': 29165, 'weight': 0.25},
        {'jhu_uid': 84070003, 'fips': 29037, 'weight': 0.25},
        {'jhu_uid': 84070003, 'fips': 29047, 'weight': 0.25},
        {'jhu_uid': 84002158, 'fips': 2270, 'weight': 1.},
        {'jhu_uid': 84070015, 'fips': 49000, 'weight': 1.},
        {'jhu_uid': 84070016, 'fips': 49000, 'weight': 1.},
        {'jhu_uid': 84070017, 'fips': 49000, 'weight': 1.},
        {'jhu_uid': 84070018, 'fips': 49000, 'weight': 1.},
        {'jhu_uid': 84070019, 'fips': 49000, 'weight': 1.},
        {'jhu_uid': 84070020, 'fips': 49000, 'weight': 1.},
    ])
    jhu_df = pd.read_csv(JHU_FIPS_FILE,dtype={'UID':int, 'FIPS':float})
    jhu_df = jhu_df.query("Country_Region == 'US'")
    jhu_df = jhu_df[['UID','FIPS']]\
        .rename(columns={'UID':'jhu_uid', 'FIPS':'fips'})\
        .dropna(subset=['fips'])\
        .convert_dtypes({'fips':int})
    fips_st = jhu_df['fips'].astype(str).str.len() <= 2
    jhu_df.loc[fips_st, 'fips'] = jhu_df.loc[fips_st, 'fips']\
            .astype(str).str.ljust(5, '0')\
            .astype(int)
    dup_ind = jhu_df['jhu_uid'].isin(hand_additions['jhu_uid'].values)
    jhu_df.drop(jhu_df.index[dup_ind],inplace=True)
    jhu_df['weight'] = 1.
    jhu_df = pd.concat((jhu_df,hand_additions))
    jhu_df.to_csv(path.join(OUTPUT_DIR, JHU_FIPS_OUT_FILE), index=False)
    return True

def proc_fips_to_zip():
    fips_df = pd.read_excel(FIPS_ZIP_FILE)
    fips_df.rename(columns={'COUNTY':'fips', 'ZIP':'zip', 'TOT_RATIO': 'weight'},
                   inplace=True)
    fips_df[['fips','zip','weight']].to_csv(path.join(OUTPUT_DIR, FIPS_ZIP_OUT_FILE), index=False)
    return True

if __name__ == "__main__":

    # proc_zip_fips()
    # proc_fips_msa()
    # proc_fips_hrr()
    # proc_zip_hsa_hrr()
    # proc_jhu_uid_to_fips()
    proc_fips_to_zip()
