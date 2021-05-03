"""
~-- LOAD RAW DATA
Loads the queried data from the external data directory (data/0_raw)
"""

from glob import glob
import os
import pandas as pd
import logging

def load_data(raw_data_dir, filename):
    # merge all data from directory
    data_files = glob(f'{os.path.join(raw_data_dir, filename)}*')
    logging.info(
        f"Load data from raw data dir: {raw_data_dir}, Filename: {filename}")
    # specify column dtypes
    df_list = [pd.read_csv(file) for file in data_files]
    df = pd.concat(df_list).reset_index(drop=True)
    df = set_dtypes(df)
     
    assert_raw_df(df) # check dataframe against what we expect

    return df

def set_dtypes(df):
    """ Set dtypes for columns """
    # drop rows where a column names appear (happened while appending to csv)
    df = df.loc[df[df.columns[0]] != df.columns[0]]
    # convert numerics
    df = df.apply(pd.to_numeric, errors='ignore')
    # parse timestamps
    df.query_timestamp = df.query_timestamp.apply(pd.to_datetime)
    # reset index
    df.reset_index(inplace=True, drop=True)

    return df

def assert_raw_df(df):
    """Check that dataframe meets expecations for existing columns and dtypes"""
    # assert columns
    expected_columns = ['keyword', 'results_count', 'search_url', 'query_timestamp']
    assert_columns = list(set(df.columns).difference(set(expected_columns)))
    assert len(assert_columns) == 0,\
        f"Expected columns {expected_columns}.\nFound {assert_columns} instead."
    # assert at least 1 datetime column
    assert (df.dtypes == 'datetime64[ns]').any(), "Column type 'Datetime64[ns]' not found in dataframe "



# ------------ TESTING
# logging.basicConfig(level=logging.INFO)
# df = load_data(raw_data_dir='../../data/0_raw', filename='google_results_count')
#                 # .pipe(impute_results_count)).reset_index(drop=True)

# print(df.head())