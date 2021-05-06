"""
~-- TRANSFORM
Transforms the raw data from the external data directory (data/0_raw) 
for analysis. 
"""

import pandas as pd
import logging

def impute_results_count(df):
    """ Impute timestamp to daily observations, interpolate with polynomial """
    
    # categorical columns to group on (keyword and search_url)
    groupby_columns = df.select_dtypes('object').columns.to_list()

    # assert expected grouping columns
    expect_object_columns = ['search_url', 'keyword']
    assert set(groupby_columns) == set(expect_object_columns), \
        f"Columns {groupby_columns} not in {expect_object_columns} "

    kw_list, url_list = df.keyword.unique(), df.search_url.unique()
    df_list = []
    for kw, url in dict(zip(kw_list, url_list)).items():
        df_temp = df.set_index('query_timestamp')\
                .query(f"keyword=='{kw}'")\
                .resample('D').mean()\
                .results_count.interpolate(method='polynomial', order=5)\
                .reset_index() 
        df_temp['keyword'],df_temp['search_url']  = kw, url
        df_list.append(df_temp)
        
    df = pd.concat(df_list)
    
    return df