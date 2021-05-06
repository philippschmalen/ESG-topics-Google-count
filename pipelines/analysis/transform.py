"""
~-- TRANSFORM
Prepares the raw data, making it ready for analysis. 
Stores dataset in data/2_final
"""

import pandas as pd 
import logging
from datetime import date

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
                .results_count.interpolate(method='polynomial', order=3)\
                .reset_index() 
        df_temp['keyword'],df_temp['search_url']  = kw, url
        df_list.append(df_temp)
        
    df = pd.concat(df_list)
    
    return df

def feature_engineering(df):
    """Add statistics to raw data to analyze changes across time"""

    # day-to-day change  
    df['result_count_absolute_change'] = df.groupby('keyword').results_count.diff().reset_index(drop=True) # day-to-day absolute change
    df['result_count_relative_change'] = (df.result_count_absolute_change/df.results_count*100) # day-to-day relative change 

    # overall days keyword already tracked
    df['start_date'], df['end_date'] = df.groupby('keyword').query_timestamp.transform('min'), df.groupby('keyword').query_timestamp.transform('max')
    df['days_tracked'] = (df['end_date']-df['start_date']).dt.days
    
    return df