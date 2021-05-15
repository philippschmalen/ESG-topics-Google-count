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

def subset_last_x_days(df, last_x_days=30):
    """ """
    # date_lxdays: exact date x days ago
    df['date_lxdays'] = df.groupby('keyword').query_timestamp.transform('max') - pd.Timedelta(days=last_x_days) 
    # for keywords added <30 days ago: replace with start_date
    df['date_lxdays'] = df.apply(lambda x: x.start_date if x.date_lxdays < x.start_date else x.date_lxdays, axis=1)
    
    # -- subset df to focus on overall change from start to end date 
    # select l30days per keyword (groupby)
    df = df.loc[(df.query_timestamp == df.date_lxdays) | (df.query_timestamp == df.end_date)].set_index('keyword') 
    df['lxdays_absolute_change'] = df.groupby('keyword').results_count.diff()

    # subset df to show overall changes from start_date (=when entered into list) to end_date (=mostly today)
    df_overall_change = df.dropna().reset_index()
    
    df_overall_change['results_count_xdaysago'] = df.loc[df.query_timestamp == df.date_lxdays].results_count.reset_index(drop=True)
    df_overall_change['lxdays_relative_change'] = ((df_overall_change.results_count / df_overall_change.results_count_xdaysago)-1)*100
    
    return df_overall_change