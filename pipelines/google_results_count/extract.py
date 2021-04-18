
import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime

def get_search_urls(keyword_list, url="https://www.google.com/search?q="):
    """ Compose search urls """
    search_query = [kw.replace(' ','+') for kw in keyword_list] # replace space with '+'
    return [url+sq for sq in search_query]
    
def get_results_count(keyword, user_agent):
    result = requests.get(keyword, headers=user_agent)    
    soup = BeautifulSoup(result.content, 'html.parser')
    
    #  string that contains results count 'About 1,410,000,000 results'
    total_results_text = soup.find("div", {"id": "result-stats"}).find(text=True, recursive=False) 
    
    # extract number
    results_num = int(''.join([num for num in total_results_text if num.isdigit()]) )
    
    return results_num

def assert_df(df, keyword_list, url="https://www.google.com/search?q="):
    # create dummy dataframe for comparison
    df_compare = pd.DataFrame({
        'keyword': pd.Series([*keyword_list], dtype='object'),
        'results_count': pd.Series([1 for i in keyword_list], dtype='int64'),
        'search_url': pd.Series(get_search_urls(keyword_list, url=url), dtype='object'),
        'query_timestamp': pd.Series([datetime.now() for i in keyword_list], dtype='datetime64[ns]')
    })

    # columns
    column_difference = set(df.columns).symmetric_difference(df_compare.columns)
    assert len(column_difference) == 0, f"The following columns differ to reference dataframe: {column_difference}"
    # dtypes
    assert (df_compare.dtypes == df.dtypes).all(), f"Different dtypes for {df.dtypes}\n{df_compare.dtypes}"
    # length
    assert len(df) == len(keyword_list), f"{len(df)} does not equal {len(keyword_list)}"
    
    print("Success >>>>>>>>>>\tDataframe meets expectations\n")
    
def df_build_results_count(keyword_list, user_agent, url="https://www.google.com/search?q="):
    search_urls = get_search_urls(keyword_list)
    result_count = [get_results_count(url, user_agent) for url in search_urls]  
    
    df = pd.DataFrame({'keyword': keyword_list, 
                       'results_count': result_count, 
                       'search_url': search_urls, 
                       'query_timestamp': datetime.now()})
    # testing
    assert_df(df=df, keyword_list=keyword_list, url=url)
    
    return df
