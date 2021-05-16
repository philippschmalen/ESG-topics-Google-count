# general
import yaml
from datetime import timedelta, datetime
import os
import pandas as pd

# prefect
from prefect import task, Flow, Parameter
from prefect.schedules import IntervalSchedule

# custom
from google_results_count import extract, load
from analysis import load_raw_data, transform, plot

# -- for logging
import logging
logging.basicConfig(
    level=logging.DEBUG,
    format='{asctime} {levelname:<8} {message}',
    style='{',
    filename='log/google_results_count.log',
    filemode='w'
)


# -------------------------------------------------
# Extract raw data
# -------------------------------------------------
@task
def create_search_url(keyword_list, url):
    return extract.get_search_urls(keyword_list, url)


@task(max_retries=2, retry_delay=timedelta(seconds=10))
def extract_result_count(search_urls, user_agent, url):
    result_count = [extract.get_results_count(
        url, user_agent) for url in search_urls]
    return result_count


@task
def df_build_results_count(keyword_list, result_count, search_urls):
    df = pd.DataFrame({'keyword': keyword_list,
                       'results_count': result_count,
                       'search_url': search_urls,
                       'query_timestamp': datetime.now()})
    return df


@task
def assert_df(df, keyword_list, url):
    extract.assert_df(df, keyword_list, url)


@task
def export_raw_data(df, path):
    load.write_to_csv(df, path)

# -------------------------------------------------
# Tranform data for analysis
# -------------------------------------------------


@task
def transform_raw_data():
    df = (load_raw_data.load(raw_data_dir='../data/0_raw', filename='google_results_count')
          .pipe(transform.impute_results_count)
          .pipe(transform.feature_engineering))
    return df


@task
def export_analysis_data(df, path):
    load.write_to_csv(df, path)

@task
def deploy_plots(df):
    # subset df for last x days
    df_lxdays = transform.subset_last_x_days(df, last_x_days=30)

    plot.set_layout_template()
    fig_timeline = plot.plot_timeline(df)
    fig_change = plot.plot_change(df_lxdays)

    plot.deploy_figure(fig_timeline, filename="google_results_count_timeline")
    plot.deploy_figure(fig_change, filename="google_results_count_change")

    



def main():
    # ~----------------- SETTINGS -----------------~
    with open(r'../settings.yml') as file:
        settings = yaml.full_load(file)

        PROJECT_DIR = '../'
        RAW_DATA_DIR = settings['project']['raw_data_dir']
        FINAL_DATA_DIR = settings['project']['final_data_dir']
        FILENAME = f"{settings['project']['export_filename']}_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
        FILEPATH_RAW = os.path.join(PROJECT_DIR, RAW_DATA_DIR, FILENAME)
        FILEPATH_ANALYSIS = os.path.join(PROJECT_DIR, FINAL_DATA_DIR, FILENAME)
        KEYWORDS = settings['query']['keywords']
        USER_AGENT = settings['query']['user_agent']
        GOOGLE_URL = settings['query']['google_url']

    # ~----------------- FLOW -----------------~
    # ~-- daily schedule
    # schedule = IntervalSchedule(
    #     start_date=datetime.strptime("20210424-030500UTC", "%Y%m%d-%H%M%S%Z"),
    #     interval=timedelta(days=1),
    # )
    # , schedule=schedule
    with Flow("etl") as flow: 

        # parameter
        filepath_raw = Parameter(name="filepath_raw")
        filepath_analysis = Parameter(name="filepath_analysis")
        keywords = Parameter(name="keywords")
        user_agent = Parameter(name="user_agent")
        google_url = Parameter(name="google_url")

        # task flow
        # -- raw data
        search_urls = create_search_url(keywords, google_url)
        results_count = extract_result_count(
            search_urls, user_agent, google_url)
        df = df_build_results_count(keywords, results_count, search_urls)
        assert_df(df, keywords, google_url)
        export_raw_data(df, filepath_raw)

        # -- analysis data and plot
        df = transform_raw_data()
        export_analysis_data(df, filepath_analysis)
        deploy_plots(df)


    # ~----------------- RUN -----------------~
    flow.run(filepath_raw=FILEPATH_RAW,
    		filepath_analysis=FILEPATH_ANALYSIS,
			keywords=KEYWORDS,
	        user_agent=USER_AGENT,
	        google_url=GOOGLE_URL)


if __name__ == "__main__":
    main()
