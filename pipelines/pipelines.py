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

# -- for logging
import logging
logging.basicConfig(
	level=logging.DEBUG,
	format='{asctime} {levelname:<8} {message}',
	style='{',
	filename='log/google_results_count.log', 
	filemode='w'
)



@task
def create_search_url(keyword_list, url):
	return extract.get_search_urls(keyword_list, url)

@task(max_retries=1, retry_delay=timedelta(seconds=10))
def extract_result_count(search_urls, user_agent, url):
	result_count = [extract.get_results_count(url, user_agent) for url in search_urls]
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
def export(df, path):
	load.write_to_csv(df, path)



def main():
	# ~----------------- SETTINGS -----------------~
	with open(r'../settings.yml') as file:
	    settings = yaml.full_load(file)

	    PROJECT_DIR  = '../'
	    RAW_DATA_DIR = settings['project']['raw_data_dir']
	    FILENAME     = f"{settings['project']['export_filename']}_{datetime.now().strftime('%Y%m%d_%H%M')}.csv" 
	    FILEPATH     = os.path.join(PROJECT_DIR, RAW_DATA_DIR, FILENAME)
	    KEYWORDS     = settings['query']['keywords']
	    USER_AGENT   = settings['query']['user_agent']
	    GOOGLE_URL   = settings['query']['google_url']

	# ~----------------- FLOW -----------------~
	# ~-- daily schedule
	schedule = IntervalSchedule(
		start_date= datetime.strptime("20210424-041000UTC", "%Y%m%d-%H%M%S%Z"), 
		# start_date=datetime.utcnow() + timedelta(seconds=1),
		interval=timedelta(days=1),
	)

	with Flow("etl", schedule=schedule) as flow:

		# parameter
		filepath 	 = Parameter(name="filepath")
		keywords 	 = Parameter(name="keywords")
		user_agent 	 = Parameter(name="user_agent")
		google_url 	 = Parameter(name="google_url")

		# task flow
		search_urls = create_search_url(keywords, google_url)
		results_count = extract_result_count(search_urls, user_agent, google_url)
		df = df_build_results_count(keywords, results_count, search_urls)
		assert_df(df, keywords, google_url)
		export(df, filepath)

	# ~----------------- RUN -----------------~
	flow.run(filepath=FILEPATH, 
		keywords=KEYWORDS, 
		user_agent=USER_AGENT, 
		google_url=GOOGLE_URL)


if __name__ == "__main__":
    main()
