
from datetime import datetime, timedelta
from airflow import DAG
import os
import sys
from utils_2.spotify_api_2 import Spotify
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
## Set the default arguments for the inital DAG, extracting data from spotify_api and loading
## loading to snowflake. Setting the rety to 1 and the dealy to 24hrs because of spotifies
## time limit when pulling data

default_args = {
    'owner': 'jf_data_eng_project',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 29),
    'retries': 0
}
main_data_model = Variable.get('main_dbt_model')
# def extract_spotify():
#     spotify_client = Spotify()
#     spotify_client.write_to_sf()

# def build_dbt_models():
#     bash_commad = 'cd /Users/justinfarnan_hakkoda/Fundamentals_Data_Engineering/Spotify_Data_Eng_GenAI/Spotify_transformations && dbt build --select +main_spotify'
#     return bash_commad

with DAG('spotify_extract', default_args=default_args,
         description='The purpose of this is to extract the RAW data from spotify and load to SF',
         schedule_interval=timedelta(days=7),
         catchup=False):
    
    # task_a = PythonOperator(task_id='spotify_a', python_callable = extract_spotify)

    task_b = BashOperator(task_id = 'spotify_b', bash_command= f'dbt build --select +{main_data_model}'
)

task_b
