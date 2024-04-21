
from datetime import datetime, timedelta
from airflow import DAG
import os
import sys
from utils_2.spotify_api_2 import Spotify
from utils_2.sql_transformations import spotify_transformation
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, DbtDag
## Set the default arguments for the inital DAG, extracting data from spotify_api and loading
## loading to snowflake. Setting the rety to 1 and the dealy to 24hrs because of spotifies
## time limit when pulling data

default_args = {
'owner': 'jf_data_eng_project',
"depends_on_past": False,
'start_date': datetime(2024, 3, 29),
' retries': 0
}


def extract_spotify():
    spotify_client = Spotify()
    spotify_client.write_to_sf()

def build_refined_playlist():
    spotify_transformation_client = spotify_transformation()
    spotify_transformation_client.refined_playlists()

def build_refined_audio_features():
    spotify_transformation_client = spotify_transformation()
    spotify_transformation_client.refined_audio_features()

def build_refined_track_information():
    spotify_transformation_client = spotify_transformation()
    spotify_transformation_client.refined_track_information()

def build_main_spotify_model():
    spotify_transformation_client = spotify_transformation()
    spotify_transformation_client.final_spotify_model()


with DAG('spotify_extract', default_args=default_args,
         description='The purpose of this is to extract the RAW data from spotify and load to SF',
         schedule_interval=timedelta(days=7),
         catchup=False):
    
    task_a = PythonOperator(task_id='Extract_Raw_Data', python_callable = extract_spotify)

    task_b = PythonOperator(task_id='Build_Refined_Playlist', python_callable = build_refined_playlist)

    task_c = PythonOperator(task_id='Build_Refined_Audio_Features', python_callable = build_refined_audio_features)

    task_d = PythonOperator(task_id='Build_Refined_track_Information', python_callable = build_refined_track_information)

    task_e = PythonOperator(task_id='Build_Main_Spotify_Model', python_callable = build_main_spotify_model)
    # task_b = BashOperator(task_id = 'spotify_b', 
    #     bash_command=f'/Users/justinfarnan_hakkoda/.pyenv/shims/dbt build --select +{main_data_model}',
    #     # Ensure any needed environment variables are also included or exported as part of the command    env={'PATH': '/Users/justinfarnan_hakkoda/.pyenv/shims:' + os.environ['PATH']}
    # )

task_a >> [task_b, task_c, task_d] >> task_e
