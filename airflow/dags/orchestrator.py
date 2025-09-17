import os
import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta
from docker.types import Mount

sys.path.append('/opt/airflow/scripts')
from extract.web_scraper import fetch_trails
from extract.api_request import fetch_weather
from extract.insert_records import insert_trail, insert_weather

default_args = {
    'description': 'main orchestrator DAG',
    'start_date': datetime(2025, 8, 12),
    'catchup': False
}

def insert_trail_task(ti):
    trail_lst = ti.xcom_pull(task_ids='webscrapper_task')
    insert_trail(trail_lst)

def fetch_weather_task(ti):
    trail_lst = ti.xcom_pull(task_ids='webscrapper_task')
    return [fetch_weather(trail['lat'], trail['lon']) for trail in trail_lst]

def insert_weather_task(ti):
    weather_lst = ti.xcom_pull(task_ids='weather_api')
    insert_weather(weather_lst)

with DAG(
    dag_id = 'safesummit_orchestrator',
    default_args = default_args,
    schedule = timedelta(hours=1)
) as dag:
    DBT_PATH = os.environ.get('DBT_PATH')
    if not DBT_PATH:
        raise ValueError('Error: Missing "DBT_PATH" enviroment variable')
    DB_NAME = os.getenv('POSTGRES_DB')
    if not DB_NAME:
        raise ValueError('Error: Missing "POSTGRES_DB" enviroment variable')
    USER = os.getenv('POSTGRES_USER')
    if not USER:
        raise ValueError('Error: Missing "POSTGRES_USER" enviroment variable')
    PASSWORD = os.getenv('POSTGRES_PASSWORD')
    if not PASSWORD:
        raise ValueError('Error: Missing "POSTGRES_PASSWORD" enviroment variable')
    
    task_A = PythonOperator(
        task_id = 'webscrapper_task',
        python_callable = fetch_trails
    )

    task_B1 = PythonOperator(
        task_id = 'insert_trails',
        python_callable = insert_trail_task
    )

    task_B2 = PythonOperator(
        task_id = 'weather_api',
        python_callable = fetch_weather_task
    )

    task_C2 = PythonOperator(
        task_id = 'insert_weather',
        python_callable = insert_weather_task
    )

    task_D = DockerOperator(
        task_id = 'transform_data',
        image = 'ghcr.io/dbt-labs/dbt-postgres:1.9.0',
        command = 'run',
        working_dir = '/usr/app',
        mounts = [
            Mount(source=f'{DBT_PATH}/safesummit',
                target='/usr/app',
                type='bind'),
            Mount(source=f'{DBT_PATH}/profiles.yml',
                target='/root/.dbt/profiles.yml',
                type='bind')
        ],
        environment={
            'POSTGRES_DB': DB_NAME,
            'POSTGRES_USER': USER,
            'POSTGRES_PASSWORD': PASSWORD,
        },
        network_mode = 'safesummit_main_network',
        docker_url = 'unix://var/run/docker.sock',
        auto_remove = 'success'
    )

    task_A >> [task_B1, task_B2]
    task_B2 >> task_C2
    [task_B1, task_C2] >> task_D