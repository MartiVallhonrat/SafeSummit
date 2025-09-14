import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

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
    schedule = timedelta(minutes=5)
) as dag:
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

    task_A >> [task_B1, task_B2]
    task_B2 >> task_C2