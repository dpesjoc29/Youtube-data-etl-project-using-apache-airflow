from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
# from airflow.dags.youtube_etl.extract_youtube_data import save_to_csv , extract_video_info
from extract import main
from transform import transform_and_load_data

default_args = {
    'owner':'Dipesh',
    'start_date': days_ago(7),
    'depends_on_past': False,
    'email':['abc@gmail.com'],
    'email_on_failure': False,
    'email_on_retries': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'youtube_dag',
    default_args=default_args,
    description = "Dag to extract youtube data and commments from youtube videos.",
    schedule_interval= "@once",

)

run_extract = PythonOperator(
    task_id = 'extract_data',
    python_callable=main,
    dag = dag
)

run_transform = PythonOperator(
    task_id = 'transform_data',
    python_callable=transform_and_load_data,
    dag=dag,

)

# run_extract >> run_transform
run_extract.set_downstream(run_transform)
