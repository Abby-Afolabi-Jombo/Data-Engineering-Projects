from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'run_python_script_dag',
    default_args=default_args,
    description='A DAG to run a standalone Python script',
    schedule='@daily',  # Adjust the schedule as needed
    start_date=datetime(2024, 6, 13),
    catchup=False,
)

# Adjust the path to your Python script
script_path = '/Users/user/airflow/dags/scripts/#weather_api_lagos.py'

run_python_script = BashOperator(
    task_id='run_python_script',
    bash_command=f'python {script_path}',
    dag=dag,
)

run_python_script
