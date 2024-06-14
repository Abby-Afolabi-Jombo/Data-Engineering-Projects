from datetime import timedelta, datetime
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowException

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'Append_postgresdb_dag',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule='@daily',  # or use a cron expression like '0 12 * * *'
    start_date=datetime(2024, 6, 10, 8, 0),
    catchup=False,
)

def append_data_to_table(**kwargs):
    try:
        # Connect to PostgreSQL database
        conn = psycopg2.connect(
            dbname='Myairflowpostgresql',
            user='airflow_user',
            password='Abbysairflow',
            host='localhost',
            port='5432'
        )
        cur = conn.cursor()

        # Define the SQL query to insert data
        insert_query = """
        INSERT INTO Table_dag_test (event_date, event_time, description)
        VALUES (%s, %s, %s);
        """

        # Example data to be inserted
        event_date = datetime.now().date()
        event_time = datetime.now().time()
        description = 'Appended event'
        data = (event_date, event_time, description)

        # Execute the query
        cur.execute(insert_query, data)

        # Commit the transaction
        conn.commit()

        # Close the connection
        cur.close()
        conn.close()
    except Exception as e:
        raise AirflowException(f"Error appending data to PostgreSQL: {e}")

# Define the PythonOperator to run the append_data_to_table function
append_data_task = PythonOperator(
    task_id='append_data_to_table',
    python_callable=append_data_to_table,
    provide_context=True,
    dag=dag,
)

# Set the task in the DAG
append_data_task
