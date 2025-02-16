import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess
import pandas as pd
# Define a function to execute the script
def run_test_py():
    df = pd.read_csv('/opt/airflow/exps/input/data.csv')
    return df.to_csv('/opt/airflow/exps/output/data_output.csv', index=False)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id='run_test_file',
    default_args=default_args,
    description='Run the entire test.py file',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    run_test_file = PythonOperator(
        task_id='execute_test_py',
        python_callable=run_test_py,
    )

    run_test_file
