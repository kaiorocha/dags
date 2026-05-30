from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

# Define DAG
dag = DAG(
    dag_id='postgres_to_snowflake',
    default_args=default_args,
    description='Load data incrementally from Postgres to Snowflake',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

# Table names
table_names = ['veiculos', 'estados', 'cidades', 'concessionarias', 'vendedores', 'clientes', 'vendas']

# Create tasks dynamically
previous_task = None
for table_name in table_names:
    # Task to get max ID
    get_max_id_task = BashOperator(
        task_id=f'get_max_id_{table_name}',
        bash_command=f'echo "Getting max ID for table {table_name}"',
        dag=dag,
    )

    # Task to load data
    load_data_task = BashOperator(
        task_id=f'load_data_{table_name}',
        bash_command=f'echo "Loading data for table {table_name}"',
        dag=dag,
    )

    # Set dependencies
    get_max_id_task >> load_data_task

    if previous_task:
        previous_task >> get_max_id_task

    previous_task = load_data_task


