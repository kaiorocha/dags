from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeSQLOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    dag_id='postgres_to_snowflake',
    default_args=default_args,
    description='Load data incrementally from Postgres to Snowflake',
    schedule=timedelta(days=1),
    catchup=False,
)

table_names = ['veiculos', 'estados', 'cidades', 'concessionarias', 'vendedores', 'clientes', 'vendas']

def extract_from_postgres(table_name, **context):
    """Extrai dados do PostgreSQL"""
    hook = PostgresHook(postgres_conn_id='postgres')

    # Obter max ID
    max_id_query = f"SELECT COALESCE(MAX(id), 0) FROM {table_name}"
    max_id = hook.get_first(max_id_query)[0]

    print(f"📊 Table: {table_name}")
    print(f"🔍 Max ID: {max_id}")

    # Salvar para próxima task
    context['task_instance'].xcom_push(key='max_id', value=max_id)

    return max_id

previous_task = None
for table_name in table_names:
    # Task 1: Extrair do PostgreSQL
    extract_task = PythonOperator(
        task_id=f'extract_{table_name}',
        python_callable=extract_from_postgres,
        op_kwargs={'table_name': table_name},
        dag=dag,
    )

    # Task 2: Placeholder para carregar no Snowflake (próximo passo)
    load_task = PythonOperator(
        task_id=f'load_{table_name}',
        python_callable=lambda table=table_name: print(f"✓ Loading {table} to Snowflake"),
        dag=dag,
    )

    # Dependências
    extract_task >> load_task

    if previous_task:
        previous_task >> extract_task

    previous_task = load_task