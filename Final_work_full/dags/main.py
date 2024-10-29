from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from scripts.processing import get_random_sales
from airflow.utils.dates import days_ago
import subprocess

# Задаем параметры подключения
postgres_conn_id = 'postgres_connection'


def create_random_sales_data(postgres_conn_id):
    # Вызываем функцию, генерирующую случайные продажи с дефольтными аргументами
    data = get_random_sales()

    # Создаем Хук и инсертим сгенерированные данные
    postgres_sql_upload = PostgresHook(postgres_conn_id=postgres_conn_id, schema='test') 
    postgres_sql_upload.insert_rows('Sales', data)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
}

dag = DAG(
    'final_work',
    default_args=default_args,
    description='Full work 10 processing',
    schedule_interval='@once',
    catchup=False
)

create_postgres_table_task = PostgresOperator(
    task_id='create_postgres_table',
    sql='''create table if not exists Sales
            (
                Sale_UId varchar(20) primary key,
                Customer_UId varchar(20),
                Product_UId varchar(20),
                Quantity int,
                Sale_Date date,
                Sale_Amount int,
                Region varchar
                );''',
    postgres_conn_id = postgres_conn_id,
    dag=dag,
)

create_sales_data_task = PythonOperator(
    task_id='create_sales_data',
    python_callable=create_random_sales_data,
	op_kwargs={'postgres_conn_id': postgres_conn_id},
    dag=dag,
)

create_postgres_table_task >> create_sales_data_task
