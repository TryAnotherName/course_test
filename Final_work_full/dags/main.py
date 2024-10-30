from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from scripts.processing import get_random_sales
from airflow.utils.dates import days_ago
from datetime import date

# Устанавливаем временную зону
import pendulum
local_tz = pendulum.timezone("Europe/Moscow")

# Задаем параметры подключения
postgres_conn_id = 'postgres_connection'
clickhouse_conn_id = 'clickhouse_connection'


def do_data_transfer(postgres_conn_id, clickhouse_conn_id, table_names):
    # Устанавливаем подключения к каждой из БД
    postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id, schema='test')
    clickhouse_hook = ClickHouseHook(clickhouse_conn_id=clickhouse_conn_id)

    # Последовательно загружаем данные из Postgres в Clickhouse
    for table_name in table_names:
        df = postgres_hook.get_pandas_df(sql=f"SELECT * FROM {table_name};")
        df['import_date'] = date.today()
        df_list = df.values.tolist()

        clickhouse_hook.execute(f"INSERT INTO {table_name} VALUES", df_list)

    print('Загрузка агрегированных данных прошла успешно!')


def create_random_sales_data(postgres_conn_id):
    # Вызываем функцию, генерирующую случайные продажи с дефольтными аргументами
    data = get_random_sales()

    # Создаем Хук и инсертим сгенерированные данные
    postgres_sql_upload = PostgresHook(postgres_conn_id=postgres_conn_id, schema='test')
    postgres_sql_upload.insert_rows(table='Sales', rows=data)
    print('Генерирование данных прошло успешно!')

# Дефолтные аргументы
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
}

# Объявляем ДАГ
dag = DAG(
    'final_work',
    default_args=default_args,
    description='Full work 10 processing',
    schedule_interval='45 12 * * 2',
    catchup=False
)

# Создаем таблицу в Postgres для хранения данных
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
    postgres_conn_id=postgres_conn_id,
    dag=dag,
)

# Генерируем данные
create_sales_data_task = PythonOperator(
    task_id='create_sales_data',
    python_callable=create_random_sales_data,
	op_kwargs={'postgres_conn_id': postgres_conn_id},
    dag=dag,
)

# Для каждой задачи создаем таблицу, если ее нет, и инсертим агрегированные данные в нее
do_postgres_data_aggregation_task = PostgresOperator(
    task_id='do_postgres_data_aggregation',
    sql=('create table if not exists count_data (sales_count bigserial);',
         '''INSERT INTO count_data
            select count(*)
            from sales;''',

        '''create table if not exists region_sum_sales
            (region varchar,
            sum_sales bigserial);''',
        '''INSERT INTO region_sum_sales
            select region, sum(sale_amount)
            from sales
            group by region;''',
        
        '''create table if not exists product_sum_sales
            (product_uid varchar(20),
            sum_sales bigserial);''',
        '''INSERT INTO product_sum_sales
            select product_uid, sum(sale_amount)
            from sales
            group by product_uid;''',

        '''create table if not exists region_avg_sales
            (region varchar,
            avg_sales float);''',
        '''INSERT INTO region_avg_sales
            select region, avg(sale_amount)
            from sales
            group by region;''',

        '''create table if not exists product_avg_sales
            (product_uid varchar(20),
            avg_sales float);''',
        '''INSERT INTO product_avg_sales
            select product_uid, avg(sale_amount)
            from sales
            group by product_uid;'''),
    postgres_conn_id=postgres_conn_id,
    dag=dag,
)

# Для каждой задачи создаем таблицу в Clickhouse для агрегированных данных 
create_clickhouse_tables_task = ClickHouseOperator(
    task_id='create_clickhouse_tables',
    database='default',
    sql=(''' create table if not exists count_data
            (id UInt64,
            import_date Date)
            engine = MergeTree
            order by import_date;
        ''',
        '''create table if not exists region_sum_sales
            (region String,
            sum_sales UInt64,
            import_date Date)
            engine = MergeTree
            order by (region, import_date);''',
        '''create table if not exists product_sum_sales
            (product_uid String,
            sum_sales UInt64,
            import_date Date)
            engine = MergeTree
            order by (product_uid, import_date);''',
        '''create table if not exists region_avg_sales
            (region String,
            avg_sales Float32,
            import_date Date)
            engine = MergeTree
            order by (region, import_date);''',
        '''create table if not exists product_avg_sales
            (product_uid String,
            avg_sales Float32,
            import_date Date)
            engine = MergeTree
            order by (product_uid, import_date);'''),
    clickhouse_conn_id=clickhouse_conn_id,
    dag=dag,
)

# Перемешаем агрегированные данные из Postgres в Clickhouse
do_data_transfer_task = PythonOperator(
    task_id='do_data_transfer',
    python_callable=do_data_transfer,
	op_kwargs={'postgres_conn_id': postgres_conn_id,
                'clickhouse_conn_id': clickhouse_conn_id,
                'table_names': ['count_data',
                               'region_sum_sales',
                               'product_sum_sales',
                               'region_avg_sales',
                               'product_avg_sales']},
    dag=dag,
)

create_postgres_table_task >> create_sales_data_task
create_sales_data_task >> [do_postgres_data_aggregation_task, create_clickhouse_tables_task]
[do_postgres_data_aggregation_task, create_clickhouse_tables_task] >> do_data_transfer_task
