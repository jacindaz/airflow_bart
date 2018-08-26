import datetime as dt

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator

from openpyxl import load_workbook

default_args = {
    'owner': 'me',
    'start_date': dt.datetime(2018, 6, 27),
    'retries': 0,
    'retry_delay': dt.timedelta(minutes=5),
}

def load_xlsx_file():
    wb = load_workbook('/Users/jacinda.zhong/Downloads/ridership_2017/Ridership_April2017.xlsx')
    print(wb.sheetnames)


with DAG('process_xls',
    default_args = default_args,
    schedule_interval='@hourly',
    ) as dag:

    print_sheetnames = PythonOperator(
            task_id='print_sheetnames',
            python_callable=load_xlsx_file
        )

    create_table = PostgresOperator(
            task_id='postgres_create_table',
            sql="CREATE TABLE jacinda_test(id serial, name text);",
            postgres_conn_id='postgres_default',
            autocommit=True,
            dag=dag
        )

print_sheetnames >> create_table
