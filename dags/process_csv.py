import datetime as dt
import ipdb
import xlrd

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


def process_station_names():
    book = xlrd.open_workbook("/Users/jacinda.zhong/Downloads/Station_Names.xls")
    sh = book.sheet_by_index(0)

    header = sh.row(0)
    header.pop(0)

    data = []
    for row_number in range(2,sh.nrows):
        row_values = sh.row_values(row_number)
        row_values.pop(0)
        data.append(row_values)

    print(data)


with DAG('process_file',
    default_args = default_args,
    schedule_interval='@hourly',
    ) as dag:

    station_names = PythonOperator(
        task_id='station_names',
        python_callable=process_station_names
    )

station_names
