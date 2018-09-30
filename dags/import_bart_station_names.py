import datetime as dt
import os
import xlrd

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from openpyxl import load_workbook
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String

default_args = {
    'owner': 'me',
    'start_date': dt.datetime(2018, 6, 27),
    'retries': 0,
    'retry_delay': dt.timedelta(minutes=5),
}

DB_URI = 'postgresql+psycopg2://jacinda.zhong@localhost:5432/sf_data'
FILE_PATH = '/Users/jacinda.zhong/Downloads/Station_Names.xls'


def create_station_name_table():
    engine = create_engine(DB_URI)
    engine.execute('CREATE SCHEMA IF NOT EXISTS "bart"')

    meta = MetaData(engine, schema="bart")
    table = Table('station_names', meta,
                     Column('id', Integer, primary_key=True),
                     Column('name', String),
                     Column('two_letter_code', String)
                 )
    meta.create_all()


def process_station_names():
    book = xlrd.open_workbook(FILE_PATH)
    sh = book.sheet_by_index(0)

    header = sh.row(0)
    header.pop(0)

    data = []
    for row_number in range(2, sh.nrows):
        row_values = sh.row_values(row_number)
        db_row = {'name': row_values[2], 'two_letter_code': row_values[1]}
        data.append(db_row)

    engine = create_engine(DB_URI)
    meta = MetaData(engine)
    table = Table('station_names', meta, schema='bart', autoload=True)
    engine.execute(table.insert(), data)


dag = DAG('import_bart_station_names',
          default_args=default_args,
          schedule_interval='@hourly',
      )

create_table = PythonOperator(
                  task_id='create_table',
                  python_callable=create_station_name_table,
                  dag=dag
              )

process_station_names = PythonOperator(
                            task_id='process_station_names',
                            python_callable=process_station_names,
                            dag=dag
                        )

process_station_names.set_upstream(create_table)
