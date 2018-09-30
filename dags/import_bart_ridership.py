import datetime as dt
import ipdb
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
FILE_NAME = '/Users/jacinda.zhong/Downloads/ridership_2017/Ridership_December2017'


def create_ridership_table():
    engine = create_engine(DB_URI)
    engine.execute('CREATE SCHEMA IF NOT EXISTS "bart"')

    meta = MetaData(engine, schema="bart")
    table = Table('ridership', meta,
                      Column('id', Integer, primary_key=True),
                      Column('station_entry', String),
                      Column('station_exit', String),
                      Column('ridership', Integer),
                      Column('weekday', String),
                      Column('saturday', String),
                      Column('sunday', String),
                      Column('month', String),
                      Column('year', String)
                  )
    meta.create_all()


dag = DAG('import_bart_ridership',
          default_args=default_args,
          schedule_interval='@hourly',
      )

create_ridership_table = PythonOperator(
                             task_id='create_ridership_table',
                             python_callable=create_ridership_table,
                             dag=dag
                         )

create_ridership_table
