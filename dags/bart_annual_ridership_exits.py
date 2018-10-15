import datetime as dt
import xlrd

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from openpyxl import load_workbook
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, Date

default_args = {
    'owner': 'me',
    'start_date': dt.datetime(2018, 6, 27),
    'retries': 0,
    'retry_delay': dt.timedelta(minutes=5),
}

DB_URI = 'postgresql+psycopg2://jacinda.zhong@localhost:5432/sf_data'
TABLE_NAME = 'fact_annual_ridership'

def create_table():
    engine = create_engine(DB_URI)
    engine.execute('CREATE SCHEMA IF NOT EXISTS "bart"')

    meta = MetaData(engine, schema="bart")
    table = Table(TABLE_NAME, meta,
                   Column('id', Integer, primary_key=True),
                   Column('total_annual_exits', Integer),
                   Column('average_weekday', Integer),
                   Column('average_saturday', Integer),
                   Column('average_sunday', Integer),
                   Column('year', Integer),

                   Column('date_created', Date),
                   Column('date_modified', Date)
                  )
    meta.create_all()

dag = DAG('annual_exits',
    default_args=default_args,
    schedule_interval='@once'
)

create_table = PythonOperator(
    task_id='create_table',
    python_callable=create_table,
    dag=dag
)

create_table
