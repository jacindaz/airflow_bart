import datetime as dt
import ipdb
import os
import xlrd

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator

from openpyxl import load_workbook
from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String
from sqlalchemy.ext import declarative

default_args = {
    'owner': 'me',
    'start_date': dt.datetime(2018, 6, 27),
    'retries': 0,
    'retry_delay': dt.timedelta(minutes=5),
}

class Base(declarative.declarative_base()):
    __abstract__ = True

class StationName(Base):
    __tablename__ = 'bart_station_names'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    two_letter_code = Column(String)

    def __repr__(self):
        return "<StationName(id='%s', name='%s', two_letter_code='%s')>" % (
            self.id, self.name, self.two_letter_code)

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

    engine = create_engine('postgresql+psycopg2://jacinda.zhong@localhost:5432/sf_bart')
    StationName.__table__
    Base.metadata.create_all(engine)

with DAG('process_file',
    default_args = default_args,
    schedule_interval='@hourly',
    ) as dag:

    station_names = PythonOperator(
        task_id='station_names',
        python_callable=process_station_names
    )

station_names
