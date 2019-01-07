import csv
import datetime as dt
import gzip, os, psycopg2, re, requests

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from openpyxl import load_workbook
from sqlalchemy import create_engine, MetaData, Table, Column, Date, Integer, String, select, func

import helpers.constants as constants

default_args = {
    'owner': 'me',
    'start_date': dt.datetime(2018, 6, 27),
    'retries': 0,
    'retry_delay': dt.timedelta(minutes=5),
}

SCHEMA = 'bart'
FILE_PATH = 'data/bart/hourly_ridership_by_origin_destination_pairs.csv'
FILE_YEARS = [2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018]


def _table_name(year):
    return f"fact_hourly_ridership_{year}"


def _table_count(table_object):
    return select([func.count()]).select_from(table_object).execute().first()[0]

def _temp_file_name(year):
    return f"temp_file_{year}.csv.gz"

def _create_temp_data_file(year):
    url = f"http://64.111.127.166/origin-destination/date-hour-soo-dest-{year}.csv.gz"
    r = requests.get(url)

    buffered_writer = open(_temp_file_name(year), 'wb')
    response_body = r.content
    buffered_writer.write(response_body)


def create_table(db_uri=constants.DB_URI, years=FILE_YEARS):
    engine = create_engine(db_uri)
    engine.execute(f"CREATE SCHEMA IF NOT EXISTS \"{SCHEMA}\"")

    meta = MetaData(engine, schema=SCHEMA)

    for year in years:
        if not engine.dialect.has_table(engine, _table_name(year), schema=SCHEMA):
            table = Table(_table_name(year), meta,
                             Column('id', Integer, primary_key=True),
                             Column('date', Date),
                             Column('hour', Integer),
                             Column('origin_station', String),
                             Column('destination_station', String),
                             Column('ridership', Integer)
                         )
            meta.create_all()


def import_hourly_ridership(db_uri=constants.DB_URI, years=FILE_YEARS):
    for year in years:
        table_name = _table_name(year)

        engine = create_engine(db_uri)
        meta = MetaData(engine, schema=SCHEMA)

        table = Table(table_name, meta)
        if _table_count(table) == 0:
            _create_temp_data_file(year)

            conn = psycopg2.connect("host=localhost dbname=sf_data user=jacinda.zhong")
            cur = conn.cursor()

            with gzip.open(_temp_file_name(year), 'rb') as file:
                cur.copy_from(file, f"bart.{table_name}", sep=',',
                    columns=('date', 'hour', 'origin_station', 'destination_station', 'ridership')
                )

                table = Table(table_name, meta)
                print(f"Finished writing to {table_name}. It has count: (_table_count(table))")

                conn.commit()


def temp_file_cleanup(directory=".", pattern="temp_file"):
    for f in os.listdir(directory):
        if re.search(pattern, f):
            os.remove(f)


dag = DAG('hourly_ridership_origin_dest_pairs',
          default_args=default_args,
          schedule_interval='@hourly',
      )

create_table_task = PythonOperator(
     task_id='create_table_id',
     python_callable=create_table,
     dag=dag
 )

import_hourly_ridership_task = PythonOperator(
    task_id='import_hourly_ridership_id',
    python_callable=import_hourly_ridership,
    dag=dag
)

temp_file_cleanup_task = PythonOperator(
    task_id='temp_file_cleanup_id',
    python_callable=temp_file_cleanup,
    dag=dag
)

import_hourly_ridership_task.set_upstream(create_table_task)
temp_file_cleanup_task.set_upstream(import_hourly_ridership_task)
