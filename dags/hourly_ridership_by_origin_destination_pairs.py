import csv
import datetime as dt
import ipdb

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from openpyxl import load_workbook
from sqlalchemy import create_engine, MetaData, Table, Column, Date, Integer, String

import helpers.constants as constants

default_args = {
    'owner': 'me',
    'start_date': dt.datetime(2018, 6, 27),
    'retries': 0,
    'retry_delay': dt.timedelta(minutes=5),
}

DB_URI = 'postgresql+psycopg2://jacinda.zhong@localhost:5432/sf_data'
FILE_PATH = 'data/bart/hourly_ridership_by_origin_destination_pairs.csv'
TABLE_NAME = 'fact_hourly_ridership_2011'


def create_table():
    engine = create_engine(DB_URI)
    engine.execute('CREATE SCHEMA IF NOT EXISTS "bart"')

    # Day, Hour, Origin Station, Destination Station, and Trip Count
    meta = MetaData(engine, schema="bart")
    table = Table(TABLE_NAME, meta,
                     Column('id', Integer, primary_key=True),
                     Column('date', Date),
                     Column('hour', Integer),
                     Column('origin_station', String),
                     Column('destination_station', String),
                     Column('ridership', Integer)
                 )
    meta.create_all()


def import_hourly_ridership():
    engine = create_engine(DB_URI)
    meta = MetaData(engine)
    table = Table(TABLE_NAME, meta, schema='bart', autoload=True)

    with open(FILE_PATH) as csvfile:
        csv_reader = csv.reader(csvfile, delimiter=',')

        data = []
        for row in csv_reader:
            db_row = {
                'date': row[0],
                'hour': row[1],
                'origin_station': row[2],
                'destination_station': row[3],
                'ridership': row[4]
            }
            data.append(db_row)

            if csv_reader.line_num % 10000 == 0:
                engine.execute(table.insert(), data)
                print(".")
                data = []


dag = DAG('hourly_ridership_origin_dest_pairs',
          default_args=default_args,
          schedule_interval='@hourly',
      )

create_table = PythonOperator(
                             task_id='create_table',
                             python_callable=create_table,
                             dag=dag
                         )

import_hourly_ridership = PythonOperator(
                        task_id='import_hourly_ridership',
                        python_callable=import_hourly_ridership,
                        dag=dag
                    )

import_hourly_ridership.set_upstream(create_table)
