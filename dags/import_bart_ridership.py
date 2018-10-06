import datetime as dt
import ipdb
import os
import xlrd

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from openpyxl import load_workbook
from sqlalchemy import create_engine, MetaData, Table, Column, Boolean, Date, Integer, String

default_args = {
    'owner': 'me',
    'start_date': dt.datetime(2018, 6, 27),
    'retries': 0,
    'retry_delay': dt.timedelta(minutes=5),
}

DB_URI = 'postgresql+psycopg2://jacinda.zhong@localhost:5432/sf_data'
FILE_PATH = '/Users/jacinda.zhong/Downloads/ridership_2017/Ridership_December2017.xlsx'


def create_ridership_table():
    engine = create_engine(DB_URI)
    engine.execute('CREATE SCHEMA IF NOT EXISTS "bart"')

    meta = MetaData(engine, schema="bart")
    table = Table('ridership', meta,
                      Column('id', Integer, primary_key=True),
                      Column('station_entry', String),
                      Column('station_exit', String),
                      Column('ridership', Integer),
                      Column('weekday', Boolean),
                      Column('saturday', Boolean),
                      Column('sunday', Boolean),
                      Column('month', String),
                      Column('year', String),
                      Column('date_created', Date),
                      Column('date_modified', Date)
                  )
    meta.create_all()

def process_ridership():
    engine = create_engine(DB_URI)
    meta = MetaData(engine)
    table = Table('ridership', meta, schema='bart', autoload=True)

    book = xlrd.open_workbook(FILE_PATH)
    sh = book.sheet_by_index(0)
    sheet_name = sh.name.lower()

    header = sh.row(1)
    header.pop(0)
    header_values = [ cell.value for cell in header]

    data = []
    for row_number in range(2, sh.nrows):
        row_values = sh.row_values(row_number)

        exit_station = row_values[0]
        row_values.pop(0)

        db_row = {}
        for index, entry_station in enumerate(header_values):
            ridership_value = round(row_values[index])
            db_row['ridership'] = ridership_value

            if "weekday" in sheet_name:
                db_row["weekday"] = True
            elif "saturday" in sheet_name:
                db_row["saturday"] = True
            elif "sunday" in sheet_name:
                db_row["sunday"] = True

            db_row["station_entry"] = entry_station
            db_row["station_exit"] = exit_station
            db_row["date_created"] = dt.datetime.now()
            db_row["date_modified"] = dt.datetime.now()

        data.append(db_row)

    engine.execute(table.insert(), data)


dag = DAG('import_bart_ridership',
          default_args=default_args,
          schedule_interval='@hourly',
      )

create_ridership_table = PythonOperator(
                             task_id='create_ridership_table',
                             python_callable=create_ridership_table,
                             dag=dag
                         )

process_ridership = PythonOperator(
                        task_id='process_ridership',
                        python_callable=process_ridership,
                        dag=dag
                    )

create_ridership_table
