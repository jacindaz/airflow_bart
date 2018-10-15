import datetime as dt
import ipdb
from os import listdir
from os.path import isfile, join
import re
import xlrd

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from openpyxl import load_workbook
from sqlalchemy import create_engine, MetaData, Table, Column, Boolean, Date, Integer, String

import helpers.constants as constants


default_args = {
    'owner': 'me',
    'start_date': dt.datetime(2018, 6, 27),
    'retries': 0,
    'retry_delay': dt.timedelta(minutes=5),
}

FILE_PATH = 'data/bart/ridership_2017/'
TABLE_NAME = 'fact_ridership'


def create_table():
    engine = create_engine(constants.DB_URI)
    engine.execute('CREATE SCHEMA IF NOT EXISTS "bart"')

    meta = MetaData(engine, schema="bart")
    table = Table(TABLE_NAME, meta,
                   Column('id', Integer, primary_key=True),
                   Column('station_entry', String),
                   Column('station_exit', String),
                   Column('ridership', Integer),
                   Column('weekday', Boolean),
                   Column('saturday', Boolean),
                   Column('sunday', Boolean),
                   Column('month', String),
                   Column('year', Integer),
                   Column('date_created', Date),
                   Column('date_modified', Date)
                  )
    meta.create_all()


def import_ridership():
    engine = create_engine(constants.DB_URI)
    meta = MetaData(engine)
    table = Table(TABLE_NAME, meta, schema='bart', autoload=True)

    onlyfiles = [f for f in listdir(FILE_PATH) if isfile(join(FILE_PATH, f))]

    for file_name in onlyfiles:
        print(f"processing file: {file_name}")

        book = xlrd.open_workbook(FILE_PATH + file_name)

        regex_decimals = re.compile(r'\d+')
        file_year = regex_decimals.findall(file_name)[0]

        regex_characters = re.compile(r'[a-z]+')
        file_month = regex_characters.findall(file_name.lower())[1]

        for sheet in book.sheets():
            sheet_name = sheet.name.lower()
            header = sheet.row(1)
            header.pop(0)
            header_values = [cell.value for cell in header]

            data = []
            for row_number in range(2, sheet.nrows):
                row_values = sheet.row_values(row_number)

                exit_station = row_values[0]
                row_values.pop(0)

                db_row = {}
                for index, entry_station in enumerate(header_values):
                    ridership_value = round(row_values[index])
                    db_row['ridership'] = ridership_value

                    if "weekday" in sheet_name:
                        db_row["weekday"] = True

                        db_row["saturday"] = False
                        db_row["sunday"] = False
                    elif "saturday" in sheet_name:
                        db_row["saturday"] = True

                        db_row["weekday"] = False
                        db_row["sunday"] = False
                    elif "sunday" in sheet_name:
                        db_row["sunday"] = True

                        db_row["weekday"] = False
                        db_row["saturday"] = False

                    db_row["station_entry"] = entry_station
                    db_row["station_exit"] = exit_station
                    db_row["date_created"] = dt.datetime.now()
                    db_row["date_modified"] = dt.datetime.now()

                    db_row["year"] = file_year
                    db_row["month"] = file_month

                data.append(db_row)

            engine.execute(table.insert(), data)


dag = DAG('ridership',
          default_args=default_args,
          schedule_interval='@hourly',
      )

create_table = PythonOperator(
                             task_id='create_table',
                             python_callable=create_table,
                             dag=dag
                         )

import_ridership = PythonOperator(
                        task_id='import_ridership',
                        python_callable=import_ridership,
                        dag=dag
                    )

import_ridership.set_upstream(create_table)
