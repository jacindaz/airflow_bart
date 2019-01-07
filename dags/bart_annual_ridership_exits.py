import datetime as dt
import re
import xlrd

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from openpyxl import load_workbook
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, Date

import helpers.constants as constants


default_args = {
    'owner': 'me',
    'start_date': dt.datetime(2018, 6, 27),
    'retries': 0,
    'retry_delay': dt.timedelta(minutes=5),
}

FILE_PATH='data/bart/BART_Ridership_FY73_FY18.xlsx'
TABLE_NAME = 'fact_annual_ridership'


def create_table(db_uri=constants.DB_URI):
    engine = create_engine(db_uri)
    engine.execute('CREATE SCHEMA IF NOT EXISTS "bart"')

    meta = MetaData(engine, schema="bart")
    table = Table(TABLE_NAME, meta,
                   Column('id', Integer, primary_key=True),
                   Column('year', Integer),
                   Column('total_annual_exits', Integer),
                   Column('average_weekday', Integer),
                   Column('average_saturday', Integer),
                   Column('average_sunday', Integer),

                   Column('date_created', Date),
                   Column('date_modified', Date)
                  )
    meta.create_all()


def import_annual_ridership(db_uri=constants.DB_URI, file_path=FILE_PATH):
    book = xlrd.open_workbook(file_path)
    sh = book.sheet_by_index(0)

    header = sh.row(2)

    data = []
    for row_number in range(4, sh.nrows-1):
        row_values = sh.row_values(row_number)

        year = row_values[0]

        if year != '':
            db_row = {
                'year': _year_as_integer(year),
                'total_annual_exits': row_values[1],
                'average_weekday': _nullif(row_values[3]),
                'average_saturday': _nullif(row_values[5]),
                'average_sunday': _nullif(row_values[7]),
                'date_created': dt.datetime.now(),
                'date_modified': dt.datetime.now(),
            }
            data.append(db_row)
        else:
            pass


    engine = create_engine(db_uri)
    meta = MetaData(engine)
    table = Table(TABLE_NAME, meta, schema='bart', autoload=True)
    engine.execute(table.insert(), data)


def _year_as_integer(string):
    regex_decimals = re.compile(r'\d+')
    year = regex_decimals.findall(string)[0]

    if int(year) >= 73 and int(year) <= 99:
        return int('19' + year)
    else:
        return int('20' + year)


def _nullif(integer):
    string = str(integer)
    if string.strip() == '':
        return None
    else:
        if isinstance(integer, str):
            regex_decimals = re.compile(r'\d+')
            year = regex_decimals.findall(integer)
            integer = ''.join(year)

        return integer


dag = DAG('annual_exits',
    default_args=default_args,
    schedule_interval='@once'
)

create_table_task = PythonOperator(
    task_id='create_table_id',
    python_callable=create_table,
    dag=dag
)

import_annual_ridership_task = PythonOperator(
    task_id='import_annual_ridership_id',
    python_callable=import_annual_ridership,
    dag=dag
)

import_annual_ridership_task.set_upstream(create_table_task)
