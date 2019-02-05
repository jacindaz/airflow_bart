import csv
import datetime as dt

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Boolean, DateTime
import helpers.constants as constants

default_args = {
    'owner': 'me',
    'start_date': dt.datetime(2019, 1, 6),
    'retries': 0,
    'retry_delay': dt.timedelta(minutes=5),
}

FILE_PATH = "data/city_of_sf/eviction_notices.csv"
TEST_FILE = "eviction_tenlines.csv"
SCHEMA = 'sf'
TABLE_NAME = 'eviction_notices'

def create_table(db_uri=constants.DB_URI, file_path=TEST_FILE):
    engine = create_engine(db_uri)
    engine.execute(f"CREATE SCHEMA IF NOT EXISTS \"{SCHEMA}\"")
    meta = MetaData(engine, schema=SCHEMA)

    table = Table(TABLE_NAME, meta,
                Column('eviction_id', String),
                Column('address', String),
                Column('city', String),
                Column('state', String),
                Column('zipcode', String),
                Column('file_date', DateTime),
                Column('non_payment', Boolean),
                Column('breach', Boolean),
                Column('nuisance', Boolean),
                Column('illegal_use', Boolean),
                Column('failure_to_sign_renewal', Boolean),
                Column('access_denial', Boolean),
                Column('unapproved_subtenant', Boolean),
                Column('owner_move_in', Boolean),
                Column('demolition', Boolean),
                Column('capital_improvement', Boolean),
                Column('substantial_rehab', Boolean),
                Column('ellis_act_withdrawal', Boolean),
                Column('condo_conversion', Boolean),
                Column('roommate_same_unit', Boolean),
                Column('other_cause', Boolean),
                Column('late_payments', Boolean),
                Column('lead_remediation', Boolean),
                Column('development', Boolean),
                Column('good_samaritan_ends', Boolean),
                Column('constraints_date', DateTime),
                Column('supervisor_district', Integer),
                Column('neighborhood', String),
                Column('client_location_city', String),
                # Add in when have Postgis set up
                # Column('client_location', Point),
                Column('client_location', String),
                Column('client_location_address', String),
                Column('client_location_zip', String),
                Column('client_location_state', String),
            )
    meta.create_all()



dag = DAG('eviction_notices',
          default_args=default_args,
          schedule_interval='@hourly',
      )

create_table_task = PythonOperator(
     task_id='create_table_task_id',
     python_callable=create_table,
     dag=dag
 )
