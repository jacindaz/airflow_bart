from os import listdir
from os.path import isfile, join
import xlrd

import dags.bart_ridership as dag

def test_create_table(postgresql_db):
    dag.create_table(postgresql_db.postgresql_url)

    assert postgresql_db.has_table(dag.TABLE_NAME) == True


def test_import_ridership(postgresql_db):
    dag.create_table(postgresql_db.postgresql_url)

    test_file_path = 'tests/test_data/ridership_2017/'
    dag.import_ridership(db_uri=postgresql_db.postgresql_url, file_dir=test_file_path)

    result_proxy_obj = postgresql_db.session.execute(f"select count(*) from bart.{dag.TABLE_NAME}")
    result_count = result_proxy_obj.first()[0]

    files = [f for f in listdir(test_file_path) if isfile(join(test_file_path, f))]

    expected_count = 0
    for file_name in files:
        book = xlrd.open_workbook(test_file_path + file_name)

        for sheet in book.sheets():
            expected_count += sheet.nrows - 2 # minus the title + header rows

    assert result_count == expected_count
