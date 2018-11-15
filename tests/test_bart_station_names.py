import xlrd

import dags.bart_station_names as dag

def test_create_table(postgresql_db):
    dag.create_table(postgresql_db.postgresql_url)

    assert postgresql_db.has_table(dag.TABLE_NAME) == True

def test_import_station_names(postgresql_db):
    dag.create_table(postgresql_db.postgresql_url)

    test_file_path = 'tests/test_data/test_bart_station_names.xls'
    dag.import_station_names(db_uri=postgresql_db.postgresql_url, file_path=test_file_path)

    result_proxy_obj = postgresql_db.session.execute(f"select count(*) from bart.{dag.TABLE_NAME}")
    result_count = result_proxy_obj.first()[0]

    book = xlrd.open_workbook(test_file_path)
    first_sheet = book.sheet_by_index(0)
    expected_count = first_sheet.nrows - 1 # minus the header

    assert result_count == expected_count
