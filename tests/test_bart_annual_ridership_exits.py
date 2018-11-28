import xlrd

import dags.bart_annual_ridership_exits as dag

def test_create_table(postgresql_db):
    dag.create_table(postgresql_db.postgresql_url)

    assert postgresql_db.has_table(dag.TABLE_NAME) == True

def test_import_annual_ridership(postgresql_db):
    dag.create_table(postgresql_db.postgresql_url)

    test_file_path = 'tests/test_data/test_BART_Ridership_FY73_FY18.xlsx'
    dag.import_annual_ridership(db_uri=postgresql_db.postgresql_url, file_path=test_file_path)

    result_proxy_obj = postgresql_db.session.execute(f"select count(*) from bart.{dag.TABLE_NAME}")
    result_count = result_proxy_obj.first()[0]

    book = xlrd.open_workbook(test_file_path)
    sheet = book.sheet_by_index(0)

    # minus the title, header,random rows at the end
    expected_count = sheet.nrows - 6

    assert result_count == expected_count
