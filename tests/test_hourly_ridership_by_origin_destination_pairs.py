import gzip
import pytest
import dags.hourly_ridership_by_origin_destination_pairs as dag

TEST_YEAR = 1111
TEST_FILE_PATH = f"/Users/jacinda.zhong/Documents/jacinda/jacinda_airflow/tests/test_data/date-hour-soo-dest-{TEST_YEAR}.csv"


def test_create_table(postgresql_db):
    dag.create_table(postgresql_db.postgresql_url, [TEST_YEAR])

    assert postgresql_db.has_table(dag._table_name(TEST_YEAR)) == True
    assert postgresql_db.has_schema('bart') == True


def _gzip_test_file():
    f_in = open(TEST_FILE_PATH, 'rb')
    f_out = gzip.open(TEST_FILE_PATH + '.gz', 'wb')
    f_out.writelines(f_in)
    f_out.close()
    f_in.close()

    return TEST_FILE_PATH + '.gz'


def test_import_hourly_ridership(postgresql_db, mocker):
    dag.create_table(postgresql_db.postgresql_url, [TEST_YEAR])
    assert postgresql_db.has_table(dag._table_name(TEST_YEAR)) == True

    mocker.patch('dags.hourly_ridership_by_origin_destination_pairs._temp_file_name', return_value=_gzip_test_file())
    mocker.patch('dags.hourly_ridership_by_origin_destination_pairs._create_temp_data_file')

    dag.import_hourly_ridership(postgresql_db.postgresql_url, [TEST_YEAR])

    count = postgresql_db.engine.execute(f"select count(*) from bart.{dag._table_name(TEST_YEAR)}").scalar()
    assert count == 10
