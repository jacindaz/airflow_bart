import gzip
import pytest
import dags.hourly_ridership_by_origin_destination_pairs as dag

TEST_YEAR = 1111
TEST_FILE_PATH = f"/Users/jacinda.zhong/Documents/jacinda/jacinda_airflow/tests/test_data/date-hour-soo-dest-{TEST_YEAR}.csv"
EXPECTED_SCHEMA = "bart"

def test_create_table(postgresql_db):
    dag.create_table(postgresql_db.postgresql_url, [TEST_YEAR])
    expected_schema_table = EXPECTED_SCHEMA + "." + dag._table_name(TEST_YEAR)

    assert postgresql_db.has_schema(EXPECTED_SCHEMA) == True
    assert postgresql_db.has_table(expected_schema_table) == True
