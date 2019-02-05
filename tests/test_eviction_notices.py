import pytest
import dags.eviction_notices as dag

TEST_FILE_PATH = f"tests/test_data/test_eviction_notices.csv"
EXPECTED_SCHEMA = "sf"

def test_create_table(postgresql_db):
    dag.create_table(postgresql_db.postgresql_url)
    expected_schema_table = EXPECTED_SCHEMA + "." + dag.TABLE_NAME

    assert postgresql_db.has_schema(EXPECTED_SCHEMA) == True
    assert postgresql_db.has_table(expected_schema_table) == True
