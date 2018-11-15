import dags.bart_station_names as dag

def test_create_table(postgresql_db):
    dag.create_table(postgresql_db.postgresql_url)

    assert postgresql_db.has_table(dag.TABLE_NAME) == True
