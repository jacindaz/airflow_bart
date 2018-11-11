import dags.bart_annual_ridership_exits as dag

def test_create_table(postgresql_db):
    dag.create_table(postgresql_db.postgresql_url)

    assert postgresql_db.has_table(dag.TABLE_NAME) == True
