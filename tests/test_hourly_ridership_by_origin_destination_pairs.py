import dags.hourly_ridership_by_origin_destination_pairs as dag

def test_orm(postgresql_db):
    dag.create_table(postgresql_db.postgresql_url)

    for year in dag.FILE_YEARS:
        assert postgresql_db.has_table(dag._table_name(year)) == True
