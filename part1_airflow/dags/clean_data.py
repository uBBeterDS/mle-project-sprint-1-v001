from airflow.decorators import dag, task
from steps.messages import send_telegram_failure_message, send_telegram_success_message
from steps.cleaning import remove_duplicates, remove_outliers, fill_missing_values
import pendulum


@dag(
    schedule='@once',
    start_date=pendulum.datetime(2023, 1, 1, tz='UTC'),
    catchup=False,
    tags=["ETL"],
    on_success_callback=send_telegram_success_message,
    on_failure_callback=send_telegram_failure_message
)
def clean_dataset():
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from sqlalchemy import MetaData, Table, Column, Boolean, Integer, Float, UniqueConstraint, Numeric
    import sqlalchemy
    import pandas as pd
    import numpy as np
    @task()
    def create_table():
        hook = PostgresHook('destination_db')
        db_conn = hook.get_sqlalchemy_engine()
        metadata = MetaData()
        flats_churn_table = Table(
            'clean_flats_churn',
            metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('floor', Integer),
            Column('is_apartment', Boolean),
            Column('kitchen_area', Float),
            Column('living_area', Float),
            Column('rooms', Integer),
            Column('studio', Boolean),
            Column('total_area', Float),
            Column('price', Numeric),
            Column('build_year', Integer),
            Column('building_type_int', Integer),
            Column('latitude', Float),
            Column('longitude', Float),
            Column('ceiling_height', Float),
            Column('flats_count', Integer),
            Column('floors_total', Integer),
            Column('has_elevator', Boolean),
            UniqueConstraint('id', name='unique_clean_flat_id')
        )

        if not sqlalchemy.inspect(db_conn).has_table(flats_churn_table.name):
            metadata.create_all(db_conn)

    @task
    def extract():
        hook = PostgresHook('destination_db')
        conn = hook.get_conn()
        sql = f"""
        SELECT
            f.id, f.floor, f.is_apartment, f.kitchen_area, f.living_area, f.rooms, f.studio, f.total_area, f.price, 
            b.build_year, b.building_type_int, b.latitude, b.longitude, b.ceiling_height, b.flats_count, b.floors_total, b.has_elevator
        FROM flats as f
        LEFT JOIN buildings as b
        ON f.building_id = b.id
        """
        data = pd.read_sql(sql, conn)
        conn.close()
        return data

    @task
    def transform(data):
        data = fill_missing_values(data)
        data = remove_duplicates(data)
        data = remove_outliers(data)
        return data
    
    @task
    def load(data):
        hook = PostgresHook('destination_db')
        hook.insert_rows(
            table="clean_flats_churn",
            replace=True,
            target_fields=data.columns.tolist(),
            replace_index=['id'],
            rows=data.values.tolist()
        )
    create_table()
    data = extract()
    data = transform(data)
    load(data)
clean_dataset()