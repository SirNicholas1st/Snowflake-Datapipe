from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from sqlalchemy import create_engine

default_args = {
    "owner": "SirNicholas1st",
    "retries": 2,
    "retry_delay": timedelta(minutes=2)
}

@dag(
    dag_id = "Orchestrator",
    start_date = datetime(2023, 9, 3),
    default_args = default_args,
    schedule = timedelta(minutes = 10),
    catchup = False
)
def data_to_other_tables():
    
    # the purpose of this task is to retrieve the customerids that are not already in the customerID table
    @task
    def get_customer_ids():
        snowflake_hook = SnowflakeHook(snowflake_conn_id = "Snowflake")
        connection = snowflake_hook.get_uri()
        engine = create_engine(connection)

        query = """SELECT DISTINCT(customer_id)
                FROM weather_data
                WHERE customer_id NOT IN (
                    SELECT customer_id
                    FROM customer_ids
                )
                """

        result = engine.execute(query)

        data = [dict(row) for row in result.fetchall()]

        return data
    
    @task
    def add_customer_ids(data_from_get_customer_ids: dict):
        snowflake_hook = SnowflakeHook(snowflake_conn_id = "Snowflake")
        connection = snowflake_hook.get_uri()
        engine = create_engine(connection)

        # TODO loop for adding the customer ids to the customer_id table.
        for id in data_from_get_customer_ids:
            print(id["customer_id"])
    

    task1 = get_customer_ids()

    task2 = add_customer_ids(task1)

    task1 >> task2

data_to_other_tables()