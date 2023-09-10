from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from sqlalchemy import create_engine
import logging

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
                );
                """

        result = engine.execute(query)

        data = [dict(row) for row in result.fetchall()]
        logging.info(f"Retrieved {len(data)} values.")

        return data
    
    @task
    def add_customer_ids(data_from_get_customer_ids: dict):
        if not len(data_from_get_customer_ids):
            logging.info(f"No data to insert.")
            return None
        
        snowflake_hook = SnowflakeHook(snowflake_conn_id = "Snowflake")
        connection = snowflake_hook.get_uri()
        engine = create_engine(connection)

        # md5 purely for practise purposes and to test out an idea if we have a standard way of forming keys
        # we dont always have to look for the keyvalue from other tables when inserting rows.
        for id in data_from_get_customer_ids:
            customer_id = id["customer_id"]
            query = f"""INSERT INTO customer_ids (customer_id, customer_id_hash)
                        SELECT $1, MD5($2) FROM VALUES ('{customer_id}', '{customer_id}');
                        """
            engine.execute(query)
            logging.info(f"Inserted {customer_id} into customer_ids.")
        return None
    
    @task
    def get_location_data():
        snowflake_hook = SnowflakeHook(snowflake_conn_id = "Snowflake")
        connection = snowflake_hook.get_uri()
        engine = create_engine(connection)
        query = f"""SELECT DISTINCT(location_name), latitude, longitude
                    FROM weather_data
                    WHERE location_name NOT IN (
                        SELECT location_name
                        FROM locations
                    );
                    """
        result = engine.execute(query)
        data = [dict(row) for row in result.fetchall()]
        logging.info(f"Retrieved {len(data)} values.")
        return data

    @task
    def add_location_data(data_from_get_location_data: dict):
        snowflake_hook = SnowflakeHook(snowflake_conn_id = "Snowflake")
        connection = snowflake_hook.get_uri()
        engine = create_engine(connection)

        for location_data in data_from_get_location_data:
            location = location_data["location_name"]
            latitude = location_data["latitude"]
            longitude = location_data["longitude"]
            query = f"""INSERT INTO locations (location_name, location_name_hash, latitude, longitude)
                        SELECT $1, MD5($2), $3, $4 FROM VALUES ('{location}', '{location}', {latitude}, {longitude})
                        """
            engine.execute(query)
            logging.info(f"Inserted {location} to locations.")
        return None
    
    @task 
    def get_current_weather_data():
        snowflake_hook = SnowflakeHook(snowflake_conn_id = "Snowflake")
        connection = snowflake_hook.get_uri()
        engine = create_engine(connection)

        query = f"""SELECT customer_id, location_name, current_weather
                    FROM weather_data
                    WHERE processed IS NULL;
                    """
        result = engine.execute(query)
        data = [dict(row) for row in result.fetchall()]
        logging.info(f"Retrieved {len(data)} rows.")
        return data
    
    @task 
    def add_current_weather_data(data_from_get_current_weather_data: dict):
        # TODO add the data retrieved from the snowpipe landing table to the current weather data table
        # TODO mark the rows processed to processed = true when the data has been added to the current weather data table
        pass

        

    # customer id related tasks
    c_task1 = get_customer_ids()
    c_task2 = add_customer_ids(c_task1)
    
    # location data related tasks
    l_task1 = get_location_data()
    l_task2 = add_location_data(l_task1)

    # actual weather data related tasks
    a_task1 = get_current_weather_data()
    a_task2 = add_current_weather_data(a_task1)

    c_task1 >> c_task2
    l_task1 >> l_task2
    a_task1 >> a_task2

data_to_other_tables()