from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import json



# Define the DAG
with DAG (
    dag_id = 'nasa-apod-postgres',
    start_date = days_ago(1),
    schedule = '@daily',
    catchup=False
) as dag:
    
    # Step 1: Create the table if it doesnt exist
    @task
    def create_table():
        # Initialize postgres hook (To interact with postgresql)
        postgres_hook = PostgresHook(postgres_conn_id="my_postgres_connection")

        # SQL Query to create the table
        create_table_query = """
        CREATE TABLE IF NOT EXISTS apod_data (
            id SERIAL PRIMARY KEY,
            title VARCHAR(255),
            explanation TEXT,
            url TEXT,
            date DATE,
            media_type VARCHAR(50)
            );
        """

        # Executing the table creation query
        postgres_hook.run(create_table_query)


    # Step 2: Extract NASA API Data (APOD Data) [Extract Pipeline]
    extract_apod = SimpleHttpOperator(
        task_id = 'extract_apod', 
        http_conn_id = 'nasa_api', # Connection ID Defined In Airflow for NASA API
        endpoint = 'planetary/apod', # NASA API endpoint
        method = 'GET',
        data = {"api_key":"{{ conn.nassa_api.extra_dejson.api_key }}"} # API Key from the connection
    )

    # Step 3: transform the data (Pick the information we need to save)

    # Step 4: Loading Data into Postgres SQL

    # Step 5: verifying the data with DBViewer

    # Step 6: Define the task dependencies

    pass