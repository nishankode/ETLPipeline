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
        data = {"api_key":"{{ conn.nasa_api.extra_dejson.api_key }}"}, # API Key from the connection
        response_filter = lambda response: response.json(), # Convert response to json
    )

    # Step 3: transform the data (Pick the information we need to save)
    @task
    def transform_apod_data(response):
        apod_data = {
            'title' : response.get('title', ''),
            'explanation' : response.get('explanation', ''),
            'url' : response.get('url', ''),
            'date' : response.get('date', ''),
            'media_type' : response.get('media_type', '')
        }
        return apod_data

    # Step 4: Loading Data into Postgres SQL
    @task
    def load_data_to_postgres(apod_data):
        # Initialize the postgresHook (To load the data to postgres)
        postgres_hook = PostgresHook(postgres_conn_id = 'my_postgres_connection')

        # Define the SQL Insert Query
        insert_query = """
        INSERT INTO apod_data (title, explanation, url, date, media_type)
        VALUES (%s, %s, %s, %s, %s)
        """

        # Executing the SQL Query
        postgres_hook.run(insert_query, parameters=(
            apod_data['title'],
            apod_data['explanation'],
            apod_data['url'],
            apod_data['date'],
            apod_data['media_type']
        ))

    # Step 5: verifying the data with DBViewer

    # Step 6: Define the task dependencies
    # Extract
    create_table() >> extract_apod # Ensuring the table is created before extraction
    api_response = extract_apod.output
    # Transform
    transformed_data = transform_apod_data(api_response)
    # Load
    load_data_to_postgres(transformed_data)
