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

    # Step 2: Extract NASA API Data (APOD Data) [Extract Pipeline]

    # Step 3: transform the data (Pick the information we need to save)

    # Step 4: Loading Data into Postgres SQL

    # Step 5: verifying the data with DBViewer

    # Step 6: Define the task dependencies

    pass