import functions_framework
import os
import io
import requests
import yaml
import logging
import google.cloud.logging
import time
import pandas as pd
import re
from datetime import datetime, timedelta
import json
from google.cloud import bigquery


# Load the config file
with open('dpl_config.yaml') as file:
    config = yaml.safe_load(file)

# Define Zuora credentials
base_url = config['base_url']
client_id = os.environ['CLIENT_ID']
client_secret = os.environ['CLIENT_SECRET']
bq_project = os.environ['BQ_PROJECT']
bq_dataset = os.environ['BQ_DATASET']

# logging.basicConfig(level=logging.DEBUG)
client = google.cloud.logging.Client(project=bq_project)
client.setup_logging()


def get_access_token():
    # generate access token    
    generate_token = requests.post('https://rest.eu.zuora.com/oauth/token',
                                   data={"grant_type": "client_credentials", "scope": "WebLinkAPI"},
                                   auth=(client_id, client_secret))
    return generate_token.json()["access_token"]


def headers_request(access_token):
    # generate headers 
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    return headers


def start_time_calculation(start_ts):
    # Calculate the start time by subtracting 10 minutes from the given timestamp
    start_time = datetime.strptime(start_ts, "%Y-%m-%d %H:%M:%S")
    start_time = start_time - timedelta(minutes=10)
    return start_time


def end_time_calculation(end_ts):
    # Parse the end timestamp into a datetime object
    end_time = datetime.strptime(end_ts, "%Y-%m-%d %H:%M:%S")
    return end_time


def bq_query(query):
    # Execute a BigQuery query and return the results
    client = bigquery.Client()
    query_job = client.query(query)
    results = query_job.result()
    return results


def truncate_table(table):
    # Truncate the specified BigQuery table
    query = f"""
    TRUNCATE TABLE `{bq_project}.{bq_dataset}.{table}`
    """
    job_result = bq_query(query)
    return logging.info(f'The {table} has been truncated. {job_result}')


def initiate_create_stg_table_if_nesessary(table_name, table_schema):
    # Create a staging table if it doesn't exist in BigQuery
    table = f'stg_{table_name}'.lower()
    query = f"""
    CREATE TABLE IF NOT EXISTS `{bq_project}.{bq_dataset}.{table}`
    ({table_schema})
    """
    job_result = bq_query(query)
    return logging.info(f'The {table} has been initiated. {job_result}')


def upload_data_to_stg_table(table_name, df):
    # Upload data from a DataFrame to a staging table in BigQuery
    table = f'stg_{table_name}'.lower()
    truncate_table(table)
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig()
    job = client.load_table_from_dataframe(df, f'{bq_project}.{bq_dataset}.{table}', job_config=job_config)
    job_result = job.result()
    return logging.info(f'The {table} table has been uploaded. {job_result}')


def create_request(table, start_time, end_time):
    # Create a job request to extract data from Zuora API for a specific table
    job_url = base_url
    json_data = {
        "query": f"SELECT deleted, * FROM {table} WHERE UpdatedDate > timestamp '{start_time}' AND UpdatedDate <= timestamp '{end_time}'",
        "outputFormat": "CSV",
        "compression": "NONE",
        "retries": 3,
        "output": {
            "target": "s3"
        }
    }
    response = requests.post(job_url, json=json_data, headers=headers)
    response_data = response.json()
    job_id = response_data['data']['id']
    return job_id


def waiting_completed_status(job_id):
    # Wait for the job to reach a completed, aborted, or cancelled status
    status_url = f'{base_url}/{job_id}'
    status = 'pending'
    while status not in ['completed', 'aborted', 'cancelled']:
        time.sleep(5)
        response = requests.get(status_url, headers=headers)
        response_data = response.json()
        status = response_data['data']['queryStatus']
    return status, response_data


def get_file_url(response_data, status):
    # Get the URL of the extracted data file
    if status == 'completed':
        data_file_url = response_data['data']['dataFile']
    else:
        logging.warning(f'Your job has been stopped with status {status}')
        exit(1)
    return data_file_url


def create_table_schema(column_names, table):
    # Create the BigQuery table schema based on column names from the data file
    bq_columns = []
    df_new_columns = []
    table = table.lower()
    for column in column_names:
        column = column.lower()
        column = column.replace(f'{table}.', '')
        column = column.replace('.', '_')
        column = column.replace(' ', '_')
        column = column.replace('/', '_')
        column = column.replace('?', '')
        column = column.lstrip('_')
        data_type = 'STRING'
        bq_columns.append(f'{column} {data_type}')
        df_new_columns.append(column)
    bq_columns = ', '.join(bq_columns)
    return bq_columns, df_new_columns


def extract_and_process_files(data_file_url, table_name):
    # Extract data from the file, create a staging table, and upload the data to it
    response = requests.get(data_file_url)
    data = response.content
    column_names = data.splitlines()[0].decode('utf-8').split(',')
    csv_file = io.StringIO(data.decode('utf-8'))
    bq_columns, df_new_columns = create_table_schema(column_names, table_name)
    df = pd.read_csv(csv_file, names=df_new_columns, header=0, dtype='str')
    initiate_create_stg_table_if_nesessary(table_name, bq_columns)
    upload_data_to_stg_table(table_name, df)
    return logging.info('Files have been processed')


# Main
access_token = get_access_token()
headers = headers_request(access_token)


@functions_framework.http
def main(request):
    request_json = request.get_json()
    
    # Extract and handle the start-time argument
    start_ts = request_json['calls'][0][0]
    start_time = start_time_calculation(start_ts)
    
    # Extract and handle the end-time argument
    end_ts = request_json['calls'][0][1]
    end_time = end_time_calculation(end_ts)
    
    # processing tables one by one
    try:
        for table in config['extracted_tables']:
            job_id = create_request(table, start_time, end_time)
            status, response_data = waiting_completed_status(job_id)
            data_file_url = get_file_url(response_data, status)
            extract_and_process_files(data_file_url, table)

        return json.dumps({"replies": ["Job is finished"]})

    except Exception as e:
        return json.dumps({"errorMessage": str(e)})

