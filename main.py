# Import necessary libraries
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import os
import numpy as np
import yaml
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Create handlers
stream_handler = logging.StreamHandler()
file_handler = logging.FileHandler('activity.log')

# Set logging level for handlers
stream_handler.setLevel(logging.INFO)
file_handler.setLevel(logging.INFO)

# Create formatter
formatter = logging.Formatter('%(asctime)s %(message)s')

# Add formatter to handlers
stream_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

# Add handlers to logger
logger.addHandler(stream_handler)
logger.addHandler(file_handler)

logger.info('Script started')

try:
    # Load the configuration file
    try:
        with open('config.yaml', 'r') as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        # If the configuration file does not exist, create a template and exit
        config_template = {
            'google_application_credentials': 'path/to/your/service/account/key.json',
            'bigquery_source_table': 'project.dataset.table',
            'date_column': 'date',
            'sales_column': 'sales',
            'bigquery_destination_table_day': 'project.dataset.table_day',
            'bigquery_destination_table_week': 'project.dataset.table_week',
            'bigquery_destination_table_month': 'project.dataset.table_month',
            'windows_day': [7, 21, 30, 50, 100, 200],
            'windows_week': [1, 2, 3, 4, 5, 10, 20],
            'windows_month': [1, 2, 3, 6, 12, 24]
        }
        with open('config.yaml', 'w') as f:
            yaml.safe_dump(config_template, f)
        logger.info('Configuration file not found. A template has been created. Please fill out the config.yaml file and run the script again.')
        exit()

    logger.info('Configuration file loaded')

    # Establish a connection to BigQuery
    credentials = service_account.Credentials.from_service_account_file(config['google_application_credentials'])
    client = bigquery.Client(credentials=credentials)

    logger.info('Connected to BigQuery')

    # Build the SQL query to get the sales data
    source_table = config['bigquery_source_table']
    date_column = config['date_column']
    sales_column = config['sales_column']
    sql_query = f"SELECT {date_column}, {sales_column} FROM `{source_table}`"

    logger.info('SQL query defined')

    # Execute the SQL query and store the result in a DataFrame
    query_job = client.query(sql_query)
    data = query_job.to_dataframe()

    logger.info('Data loaded from BigQuery')

    # Ensure the sales date column is in datetime format for time-based calculations
    data[date_column] = pd.to_datetime(data[date_column])

    # Sort data by date to ensure calculations are accurate
    data.sort_values(date_column, inplace=True)

    # Group sales data by day, week, and month. This is to prepare the data for moving average calculations.
    data_daily = data.groupby(data[date_column].dt.date)[sales_column].sum().reset_index()
    data_weekly = data.groupby(pd.Grouper(key=date_column, freq='W'))[sales_column].sum().reset_index()
    data_monthly = data.groupby(pd.Grouper(key=date_column, freq='M'))[sales_column].sum().reset_index()

    # Retrieve moving average window sizes from configuration file
    windows_daily = config['windows_day']
    windows_weekly = config['windows_week']
    windows_monthly = config['windows_month']

    # Calculate simple, weighted, and exponential moving averages for each window size and time period
    for data, windows, freq in zip([data_daily, data_weekly, data_monthly], [windows_daily, windows_weekly, windows_monthly], ['day', 'week', 'month']):
        for window in windows:
            data[f'MA_{window}{freq}'] = data[sales_column].rolling(window).mean()
            weights = np.arange(1, window + 1)
            data[f'WMA_{window}{freq}'] = data[sales_column].rolling(window).apply(lambda prices: np.dot(prices, weights) / weights.sum(), raw=True)
            data[f'EMA_{window}{freq}'] = data[sales_column].ewm(span=window, adjust=False).mean()

        data.rename(columns={date_column: 'date'}, inplace=True)

        # Define destination table in BigQuery based on time period (day, week, month)
        destination_table = config[f'bigquery_destination_table_{freq}']

        # Define table schema for BigQuery to correctly interpret the data types
        schema = [
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField(sales_column, "FLOAT64"),
        ] + [bigquery.SchemaField(column, "FLOAT64") for column in data.columns if column not in ["date", sales_column]]

        # Configure the load job to upload DataFrame to BigQuery with specified schema and partitioning by date
        job_config = bigquery.LoadJobConfig(schema=schema, time_partitioning=bigquery.TimePartitioning(field="date"))

        # Upload the DataFrame to BigQuery
        job = client.load_table_from_dataframe(data, destination_table, job_config=job_config)
        job.result()  # Wait for the job to complete

        logger.info(f'Data uploaded to {destination_table} in BigQuery')

    logger.info('Script completed successfully')

# If an error occurs, log the error message and traceback
except Exception as e:
    logger.error('An error occurred:', exc_info=True)
