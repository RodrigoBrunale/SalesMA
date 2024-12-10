import os
import logging
from google.cloud import bigquery
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def run_sales_ma(event):
    logger.info('Cloud Function started')

    # Load and validate environment variables
    required_vars = [
        'BIGQUERY_SOURCE_TABLE',
        'BIGQUERY_DESTINATION_TABLE_DAY',
        'BIGQUERY_DESTINATION_TABLE_WEEK',
        'BIGQUERY_DESTINATION_TABLE_MONTH',
        'DATE_COLUMN',
        'SALES_COLUMN'
    ]
    for var in required_vars:
        if not os.getenv(var):
            raise ValueError(f"Environment variable {var} must be set.")

    source_table = os.getenv('BIGQUERY_SOURCE_TABLE')
    day_table = os.getenv('BIGQUERY_DESTINATION_TABLE_DAY')
    week_table = os.getenv('BIGQUERY_DESTINATION_TABLE_WEEK')
    month_table = os.getenv('BIGQUERY_DESTINATION_TABLE_MONTH')
    date_column = os.getenv('DATE_COLUMN')
    sales_column = os.getenv('SALES_COLUMN')

    # Default windows if not provided
    windows_day = [int(x) for x in os.getenv('WINDOWS_DAY', '7,21,30,50,100,200').split(',')]
    windows_week = [int(x) for x in os.getenv('WINDOWS_WEEK', '4,13,26,52,78,104').split(',')]
    windows_month = [int(x) for x in os.getenv('WINDOWS_MONTH', '1,3,6,12,18,24,36').split(',')]

    client = bigquery.Client()

    # Fetch source data
    sql_query = f"""
        SELECT
            {date_column},
            pedido_number,
            {sales_column}
        FROM `{source_table}`
    """
    logger.info('Executing query: %s', sql_query)
    data = client.query(sql_query).to_dataframe()

    # Convert and validate date column
    data[date_column] = pd.to_datetime(data[date_column], errors='coerce')
    if data[date_column].isnull().any():
        raise ValueError("Some rows have invalid date values. Please check the source data.")
    
    # Filter out non-positive sales
    initial_count = len(data)
    data = data[data[sales_column] > 0]
    logger.info('Filtered out non-positive sales. Removed %d rows.', initial_count - len(data))

    # Deduplicate based on (date, pedido_number)
    before_dedup = len(data)
    data = data.drop_duplicates(subset=[date_column, 'pedido_number'], keep='first')
    logger.info('Removed %d duplicate rows.', before_dedup - len(data))

    # Aggregate daily data
    daily_data = data.groupby(data[date_column].dt.date)[sales_column].sum().reset_index()
    daily_data.rename(columns={date_column: 'date'}, inplace=True)

    # Fill missing days with zero sales
    full_date_range = pd.date_range(start=daily_data['date'].min(), end=daily_data['date'].max(), freq='D')
    daily_data = daily_data.set_index('date').reindex(full_date_range, fill_value=0).reset_index()
    daily_data.columns = ['date', sales_column]
    logger.info('Missing days filled with zero. Total daily rows: %d', len(daily_data))

    # Create weekly and monthly aggregates from daily data
    daily_data['date'] = pd.to_datetime(daily_data['date'])

    weekly_data = daily_data.set_index('date').resample('W')[sales_column].sum().reset_index()
    monthly_data = daily_data.set_index('date').resample('M')[sales_column].sum().reset_index()

    # Compute MAs
    def compute_mas(df, freq, windows):
        for window in windows:
            df[f'MA_{window}{freq}'] = df[sales_column].rolling(window).mean()
            weights = np.arange(1, window + 1)
            df[f'WMA_{window}{freq}'] = df[sales_column].rolling(window).apply(
                lambda prices: np.dot(prices, weights) / weights.sum(), raw=True
            )
            df[f'EMA_{window}{freq}'] = df[sales_column].ewm(span=window, adjust=False).mean()
        return df

    daily_data = compute_mas(daily_data, 'day', windows_day)
    weekly_data = compute_mas(weekly_data, 'week', windows_week)
    monthly_data = compute_mas(monthly_data, 'month', windows_month)

    def upload_to_bq(df, destination_table):
        if not destination_table:
            raise ValueError("Destination table not set. Please set the appropriate environment variable.")
        schema = [
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField(sales_column, "FLOAT64")
        ] + [bigquery.SchemaField(col, "FLOAT64") for col in df.columns if col not in ["date", sales_column]]

        job_config = bigquery.LoadJobConfig(schema=schema, time_partitioning=bigquery.TimePartitioning(field="date"))
        job = client.load_table_from_dataframe(df, destination_table, job_config=job_config)
        job.result()
        logger.info('Data uploaded to %s', destination_table)

    # Upload
    upload_to_bq(daily_data, day_table)
    upload_to_bq(weekly_data, week_table)
    upload_to_bq(monthly_data, month_table)

    logger.info('Cloud Function completed successfully')
    return ("MAs computed and uploaded successfully", 200)
