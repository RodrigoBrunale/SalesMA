# SalesMA - Moving Averages Analyzer for Sales Data

This repository contains the code to calculate daily, weekly, and monthly moving averages (MAs) for sales data stored in BigQuery.

## Features
- Filters out non-positive sales to prevent data skew.
- Deduplicates entries for the same order on the same day.
- Fills missing days with zero values to maintain continuous time series.
- Calculates simple (MA), weighted (WMA), and exponential (EMA) moving averages for multiple windows.

## Environment Variables
The following environment variables must be set:

- `BIGQUERY_SOURCE_TABLE`: The source table for sales data.
- `BIGQUERY_DESTINATION_TABLE_DAY`: Target table for daily MAs.
- `BIGQUERY_DESTINATION_TABLE_WEEK`: Target table for weekly MAs.
- `BIGQUERY_DESTINATION_TABLE_MONTH`: Target table for monthly MAs.
- `DATE_COLUMN`: The name of the date column in the source data.
- `SALES_COLUMN`: The name of the sales column in the source data.
- `WINDOWS_DAY`: Comma-separated list of daily windows (default: `7,21,30,50,100,200`).
- `WINDOWS_WEEK`: Comma-separated list of weekly windows (default: `4,13,26,52,78,104`).
- `WINDOWS_MONTH`: Comma-separated list of monthly windows (default: `1,3,6,12,18,24,36`).

## Deployment
Deploy as a Cloud Function triggered by Cloud Scheduler. Ensure all environment variables are set in the Cloud Function’s environment configuration.

## Contributing
- Create a feature branch for changes.
- Submit a PR for review.
