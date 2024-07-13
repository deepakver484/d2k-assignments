import pyarrow.parquet as pq
import pandas as pd
import sqlite3
import logging
import os
import sys
import re


# Set recursion limit higher to avoid recursion errors
sys.setrecursionlimit(1500)

def clean_yellow_taxi(file, database):
    """
    This function reads a parquet file, cleans the data, computes additional columns, and appends the cleaned data to an SQLite database.
    
    Parameters:
    file (str): The name of the parquet file to process.
    database (str): The path to the SQLite database where data will be stored.
    """
    date_string = re.sub(r'[^0-9-]', '', file)
    year = date_string.split('-')[0]
    month = date_string.split('-')[1]
    try:
        # Check if the parquet file exists
        file_path = f'scraped_data/{file}'
        if not os.path.exists(file_path):
            logging.warning(f'Parquet file {file_path} does not exist')
            return 'file_not_found'

        # Read the parquet file
        table = pq.read_table(file_path)
        df = table.to_pandas()
        logging.info(f'Read parquet file: {file_path}')

        # Check if the DataFrame is empty
        if len(df) == 0:
            logging.warning(f'The parquet file {file_path} is empty')
            return 'no_data_in_file'

        # Select required columns
        df = df[['VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count', 'trip_distance',
                        'RatecodeID', 'store_and_fwd_flag', 'PULocationID', 'DOLocationID', 'payment_type', 'fare_amount',
                        'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount',
                        'congestion_surcharge']]
        logging.info('Selected required columns')

        # Drop rows with null values
        df.dropna(inplace = True)
        logging.info(f'Dropped rows with null values, remaining rows: {len(df)}')

        # Check if the cleaned DataFrame is empty
        if len(df) == 0:
            logging.warning('After cleaning, the DataFrame is empty')
            return 'no_data_after_cleaning'

        # Cast columns to the appropriate data types
        df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
        df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
        logging.info('Converted datetime columns to timestamp')


        #  removing the data which is not belongs to the given month, fare_amount, passenger_count, trip_distance and year
        df = df[(df['tpep_pickup_datetime'].dt.month == int(month)) & (df['tpep_pickup_datetime'].dt.year == int(year))]
        df = df[(df['trip_distance'] >=0)&(df['passenger_count'] >=0)&(df['fare_amount'] >=0)]
        logging.info(f'Dropping rows having wrong date, trip_distance, remaining rows: {len(df)}')
        # Compute time difference in seconds
        df['trip_duration'] = (df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime']).dt.total_seconds()
        logging.info('Created trip_duration column')

        # Compute average speed
        df['average_speed'] = round(df['trip_distance'] / df['trip_duration'] ,2)
        logging.info('Created average_speed column')

        conn = sqlite3.connect(database)
        logging.info('database connection stablised')
        df.to_sql('yellow_taxi', conn, if_exists="append", index=False)
        logging.info('data saved into the database')
        conn.close()
        logging.info('connection closed')
    except Exception as e:
        logging.error(f'An error occurred while stopping the Spark session: {e}')

    return 'completed'


def clean_green_taxi(file, database):
    """
    This function reads a parquet file, cleans the data, computes additional columns, and appends the cleaned data to an SQLite database.
    
    Parameters:
    file (str): The name of the parquet file to process.
    database (str): The path to the SQLite database where data will be stored.
    """
    date_string = re.sub(r'[^0-9-]', '', file)
    year = date_string.split('-')[0]
    month = date_string.split('-')[1]
    try:
        # Check if the parquet file exists
        file_path = f'scraped_data/{file}'
        if not os.path.exists(file_path):
            logging.warning(f'Parquet file {file_path} does not exist')
            return 'file_not_found'

        # Read the parquet file
        table = pq.read_table(file_path)
        df = table.to_pandas()
        logging.info(f'Read parquet file: {file_path}')

        # Check if the DataFrame is empty
        if len(df) == 0:
            logging.warning(f'The parquet file {file_path} is empty')
            return 'no_data_in_file'

        # Select required columns
        df = df[['VendorID', 'lpep_pickup_datetime', 'lpep_dropoff_datetime', 'passenger_count', 'trip_distance',
                        'RatecodeID', 'store_and_fwd_flag', 'PULocationID', 'DOLocationID', 'payment_type', 'fare_amount',
                        'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount','trip_type',
                        'congestion_surcharge']]
        logging.info('Selected required columns')

        # Drop rows with null values
        df.dropna(inplace = True)
        logging.info(f'Dropped rows with null values, remaining rows: {len(df)}')

        # Check if the cleaned DataFrame is empty
        if len(df) == 0:
            logging.warning('After cleaning, the DataFrame is empty')
            return 'no_data_after_cleaning'

        # Cast columns to the appropriate data types
        df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
        df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])
        logging.info('Converted datetime columns to timestamp')


        #  removing the data which is not belongs to the given month, fare_amount, passenger_count, trip_distance and year
        df = df[(df['lpep_pickup_datetime'].dt.month == int(month)) & (df['lpep_pickup_datetime'].dt.year == int(year))]
        df = df[(df['trip_distance'] >=0)&(df['passenger_count'] >=0)&(df['fare_amount'] >=0)]
        logging.info(f'Dropping rows having wrong date, trip_distance, remaining rows: {len(df)}')

        # Compute time difference in seconds
        df['trip_duration'] = (df['lpep_dropoff_datetime'] - df['lpep_pickup_datetime']).dt.total_seconds()
        logging.info('Created trip_duration column')

        # Compute average speed
        df['average_speed'] = round(df['trip_distance'] / df['trip_duration'] ,2)
        logging.info('Created average_speed column')

        conn = sqlite3.connect(database)
        logging.info('database connection stablised')
        df.to_sql('green_taxi', conn, if_exists="append", index=False)
        logging.info('data saved into the database')
        conn.close()
        logging.info('connection closed')
    except Exception as e:
        logging.error(f'An error occurred while stopping the Spark session: {e}')

    return 'completed'

