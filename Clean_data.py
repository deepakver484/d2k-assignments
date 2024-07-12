from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import col, unix_timestamp
import sqlite3
import logging
import os
import sys

# Set up logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Set recursion limit higher to avoid recursion errors
sys.setrecursionlimit(1500)

def clean_yellow_taxi(file):
    """
    This function reads a parquet file, cleans the data, computes additional columns, and appends the cleaned data to an SQLite database.
    """
    spark = None
    try:
        # Initialize Spark Session
        spark = SparkSession.builder.appName('d2k').getOrCreate()
        logging.info('Spark session created successfully')

        # Check if the parquet file exists
        if not os.path.exists(f'scraped_data/{file}'):
            logging.warning(f'Parquet file scraped_data/{file} does not exist')
            return 'file_not_found'

        # Read the parquet file
        df = spark.read.parquet(f'scraped_data/{file}')
        logging.info(f'Read parquet file: scraped_data/{file}')

        # Check if the DataFrame is empty
        if df.count() == 0:
            logging.warning(f'The parquet file scraped_data/{file} is empty')
            return 'no_data_in_file'

        # Select required columns
        df = df.select(['VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count', 'trip_distance',
                        'RatecodeID', 'store_and_fwd_flag', 'PULocationID', 'DOLocationID', 'payment_type', 'fare_amount',
                        'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount',
                        'congestion_surcharge'])
        logging.info('Selected required columns')

        # Drop rows with null values
        df_cleaned = df.dropna()
        logging.info('Dropped rows with null values')

        # Check if the cleaned DataFrame is empty
        if df_cleaned.count() == 0:
            logging.warning('After cleaning, the DataFrame is empty')
            return 'no_data_after_cleaning'

        # Cast columns to the appropriate data types
        df_cleaned = df_cleaned.withColumn("tpep_pickup_datetime", col("tpep_pickup_datetime").cast("timestamp"))
        df_cleaned = df_cleaned.withColumn("tpep_dropoff_datetime", col("tpep_dropoff_datetime").cast("timestamp"))
        logging.info('Converted datetime columns to timestamp')

        # Compute time difference in seconds
        df_cleaned = df_cleaned.withColumn("trip_duration", 
                       unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime"))
        logging.info('Created trip_duration column')

        # Compute average speed
        df_cleaned = df_cleaned.withColumn('average_speed', col('trip_distance') / col('trip_duration'))
        logging.info('Created average_speed column')

        # Define a batch processing function
        def process_batch(batch_df, db_path, table_name):
            """
            Process each batch and append it to the SQLite database with error handling and logging.
            """
            try:
                pandas_df = batch_df.toPandas()
                logging.info(f'Converted batch DataFrame to Pandas DataFrame with {len(pandas_df)} rows')
                try:
                    conn = sqlite3.connect(db_path)
                    pandas_df.to_sql(table_name, conn, if_exists="append", index=False)
                    conn.commit()
                    logging.info(f'Batch data appended to table {table_name} in SQLite database successfully')
                except Exception as e:
                    logging.error(f'Error occurred while saving batch data to SQLite database: {e}')
                finally:
                    conn.close()
                    logging.info('SQLite connection closed')
            except Exception as e:
                logging.error(f'An error occurred while processing the batch: {e}')

        # Batch processing
        batch_size = 50000  # Define the batch size
        total_rows = df_cleaned.count()
        for offset in range(0, total_rows, batch_size):
            try:
                # Select the batch of data
                batch_df = df_cleaned.limit(batch_size).offset(offset)
                process_batch(batch_df, "d2k_test3.db", "yellow_taxi")
            except Exception as e:
                logging.error(f'An error occurred while processing batch starting at offset {offset}: {e}')

        logging.info('Batch processing complete.')

    except Exception as e:
        logging.error(f'An unexpected error occurred: {e}')
    finally:
        # Ensure Spark session is stopped
        try:
            if spark:
                spark.stop()
                logging.info('Spark session stopped')
        except Exception as e:
            logging.error(f'An error occurred while stopping the Spark session: {e}')

    return 'completed'

def clean_green_taxi(file):
    """
    This function reads a parquet file, cleans the data, computes additional columns, and appends the cleaned data to an SQLite database.
    """
    spark = None
    try:
        # Initialize Spark Session
        spark = SparkSession.builder.appName('d2k').getOrCreate()
        logging.info('Spark session created successfully')

        # Check if the parquet file exists
        if not os.path.exists(f'scraped_data/{file}'):
            logging.warning(f'Parquet file scraped_data/{file} does not exist')
            return 'file_not_found'

        # Read the parquet file
        df = spark.read.parquet(f'scraped_data/{file}')
        logging.info(f'Read parquet file: scraped_data/{file}')

        # Check if the DataFrame is empty
        if df.count() == 0:
            logging.warning(f'The parquet file scraped_data/{file} is empty')
            return 'no_data_in_file'

        # Select required columns
        df = df.select(['VendorID', 'lpep_pickup_datetime', 'lpep_dropoff_datetime', 'passenger_count', 'trip_distance',
                        'RatecodeID', 'store_and_fwd_flag', 'PULocationID', 'DOLocationID', 'payment_type', 'fare_amount',
                        'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount',
                        'congestion_surcharge', 'trip_type'])
        logging.info('Selected required columns')

        # Drop rows with null values
        df_cleaned = df.dropna()
        logging.info('Dropped rows with null values')

        # Check if the cleaned DataFrame is empty
        if df_cleaned.count() == 0:
            logging.warning('After cleaning, the DataFrame is empty')
            return 'no_data_after_cleaning'

        # Cast columns to the appropriate data types
        df_cleaned = df_cleaned.withColumn("lpep_pickup_datetime", col("lpep_pickup_datetime").cast("timestamp"))
        df_cleaned = df_cleaned.withColumn("lpep_dropoff_datetime", col("lpep_dropoff_datetime").cast("timestamp"))
        logging.info('Converted datetime columns to timestamp')

        # Compute time difference in seconds
        df_cleaned = df_cleaned.withColumn("trip_duration", 
                       unix_timestamp("lpep_dropoff_datetime") - unix_timestamp("lpep_pickup_datetime"))
        logging.info('Created trip_duration column')

        # Compute average speed
        df_cleaned = df_cleaned.withColumn('average_speed', col('trip_distance') / col('trip_duration'))
        logging.info('Created average_speed column')

        # Define a batch processing function
        def process_batch(batch_df, db_path, table_name):
            """
            Process each batch and append it to the SQLite database with error handling and logging.
            """
            try:
                pandas_df = batch_df.toPandas()
                logging.info(f'Converted batch DataFrame to Pandas DataFrame with {len(pandas_df)} rows')
                try:
                    conn = sqlite3.connect(db_path)
                    pandas_df.to_sql(table_name, conn, if_exists="append", index=False)
                    conn.commit()
                    logging.info(f'Batch data appended to table {table_name} in SQLite database successfully')
                except Exception as e:
                    logging.error(f'Error occurred while saving batch data to SQLite database: {e}')
                finally:
                    conn.close()
                    logging.info('SQLite connection closed')
            except Exception as e:
                logging.error(f'An error occurred while processing the batch: {e}')

        # Batch processing
        batch_size = 50000  # Define the batch size
        total_rows = df_cleaned.count()
        for offset in range(0, total_rows, batch_size):
            try:
                # Select the batch of data
                batch_df = df_cleaned.limit(batch_size).offset(offset)
                process_batch(batch_df, "d2k_test3.db", "green_taxi")
            except Exception as e:
                logging.error(f'An error occurred while processing batch starting at offset {offset}: {e}')

        logging.info('Batch processing complete.')

    except Exception as e:
        logging.error(f'An unexpected error occurred: {e}')
    finally:
        # Ensure Spark session is stopped
        try:
            if spark:
                spark.stop()
                logging.info('Spark session stopped')
        except Exception as e:
            logging.error(f'An error occurred while stopping the Spark session: {e}')

    return 'completed'

