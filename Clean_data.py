from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import col, unix_timestamp


spark =   SparkSession.builder.appName('d2k').getOrCreate()

def clean_yellow_taxi(file):
    df = spark.read_csv(f'scraped_data/{file}')
    df = df.select(['VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count', 'trip_distance',
                    'RatecodeID', 'store_and_fwd_flag', 'PULocationID', 'DOLocationID', 'payment_type', 'fare_amount',
                    'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount',
                  'congestion_surcharge'])
    df_cleaned = df.dropna()
    df = df.withColumn("tpep_pickup_datetime", col("tpep_pickup_datetime").cast("timestamp"))
    df = df.withColumn("tpep_dropoff_datetime", col("tpep_dropoff_datetime").cast("timestamp"))

        # Compute time difference in seconds
    df = df.withColumn("trip_duration", 
                   unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime"))

    df_cleaned = df_cleaned.withColumn('average_speed', col('trip_distance') / col('trip_duration'))
    return df_cleaned

def clean_green_taxi(file):
    df = spark.read.parquet(f'scraped_data/{file}')
    df = df.select(['VendorID', 'lpep_pickup_datetime', 'lpep_dropoff_datetime', 'passenger_count', 'trip_distance',
                    'RatecodeID', 'store_and_fwd_flag', 'PULocationID', 'DOLocationID', 'payment_type', 'fare_amount',
                    'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount',
                  'congestion_surcharge'])
    df_cleaned = df.dropna()
    df_cleaned = df_cleaned.withColumn("lpep_pickup_datetime", col("lpep_pickup_datetime").cast("timestamp"))
    df_cleaned = df_cleaned.withColumn("lpep_dropoff_datetime", col("lpep_dropoff_datetime").cast("timestamp"))

        # Compute time difference in seconds
    df_cleaned = df_cleaned.withColumn("trip_duration", 
                   unix_timestamp("lpep_dropoff_datetime") - unix_timestamp("lpep_pickup_datetime"))

    df_cleaned = df_cleaned.withColumn('average_speed', col('trip_distance') / col('trip_duration'))
    return df_cleaned