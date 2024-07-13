import logging
import pandas as pd
import sqlite3


# Function to connect to the database and fetch data
def fetch_data(query):
    conn = sqlite3.connect('testdb.db')  # Replace with your database connection string
    df = pd.read_sql_query(query, conn)
    conn.close()
    print(df)
    return df

# Define your SQL queries
query_green_taxi = '''
SELECT 
    date(lpep_pickup_datetime) as Date, 
    COUNT(vendorid) AS total_trip,
    AVG(fare_amount) AS avg_fare_amount
FROM
    green_taxi
GROUP BY date(lpep_pickup_datetime) 
ORDER BY date(lpep_pickup_datetime) 
'''

query_yellow_taxi = '''
SELECT 
    date(tpep_pickup_datetime) as Date, 
    COUNT(vendorid) AS total_trip,
    AVG(fare_amount) AS avg_fare_amount
FROM
    yellow_taxi
GROUP BY date(tpep_pickup_datetime)
ORDER BY Date
'''

hourly_analysis_green_taxi = '''
SELECT 
    strftime('%H', lpep_pickup_datetime) as Hour, 
    COUNT(vendorid) AS total_trip
FROM
    green_taxi
GROUP BY strftime('%H', lpep_pickup_datetime) 
ORDER BY Hour
'''

hourly_analysis_yellow_taxi ='''
SELECT 
    strftime('%H', tpep_pickup_datetime) as Hour, 
    COUNT(vendorid) AS total_trip
FROM
    yellow_taxi
GROUP BY strftime('%H', tpep_pickup_datetime)
ORDER BY Hour
'''

query_fare_analysis_yellow_taxi = '''
SELECT 
    distinct passenger_count,
    fare_amount
FROM
    yellow_taxi
WHERE
    passenger_count > 0  -- Ensuring we only consider positive passenger counts
    AND fare_amount > 0  -- Ensuring we only consider positive fare amounts
'''

query_fare_analysis_green_taxi = '''
SELECT 
    distinct passenger_count,
    fare_amount
FROM
    green_taxi
WHERE
    passenger_count > 0  -- Ensuring we only consider positive passenger counts
    AND fare_amount > 0  -- Ensuring we only consider positive fare amounts
'''

def aggregated_data():
    con = sqlite3.connect("testdb.db")

    df_yellow_taxi = fetch_data(query_yellow_taxi)
    logging.info('yellow taxi grouped calculated')
    df_yellow_taxi.to_sql('yellow_taxi_aggregate_data', con, if_exists="append", index=False)
    logging.info('yellow_taxi_aggregate_data table updated with the data')

    df_green_taxi = fetch_data(query_green_taxi)
    logging.info('green taxi grouped calculated')
    df_green_taxi.to_sql('green_taxi_aggregate_data', con, if_exists="append", index=False)
    logging.info('green_taxi_aggregate_data table updated with the data')
    return 'completed'

def analysis_func():
    green_taxi = fetch_data('SELECT * FROM green_taxi_aggregate_data')
    green_taxi.to_csv('scraped_data/green_taxi.csv', index= False)
    logging.info("Fetched data from green_taxi_aggregate_data view.")

    yellow_taxi = fetch_data('SELECT * FROM yellow_taxi_aggregate_data')
    yellow_taxi.to_csv('scraped_data/yellow_taxi.csv', index= False)
    logging.info("Fetched data from yellow_taxi_aggregate_data view.")
    
    peak_green_taxi = fetch_data(hourly_analysis_green_taxi)
    peak_green_taxi.to_csv('scraped_data/peak_green_taxi.csv', index= False)
    logging.info("Fetched data from peak_time_green_taxi view.")

    peak_yellow_taxi = fetch_data(hourly_analysis_yellow_taxi)
    peak_yellow_taxi.to_csv('scraped_data/peak_yellow_taxi.csv', index= False)
    logging.info("Fetched data from peak_time_yellow_taxi view.")

    fare_yellow_taxi = fetch_data(query_fare_analysis_yellow_taxi)
    fare_yellow_taxi.to_csv('scraped_data/fare_yellow_taxi.csv', index= False)
    logging.info("Fetched data from fare_analysis_yellow_taxi view.")

    fare_green_taxi = fetch_data(query_fare_analysis_green_taxi)
    fare_green_taxi.to_csv('scraped_data/fare_green_taxi.csv', index= False)
    logging.info("Fetched data from fare_analysis_green_taxi view.")

    return 'complete'