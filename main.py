import logging 
import pandas as pd
import Extract_data as ed
import Clean_data as cd
import os
import Sql_database as sd
import analysis as a
import sqlite3


logger = logging.getLogger()

# Set the logging level
logger.setLevel(logging.INFO) 

# Create a formatter
formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

# Create file handler and set the formatter
file_handler = logging.FileHandler('app.log')
file_handler.setFormatter(formatter)

# Create console handler and set the formatter
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

# Add both handlers to the logger
logger.addHandler(file_handler)
logger.addHandler(console_handler)


link = 'https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page'

# responce = ed.call_func(link)
# data_link = ed.get_data(responce)

# directory = 'scraped_data'
# os.makedirs(directory, exist_ok=True)
# data_link.to_csv('scraped_data/data_link.csv', index=False)

df = pd.read_csv('scraped_data/data_link.csv')

# for link in df[df.columns['yellow_trips', 'green_trips']].values.flatten():
#     if pd.notna(link):
#         ed.get_file('scraped_data',link)
#     else:
#         continue
dataset = sd.database_creation('testdb.db')

# df['yellow_trips'].str.split('/').str[-1].apply(lambda file: cd.clean_yellow_taxi(file, dataset))
# logging.info('yellow_trips data saved into the database successfully')
# df['green_trips'].str.split('/').str[-1].apply(lambda file: cd.clean_green_taxi(file, dataset))
# logging.info('green_trips data saved into the database successfully')


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

con = sqlite3.connect("testdb.db")

df_yellow_taxi = a.fetch_data(query_yellow_taxi)
logging.info('yellow taxi grouped calculated')
df_yellow_taxi.to_sql('yellow_taxi_aggregate_data', con, if_exists="append", index=False)
logging.info('yellow_taxi_aggregate_data table updated with the data')

df_green_taxi = a.fetch_data(query_green_taxi)
logging.info('green taxi grouped calculated')
df_green_taxi.to_sql('green_taxi_aggregate_data', con, if_exists="append", index=False)
logging.info('green_taxi_aggregate_data table updated with the data')