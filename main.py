import logging 
import pandas as pd
import Extract_data as ed
import Clean_data as cd
import os
import Sql_database as sd

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

responce = ed.call_func(link)
data_link = ed.get_data(responce)

directory = 'scraped_data'
os.makedirs(directory, exist_ok=True)
data_link.to_csv('scraped_data/data_link.csv', index=False)

df = pd.read_csv('scraped_data/data_link.csv')

for link in df[df.columns['yellow_trips', 'green_trips']].values.flatten():
    if pd.notna(link):
        ed.get_file('scraped_data',link)
    else:
        continue
dataset = sd.database_creation('testdb.db')

df['yellow_trips'].str.split('/').str[-1].apply(lambda file: cd.clean_yellow_taxi(file, dataset))
logging.info('yellow_trips data saved into the database successfully')
df['green_trips'].str.split('/').str[-1].apply(lambda file: cd.clean_green_taxi(file, dataset))
logging.info('green_trips data saved into the database successfully')
