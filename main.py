import logging
import pandas as pd
import Extract_data as ed
import os

logging.basicConfig(
    filename='app.log',  # Name of the log file
    level=logging.INFO,  # Logging level for capturing messages
    format='%(asctime)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

link = 'https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page'

# responce = ed.call_func(link)
# data_link = ed.get_data(responce)

# directory = 'scraped_data'
# os.makedirs(directory, exist_ok=True)
# data_link.to_csv('scraped_data/data_link.csv', index=False)

df = pd.read_csv('scraped_data/data_link.csv')

for link in df[df.columns[1:]].values.flatten():
    if pd.notna(link):
        ed.get_file('scraped_data',link)
    else:
        continue
