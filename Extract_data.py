import pandas as pd
from bs4 import BeautifulSoup
import requests
import time
import logging

link = 'https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page'


logging.basicConfig(level=logging.INFO)  # Set logging level to INFO globally

def call_func(link):
    retries = 5
    for attempt in range(retries):
        response = requests.get(link)
        if response.status_code == 200:
            logging.info('Link is working')
            return response  # Return the response if successful
        else:
            logging.warning(f'Retry attempt {attempt + 1}. Status code: {response.status_code}')
            time.sleep(5)  # Wait for 5 seconds before retrying

    logging.error(f'Failed to fetch data from {link} after {retries} attempts')
    return None  # Return None if all retries fail





        

