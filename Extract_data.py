import pandas as pd
from bs4 import BeautifulSoup
import requests
import time
import logging


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

def extract_trip_type(url):
    if 'yellow' in url:
        return 'yellow_trips'
    elif 'green' in url:
        return 'green_trips'
    elif 'fhvhv_trip' in url:
        return 'fhvhv_trip'
    elif 'fhv' in url:
        return 'fhv_trips'
    else:
        return None
    
def get_data(response):
    soup = BeautifulSoup(response.text, features='html.parser')
    links = soup.find(attrs={"id": "faq2019"}).find_all('a', href=True)
    data_list = [link['href'] for link in links]
    df = pd.DataFrame(data_list, columns =['links'])
    date_part = df['links'].str.split('/').str[-1]  # Extract the last part after splitting by '/'
    df['date'] = date_part.str.replace(r'[^0-9-]', '',regex=True)
    logging.info('data scraped sucessfully')
    df['trip_type'] = df['links'].apply(extract_trip_type)
    grouped = df.groupby(['date', 'trip_type']).first().reset_index()
    pivot_df = grouped.pivot(index='date', columns='trip_type', values='links').reset_index()
    logging.info('pivot data is ready to use')
    return pivot_df


    






        

