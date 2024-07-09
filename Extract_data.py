import pandas as pd
from bs4 import BeautifulSoup
import requests
import time

link = 'https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page'

# lets check the link if its working 
call = requests.get(link)
if call.status_code == 200:
    print('link is working')
else:
    retries = 5
    for i in range(retries):
        time.sleep(5)
        call = requests.get(link)
        if call.status_code == 200:
            break
        else:
            continue
        

        

