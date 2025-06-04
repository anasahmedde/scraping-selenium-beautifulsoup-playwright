import concurrent.futures
from bs4 import BeautifulSoup
import os, io, logging, boto3, warnings, requests, time, math, re
from pymongo import MongoClient
from dotenv import load_dotenv
import pandas as pd
from pymongo import MongoClient

warnings.filterwarnings("ignore", category=DeprecationWarning) 
load_dotenv(override=True)

CONNECTION_STRING_MONGODB = os.getenv("CONNECTION_STRING")
aws_region_name = os.getenv("aws_region_name")
bucket_name = os.getenv("bucket_name")
threads = int(os.getenv("threads"))

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',datefmt='%Y-%m-%d %H:%M:%S')

log = logging.getLogger("realEstateTanzania-url-extractor")
log_stringio = io.StringIO()
handler = logging.StreamHandler(log_stringio)
handler.setFormatter(formatter)
log.addHandler(handler)


def sendData(data,columns,databaseName,collectionName):
    try:
        log.info(f'Collected {len(data)} records!')
        df=pd.DataFrame(data,columns=columns)
        mongo_insert_data=df.to_dict('records')
        log.info('Sending Data to MongoDB!')
        def get_database():
            CONNECTION_STRING = CONNECTION_STRING_MONGODB
            client = MongoClient(CONNECTION_STRING)
            return client[databaseName]
        dbname = get_database()
        collection_name = dbname[collectionName]
        for index,instance in enumerate(mongo_insert_data):
            collection_name.update_one({'propertyId':instance['propertyId']},{'$set':instance},upsert=True)
        log.info('Data sent to MongoDB successfully')
    except Exception as e:
        log.info('Some error occurred while sending data MongoDB! Following is the error.')
        log.error(e)

        
cities = ['arusha', 'pwani', 'dodoma', 'iringa', 'shinyanga', 'lindi', 'mtwara', 'mbeya', 'kilimanjaro', 'mwanza', 'njombe', 'pemba', 'tabora', 'tanga', 'zanzibar', 'dar-es-salaam']
def get_links(cityName, page):
    retries = 3
    delay = 30
    while retries > 0:
        try:
            url = f"https://real-estate-tanzania.beforward.jp/city/{cityName}/page/{page}/"
            response = requests.get(url, timeout=120)
            soup = BeautifulSoup(response.content, 'lxml')
            if (soup.find(name='h1', string='403 Forbidden') or soup.find(name='body', string='Too Many Requests.') or soup.find(name='p', string='Too Many Requests.')):
                log.info("Too many requests. Waiting....")
                time.sleep(300)
                response = requests.get(url, timeout=60)
                soup = BeautifulSoup(response.content, 'lxml')
                
            if soup.find("h1", text="Oh oh! Page not found."):
                log.info('not found', url)
                return
            
            return [[link, ids, price] for link, ids, price in zip(
                [a.get('href') for a in soup.select('div.listing-view.list-view.card-deck h2.item-title a[href]')],
                ['BFR-'+span['data-listid'] for span in soup.select('span.hz-show-lightbox-js')],
                [float(re.findall(r'\d{1,3}(?:,\d{3})*(?:\.\d+)?', price.text.split('/')[0])[-1].replace(',', '')) if (re.findall(r'\d{1,3}(?:,\d{3})*(?:\.\d+)?', price.text) != []) else None for price in soup.select('div.item-body.flex-grow-1 li.item-price')]
            )]
        
        except (requests.exceptions.Timeout, requests.exceptions.SSLError):
            log.info("Timeout error occurred. Retrying in {} seconds...".format(delay))
            retries -= 1
            time.sleep(delay)
        except Exception as e:
            log.info(f"Failed to scrape data for {url}: {e}")

databaseName='realEstateTanzania'
links, all_data = [], []

log.info('Gathering property links !')
with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
    futures, links, all_data = [], [], []
    for cityName in cities:
        try:
            res = requests.get(f"https://real-estate-tanzania.beforward.jp/city/{cityName}/", timeout=180)
            soup1 = BeautifulSoup(res.content, 'lxml')
            pages = math.ceil(int(soup1.select_one("div.listing-tabs.flex-grow-1").text.replace(' Properties', '').replace(' Property', ''))/9)
            for page in range(1, pages+1):
                futures.append(executor.submit(get_links, cityName, page))
        except Exception as e:
            log.info(e)

    for future in concurrent.futures.as_completed(futures):
        try:
            links += future.result()
        except Exception as e:
            log.info(e)

sendData(links, ['url', 'propertyId', 'price'], databaseName, 'propertyURLs')
log.info("URL extraction completed successfully.")
s3 = boto3.client("s3", region_name=aws_region_name)
s3.put_object(Body=log_stringio.getvalue(), Bucket=bucket_name, Key="logs/realEstateTanzania/url-extractor-logs.txt")  
log.info("Logs transfered to s3 completed")