import concurrent.futures
from bs4 import BeautifulSoup
import os, io, logging, boto3, warnings, requests
from pymongo import MongoClient
from dotenv import load_dotenv
import pandas as pd
from pymongo import MongoClient
import numpy as np

warnings.filterwarnings("ignore", category=DeprecationWarning) 
load_dotenv(override=True)

CONNECTION_STRING_MONGODB = os.getenv("CONNECTION_STRING")
aws_region_name = os.getenv("aws_region_name")
bucket_name = os.getenv("bucket_name")
threads = int(os.getenv("threads"))

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',datefmt='%Y-%m-%d %H:%M:%S')

log = logging.getLogger("buyRentKenya-url-extractor")
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


def extract_links(url):
    try:
        response = requests.get(url, timeout=60)
        soup = BeautifulSoup(response.text, 'lxml')
        log.info(url)
        return [[link, ids, price, listingType] for link, ids, price, listingType in zip(
            ['https://www.buyrentkenya.com'+a.select('a')[0].get('href') for a in soup.select('div.listing-card')],
            [a.select('a')[0].get('href').split('-')[-1] for a in soup.select('div.listing-card')],
            ['' if a.select('a')[3].text.strip() == "Price not communicated" else float(a.select('a')[3].text.strip().split(' ')[1].replace('\n\n/', '').replace(',', '')) for a in soup.select('div.listing-card')],
            ["sale" if "-for-sale" in url else "rent" if "-for-rent" in url else "project" for i in range(len(soup.select('div.listing-card')))]
        )]
    except:
        log.info('error', url)

urls = ["https://www.buyrentkenya.com/property-for-sale", "https://www.buyrentkenya.com/property-for-rent"]
urls += [f"https://www.buyrentkenya.com/property-for-rent?page={i}" for i in range(2, 300)]
urls += [f"https://www.buyrentkenya.com/property-for-sale?page={i}" for i in range(2, 450)]

databaseName = 'buyrentkenya'
links = []

if __name__ == '__main__':

    log.info('Gathering property links !')
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        results = executor.map(extract_links, urls)
        for result in results:
            links += result
            
    sendData(links, ['url', 'propertyId', 'price', 'listingType'], databaseName, 'propertyURLs')
    log.info("URL extraction completed successfully.")
    s3 = boto3.client("s3", region_name=aws_region_name)
    s3.put_object(Body=log_stringio.getvalue(), Bucket=bucket_name, Key="logs/buyRentKenya/url-extractor-logs.txt")  
    log.info("Logs transferred to s3 completed")