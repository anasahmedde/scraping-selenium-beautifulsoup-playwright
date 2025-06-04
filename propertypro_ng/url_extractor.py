import concurrent.futures
from bs4 import BeautifulSoup
import os, io, logging, boto3, warnings, requests
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

log = logging.getLogger("property-pro-ng-url-extractor")
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
    response = requests.get(url, timeout=120)
    soup = BeautifulSoup(response.text, 'lxml')
    if soup.find(string='Sorry we could not find any property matching your criteria.', name='p') or soup.find(string='Oops, an error occurred', name='h1'):
        return []
    print(url)
    return [[link, ids, listingType, price, currency] for link, ids, listingType, price, currency in zip(
        ['https://www.propertypro.ng'+price.parent.parent.parent.find('a').get('href') for price in soup.select('span:contains("₦")')],
        [price.parent.parent.parent.find('a').get('href').split('-')[-1] for price in soup.select('span:contains("₦")')],
        ["sale" if "-for-sale" in url else "rent" if "-for-rent" in url else "shortlet" for i in range(len(soup.select('span:contains("₦")')))],
        [float(price.find_next_sibling('span').get('content').split('/')[0].replace(',', '')) for price in soup.select('span:contains("₦")')],
        [currency.get('content') for currency in soup.select('span:contains("₦")')]
    )][:51]


urls = [f"https://propertypro.ng/property-for-sale?page={i}" for i in range(800)]
urls += [f"https://propertypro.ng/property-for-rent?page={i}" for i in range(325)]
urls += [f"https://propertypro.ng/property-for-shortlet?page={i}" for i in range(75)]

databaseName = 'propertypro_ng'
links = []

if __name__ == '__main__':

    log.info('Gathering property links !')
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        results = executor.map(extract_links, urls)
        for result in results:
            links += result
            
    sendData(links, ['url','propertyId','listingType','price', 'currency'], databaseName, 'propertyURLs')
    log.info("URL extraction completed successfully.")
    s3 = boto3.client("s3", region_name=aws_region_name)
    s3.put_object(Body=log_stringio.getvalue(), Bucket=bucket_name, Key="logs/property-pro-ng/url-extractor-logs.txt")  
    log.info("Logs transfered to s3 completed")
