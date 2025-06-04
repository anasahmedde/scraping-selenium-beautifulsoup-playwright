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

log = logging.getLogger("kenyaPropertyCenter-url-extractor")
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
    if soup.find(string='Sorry we could not find any property for sale in  kenya matching your criteria.', name='p'):
        return
    log.info(url)
    return [[link, ids, price] for link, ids, price in zip(
            ['https://kenyapropertycentre.com'+a.parent.get('href') for a in soup.select('div.row.property-list h4.content-title')],
            [i['id'].replace('fav-', '') for i in soup.select("div.row.property-list li.save-favourite-button[id]")],
            [float(price.text.replace(',', '')) for price in soup.select("span.price:nth-of-type(2)")]
        )]


urls = [f"https://kenyapropertycentre.com/for-rent/short-let?page={i}" for i in range(1, 10)]
urls += [f"https://kenyapropertycentre.com/for-rent?page={i}" for i in range(1, 125)]
urls += [f"https://kenyapropertycentre.com/for-sale?page={i}" for i in range(1, 225)]

databaseName = 'kenyaPropertyCentre'
links = []

if __name__ == '__main__':
    
    log.info('Gathering property links !')
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        results = executor.map(extract_links, urls)
        for result in results:
            links += result
            
    sendData(links, ['url', 'propertyId', 'price'], databaseName, 'propertyURLs')
    s3 = boto3.client("s3", region_name=aws_region_name)
    s3.put_object(Body=log_stringio.getvalue(), Bucket=bucket_name, Key="logs/kenyaPropertyCenter/url-extractor-logs.txt")  
    log.info("URL extraction completed successfully.")