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

log = logging.getLogger("prophuntgh-url-extractor")
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

        
def getExcel_links(url):
    response = requests.get(url, timeout=300)
    soup = BeautifulSoup(response.text, 'lxml')
    records = int(soup.select_one("#searchPropertyCount").text)
    if records != 0:
        for page in range(1, (records//9)+1 ):
            response = requests.get(url + f"/page/{page}/", timeout=300)
            soup = BeautifulSoup(response.text, 'lxml')
            log.info(url + f"/page/{page}/")
            urls = ['https://www.prophuntgh.com/'+url['href'] for url in soup.select("div#listingTab a.text-dark")]
            prop_ids = [prop.split('/')[-2] for prop in urls]
            prices = [float(price.text.strip().split(' ')[1].replace(',', '')) for price in soup.select('span[id^="originalPriceSpan"]')]

            links.extend([[link, ids, price] for link, ids, price in zip(urls, prop_ids, prices)])

df = pd.read_excel("/usr/src/app/prophuntgh/BaseLinksForProphuntgh.xlsx")
excelLinks = [i[0] for i in df.values.tolist()]
databaseName = 'prophunt'

links = []
log.info('Gathering property links !')
with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
    futures, links = [], []
    for excelLink in excelLinks:
        futures.append(executor.submit(getExcel_links, excelLink))
    concurrent.futures.wait(futures)

sendData(links, ['url', 'propertyId', 'price'], databaseName, 'propertyURLs')
log.info("URL extraction completed successfully.")
s3 = boto3.client("s3", region_name=aws_region_name)
s3.put_object(Body=log_stringio.getvalue(), Bucket=bucket_name, Key="logs/prophuntgh/url-extractor-logs.txt")  
log.info("Logs transfered to s3 completed")
