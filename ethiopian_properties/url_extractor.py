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

log = logging.getLogger("ethiopianProperties-url-extractor")
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
            collection_name.update_one({'url':instance['url']},{'$set':instance},upsert=True)
        log.info('Data sent to MongoDB successfully')
    except Exception as e:
        log.info('Some error occurred while sending data MongoDB! Following is the error.')
        log.error(e)


def get_links(page_num):
    url = f"https://www.ethiopianproperties.com/property-search/page/{page_num}/"
    response = requests.get(url, headers=headers, timeout=300)
    soup = BeautifulSoup(response.content, 'lxml')
    
    return [[link, price] for link, price in zip(
            [link['href'] for link in soup.select('div.list-container.clearfix h4 a[href]')],
            [float(price.text.strip().split('-')[0].split(' ')[0].replace('$', '').replace(',', '')) if price.text.strip().split('-')[0].split(' ')[0].replace('$', '').replace(',', '') != '' else None for price in soup.select('h5.price')]
        )]
                  
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1'
}
databaseName='EthiopianProperties'

if __name__ == '__main__':
    
    links, all_data = [], []
    log.info('Gathering property links !')
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        future_to_links = {executor.submit(get_links, i): i for i in range(1, 125)}
        for future in concurrent.futures.as_completed(future_to_links):
            try:
                links += future.result()
            except Exception as e:
                continue
                
    sendData(links, ['url', 'price'], databaseName, 'propertyURLs')
    log.info("URL extraction completed successfully.")
    s3 = boto3.client("s3", region_name=aws_region_name)
    s3.put_object(Body=log_stringio.getvalue(), Bucket=bucket_name, Key="logs/ethiopianProperties/url-extractor-logs.txt")
    log.info("Logs transfered to s3 completed")
