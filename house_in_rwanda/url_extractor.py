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

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',datefmt='%Y-%m-%d %H:%M:%S')

log = logging.getLogger("houseInRwanda-url-extractor")
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

          
databaseName='HouseInRwanda'
columns=['propertyId', 'propertyTitle', 'url', 'price', 'currency', 'priceDiff', 'priceChange', 'priceStatus', 'imgUrls', 'description', 'amenities', 'beds', 'baths', 'totalFloors', 'address', 'advertType', 'plotSize', 'furnished', 'propertyType', 'expiryDate', 'agentName', 'agentCellPhone', 'agentEmailAddress']

if __name__ == '__main__':
    log.info("Gathering property links...")
    links = []
    for i in range(3):
        response = requests.get(f'https://www.houseinrwanda.com/?page={i}', timeout=300)
        soup = BeautifulSoup(response.content, 'lxml')

        links += [[link, ids, price] for link, ids, price in zip(
            ['https://www.houseinrwanda.com' + link['href'].replace(':', '') for link in soup.select('h5 a[href]')],
            [prop.text.replace('| Ref: ', '') for prop in soup.find_all(attrs={'class': 'text-muted'})],
            [(float(prop.text.split(' ')[0].replace(',', '')) if (prop.text.split(' ')[0] != 'Price' and prop.text.split(' ')[0] != 'Auction') else None) for prop in soup.find_all(class_='badge bg-light text-dark')]
        )]

    sendData(links, ['url', 'propertyId', 'price'], databaseName, 'propertyURLs')
    log.info("URL extraction completed successfully.")
    s3 = boto3.client("s3", region_name=aws_region_name)
    s3.put_object(Body=log_stringio.getvalue(), Bucket=bucket_name, Key="logs/houseInRwanda/url-extractor-logs.txt")
    log.info("Logs transfered to s3 completed")
