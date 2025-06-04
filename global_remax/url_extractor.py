from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup
from lxml import etree
import os, io, logging, boto3, warnings, requests
from pymongo import MongoClient
from dotenv import load_dotenv
import pandas as pd

warnings.filterwarnings("ignore", category=DeprecationWarning) 
load_dotenv(override=True)

CONNECTION_STRING_MONGODB = os.getenv("CONNECTION_STRING")
aws_region_name = os.getenv("aws_region_name")
bucket_name = os.getenv("bucket_name")
threads = int(os.getenv("threads"))

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',datefmt='%Y-%m-%d %H:%M:%S')

log = logging.getLogger("global-remax-url-extractor")
log_stringio = io.StringIO()
handler = logging.StreamHandler(log_stringio)
handler.setFormatter(formatter)
log.addHandler(handler)

def sendData(data, columns, databaseName, collectionName):
    try:
        log.info(f'Collected {len(data)} records!')
        df = pd.DataFrame(data, columns=columns)
        mongo_insert_data = df.to_dict('records')
        log.info('Sending Data to MongoDB!')

        def get_database():
            #CONNECTION_STRING = "mongodb+srv://david:0pFvuYveY8EIwWDs@cluster0.gfzw4mh.mongodb.net/?retryWrites=true&w=majority"
            #client = MongoClient(CONNECTION_STRING)
            client = MongoClient(CONNECTION_STRING_MONGODB)
            return client[databaseName]

        dbname = get_database()
        collection_name = dbname[collectionName]

        for index, instance in enumerate(mongo_insert_data):
            collection_name.update_one({'propertyId': instance['propertyId']}, {'$set': instance}, upsert=True)

        log.info('Data sent to MongoDB successfully')

    except Exception as e:
        log.info('Some error occurred while sending data to MongoDB! Following is the error.')
        log.info(e)
        log.info('-----------------------------------------')

columns = ['url', 'propertyId', 'localPrice', 'price', 'propertyType', 'location', 'country', 'listingType', 'currency']
databaseName = 'global_remax'
collectionName = 'propertyURLs'

country_codes = {
    "Kenya": {"code": "106"},
    "Uganda": {"code": "103"},
    "Tanzania": {"code": "115"},
    "Ghana": {"code": "125"},
    "Nigeria": {"code": "117"},
    "Rwanda": {"code": "486"},
    "South Africa": {"code": "1031"},
    "Egypt": {"code": "91"},
    "Morocco": {"code": "54"}
}

category_dict = {'buy': "261", 'rent': "260"}
count = 1000
baseURL = 'https://global.remax.com/Handlers/ListingHandler.ashx?action=list&'

def scrape_data(listingType, country):
    start = 1
    page = 1
    while True:
        URL = baseURL + f'start={start}&page={page}&count={count}&currency=USD&min=1&max=none&sb=MostRecent&sort=MostRecent&countries=' + \
              country_codes[country]['code'] + '&type=' + category_dict[listingType]
        resp = requests.get(URL)
        if resp.json()['count'] == 0:
            break
        else:
            start += count
            page += 1

        soup = BeautifulSoup(resp.json()['html'], 'lxml')
        root = etree.fromstring(str(soup))
        propertyURLs = ['https://global.remax.com' + item for item in root.xpath('//div[@class="image-popup thumb-item"]/a/@href')]
        propertyIds = root.xpath('//div[@class="image-popup thumb-item"]/@data-listing-mlsid')
        prices = root.xpath('//div[@class="image-popup thumb-item"]/a/div[@class="thumb-data"]//span[@class="thumb-price"]/text()[normalize-space()]')
        locations = root.xpath('//div[@class="image-popup thumb-item"]/a/div[@class="thumb-data"]//span[@class="thumb-location"]/text()')
        propertyTypes = root.xpath('//div[@class="image-popup thumb-item"]/a/div[@class="thumb-data"]//span[@class="thumb-type"]/text()')

        currency = None
        localPrices = []
        usdPrices = []
        for item in prices:
            if len(item.strip().split('/')) == 2:
                currency = item.strip().split('/')[0].split()[-1].strip()
                localPrice = float(item.strip().split('/')[0].split()[0].strip().replace(',', ''))
                usdPrice = float(item.strip().split('/')[1].split()[0].strip().replace(',', ''))
            elif 'USD' in item:
                localPrice = None
                usdPrice = float(item.strip().split()[0].strip().replace(',', ''))
            else:
                localPrice = None
                usdPrice = None
            localPrices.append(localPrice)
            usdPrices.append(usdPrice)

        data_collected = [list(item) for item in
            zip(propertyURLs, propertyIds, localPrices, usdPrices, propertyTypes, locations,
            [country] * len(propertyURLs), [listingType] * len(propertyURLs), [currency] * len(propertyURLs))
        ]

        sendData(data_collected, columns, databaseName, collectionName)

with ThreadPoolExecutor(max_workers=threads) as executor:
    for listingType in category_dict:
        for country in country_codes:
            executor.submit(scrape_data, listingType, country)

log.info("URL extraction completed successfully.")
s3 = boto3.client("s3", region_name=aws_region_name)
s3.put_object(Body=log_stringio.getvalue(), Bucket=bucket_name, Key="logs/global-remax/url-extractor-logs.txt")
log.info("Logs transfered to s3 completed")
