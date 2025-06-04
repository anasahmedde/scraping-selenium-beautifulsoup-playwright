from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup
from lxml import etree
import os, io, logging, boto3, warnings, requests, re
from pymongo import MongoClient
from dotenv import load_dotenv
import pandas as pd

warnings.filterwarnings("ignore", category=DeprecationWarning) 
load_dotenv(override=True)

CONNECTION_STRING_MONGODB = os.getenv("CONNECTION_STRING")
aws_region_name = os.getenv("aws_region_name")
bucket_name = os.getenv("bucket_name")
threads = int(os.getenv("threads"))
list_pool_size = int(os.getenv("list_pool_size"))

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',datefmt='%Y-%m-%d %H:%M:%S')

log = logging.getLogger("zambianhome-url-extractor")
log_stringio = io.StringIO()
handler = logging.StreamHandler(log_stringio)
handler.setFormatter(formatter)
log.addHandler(handler)

def extract_currency_price_list(prices_list):
    currencies = []
    prices = []
    
    for text in prices_list:
        # Check if there's any digit in the string
        if not any(char.isdigit() for char in text):
            currencies.append(None)
            prices.append(None)
        else:
            # Regular expression to find currency and price
            match = re.search(r'([A-Za-z]+)\s*([\d,]+)', text)
            
            if match:
                currency = match.group(1)
                price = match.group(2).replace(',', '')  # Remove commas from the price
                currencies.append(currency)
                prices.append(int(price))
            else:
                currencies.append(None)
                prices.append(None)
    
    return currencies, prices

def sendData(data, columns, databaseName, collectionName):
    try:
        log.info(f'Collected {len(data)} records!')
        df = pd.DataFrame(data, columns=columns)
        mongo_insert_data = df.to_dict('records')
        log.info('Sending Data to MongoDB!')

        def get_database():
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

columns = ['url','propertyId','localPrice','propertyType','location','country','listingType','currency']
databaseName = 'zambianhome'
collectionName = 'propertyURLs'

base_urls= {'https://www.zambianhome.com/status/for-sale/':'sale', 'https://www.zambianhome.com/status/for-rent/':'rent'}
country = 'Zambia'
data_collected = []

def scrape_data(base_url):
    data_collected = []
    listingType = base_urls[base_url]
    page = 1
    while True:
        try:
            log.info(base_url+f'page/{page}/')
            resp = requests.get(base_url+f'page/{page}/')
            soup = BeautifulSoup(resp.content, 'lxml')
            parser = etree.XMLParser(recover=True)
            root = etree.fromstring(str(soup), parser=parser)
            if "Oh oh! Page not found." in str(soup):
                log.info('Condition found!')
                break
            if len(data_collected) > list_pool_size:
                sendData(data_collected, columns, databaseName, collectionName)  
                data_collected = []
            propertyURLs=root.xpath('//div[contains(@class, "property-item")]//h2/a/@href')
            propertyIds=root.xpath("//div[contains(@class,'property-listing')]/div/div/@id")
            propertyPricesInfo = root.xpath('//div[contains(@class, "info-row")]/span/text()')
            currencies, propertyPrices = extract_currency_price_list(propertyPricesInfo)
            propertyTypes = root.xpath('//div[contains(@class,"info-row amenities hide-on-grid")]//p[last()]/text()')
            propertyLocations = root.xpath('//div[contains(@class, "property-item")]//address/text()')
            data_collected_current_page = [list(item) for item in zip(propertyURLs,propertyIds,propertyPrices,propertyTypes,propertyLocations,[country]*len(propertyURLs),[listingType]*len(propertyURLs),currencies)]
            data_collected.extend(data_collected_current_page)
            page = page + 1
        except Exception as e:
            log.info(e)
            log.info(base_url+f'page/{page}/')
            page = page + 1
    if len(data_collected) <= list_pool_size:
        sendData(data_collected, columns, databaseName, collectionName)  
    return

with ThreadPoolExecutor(max_workers=threads) as executor:
    for base_url in base_urls:
        executor.submit(scrape_data, base_url)

log.info("URL extraction completed successfully.")
s3 = boto3.client("s3", region_name=aws_region_name)
s3.put_object(Body=log_stringio.getvalue(), Bucket=bucket_name, Key="logs/zambianhome/url-extractor-logs.txt")
log.info("Logs transfered to s3 completed")
