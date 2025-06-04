import concurrent.futures
import requests
import pandas as pd
from pymongo import MongoClient
from lxml import html
from dotenv import load_dotenv
import os, io, logging, boto3, warnings, requests, time
import threading
from datetime import datetime

thread_results = {}

warnings.filterwarnings("ignore", category=DeprecationWarning) 
load_dotenv(override=True)

CONNECTION_STRING_MONGODB = os.getenv("CONNECTION_STRING")
aws_region_name = os.getenv("aws_region_name")
bucket_name = os.getenv("bucket_name")
threads = int(os.getenv("threads"))
list_pool_size = int(os.getenv("list_pool_size"))

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',datefmt='%Y-%m-%d %H:%M:%S')

log = logging.getLogger("global-remax-detail-extractor")
log_stringio = io.StringIO()
handler = logging.StreamHandler(log_stringio)
handler.setFormatter(formatter)
log.addHandler(handler)

columnsURLs = ['url','propertyId','localPrice','price','propertyType','location','country','listingType','currency']
databaseName='global_remax'
collectionNameURLs='propertyURLs'
collectionNameDetails='propertyDetails'


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

        # for index, instance in enumerate(mongo_insert_data):
        #     collection_name.update_one({'propertyId': instance['propertyId']}, {'$set': instance}, upsert=True)
        for instance in mongo_insert_data:
            query = {'url': instance['url']}
            existing_entry = collection_name.find_one(query)
            if existing_entry is None:
                instance['dateListed'] = datetime.today().strftime('%Y-%m-%d')
                collection_name.insert_one(instance)
            else:
                collection_name.update_one(query, {'$set': instance})

        log.info('Data sent to MongoDB successfully')

    except Exception as e:
        log.info('Some error occurred while sending data to MongoDB! Following is the error.')
        log.info(e)
        log.info('-----------------------------------------')
        
def getData(databaseName, collectionNameURLs):
    print("Fetching Stored URLs.")
    client = MongoClient(CONNECTION_STRING_MONGODB)
    db = client[databaseName]
    collection = db[collectionNameURLs]
    data = collection.find()
    return list(data)

def continuous_connection(databaseName, collectionNameURLs):
    client = MongoClient(CONNECTION_STRING_MONGODB)
    db = client[databaseName]
    return db[collectionNameURLs]

columnsDetails = ['propertyId', 'country', 'localCurrency', 'propertyType', 'localPrice', 'location', 'price', 'usdPrice', 'housingType', 'url', 'propertyTitle', 'rooms', 'beds', 'baths', 'internalArea', 'internalAreaUnit', 'lotSize', 'lotSizeUnit', 'builtArea', 'builtAreaUnit', 'floors', 'yearBuilt', 'parkingSpaces', 'agentContact', 'agent', 'dateAvailable', 'status', 'amenities', 'imgsUrls', 'address', 'priceChange', 'priceStatus', 'priceDiff', 'description']

def scrape_data(item):
    thread_id = threading.get_ident()
    if thread_id not in thread_results:
        thread_results[thread_id]=[]    

    if len(thread_results[thread_id])==list_pool_size:
        sendData(thread_results[thread_id], columnsDetails, databaseName, collectionNameDetails)
        thread_results[thread_id]=[]

    propertyId = item[0]
    country = item[1]
    localCurrency = item[2]
    propertyType = item[3]
    localPrice = item[4]
    location = item[5]
    prevPrice = item[6]
    price = item[6]
    usdPrice = item[6]
    housingType = item[7]    
    url = item[8]

    retries = 3
    delay = 10
    while retries > 0:
        try:
            response = requests.get(url, timeout=60)
            tree = html.fromstring(response.content)
            title = None
            rooms = None
            beds = None
            baths = None
            internalArea = None
            internalAreaUnit = None
            lotSize = None
            lotSizeUnit = None
            builtArea = None
            builtAreaUnit = None
            floors = None
            address = None
            yearBuilt = None
            parkingSpaces = None
            agentContact = None
            agent = None
            date_available = None
            status = None
            amenities = []
            description = None
            imgsUrls = []
            if tree.xpath("//h2[@itemprop='name']"):
                title = tree.xpath("//h2[@itemprop='name']")[0].text.strip()
            if tree.xpath("//span[contains(@title,'Total Rooms')]/following-sibling::div/@title"):
                rooms = tree.xpath("//span[contains(@title,'Total Rooms')]/following-sibling::div/@title")[0]

            if tree.xpath("//span[contains(@title,'Bedrooms')]/following-sibling::div/@title"):
                beds = tree.xpath("//span[contains(@title,'Bedrooms')]/following-sibling::div/@title")[0]

            if tree.xpath("//span[contains(@title,'Bathrooms')]/following-sibling::div/@title"):
                baths = tree.xpath("//span[contains(@title,'Bathrooms')]/following-sibling::div/@title")[0]

            if tree.xpath("//span[contains(@title,'Total SqM')]/following-sibling::div/@title"):
                internalArea = tree.xpath("//span[contains(@title,'Total SqM')]/following-sibling::div/@title")[0]
                internalAreaUnit = "sqm"

            if tree.xpath("//span[contains(@title,'Lot Size (m2)')]/following-sibling::div/@title"):
                lotSize = tree.xpath("//span[contains(@title,'Lot Size (m2)')]/following-sibling::div/@title")[0]
                lotSizeUnit = "m2"
            elif tree.xpath("//span[contains(@title,'Lot Size (acre')]/following-sibling::div/@title"):
                lotSize = tree.xpath("//span[contains(@title,'Lot Size (acre')]/following-sibling::div/@title")[0]
                lotSizeUnit = "acre"
            elif tree.xpath("//span[contains(@title,'Lot Size (')]/following-sibling::div/@title"):
                lotSize = tree.xpath("//span[contains(@title,'Lot Size (')]/following-sibling::div/@title")[0]
                lotSizeUnit = tree.xpath("//span[contains(@title,'Lot Size (')]/@title")[0].split()[-1]
            elif tree.xpath("//span[contains(@title,'Lot Size')]/following-sibling::div/@title"):
                lotSize = tree.xpath("//span[contains(@title,'Lot Size')]/following-sibling::div/@title")[0]
                lotSizeUnit = None

            if tree.xpath("//span[contains(@title,'Built Area (m²)')]/following-sibling::div/@title"):
                builtArea = tree.xpath("//span[contains(@title,'Built Area (m²)')]/following-sibling::div/@title")[0]
                builtAreaUnit = 'm2'    
            elif tree.xpath("//span[contains(@title,'Built Area (acre')]/following-sibling::div/@title"):
                builtArea = tree.xpath("//span[contains(@title,'Built Area (acre')]/following-sibling::div/@title")[0]
                builtAreaUnit = "acre"
            elif tree.xpath("//span[contains(@title,'Built Area (')]/following-sibling::div/@title"):
                builtArea = tree.xpath("//span[contains(@title,'Built Area (')]/following-sibling::div/@title")[0]
                builtAreaUnit = tree.xpath("//span[contains(@title,'Built Area (')]/@title")[0].split()[-1]
            elif tree.xpath("//span[contains(@title,'Built Area')]/following-sibling::div/@title"):
                builtArea = tree.xpath("//span[contains(@title,'Built Area')]/following-sibling::div/@title")[0]
                builtAreaUnit = None

            if tree.xpath("//span[contains(@title,'Floors')]/parent::div/following-sibling::div/@title"):
                floors = tree.xpath("//span[contains(@title,'Floors')]/parent::div/following-sibling::div/@title")[0]

            if tree.xpath("//span[contains(@title,'Year')]/following-sibling::div/@title"):
                yearBuilt = tree.xpath("//span[contains(@title,'Year')]/following-sibling::div/@title")[0]

            if tree.xpath("//span[contains(@title,'Parking')]/following-sibling::div/@title"):
                parkingSpaces = tree.xpath("//span[contains(@title,'Parking')]/following-sibling::div/@title")[0]
            if tree.xpath("//div[@itemprop='description']"):
                description = tree.xpath("//div[@itemprop='description']")[0].text
            if tree.xpath("//div[@id='divWhatsApp']/a[contains(@href,'tel')]/@href"):
                agentContact = tree.xpath("//div[@id='divWhatsApp']/a[contains(@href,'tel')]/@href")[0].replace('tel:','')

            if tree.xpath('//div[@class="agentcard-main"]/h3/a/span[@itemprop="name"]'):
                agent = tree.xpath('//div[@class="agentcard-main"]/h3/a/span[@itemprop="name"]')[0].text.strip()

            if tree.xpath("//div[contains(@class,'key-address')]"):
                address = tree.xpath("//div[contains(@class,'key-address')]")[0].text.strip()
            if tree.xpath("//div[contains(@class,'key-other') and contains(text(),'Available')]"):
                date_available = tree.xpath("//div[contains(@class,'key-other') and contains(text(),'Available')]")[0].text.strip().split()[-1]

            if tree.xpath("//div[contains(@class,'key-status')]"):
                status = tree.xpath("//div[contains(@class,'key-status')]")[0].text.strip()
            if tree.xpath("//div[@class='features-container']/div/div/span/@title"):
                amenities = tree.xpath("//div[@class='features-container']/div/div/span/@title")
            if tree.xpath("//img[@class='sp-image']/@data-large"):
                imgsUrls = ["https://global.remax.com"+item for item in tree.xpath("//img[@class='sp-image']/@data-large")]

            if price!=None:
                if price==prevPrice:
                    priceChange=False
                else:
                    priceChange=True
                priceDiff=max(prevPrice,price)-min(prevPrice,price)
                if prevPrice<price:
                    priceStatus='increased'
                elif prevPrice>price:
                    priceStatus='decreased'
                else:
                    priceStatus=None
            else:
                priceChange=False
                priceDiff=None
                priceStatus=None

        except (requests.exceptions.Timeout, requests.exceptions.SSLError):
            retries -= 1
            time.sleep(delay)

        except Exception as e:
            print(f"Failed to scrape data for {url} : {e}")
            retries -= 1
        finally:
            try:
                thread_results[thread_id].append([propertyId, country, localCurrency, propertyType, localPrice, location, price, usdPrice, housingType, url, title, rooms, beds, baths, internalArea, internalAreaUnit, lotSize, lotSizeUnit, builtArea, builtAreaUnit, floors, yearBuilt, parkingSpaces, agentContact, agent, date_available, status, amenities, imgsUrls, address, priceChange, priceStatus, priceDiff, description])
                print(url, price)
                return
            except Exception as e:
                log.info(f'Error while scraping url: {url} --> {e}')
                return
    print(f"Max retries reached. Could not scrape {url}")
    return


raw_data_URLs = getData(databaseName, collectionNameURLs)
property_data_URLs = [[value for value in list(data.values())[1:]] for data in raw_data_URLs]

with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
    executor.map(scrape_data, property_data_URLs)

for thread_id, result_list in thread_results.items():
    sendData(result_list, columnsDetails, databaseName, collectionNameDetails)  

log.info("Details extraction completed successfully.")
s3 = boto3.client("s3", region_name=aws_region_name)
s3.put_object(Body=log_stringio.getvalue(), Bucket=bucket_name, Key="logs/global-remax/detail-extractor-logs.txt")
log.info("Logs transfered to s3 completed") 
