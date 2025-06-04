import concurrent.futures
from bs4 import BeautifulSoup
import os, io, logging, boto3, warnings, requests, time, re
from pymongo import MongoClient
from dotenv import load_dotenv
import pandas as pd
from pymongo import MongoClient
from datetime import datetime
import threading

thread_results = {}
warnings.filterwarnings("ignore", category=DeprecationWarning) 
load_dotenv(override=True)

CONNECTION_STRING_MONGODB = os.getenv("CONNECTION_STRING")
aws_region_name = os.getenv("aws_region_name")
bucket_name = os.getenv("bucket_name")
# threads = int(os.getenv("threads"))
threads=1
list_pool_size = int(os.getenv("list_pool_size"))
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',datefmt='%Y-%m-%d %H:%M:%S')

log = logging.getLogger("realEstateTanzania-detail-extractor")
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


def scrape_data(link):
    global singleItem
    thread_id = threading.get_ident()
    if thread_id not in thread_results:
        thread_results[thread_id]=[]    

    if len(thread_results[thread_id])==list_pool_size:
        sendData(thread_results[thread_id], columns, databaseName, 'propertyDetails')
        thread_results[thread_id]=[] 

    retries = 3
    delay = 10
    while retries > 0:
        try:
            response = requests.get(link[0], timeout=120)
            soup = BeautifulSoup(response.content, 'html.parser')

            if (soup.find(name='h1', string='403 Forbidden') or soup.find(name='body', string='Too Many Requests.') or soup.find(name='p', string='Too Many Requests.')):
                log.info("Too many requests. Waiting....")
                time.sleep(300)
                response = requests.get(link[0], timeout=60)
                soup = BeautifulSoup(response.content, 'html.parser')

                
            if soup.find("h1", text="Oh oh! Page not found."):
                log.info('not found', link[0])
                return

            propertyId = soup.find(name='strong', string='Property ID:').parent.text.replace("Property ID:", '').strip() if (soup.find(name='strong', string='Property ID:')) else None
            propertyTitle = soup.select_one('div.page-title').text.strip() if (soup.select_one('div.page-title').text.strip() != '') else None
            size = soup.find(name='strong', string='Property Size:').parent.text.replace("Property Size:", '').strip() if (soup.find(name='strong', string='Property Size:')) else None
            propertyType = soup.find(name='strong', string='Property Type:').parent.text.replace("Property Type:", '').strip() if (soup.find(name='strong', string='Property Type:')) else None
            beds = float(soup.find(name='strong', string='Bedrooms:').parent.text.replace("Bedrooms:", '').strip()) if (soup.find(name='strong', string='Bedrooms:')) else None
            baths = float(soup.find(name='strong', string='Bathrooms:').parent.text.replace("Bathrooms:", '').strip()) if (soup.find(name='strong', string='Bathrooms:')) else None
            priceStr = soup.select_one('div.page-title-wrap li.item-price').text if (soup.select_one('div.page-title-wrap li.item-price')) else None
            price = float(re.findall(r'\d{1,3}(?:,\d{3})*(?:\.\d+)?', priceStr.split('/')[0])[-1].replace(',', '')) if (re.findall(r'\d{1,3}(?:,\d{3})*(?:\.\d+)?', priceStr.split('/')[0])) else None
            
            currency, priceStatus, priceDiff, pricingCriteria, listingType = None, None, None, None, None
            if (price is not None):
                currency = re.search(r'[A-Z]{3}', priceStr).group() if (re.search(r'[A-Z]{3}', priceStr)) else None
                priceType = ' '.join(priceStr.split('/')[1:])
                pricingCriteria = None if (priceType.isdigit() or priceType == '') else priceType
                listingType = soup.select_one('div.property-labels-wrap a').text.strip() if (soup.select_one('div.property-labels-wrap a')) else None

                data = singleItem.find_one({"url": link[0]})
                oldPrice = data['price'] if data else None
                priceDiff = max(oldPrice, price) - min(oldPrice, price) if oldPrice else 0
                if price != oldPrice:
                    priceStatus = 'increased' if (price > oldPrice) else 'decreased'
                else:
                    priceStatus = None

            imgUrls = [img['src'] for img in soup.select('#property-gallery-js img.img-fluid')] if (soup.select('#property-gallery-js img.img-fluid')) else None
            state = soup.select_one('li.detail-state span').text if (soup.select_one('li.detail-state span')) else None
            country = soup.select_one('li.detail-country span').text if (soup.select_one('li.detail-country span')) else None
            city = soup.select_one('li.detail-city span').text if (soup.select_one('li.detail-city span')) else None
            address = soup.select_one('li.detail-address span').text if (soup.select_one('li.detail-address span')) else None
            location = soup.select_one('.page-title-wrap address').text if (soup.select_one('.page-title-wrap address')) else None
            dateUpdated = datetime.strptime(soup.select_one('span.small-text.grey').text.replace('Updated on', '').split('at')[0].strip(), "%B %d, %Y") if (soup.select_one('span.small-text.grey')) else datetime.today()
            description = soup.find(name='h2', string='Description').parent.parent.text.replace("Description", '').strip() if (soup.find(name='h2', string='Description')) else None
            agent = soup.select_one("div#property-contact-agent-wrap li.agent-name").text.strip() if (soup.select_one("div#property-contact-agent-wrap li.agent-name")) else None
            agentNumber = soup.select_one("div#property-contact-agent-wrap span.agent-phone").text.strip() if (soup.select_one("div#property-contact-agent-wrap span.agent-phone")) else None
            print(link[0], propertyTitle, propertyId)
            thread_results[thread_id].append([link[0], propertyTitle, propertyId, listingType, beds, baths, size, description, propertyType, state, country, city, address, location, dateUpdated, imgUrls, price, currency, priceDiff, priceStatus, True if (priceDiff is not None and (priceDiff > 0 and priceDiff != '')) else False, pricingCriteria, agent, agentNumber])
            return
        
        except (requests.exceptions.Timeout, requests.exceptions.SSLError):
            log.info("Timeout error occurred. Retrying in {} seconds...".format(delay))
            retries -= 1
            time.sleep(delay)
        except Exception as e:
            retries -= 1
            log.info(f"Failed to scrape data for {link[0]}: {e}")

    log.info(f"Max retries reached. Could not scrape {link[0]}")
    return 

def getData():
    log.info("Fetching Stored URLs.")
    client = MongoClient(CONNECTION_STRING_MONGODB)
    db = client['realEstateTanzania']
    collection = db['propertyURLs']
    data = collection.find()
    return list(data)

def continuous_connection():
    clientC = MongoClient(CONNECTION_STRING_MONGODB)
    db = clientC['realEstateTanzania']
    return db['propertyURLs']

databaseName='realEstateTanzania'
columns=['url', 'propertyTitle', 'propertyId', 'listingType', 'beds', 'baths', 'size', 'description', 'propertyType', 'state', 'country', 'city', 'address', 'location', 'dateUpdated', 'imgUrls', 'price', 'currency', 'priceDiff', 'priceStatus', 'priceChange', 'pricingCriteria', 'agent', 'agentNumber']

links = []
datas = getData()
links = [list(data['url'].strip().split()) for data in datas]

singleItem = continuous_connection()
with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
    executor.map(scrape_data, links)

for thread_id, result_list in thread_results.items():
    sendData(result_list, columns, databaseName, 'propertyDetails')
log.info("Details extraction completed successfully.")
s3 = boto3.client("s3", region_name=aws_region_name)
s3.put_object(Body=log_stringio.getvalue(), Bucket=bucket_name, Key="logs/realEstateTanzania/detail-extractor-logs.txt")  
log.info("Logs transferred to s3 completed")
