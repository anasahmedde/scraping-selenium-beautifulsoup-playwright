import concurrent.futures
from bs4 import BeautifulSoup
import os, io, logging, boto3, warnings, requests, time
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
threads = int(os.getenv("threads"))
list_pool_size = int(os.getenv("list_pool_size"))

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',datefmt='%Y-%m-%d %H:%M:%S')

log = logging.getLogger("houseInRwanda-detail-extractor")
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
            CONNECTION_STRING = CONNECTION_STRING_MONGODB
            client = MongoClient(CONNECTION_STRING)
            return client[databaseName]
        
        dbname = get_database()
        collection_name = dbname[collectionName]
        
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
    delay = 5
    while retries > 0:
        try:
            response = requests.get(link[0], timeout=120)
            soup = BeautifulSoup(response.content, 'lxml')
            if soup.find("h1", text="Oops, 403!") or soup.find("h1", text="Oops, 404!"):
                log.info(f'nothing found in {link[0]}')
                break
            propertyTitle = soup.find(class_='field field--name-title field--type-string field--label-hidden').text if soup.find(class_='field field--name-title field--type-string field--label-hidden') else None
            propertyId = soup.find(class_='card-header bg-secondary text-white text-center').text.replace('Summary - Ref: ', '') if (soup.find(class_='card-header bg-secondary text-white text-center')) else None
            price = soup.find(name='strong', string='Price:').parent.text.replace('Price: ', '') if soup.find(name='strong', string='Price:') else None
            price = None if price == "Price on request " else price
            priceStatus, currency, priceDiff = None, None, None
            if price:
                priceLst = price.split(' ')
                if (priceLst[0] != 'Price' and priceLst[0] != 'Auction'):
                    price = float(priceLst[0].replace(',', ''))
                    currency = priceLst[1]

                    data = singleItem.find_one({"url": link[0]})
                    oldPrice = data['price'] if data else None
                    priceDiff = max(oldPrice, price) - min(oldPrice, price) if oldPrice else 0
                    if price != oldPrice:
                        priceStatus = 'increased' if (price > oldPrice) else 'decreased'

            imgUrls = list(set([a['href'] for a in soup.select('#carousel a')]))
            description = soup.select_one('meta[property="og:description"]')['content'] if soup.select_one('meta[property="og:description"]') else None 
            amenities = []
            if description:
                amenities = [item for item in description.split('.') if "AMENITIES" in item][0].strip().replace('(AMENITIES) ', '').split(' - ') if (len([item for item in description.split('.') if "AMENITIES" in item]) > 0) else []

            beds = soup.find(name='strong', string='Bedrooms:').parent.text.replace('Bedrooms: ', '') if soup.find(name='strong', string='Bedrooms:') else None
            baths = soup.find(name='strong', string='Bathrooms:').parent.text.replace('Bathrooms: ', '') if soup.find(name='strong', string='Bathrooms:') else None
            totalFloors = soup.find(name='strong', string='Total floors:').parent.text.replace('Total floors: ', '') if soup.find(name='strong', string='Total floors:') else None
            address = soup.find(name='strong', string='Address:').parent.text.replace('Address: ', '') if soup.find(name='strong', string='Address:') else  None
            advertType = soup.find(name='strong', string='Advert type:').parent.text.replace('Advert type: ', '') if soup.find(name='strong', string='Advert type:') else None
            pricingCriteria = ('Month' if 'rent' in advertType.lower() else None) if advertType else None
            plotSize = soup.find(name='strong', string='Plot size:').parent.text.replace('Plot size: ', '') if soup.find(name='strong', string='Plot size:') else None
            furnished = soup.find(name='strong', string='Furnished:').parent.text.replace('Furnished: ', '') if soup.find(name='strong', string='Furnished:') else None
            propertyType = soup.find(name='strong', string='Property type:').parent.text.replace('Property type: ', '') if soup.find(name='strong', string='Property type:') else None
            expiryDate = soup.find(name='strong', string='Expiry date:').parent.text.replace('Expiry date: ', '') if soup.find(name='strong', string='Expiry date:') else None
            agentName = soup.find(name='strong', string='Name:').parent.text.replace('Name: ', '') if soup.find(name='strong', string='Name:') else None
            agentCellPhone = soup.find(name='strong', string='Cell phone:').parent.text.replace('Cell phone: ', '') if soup.find(name='strong', string='Cell phone:') else None
            agentEmailAddress = soup.find(name='strong', string='Email address:').parent.text.replace('Email address: ', '') if soup.find(name='strong', string='Email address:') else None
        
        except (requests.exceptions.Timeout, requests.exceptions.SSLError, requests.exceptions.ConnectionResetError):
            log.info(f"Timeout error occurred in {link[0]}. Retrying in {delay} seconds...")
            retries -= 1
            time.sleep(delay)
            continue

        except Exception as e:
            log.info(f"Failed to scrape data for {link[0]}: {e}")
            break

        try:
            thread_results[thread_id].append([propertyId, propertyTitle, link[0], price, currency, float(priceDiff) if (priceDiff is not None and priceDiff != '') else None, True if (priceDiff is not None and (priceDiff > 0 and priceDiff != '')) else False, priceStatus, imgUrls, description, amenities, int(beds) if (beds != '') else None, int(baths) if (baths != '') else None, int(totalFloors) if (totalFloors != '') else None, address, advertType, pricingCriteria, plotSize if (plotSize != '') else None, furnished, propertyType, datetime.strptime(expiryDate, "%B %d, %Y"), agentName, agentCellPhone, agentEmailAddress])
            break
        except Exception as e:
            log.info(f"ERROR:{link[0]}")
            return

def getData():
    client = MongoClient(CONNECTION_STRING_MONGODB)
    db = client['HouseInRwanda']
    collection = db['propertyURLs']
    data = collection.find()
    log.info("Scraping stored URLs ...")
    return list(data)

def continous_connection():
    clientC = MongoClient(CONNECTION_STRING_MONGODB)
    db = clientC['HouseInRwanda']
    return db['propertyURLs']


databaseName='HouseInRwanda'
columns=['propertyId', 'propertyTitle', 'url', 'price', 'currency', 'priceDiff', 'priceChange', 'priceStatus', 'imgUrls', 'description', 'amenities', 'beds', 'baths', 'totalFloors', 'address', 'advertType', 'pricingCriteria', 'plotSize', 'furnished', 'propertyType', 'expiryDate', 'agentName', 'agentCellPhone', 'agentEmailAddress']    

if __name__ == '__main__':
    all_data = []
    datas = getData()
    links = [list(data['url'].strip().split()) for data in datas]

    singleItem = continous_connection()
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        executor.map(scrape_data, links)

    for thread_id, result_list in thread_results.items():
        sendData(result_list, columns, databaseName, 'propertyDetails') 

    log.info("Details extraction completed successfully.")
    s3 = boto3.client("s3", region_name=aws_region_name)
    s3.put_object(Body=log_stringio.getvalue(), Bucket=bucket_name, Key="logs/houseInRwanda/detail-extractor-logs.txt")
    log.info("Logs transfered to s3 completed")
