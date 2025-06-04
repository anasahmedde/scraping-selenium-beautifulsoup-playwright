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

log = logging.getLogger("nigeriaPropertyCentre-detail-extractor")
log_stringio = io.StringIO()
handler = logging.StreamHandler(log_stringio)
handler.setFormatter(formatter)
log.addHandler(handler)


def sendData(data,columns,databaseName,collectionName):
    try:
        log.info(f'Collected {len(data)} records!')
        df=pd.DataFrame(data,columns=columns)
        datetime_columns = df.select_dtypes(include=['datetime64']).columns
        for col in datetime_columns:
            df[col] = df[col].fillna(pd.NaT).astype(str)
        mongo_insert_data=df.to_dict('records')
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
            
            if instance['addedOn'] is None and existing_entry is None:
                instance['addedOn'] = datetime.today().strftime('%Y-%m-%d')

            if existing_entry is None:
                collection_name.insert_one(instance)
            else:
                collection_name.update_one(query, {'$set': instance})
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
            soup = BeautifulSoup(response.text, 'lxml')

            propertyTitle = soup.select_one("h4.content-title").text if (soup.select_one("h4.content-title")) else None
            propertyId = soup.select_one("li.save-favourite-button[id]")['id'].replace('fav-', '') if (soup.select_one("li.save-favourite-button[id]")) else None
            addedOn = soup.find(string='Added On:', name='strong').parent.text.replace('Added On: ', '') if (soup.find(string='Added On:', name='strong')) else None
            lastUpdated = soup.find(string='Last Updated:', name='strong').parent.text.replace('Last Updated: ', '') if (soup.find(string='Last Updated:', name='strong')) else None
            marketStatus = soup.find(string='Market Status:', name='strong').parent.text.replace('Market Status: ', '') if (soup.find(string='Market Status:', name='strong')) else None
            propertyType = soup.find(string='Type:', name='strong').parent.text.replace('Type: ', '') if (soup.find(string='Type:', name='strong')) else None
            beds = float(soup.find(string='Bedrooms:', name='strong').parent.text.replace('Bedrooms: ', '')) if (soup.find(string='Bedrooms:', name='strong')) else None
            baths = float(soup.find(string='Bathrooms:', name='strong').parent.text.replace('Bathrooms: ', '')) if (soup.find(string='Bathrooms:', name='strong')) else None
            toilets = float(soup.find(string='Toilets:', name='strong').parent.text.replace('Toilets: ', '')) if (soup.find(string='Toilets:', name='strong')) else None
            parkingSpaces = float(soup.find(string='Parking Spaces:', name='strong').parent.text.replace('Parking Spaces: ', '')) if (soup.find(string='Parking Spaces:', name='strong')) else None
            description = soup.find("p", attrs={"itemprop": "description"}).text.strip() if (soup.find("p", attrs={"itemprop": "description"})) else None
            imgUrls = [img['src'] for img in soup.select('ul li img')] if (soup.select('ul li img')) else None
            agentNumber = soup.find('input', {'id': 'fullPhoneNumbers'})['value'] if (soup.find('input', {'id': 'fullPhoneNumbers'})) else None
            agent = soup.select_one('img.company-logo')['alt'] if (soup.select_one('img.company-logo')) else None
            plotSize = soup.find(string='Total Area:', name='strong').parent.text.replace('Total Area: ', '') if (soup.find(string='Total Area:', name='strong')) else None
            size = soup.find(string='Covered Area:', name='strong').parent.text.replace('Covered Area: ', '') if (soup.find(string='Covered Area:', name='strong')) else None
            address = soup.find('address').text.strip() if (soup.find('address')) else None
            listingType = 'For Sale' if 'for-sale' in link[0] else ('For Rent' if 'for-rent' in link[0] else None)

            price = float(soup.select('span.pull-right.property-details-price span.price')[1].text.replace(',', ''))
            currency = soup.select('span.pull-right.property-details-price span.price')[0].text.strip()
            pricingCriteria = soup.select_one("span.period").text.strip()

            priceStatus, priceDiff, priceChange = None, None, None
            if price:
                data = singleItem.find_one({"url": link[0]})
                oldPrice = data['price'] if data else None
                priceDiff = max(oldPrice, price) - min(oldPrice, price) if oldPrice else 0
                priceChange = True if (priceDiff > 0) else False
                if price != oldPrice:
                    priceStatus = 'increased' if (price > oldPrice) else 'decreased'
                else:
                    priceStatus = None

            log.info(link[0], propertyTitle, propertyId)

        except (requests.exceptions.Timeout, requests.exceptions.SSLError):
            log.info("Timeout error occurred. Retrying in {} seconds...".format(delay))
            retries -= 1
            time.sleep(delay)
        except Exception as e:
            retries -= 1
            log.info(f"Failed to scrape data for {link[0]}: {e}")

        finally:
            try:
                thread_results[thread_id].append([link[0], propertyTitle, propertyId, datetime.strptime(addedOn, '%d %b %Y') if addedOn else None, datetime.strptime(lastUpdated, '%d %b %Y') if lastUpdated else None, marketStatus, propertyType, beds, baths, toilets, parkingSpaces, description, imgUrls, agent, agentNumber, size, plotSize, address, price, currency, pricingCriteria, priceDiff, priceChange, priceStatus, listingType])
                return
            except Exception as e:
                log.info(f'Some error occured while scraping url:{link[0]}-->{e}')
                return
    log.info(f"Max retries reached. Could not scrape {link[0]}")
    return 

def getData():
    log.info('Fetching stored URLs...')
    client = MongoClient(CONNECTION_STRING_MONGODB)
    db = client['nigeriaPropertyCentre']
    collection = db['propertyURLs']
    data = collection.find()
    return list(data)

def continuous_connection():
    clientC = MongoClient(CONNECTION_STRING_MONGODB)
    db = clientC['nigeriaPropertyCentre']
    return db['propertyURLs']

columns = ['url', 'propertyTitle', 'propertyId', 'addedOn', 'lastUpdated', 'marketStatus', 'propertyType', 'beds', 'baths', 'toilets', 'parkingSpaces', 'description', 'imgUrls', 'agent', 'agentNumber', 'size', 'plotSize', 'address', 'price', 'currency', 'pricingCriteria', 'priceDiff', 'priceChange', 'priceStatus', 'listingType']
databaseName = 'nigeriaPropertyCentre'

if __name__ == '__main__':
    
    links, all_data = [], []
    
    datas = getData()
    links = [list(data['url'].strip().split()) for data in datas]

    singleItem = continuous_connection()
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        executor.map(scrape_data, links)
        
    for thread_id, result_list in thread_results.items():
        sendData(result_list, columns, databaseName, 'propertyDetails')

    log.info("Details extraction completed successfully.")
    s3 = boto3.client("s3", region_name=aws_region_name)
    s3.put_object(Body=log_stringio.getvalue(), Bucket=bucket_name, Key="logs/nigeriaPropertyCentre/detail-extractor-logs.txt")
    log.info("Logs transfered to s3 completed")