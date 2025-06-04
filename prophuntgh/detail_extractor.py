import concurrent.futures
from bs4 import BeautifulSoup
import os, io, logging, boto3, warnings, requests, time, re
from pymongo import MongoClient
from dotenv import load_dotenv
import pandas as pd
import numpy as np
from pymongo import MongoClient
import threading

thread_results = {}

warnings.filterwarnings("ignore", category=DeprecationWarning) 
load_dotenv(override=True)

CONNECTION_STRING_MONGODB = os.getenv("CONNECTION_STRING")
aws_region_name = os.getenv("aws_region_name")
bucket_name = os.getenv("bucket_name")
list_pool_size = int(os.getenv("list_pool_size"))
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',datefmt='%Y-%m-%d %H:%M:%S')

log = logging.getLogger("prophunt-detail-extractor")
log_stringio = io.StringIO()
handler = logging.StreamHandler(log_stringio)
handler.setFormatter(formatter)
log.addHandler(handler)


def scrape_data(link):
    global singleItem
    thread_id = threading.get_ident()
    if thread_id not in thread_results:
        thread_results[thread_id]=[]    

    if len(thread_results[thread_id])==list_pool_size:
        sendData(thread_results[thread_id], columns, databaseName, 'propertyDetails')
        thread_results[thread_id]=[]     
    retries = 2
    delay = 10
    while retries > 0:
        try:
            response = requests.get(link[0], timeout=120)
            soup = BeautifulSoup(response.text, 'lxml')

            url = link[0]
            propertyId = url.split('/')[-2]
            listingType = "sale" if "-for-sale" in url else "rent" if "-for-rent" in url else "project"
            title = soup.find('title').text
            location = soup.select_one('div.title-block').text.strip().split('\n')[1].strip() if soup.select_one('div.title-block') else None
            agent = soup.select_one('#right-contact figcaption').get_text(strip=True) if soup.select_one('#right-contact figcaption') else None
            agentNumber = soup.select_one('span.icon-phone-call + span').get_text(strip=True) if soup.select_one('span.icon-phone-call + span') else None
            beds = float(soup.select_one('i.icon-bedroom').find_previous_sibling('span').text.replace('+', '')) if soup.select_one('i.icon-bedroom') else None
            baths = float(soup.select_one('i.icon-bathroom').find_previous_sibling('span').text.replace('+', '')) if soup.select_one('i.icon-bathroom') else None
            parkingVehicles = float(soup.select_one('i.icon-parking').find_previous_sibling('span').text.replace('+', '').split(' ')[0]) if soup.select_one('i.icon-parking') else None
            parking = True if parkingVehicles else False
            internalArea = float(soup.select_one('i.icon-area').find_previous_sibling('span').text.split(' ')[0]) if soup.select_one('i.icon-area') else None
            amenities = [item.get_text(strip=True) for item in soup.select('#amenities ul li')] if soup.select('#amenities ul li') else []
            imgUrls = [img['src'] for img in soup.select('div.property-img-slider-nav img.lazy.img-fluid.w-100.h-100.img-cover')] if soup.select('div.property-img-slider-nav img.lazy.img-fluid.w-100.h-100.img-cover') else []
            description = soup.find('h2', text='About The Property').find_next_sibling('div').get_text(strip=True) if soup.find('h2', text='About The Property') else None
            dateListed=soup.select_one('div.property-contnent span.text-dark').text if soup.select_one('div.property-contnent span.text-dark') else None
            latitude = float(soup.select_one('input#mapLatitude').get('value')) if soup.select_one('input#mapLatitude') else None
            longitude = float(soup.select_one('input#mapLongitude').get('value')) if soup.select_one('input#mapLongitude') else None

            pricingCriteria = None
            if listingType=='rent':
                pricing_criteria = soup.find('span', class_='neg_price').get_text(strip=True).split('/')[-1] if soup.find('span', class_='neg_price') else None
                
            price = soup.select_one('div.price').find('span').text.replace('\n', '').replace(' ', '').strip()
            price = float(re.search(r'\d+(?:,\d+)?', price).group().replace(',', ''))
            currency = "gh₵"
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

            print(url, title, propertyId)
            thread_results[thread_id].append([
                url,listingType,propertyId,title,location,dateListed,agent,agentNumber,
                price,currency,beds,baths,parkingVehicles,parking,internalArea,amenities,imgUrls,description,
                priceChange,priceStatus,priceDiff,pricingCriteria,latitude,longitude
            ])
            return

        except (requests.exceptions.Timeout, requests.exceptions.SSLError):
            print("Timeout error occurred. Retrying in {} seconds...".format(delay))
            retries -= 1
            time.sleep(delay)
        except Exception as e:
            retries -= 1
            print(f"Failed to scrape data for {link[0]}: {e}")

    print(f"Max retries reached. Could not scrape {link[0]}")
    return 


def getData():
    log.info('Fetching stored URLs...')
    client = MongoClient(CONNECTION_STRING_MONGODB)
    db = client['prophunt']
    collection = db['propertyURLs']
    data = collection.find()
    return list(data)

def continuous_connection():
    clientC = MongoClient(CONNECTION_STRING_MONGODB)
    db = clientC['prophunt']
    return db['propertyURLs']

def sendData(data,columns,databaseName,collectionName):
    try:
        log.info(f'Collected {len(data)} records!')
        df=pd.DataFrame(data,columns=columns)
        df.replace({np.nan: None}, inplace=True)
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

columns = [
    'url','listingType','propertyId','title','location','dateListed','agent','agentNumber',
    'price','currency','beds','baths','parkingVehicles','parking','internalArea','amenities','imgUrls','description',
    'priceChange','priceStatus','priceDiff','pricingCriteria','latitude','longitude'
]
databaseName = 'prophunt'
threads = int(os.getenv("threads"))

if __name__ == '__main__':

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
    s3.put_object(Body=log_stringio.getvalue(), Bucket=bucket_name, Key="logs/prophunt/detail-extractor-logs.txt")  
    log.info("Logs transfered to s3 completed")