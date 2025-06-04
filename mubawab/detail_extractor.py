import concurrent.futures
from bs4 import BeautifulSoup
import os, io, logging, boto3, warnings, requests, time, re
from pymongo import MongoClient
from dotenv import load_dotenv
import pandas as pd
from pymongo import MongoClient
import threading
import numpy as np
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

log = logging.getLogger("mubawab-detail-extractor")
log_stringio = io.StringIO()
handler = logging.StreamHandler(log_stringio)
handler.setFormatter(formatter)
log.addHandler(handler)


def sendData(data,columns,databaseName,collectionName):
    try:
        log.info(f'Collected {len(data)} records!')
        df=pd.DataFrame(data,columns=columns)
        df.replace({np.nan: None}, inplace=True)
        mongo_insert_data=df.to_dict('records')
        log.info('Sending Data to MongoDB!')
        def get_database():
            client = MongoClient(CONNECTION_STRING_MONGODB)
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

            title = soup.select_one("h1.searchTitle").text.strip() if soup.select_one("h1.searchTitle") else None
            propertyId = link[0].split("/")[-2]
            description = soup.select_one("i.icon-doc-text").parent.parent.find('p').text.strip() if soup.select_one("i.icon-doc-text") else None
            imgUrls = [imgs['src'].replace('/s/', '/h/') for imgs in soup.select("img.imgThumb")]
            amenities = [amenity.text.strip().replace('\n', '').replace('\t', '') for amenity in soup.select("span.characIconText") + soup.select("span.tagProp.tagPromo")]
            listingType = 'rent' if '-for-rent-' in link[0] else 'sale'
            address = soup.select_one("i.icon-location").parent.text.strip() if soup.select_one("i.icon-location") else None
            attrs = [attr.text.strip().replace('\n', ' ').replace('\t', '') for attr in soup.select("span.tagProp")]
            baths = next((i for i in attrs if 'Bathroom' in i), None)
            beds = next((i for i in attrs if 'Room' in i), None)
            size = next((i for i in attrs if 'm²' in i), None)
            district = (soup.select_one('h3.greyTit').text.strip().replace('\n', ' ').replace('\t', '').split(' in ')[0] if 'in' in soup.select_one('h3.greyTit').text else None) if soup.select_one('h3.greyTit') else None
            latitude = soup.select_one("div#mapOpen")['lat'] if soup.select_one("div#mapOpen") else None
            longitude = soup.select_one("div#mapOpen")['lon'] if soup.select_one("div#mapOpen") else None
            
            price = float(re.search(r"[\d,]+", soup.select_one("h3.orangeTit").text).group().replace(",", "")) if re.search(r"[\d,]+", soup.select_one("h3.orangeTit").text) else None
            currency = soup.select_one("h3.orangeTit").text.replace('\n', ' ').split(' ')[1] if price else None
            pricingCriteria = (soup.select_one("h3.orangeTit").find('em').text if soup.select_one("h3.orangeTit").find('em') else None) if price else None

            priceStatus, priceDiff, priceChange, city, housingType = None, None, None, None, None
            for prop in datas:
                if prop['propertyId'] == propertyId:
                    city = prop['city']
                    housingType = prop['housingType']
                    if price:
                        oldPrice = prop['price']
                        priceDiff = max(oldPrice, price) - min(oldPrice, price) if oldPrice else 0
                        priceChange = True if (priceDiff > 0) else False
                        if price != oldPrice:
                            priceStatus = 'increased' if (price > oldPrice) else 'decreased'
                        else:
                            priceStatus = None
            
            agent = soup.select_one("p.link a").text.strip() if soup.select_one("p.link a") else None
            if agent:
                res = requests.get(soup.select_one("p.link a")['href'], timeout=60)
                agentSoup = BeautifulSoup(res.text, 'lxml')
                agentNumber = agentSoup.select_one("a.agencyLink p").text if agentSoup.select_one("a.agencyLink p") else None
            else:
                agentNumber = None if soup.select_one("div.refBox") else '+212 6 61 32 55 35'
            
            print(link[0], title, propertyId)

        except (requests.exceptions.Timeout, requests.exceptions.SSLError):
            log.info("Timeout error occurred. Retrying in {} seconds...".format(delay))
            retries -= 1
            time.sleep(delay)
        except Exception as e:
            retries -= 1

        finally:
            try:
                thread_results[thread_id].append([link[0], title, propertyId, description, imgUrls, amenities, listingType, address, city, housingType, beds, baths, size, price, currency, pricingCriteria, priceDiff, priceChange, priceStatus, district, agent, agentNumber, latitude, longitude])
                return
            except Exception as e:
                log.info(e, {link[0]})
                return
    log.info(f"Max retries reached. Could not scrape {link[0]}")
    return 

def getData():
    client = MongoClient(CONNECTION_STRING_MONGODB)
    log.info("Fetching URLs from database !")
    db = client['mubawab']
    collection = db['propertyURLs']
    data = collection.find()
    return list(data)

def continous_connection():
    clientC = MongoClient(CONNECTION_STRING_MONGODB)
    db = clientC['mubawab']
    return db['propertyURLs']


columns=['url', 'title', 'propertyId', 'description', 'imgUrls', 'amenities', 'listingType', 'address', 'city', 'housingType', 'beds', 'baths', 'size', 'price', 'currency', 'pricingCriteria', 'priceDiff', 'priceChange', 'priceStatus', 'district', 'agent', 'agentNumber', 'latitude', 'longitude']
databaseName = 'mubawab'

links = []
datas = getData()
links = [list(data['url'].strip().split()) for data in datas]

singleItem = continous_connection()
with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
    executor.map(scrape_data, links)
for thread_id, result_list in thread_results.items():
    sendData(result_list, columns, databaseName, 'propertyDetails')

log.info("Details extraction completed successfully.")
s3 = boto3.client("s3", region_name=aws_region_name)
s3.put_object(Body=log_stringio.getvalue(), Bucket=bucket_name, Key="logs/mubawab/detail-extractor-logs.txt")
log.info("Logs transfered to s3 completed")
