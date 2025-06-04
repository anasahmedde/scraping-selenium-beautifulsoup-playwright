import concurrent.futures
from bs4 import BeautifulSoup
import os, io, logging, boto3, warnings, requests, time, datetime, re
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

log = logging.getLogger("property-pro-ng-detail-extractor")
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
            propertyId = url.split('-')[-1]
            listingType = "sale" if "-for-sale" in url else "rent" if "-for-rent" in url else "project"
            title = soup.select_one('div.duplex-text').find('h1').text.strip() if soup.select_one('div.duplex-text') else None
            location = soup.select_one('div.duplex-text').find('h6').text.strip() if soup.select_one('div.duplex-text') else None
            agent = soup.find('div', class_='consulting-top text-center').text.strip() if soup.find('div', class_='consulting-top text-center') else None
            agentNumber = soup.find_all('p', class_='call-hide')[1].find_next_sibling('a').text.strip() if soup.find_all('p', class_='call-hide') else None
            beds = float(re.search(r'\d+', soup.find('div', class_="slider-feature").find('img', title='bed-icon').parent.text).group()) if (soup.find('div', class_="slider-feature").find('img', title='bed-icon') and soup.find('div', class_="slider-feature").find('img', title='bed-icon').parent.text.replace(' beds', '')) else None
            baths = float(re.search(r'\d+', soup.find('div', class_="slider-feature").find('img', title='bath-icon').parent.text).group()) if (soup.find('div', class_="slider-feature").find('img', title='bath-icon') and soup.find('div', class_="slider-feature").find('img', title='bath-icon').parent.text.replace(' baths', '')) else None
            toilets = float(re.search(r'\d+', soup.find('div', class_="slider-feature").find('img', title='toilet-icon').parent.text).group()) if (soup.find('div', class_="slider-feature").find('img', title='toilet-icon') and soup.find('div', class_="slider-feature").find('img', title='toilet-icon').parent.text.replace(' Toilets', '')) else None
            housingType = soup.select_one('select[name="type"]').find('option', selected=True).text if soup.select_one('select[name="type"]').find('option', selected=True) else None

            amenities = [item.text.strip() for item in soup.find_all('div', class_='key-features-list')[0].find_all('li')] if soup.find_all('div', class_='key-features-list') else []
            imgUrls = [item['src'] if 'src' in item.attrs else item['data-lazy'] for item in soup.find_all('img', class_='slider-img')] if soup.find_all('img', class_='slider-img') else []

            description = soup.find('div', class_='description-text').text.strip() if soup.find('div', class_='description-text') else None
            city = soup.select_one('select[class="classic"][name="state"] option[selected]').get('value').capitalize()
            
            dates = soup.select_one('h5.show-date').text.strip().split(',')
            if len(dates) == 2:
                dateAdded = datetime.datetime.strptime(dates[1].strip().replace('Added ', ''), '%d %b %Y')
                lastUpdated = datetime.datetime.strptime(dates[0].strip().replace('Updated ', ''), '%d %b %Y')
            else:
                dateAdded = datetime.datetime.strptime(dates[0].strip().replace('Added ', ''), '%d %b %Y')
                lastUpdated = None

            price = float(soup.select_one('div.duplex-view-text').find_all('strong')[1].get('content')) if soup.select_one('div.duplex-view-text').find_all('strong')[1] else None
            currency = soup.select_one('div.duplex-view-text').find_all('strong')[0].get('content') if soup.select_one('div.duplex-view-text').find_all('strong')[1] else None
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

            print(link[0], title, propertyId)
            thread_results[thread_id].append([
                url,propertyId,listingType,title,location,agent,agentNumber,price,currency,beds,baths,toilets,housingType,
                amenities,imgUrls,description,city,dateAdded,lastUpdated,priceChange,priceStatus,priceDiff
            ])
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
    log.info('Fetching stored URLs...')
    client = MongoClient(CONNECTION_STRING_MONGODB)
    db = client['propertypro_ng']
    collection = db['propertyURLs']
    data = collection.find()
    return list(data)

def continuous_connection():
    clientC = MongoClient(CONNECTION_STRING_MONGODB)
    db = clientC['propertypro_ng']
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
    'url','propertyId','listingType','title','location','agent','agentNumber','price','currency','beds','baths','toilets','housingType',
    'amenities','imgUrls','description','city','dateAdded','lastUpdated','priceChange','priceStatus','priceDiff'
]
databaseName = 'propertypro_ng'
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
    s3.put_object(Body=log_stringio.getvalue(), Bucket=bucket_name, Key="logs/property-pro-ng/detail-extractor-logs.txt")  
    log.info("Logs transfered to s3 completed")