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

log = logging.getLogger("property-pro-co-ke-detail-extractor")
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
            title = soup.find('h3', {'itemprop': 'name'}).text if soup.find('h3', {'itemprop': 'name'}) else None
            location = soup.find('h3', {'itemprop': 'name'}).find_next_sibling('h6').text if soup.find('h3', {'itemprop': 'name'}).find_next_sibling('h6') else None
            agent = soup.find('div', class_='consulting-top text-center').text.strip() if soup.find('div', class_='consulting-top text-center') else None
            agentNumber = soup.find('p', class_='call-hide').find_next_sibling('a').text.strip() if soup.find('p', class_='call-hide') and soup.find('p', class_='call-hide').find_next_sibling('a') else None
            beds = float(re.search(r'\d+', soup.find('img', alt='bed-icon').parent.text).group()) if soup.find('img', alt='bed-icon') and soup.find('img', alt='bed-icon').parent.text and re.search(r'\d+', soup.find('img', alt='bed-icon').parent.text) else None
            baths = float(re.search(r'\d+', soup.find('img', alt='bath-icon').parent.text).group()) if soup.find('img', alt='bath-icon') and soup.find('img', alt='bath-icon').parent.text and re.search(r'\d+', soup.find('img', alt='bath-icon').parent.text) else None
            toilets = float(re.search(r'\d+', soup.find('img', alt='toilet-icon').parent.text).group()) if soup.find('img', alt='toilet-icon') and soup.find('img', alt='toilet-icon').parent.text and re.search(r'\d+', soup.find('img', alt='toilet-icon').parent.text) else None
            amenities = [item.text.strip() for item in soup.find_all('div', class_='key-features-list')[0].find_all('li')] if soup.find_all('div', class_='key-features-list') else []
            imgUrls = [item['src'] for item in soup.find_all('img', class_='slider-img')] if soup.find_all('img', class_='slider-img') else []
            description = soup.find('div', class_='description-text').text.strip() if soup.find('div', class_='description-text') else None
            city = soup.select_one('select[class="classic"][name="state"] option[selected]').get('value').capitalize()
            dateAdded = datetime.datetime.strptime(soup.select_one('h5.show-date').text.strip().replace('Added ', ''), '%d %b %Y') if soup.select_one('h5.show-date') else None
            housingType = soup.select_one('select[name="type"]').find('option', selected=True).text if soup.select_one('select[name="type"]').find('option', selected=True) else None

            price = float(soup.find(attrs={'itemprop': 'price'}).get('content')) if soup.find(attrs={'itemprop': 'price'}) else None
            currency="KES"
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
                url,propertyId,listingType,title,location,agent,agentNumber,price,currency,beds,baths,toilets,
                amenities,imgUrls,description,city,dateAdded,housingType,priceChange,priceStatus,priceDiff
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
    db = client['propertypro_co_ke']
    collection = db['propertyURLs']
    data = collection.find()
    return list(data)

def continuous_connection():
    clientC = MongoClient(CONNECTION_STRING_MONGODB)
    db = clientC['propertypro_co_ke']
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
    'url','propertyId','listingType','title','location','agent','agentNumber','price','currency','beds','baths','toilets',
    'amenities','imgUrls','description','city','dateAdded','housingType','priceChange','priceStatus','priceDiff'
]
databaseName = 'propertypro_co_ke'
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
    s3.put_object(Body=log_stringio.getvalue(), Bucket=bucket_name, Key="logs/property-pro-co-ke/detail-extractor-logs.txt")  
    log.info("Logs transfered to s3 completed")