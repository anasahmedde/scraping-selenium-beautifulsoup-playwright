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
threads = int(os.getenv("threads"))
list_pool_size = int(os.getenv("list_pool_size"))

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',datefmt='%Y-%m-%d %H:%M:%S')

log = logging.getLogger("lamudi-detail-extractor")
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
        # for index,instance in enumerate(mongo_insert_data):
        #     collection_name.update_one({'propertyId':instance['propertyId']},{'$set':instance},upsert=True)
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
            response = requests.get(link[0], timeout=180)
            soup = BeautifulSoup(response.text, 'lxml')

            propertyTitle = soup.find('meta', {'name': 'keywords'})['content'] if (soup.find('meta', {'name': 'keywords'})) else None
            propertyId = soup.find('span', {'id': 'ContentPlaceHolder1_DetailsFormView_CodeLabel'}).text if (soup.find('span', {'id': 'ContentPlaceHolder1_DetailsFormView_CodeLabel'})) else None
            category = soup.find('span', {'id': "ContentPlaceHolder1_DetailsFormView_CategoryLabel"}).text if (soup.find('span', {'id': 'ContentPlaceHolder1_DetailsFormView_CategoryLabel'})) else None
            listingType = soup.find('span', {'id': 'ContentPlaceHolder1_DetailsFormView_StatusLabel'}).text if (soup.find('span', {'id': 'ContentPlaceHolder1_DetailsFormView_StatusLabel'})) else None
            beds = soup.find('span', {'id': 'ContentPlaceHolder1_DetailsFormView_BedsInWordsLabel'}).text if (soup.find('span', {'id': 'ContentPlaceHolder1_DetailsFormView_BedsInWordsLabel'})) else None
            baths = soup.find('span', {'id': 'ContentPlaceHolder1_DetailsFormView_BathsInWordsLabel'}).text if (soup.find('span', {'id': 'ContentPlaceHolder1_DetailsFormView_BathsInWordsLabel'})) else None
            size = soup.find('span', {'id': 'ContentPlaceHolder1_DetailsFormView_SizeLabel'}).text if (soup.find('span', {'id': 'ContentPlaceHolder1_DetailsFormView_SizeLabel'})) else None
            tenure = soup.find('span', {'id': 'ContentPlaceHolder1_DetailsFormView_TenureLabel'}).text if (soup.find('span', {'id': 'ContentPlaceHolder1_DetailsFormView_TenureLabel'})) else None
            district = soup.find('span', {'id': 'ContentPlaceHolder1_DetailsFormView_DistrictLabel'}).text if (soup.find('span', {'id': 'ContentPlaceHolder1_DetailsFormView_DistrictLabel'})) else None
            agent = soup.find('span', {'id': 'ContentPlaceHolder1_DetailsFormView_AgentLabel'}).text if (soup.find('span', {'id': 'ContentPlaceHolder1_DetailsFormView_AgentLabel'})) else None
            agentNumber = soup.find('span', {'id': 'ContentPlaceHolder1_DetailsFormView_MobileLabel'}).text if (soup.find('span', {'id': 'ContentPlaceHolder1_DetailsFormView_MobileLabel'})) else None
            agentEmail = soup.find('span', {'id': 'ContentPlaceHolder1_FormView1_ContactEmailLabel'}).text if (soup.find('span', {'id': 'ContentPlaceHolder1_FormView1_ContactEmailLabel'})) else None
            description = soup.find('meta', {'name': 'description'})['content'] if (soup.find('meta', {'name': 'description'})) else None
            amenities = [amenity.text for amenity in soup.find_all('span', {'class': 'FourTables'})]
            location = soup.find('span', {'id': 'ContentPlaceHolder1_DetailsFormView_LocationLabel'}).parent.text.strip() if (soup.find('span', {'id': 'ContentPlaceHolder1_DetailsFormView_LocationLabel'})) else None

            priceStr = soup.find('span', {'id': 'ContentPlaceHolder1_DetailsFormView_Shillings'}).parent.text if (soup.find('span', {'id': 'ContentPlaceHolder1_DetailsFormView_Shillings'})) else (soup.find('span', {'id': 'ContentPlaceHolder1_DetailsFormView_Dollars'}).parent.text if (soup.find('span', {'id': 'ContentPlaceHolder1_DetailsFormView_Dollars'})) else None)
            price, currency, priceStatus, priceDiff, priceChange = None, None, None, None, None
            if (priceStr is not None and priceStr != ''):
                price = float(re.findall(r"([\d,]+)", priceStr)[0].replace(",", ""))
                currency = 'Ugx' if '\xa0' in priceStr else '$'

                data = singleItem.find_one({"url": link[0]})
                oldPrice = data['price'] if data else None
                priceDiff = max(oldPrice, price) - min(oldPrice, price) if oldPrice else 0
                priceChange = True if (priceDiff > 0) else False
                if price != oldPrice:
                    priceStatus = 'increased' if (price > oldPrice) else 'decreased'
                else:
                    priceStatus = None

            imgUrls = [img["src"] for img in soup.select("#wowslider-container2 a img")]

        except (requests.exceptions.Timeout, requests.exceptions.SSLError):
            log.info("Timeout error occurred. Retrying in {} seconds...".format(delay))
            retries -= 1
            time.sleep(delay)
        except Exception as e:
            retries -= 1
            log.info(f"Failed to scrape data for {link[0]}: {e}")

        finally:
            try:
                thread_results[thread_id].append([link[0], propertyTitle, propertyId, category, listingType, beds, baths, size, tenure, district, agent, agentNumber, agentEmail, description, amenities, location, price, currency, priceDiff, priceChange, priceStatus, imgUrls])
                return
            except Exception as e:
                log.info('Some error occured while scraping url: {link[0]}-->{e}')
                return
    log.info(f"Max retries reached. Could not scrape {link[0]}")
    return 


def getData():
    log.info("Fetching Stored URLs.")
    client = MongoClient(CONNECTION_STRING_MONGODB)

    db = client['lamudi']
    collection = db['propertyURLs']
    data = collection.find()
    return list(data)

def continuous_connection():
    clientC = MongoClient(CONNECTION_STRING_MONGODB)
    db = clientC['lamudi']
    return db['propertyURLs']

        
columns=['url', 'propertyTitle', 'propertyId', 'category', 'listingType', 'beds', 'baths', 'size', 'tenure', 'district', 'agent', 'agentNumber', 'agentEmail', 'description', 'amenities', 'location', 'price', 'currency', 'priceDiff', 'priceChange', 'priceStatus', 'imgUrls']
databaseName = 'lamudi'


if __name__ == '__main__':
    
    links = []
    
    datas = getData()
    links = [list(data['url'].strip().split()) for data in datas]
    
    singleItem = continuous_connection()
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        log.info("Extracting details...")
        executor.map(scrape_data, links)
    for thread_id, result_list in thread_results.items():
        sendData(result_list, columns, databaseName, 'propertyDetails')

    log.info("Details extraction completed successfully.")
    s3 = boto3.client("s3", region_name=aws_region_name)
    s3.put_object(Body=log_stringio.getvalue(), Bucket=bucket_name, Key="logs/lamudi/detail-extractor-logs.txt")  
    log.info("Logs transfered to s3 completed")