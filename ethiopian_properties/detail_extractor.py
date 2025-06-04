import concurrent.futures
from bs4 import BeautifulSoup
import os, io, logging, boto3, warnings, requests, time
from pymongo import MongoClient
from dotenv import load_dotenv
import pandas as pd
from pymongo import MongoClient
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

log = logging.getLogger("ethiopianProperties-detail-extractor")
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
        sendData(thread_results[thread_id],columns, databaseName, 'propertyDetails')
        thread_results[thread_id]=[]

    retries = 3
    delay = 10
    while retries > 0:
        try:
            response = requests.get(link[0], headers=headers, timeout=120)
            soup = BeautifulSoup(response.content, 'lxml')
            if soup.find("span", text="404 - Page Not Found!"):
                log.info(f'Listing not found: {link[0]}')
                return

            propertyTitle = soup.select_one('h1.page-title').text if soup.select_one('h1.page-title') else None
            propertyId = soup.select_one('h4.title').text.split(':')[1].strip() if (len(soup.select_one('h4.title').text.split(':')) > 1) else None
            priceLst = soup.select_one('h5.price span:nth-child(2)').text.strip().split('-')[0].strip().replace('$', '').replace(',', '') if soup.select_one('h5.price span:nth-child(2)') else None
            price = priceLst.split(' ')[0]
            currency, priceStatus, priceDiff = None, None, None
            if (price is not None and price != ''):
                price = float(price)  
                currency = "USD"

                data = singleItem.find_one({"url": link[0]})
                oldPrice = data['price'] if data else None
                priceDiff = max(oldPrice, price) - min(oldPrice, price) if oldPrice else 0
                if price != oldPrice:
                    priceStatus = 'increased' if (price > oldPrice) else 'decreased'
                else:
                    priceStatus = None

            priceType = ' '.join(priceLst.split(' ')[1:])
            listingType = soup.select_one('h5.price span').text.strip()
            imgUrls = [a['href'] for a in soup.select('ul.slides li a')]
            features = [feature.text for feature in soup.select('ul.arrow-bullet-list.clearfix a')]
            city = soup.select_one('nav.property-breadcrumbs li:nth-of-type(2)').text if soup.select_one('nav.property-breadcrumbs li:nth-of-type(2)') else None
            neighbourhood = soup.select_one('nav.property-breadcrumbs li:nth-of-type(3)').text if soup.select_one('nav.property-breadcrumbs li:nth-of-type(3)') else None
            
            props = [prop.text.replace('\n', '').replace('\xa0', '') for prop in soup.select('div.property-meta.clearfix span')[:-3]]
            beds, baths, garage, size = None, None, None, None
            for i in props:
                if 'Bedroom' in i:
                    beds = float(i.replace('Bedroom', '').replace('s', ''))
                elif 'Bathroom' in i:
                    baths = float(i.replace('Bathroom', '').replace('s', ''))
                elif 'Garage' in i:
                    garage = float(i.replace('Garage', '').replace('s', ''))
                else:
                    size = i

            content = "\n".join([desc.text for desc in soup.select('div.content.clearfix')]).replace('\xa0', '')
            description = '\n'.join(content.split('Additional Amenities')[0].strip().split('\n'))
            amenities = content.split('Additional Amenities')[1].strip().split('\n') if (len(content.split('Additional Amenities')) > 1) else []
            agentNumber = soup.select('li.office')[0].text.replace('\n', '').replace('\t', '').split(':')[1].strip() if soup.select('li.office') else "+251-911-088-114"

        except (requests.exceptions.Timeout, requests.exceptions.SSLError):
            log.info("Timeout error occurred. Retrying in {} seconds...".format(delay))
            retries -= 1
            time.sleep(delay)

        except Exception as e:
            print(f"Failed to scrape data for {link[0]}: {e}")

        finally:
            try:
                print(propertyTitle, propertyId,)
                thread_results[thread_id].append([propertyTitle, propertyId, link[0], price if (price != '') else None, currency if (price != '') else None, float(priceDiff) if (priceDiff != '' and priceDiff is not None) else None, priceStatus, True if (priceDiff is not None and (priceDiff > 0 and priceDiff != '')) else False, priceType if (priceType != '') else ('Month' if 'rent' in listingType.lower() else None), listingType, imgUrls, features, beds, baths, garage, size, description, amenities, "Agent Admin", agentNumber, city, neighbourhood])
                return
            except:
                log.info(f'Some error occured while scraping url: {link[0]}-->{e}')
                return
    print(f"Max retries reached. Could not scrape {link[0]}")
            
def getData():
    log.info("Fetching Stored URLs.")
    client = MongoClient(CONNECTION_STRING_MONGODB)
    db = client['EthiopianProperties']
    collection = db['propertyURLs']
    data = collection.find()
    return list(data)

def continuous_connection():
    clientC = MongoClient(CONNECTION_STRING_MONGODB)
    db = clientC['EthiopianProperties']
    return db['propertyURLs']

                  
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1'
}
databaseName='EthiopianProperties'
columns=['propertyTitle', 'propertyId', 'url', 'price', 'currency', 'priceDiff', 'priceStatus', 'priceChange', 'pricingCriteria', 'listingType', 'imgUrls', 'features', 'beds', 'baths', 'garage', 'size', 'description', 'amenities', 'agent', 'agentNumber', 'city', 'neighbourhood']

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
    s3.put_object(Body=log_stringio.getvalue(), Bucket=bucket_name, Key="logs/ethiopianProperties/detail-extractor-logs.txt")  
    log.info("Logs transfered to s3 completed")