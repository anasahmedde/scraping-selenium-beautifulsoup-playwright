import concurrent.futures
from bs4 import BeautifulSoup
import os, io, logging, boto3, warnings, requests, time, json, datetime
from pymongo import MongoClient
from dotenv import load_dotenv
import pandas as pd
from pymongo import MongoClient
import threading

thread_results = {}

warnings.filterwarnings("ignore", category=DeprecationWarning) 
load_dotenv(override=True)

CONNECTION_STRING_MONGODB = os.getenv("CONNECTION_STRING")
aws_region_name = os.getenv("aws_region_name")
bucket_name = os.getenv("bucket_name")
threads = int(os.getenv("threads"))
list_pool_size = int(os.getenv("list_pool_size"))
zenRowsApiKey = os.getenv("ZENROWS_API_KEY")

conversion_factors = {
    "ft²": 1,          # Already in square feet
    "m²": 10.764,      # 1 square meter = 10.764 square feet
    "ac": 43560,       # 1 acre = 43,560 square feet
    "ha": 107639       # 1 hectare = 107,639 square feet
}

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',datefmt='%Y-%m-%d %H:%M:%S')

log = logging.getLogger("buyRentKenya-detail-extractor")
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
        for index,instance in enumerate(mongo_insert_data):
            collection_name.update_one({'propertyId':instance['propertyId']},{'$set':instance},upsert=True)
        log.info('Data sent to MongoDB successfully')
    except Exception as e:
        log.info('Some error occurred while sending data MongoDB! Following is the error.')
        log.error(e)


def scrape_data(urlData):
    thread_id = threading.get_ident()
    if thread_id not in thread_results:
        thread_results[thread_id]=[]    

    if len(thread_results[thread_id])==list_pool_size:
        sendData(thread_results[thread_id], columns, databaseName, 'propertyDetails')
        thread_results[thread_id]=[]    

    retries = 1
    delay = 10
    while retries > 0:
        try:
            url = urlData[0]
            # response = requests.get("https://api.zenrows.com/v1/", timeout=30, params={
            #     "apikey": zenRowsApiKey,
            #     "url": url,
            # })
            # soup = BeautifulSoup(response.text, 'lxml')

            response = requests.get(url, timeout=120)
            soup = BeautifulSoup(response.text, 'lxml')
            log.info(url)
            propertyId = url.split('-')[-1]
            listingType = "sale" if "-for-sale" in url else "rent" if "-for-rent" in url else "project"
            housingType = soup.select_one("ul li.whitespace-nowrap:nth-of-type(3)").text.strip() if soup.select_one("ul li.whitespace-nowrap:nth-of-type(3)") else None
            city = soup.select_one('nav[data-cy="breadcrumbs"] ul li:nth-of-type(4)').text.strip() if soup.select_one('nav[data-cy="breadcrumbs"] ul li:nth-of-type(4)') else None
            suburb = soup.select_one('nav[data-cy="breadcrumbs"] ul li:nth-of-type(5)').text.strip() if soup.select_one('nav[data-cy="breadcrumbs"] ul li:nth-of-type(5)') else None
            title = soup.select_one("h1[data-cy='listing-heading']").text.strip() if soup.select_one("h1[data-cy='listing-heading']") else None
            location = soup.select_one('p[data-cy="listing-address"]').text.strip() if soup.select_one('p[data-cy="listing-address"]') else None
            dateListed = datetime.datetime.strptime(soup.select_one('span[date-cy^="date-created"]').text.strip(), "%d/%m/%Y") if soup.select_one('span[date-cy^="date-created"]') else None
            beds = int(soup.select_one('span[aria-label*="bedrooms"]').text.strip().replace('\n', '')) if soup.select_one('span[aria-label*="bedrooms"]') else None
            baths = int(soup.select_one('span[aria-label*="bathrooms"]').text.strip().replace('\n', '')) if soup.select_one('span[aria-label*="bathrooms"]') else None
            size = float(soup.select_one('span[aria-label*="area"]').text.strip().split()[0]) if soup.select_one('span[aria-label*="area"]') else None
            size_unit = soup.select_one('span[aria-label*="area"]').text.strip().split()[1] if soup.select_one('span[aria-label*="area"]') else None
            size_sqft = size * conversion_factors.get(size_unit, 1) if size and size_unit in conversion_factors else None
            parking = bool(soup.select_one('ul li div div:contains("Parking")')) if soup.select_one('ul li div div:contains("Parking")') else False
            amenities = [item.text.strip().replace('\n\n|', '') for item in soup.select('div[data-cy="listing-amenities-component"] li')]
            imgUrls = [img['src'] for img in soup.select('#gallery_slider img')]

            data = json.loads(soup.select_one('div[data-bi="product-top"]').get('wire:snapshot')) if soup.select_one('div[data-bi="product-top"]') else json.loads(soup.select_one('div[data-bi="product-premium"]').get('wire:snapshot')) if soup.select_one('div[data-bi="product-premium"]') else json.loads(soup.select_one('div[data-bi="product-basic"]').get('wire:snapshot'))

            description = data['data']['listingResult'][0]['_source'][0]['description']
            agent = data['data']['listingResult'][0]['_source'][0]['agents'][0][0][0]['name']
            agentNumber = data['data']['listingResult'][0]['_source'][0]['agents'][0][0][0]['chat_link'].replace("https://wa.me/", '').replace('?text=', '') if data['data']['listingResult'][0]['_source'][0]['agents'][0][0][0]['chat_link'] else data['data']['listingResult'][0]['_source'][0]['agents'][0][0][0]['mobile_number']
                                    
            price = float(soup.select_one('span#topbar-listing-price').text.strip().split(' ')[1].replace(',', '')) if soup.select_one('span#topbar-listing-price').text.strip() != 'Price not communicated' else None
            currency="KES"
            priceStatus, priceDiff, priceChange = None, None, None
            if price:
                oldPrice = urlData[1]
                priceDiff = max(oldPrice, price) - min(oldPrice, price) if oldPrice else 0
                priceChange = True if (priceDiff > 0) else False
                if price != oldPrice:
                    priceStatus = 'increased' if (price > oldPrice) else 'decreased'
                else:
                    priceStatus = None

            thread_results[thread_id].append([
                url,listingType,propertyId,title,location,dateListed,agent,agentNumber,price,currency,beds,baths,
                size,size_unit,size_sqft,parking,amenities,imgUrls,description,
                priceChange,priceStatus,priceDiff,housingType,city,suburb
            ])
            return

        except (requests.exceptions.Timeout, requests.exceptions.SSLError):
            log.info("Timeout error occurred. Retrying in {} seconds...".format(delay))
            retries -= 1
            time.sleep(delay)
        except Exception as e:
            retries -= 1
            log.info(f"Failed to scrape data for {urlData[0]}: {e}")

    log.info(f"Max retries reached. Could not scrape {urlData[0]}")
    return 

def getData():
    log.info('Fetching stored URLs...')
    client = MongoClient(CONNECTION_STRING_MONGODB)
    db = client['buyrentkenya']
    collection = db['propertyURLs']
    data = collection.find()
    return list(data)

columns = [
    'url','listingType','propertyId','title','location','dateListed','agent','agentNumber','price','currency','beds','baths',
    'size','size_unit','size_sqft','parking','amenities','imgUrls','description',
    'priceChange','priceStatus','priceDiff','housingType','city','suburb'
]
databaseName = 'buyrentkenya'

if __name__ == '__main__':
    datas = getData()
    urls_data = [[
        data.get('url', '').strip(),
        data.get('price', '')
    ] for data in datas]

    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        executor.map(scrape_data, urls_data)

    for thread_id, result_list in thread_results.items():
        sendData(result_list, columns, databaseName, 'propertyDetails')

    log.info("Details extraction completed successfully.")
    s3 = boto3.client("s3", region_name=aws_region_name)
    s3.put_object(Body=log_stringio.getvalue(), Bucket=bucket_name, Key="logs/buyRentKenya/detail-extractor-logs.txt")  
    log.info("Logs transferred to s3 completed")