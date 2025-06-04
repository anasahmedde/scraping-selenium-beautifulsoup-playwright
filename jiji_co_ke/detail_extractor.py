import concurrent.futures
from bs4 import BeautifulSoup
import os, io, logging, boto3, warnings, requests, time
from pymongo import MongoClient
from dotenv import load_dotenv
import pandas as pd
from pymongo import MongoClient
from datetime import datetime
import threading
from zenrows import ZenRowsClient
from bs4 import BeautifulSoup
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError
import re

warnings.filterwarnings("ignore", category=DeprecationWarning) 
load_dotenv(override=True)

CONNECTION_STRING_MONGODB = os.getenv("CONNECTION_STRING")
aws_region_name = os.getenv("aws_region_name")
bucket_name = os.getenv("bucket_name")
zenRowsApiKey = os.getenv("ZENROWS_API_KEY")
threads = int(os.getenv("threads"))
aws_region_name = os.getenv("aws_region_name")
list_pool_size = int(os.getenv("list_pool_size"))

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',datefmt='%Y-%m-%d %H:%M:%S')

log = logging.getLogger("jiji-co-ke-detail-extractor")
log_stringio = io.StringIO()
handler = logging.StreamHandler(log_stringio)
handler.setFormatter(formatter)
log.addHandler(handler)


client = ZenRowsClient(zenRowsApiKey)
params = {"js_render":"true"}
thread_results = {} 


def sendData(data, columns, databaseName, collectionName):
    try:
        log.info(f"Collected {len(data)} records!")
        
        # Convert data to DataFrame and process datetime columns
        df = pd.DataFrame(data, columns=columns)
        datetime_columns = df.select_dtypes(include=["datetime64"]).columns
        df[datetime_columns] = df[datetime_columns].fillna(pd.NaT).astype(str)
        
        # Convert DataFrame to dictionary
        mongo_insert_data = df.to_dict("records")
        log.info("Preparing to send data to MongoDB...")
        
        # Establish MongoDB connection
        client = MongoClient(CONNECTION_STRING_MONGODB)
        db = client[databaseName]
        collection = db[collectionName]

        # Prepare bulk operations
        operations = []
        for record in mongo_insert_data:
            # Ensure 'propertyId' exists in the record
            if "propertyId" not in record:
                log.warning(f"Skipping record without 'propertyId': {record}")
                continue

            query = {"propertyId": record["propertyId"]}
            
            # Separate logic for new and existing entries
            update_document = {
                "$set": record,
                "$setOnInsert": {"addedOn": datetime.now().strftime("%Y-%m-%d")}
            }
            
            operations.append(
                UpdateOne(
                    filter=query,
                    update=update_document,
                    upsert=True  # Insert if the document doesn't exist
                )
            )

        # Execute bulk operations if there are any
        if operations:
            result = collection.bulk_write(operations)
            log.info(
                f"Data sent to MongoDB successfully! "
                f"Matched: {result.matched_count}, "
                f"Inserted: {result.upserted_count}, "
                f"Modified: {result.modified_count}"
            )
        else:
            log.info("No valid records to process.")
    
    except BulkWriteError as bwe:
        log.error("Bulk write error occurred while sending data to MongoDB!")
        log.error(bwe.details)
    except Exception as e:
        log.error("An error occurred while sending data to MongoDB!")
        log.exception(e)

def scrape_data(data):
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
            print('processing url: ', data[0])
            response = client.get(data[0], params=params, timeout=120)
            soup = BeautifulSoup(response.text, 'lxml')
            keys = [i.text for i in soup.select("div.b-advert-attribute__key")]
            values = [i.text for i in soup.select("div.b-advert-attribute__value")]

            key_value_map = dict(zip(keys, values))

            propertyType = key_value_map.get("Property Type", None) or key_value_map.get("Type", None)
            constructionStatus = key_value_map.get("Status of Construction", None)

            property_size_value = key_value_map.get("Property Size") or key_value_map.get("Square Metres") or key_value_map.get("Land Area")
            if property_size_value:
                size_match = re.match(r"([\d.]+)([a-zA-Z]+)", property_size_value)
                size = size_match.group(1) if size_match else None
                sizeUnit = size_match.group(2).strip() if size_match else None
            else:
                size = None
                sizeUnit = None

            beds = key_value_map.get("Number of Bedrooms", None)
            baths = key_value_map.get("Number of Bathrooms", None)
            toilets = key_value_map.get("Toilets", None)
            parking = key_value_map.get("Parking Spaces", None) or key_value_map.get("Number of Cars", None)
            address = key_value_map.get("Address", None) or key_value_map.get("Property Address", None)
            # amenities = key_value_map.get("Facilities", "").split(", ") if key_value_map.get("Facilities") else []
            amenities = (
                key_value_map.get("Facilities", "").split(", ") 
                if key_value_map.get("Facilities") 
                else [
                    amenity.text.strip() 
                    for amenity in soup.select("div.b-advert-attributes__tag") 
                    if amenity and amenity.text
                ]
            )
            listingType = 'Rent' if '-for-rent' in data[0] else 'Sale'

            if (len(soup.select("div.b-advert-icon-attribute__image")) == 4):

                propertyType = soup.select("div.b-advert-icon-attribute__image")[0].find_next_sibling().text

                bed_number_match = re.match(r'\d+', soup.select("div.b-advert-icon-attribute__image")[1].find_next_sibling().text)
                beds = bed_number_match.group() if bed_number_match else None

                bath_number_match = re.match(r'\d+', soup.select("div.b-advert-icon-attribute__image")[2].find_next_sibling().text)
                baths = bath_number_match.group() if bath_number_match else None

            if (len(soup.select("div.b-advert-icon-attribute__image")) == 3):

                propertyType = soup.select("div.b-advert-icon-attribute__image")[0].find_next_sibling().text

                bed_number_match = re.match(r'\d+', soup.select("div.b-advert-icon-attribute__image")[1].find_next_sibling().text)
                beds = bed_number_match.group() if bed_number_match else None

                bath_number_match = re.match(r'\d+', soup.select("div.b-advert-icon-attribute__image")[2].find_next_sibling().text)
                baths = bath_number_match.group() if bath_number_match else None

            if (len(soup.select("div.b-advert-icon-attribute__image")) == 2):
                bed_number_match = re.match(r'\d+', soup.select("div.b-advert-icon-attribute__image")[0].find_next_sibling().text)
                beds = bed_number_match.group() if bed_number_match else None

                bath_number_match = re.match(r'\d+', soup.select("div.b-advert-icon-attribute__image")[1].find_next_sibling().text)
                baths = bath_number_match.group() if bath_number_match else None


            description = soup.select_one("span.qa-description-text").text if soup.select_one("span.qa-description-text") else None
            price = float(soup.select_one('div[itemprop="price"]')['content']) if soup.select_one('div[itemprop="price"]') else None
            currency = soup.select_one('meta[itemprop="priceCurrency"]')['content'] if soup.select_one('meta[itemprop="priceCurrency"]') else None
            pricingCriteria = soup.select_one('span.b-alt-advert-price__period').text if soup.select_one('span.b-alt-advert-price__period') else None
            
            imgUrls = [i.select_one('img')['src'] for i in soup.select("source[type='image/webp']")] if soup.select("source[type='image/webp']") else []
            agent = soup.select_one("div.b-seller-block__name").text if soup.select_one("div.b-seller-block__name") else None
            agentNumber = soup.find('input', {'id': 'fullPhoneNumbers'})['value'] if (soup.find('input', {'id': 'fullPhoneNumbers'})) else data[4]
            propertyTitle = data[6]
            propertyId = data[5]

            priceStatus, priceDiff, priceChange = None, None, None
            if price:
                oldPrice = data[1] if data else None
                priceDiff = max(oldPrice, price) - min(oldPrice, price) if oldPrice else 0
                priceChange = True if (priceDiff > 0) else False
                if price != oldPrice:
                    priceStatus = 'increased' if (price > oldPrice) else 'decreased'
                else:
                    priceStatus = None

            thread_results[thread_id].append([data[0], propertyTitle, propertyId, amenities, propertyType, constructionStatus, beds, baths, toilets, parking, description, imgUrls, agent, agentNumber, size, sizeUnit, address, price, currency, pricingCriteria, priceDiff, priceChange, priceStatus, listingType, data[2], data[3]])
            return 

        except (requests.exceptions.Timeout, requests.exceptions.SSLError):
            log.info("Timeout error occurred. Retrying in {} seconds...".format(delay))
            retries -= 1
            time.sleep(delay)
        except Exception as e:
            retries -= 1
            log.info(f"Failed to scrape data for {data[0]}: {e}")

    log.info(f"Max retries reached. Could not scrape {data[0]}")
    return

def getData():
    log.info('Fetching stored URLs...')
    client = MongoClient(CONNECTION_STRING_MONGODB)
    db = client['jiji_co_ke']
    collection = db['propertyURLs']
    data = collection.find()
    return list(data)


columns = ['url', 'propertyTitle', 'propertyId', 'amenities', 'propertyType', 'constructionStatus', 'beds', 'baths', 'toilets', 'parking', 'description', 'imgUrls', 'agent', 'agentNumber', 'size', 'sizeUnit', 'address', 'price', 'currency', 'pricingCriteria', 'priceDiff', 'priceChange', 'priceStatus', 'listingType', 'city', 'neighborhood']
databaseName = 'jiji_co_ke'

if __name__ == '__main__':
    
    datas = getData()
    urls_data = [[
        data.get('url', '').strip(), 
        data.get('price', ''), 
        data.get('city', ''),
        data.get('neighborhood', ''), 
        data.get('user_phone', ''), 
        data.get('propertyId', ''), 
        data.get('title', '')
    
    ] for data in datas]
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        executor.map(scrape_data, urls_data)
    for thread_id, result_list in thread_results.items():
        sendData(result_list, columns, databaseName, 'propertyDetails')

    log.info("Details extraction completed successfully.")
    s3 = boto3.client("s3", region_name=aws_region_name)
    s3.put_object(Body=log_stringio.getvalue(), Bucket=bucket_name, Key="logs/jiji_co_ke/detail-extractor-logs.txt")
    log.info("Logs transferred to s3 completed")
