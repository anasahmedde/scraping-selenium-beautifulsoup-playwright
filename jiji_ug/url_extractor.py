import os, io, logging, boto3
import time
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
from zenrows import ZenRowsClient

# Load environment variables
load_dotenv(override=True)
CONNECTION_STRING_MONGODB = os.getenv("CONNECTION_STRING")
aws_region_name = os.getenv("aws_region_name")
bucket_name = os.getenv("bucket_name")
zenRowsApiKey = os.getenv("ZENROWS_API_KEY")
threads = int(os.getenv("threads"))
databaseName = 'jiji_ug'

# Configure logging

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',datefmt='%Y-%m-%d %H:%M:%S')

log = logging.getLogger("jiji-ug-url-extractor")
log_stringio = io.StringIO()
handler = logging.StreamHandler(log_stringio)
handler.setFormatter(formatter)
log.addHandler(handler)


# ZenRows client
client = ZenRowsClient(zenRowsApiKey)
params = {"js_render":"true"}

def sendData(data, databaseName, collectionName, batch_size=200):
    try:
        client = MongoClient(CONNECTION_STRING_MONGODB)
        db = client[databaseName]
        collection = db[collectionName]

        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            operations = [
                UpdateOne({'propertyId': record['propertyId']}, {'$set': record}, upsert=True)
                for record in batch
            ]
            if operations:
                collection.bulk_write(operations)
                logging.info(f"Sent {len(batch)} records to MongoDB.")
    except Exception as e:
        logging.error(f"Error sending data to MongoDB: {e}")

def process_adverts(data):
    return [
        {
            'url': f"https://jiji.ug{i.get('url')}",
            'propertyId': i.get('id'),
            'price': i.get('price_obj').get('value'),
            'user_phone': i.get('user_phone'),
            'title': i.get('title'),
            'city': i.get('region_parent_name'),
            'neighborhood': i.get('region_name'),
        }
        for i in data
    ]

def fetch_with_retries(url, params, retries=3, timeout=120):
    for attempt in range(retries):
        try:
            response = client.get(url, params=params, timeout=timeout)
            return response.json()
        except Exception as e:
            log.warning(f"Error fetching URL: {url}, attempt {attempt + 1}/{retries}: {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
    log.error(f"Failed to fetch URL: {url} after {retries} attempts.")
    return None

def fetch_data(slug):
    all_data = []
    url = f"https://jiji.ug/api_web/v1/listing?slug={slug}"
    print('Processing url:', url)
    # Initial fetch
    response = fetch_with_retries(url, params=params)
    if response:
        data = response.get('adverts_list', {}).get('adverts', [])
        all_data = process_adverts(data)
        # Handle pagination with retries
        while response.get('next_url', None):
            next_url = response.get('next_url')
            print('Processing url:', next_url)
            response = fetch_with_retries(next_url, params=params)
            if response:
                data = response.get('adverts_list', {}).get('adverts', [])
                all_data += process_adverts(data)
            else:
                log.error(f"Failed to fetch paginated data for slug {slug}. Skipping remaining pages.")
                break  # Exit pagination loop if retries fail

    return all_data

allSlugs = ['new-builds', 'event-centers-and-venues', 'land-and-plots-for-rent', 'commercial-properties', 'commercial-property-for-rent', 'temporary-and-vacation-rentals', 'houses-apartments-for-sale', 'houses-apartments-for-rent', 'land-and-plots-for-sale']

log.info('Gathering property links !')
with ThreadPoolExecutor(max_workers=threads) as executor:
    results = executor.map(fetch_data, allSlugs)
    for result in results:
        sendData(result, databaseName, 'propertyURLs')

    log.info("URL extraction completed successfully.")
    s3 = boto3.client("s3", region_name=aws_region_name)
    s3.put_object(Body=log_stringio.getvalue(), Bucket=bucket_name, Key="logs/jiji_ug/url-extractor-logs.txt")
    log.info("Logs transferred to s3 completed")
