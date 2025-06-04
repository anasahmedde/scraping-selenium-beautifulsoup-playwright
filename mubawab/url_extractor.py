import concurrent.futures
from bs4 import BeautifulSoup
import os, io, logging, boto3, warnings, requests, re
from pymongo import MongoClient
from dotenv import load_dotenv
import pandas as pd
from pymongo import MongoClient

warnings.filterwarnings("ignore", category=DeprecationWarning) 
load_dotenv(override=True)

CONNECTION_STRING_MONGODB = os.getenv("CONNECTION_STRING")
aws_region_name = os.getenv("aws_region_name")
bucket_name = os.getenv("bucket_name")
threads = int(os.getenv("threads"))

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',datefmt='%Y-%m-%d %H:%M:%S')

log = logging.getLogger("mubawab-url-extractor")
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
        for index,instance in enumerate(mongo_insert_data):
            collection_name.update_one({'propertyId':instance['propertyId']},{'$set':instance},upsert=True)
        log.info('Data sent to MongoDB successfully')
    except Exception as e:
        log.info('Some error occurred while sending data MongoDB! Following is the error.')
        log.error(e)

        
def getExcel_links(url):
    response = requests.get(url, timeout=60)
    soup = BeautifulSoup(response.text, 'lxml')
    records = int(re.search(r"[\d]+", soup.select_one('span.resultNum.floatR').text).group())
    if records != 0:
        for page in range(1, (records//33)+1 ):
            response = requests.get(url + f":p:{page}", timeout=60)
            soup = BeautifulSoup(response.text, 'lxml')
            log.info(url + f":p:{page}")
            if soup.select_one('h3.red'):
                if "Sorry! No results found" in soup.select_one('h3.red').text.strip().split('\n')[0]:
                    break

            urls = [prop['linkref'] for prop in soup.select("li.listingBox")]
            prop_ids = [propId.split('/')[-2] for propId in urls]
            prices = [float(re.search(r"[\d,]+", price.text).group().replace(",", "")) if 'Price on request' not in price.text.strip() else None for price in soup.select("span.priceTag")]
            currencies = [curr.text.strip().split(' ')[-1] if 'Price on request' not in curr.text.strip() else None for curr in soup.select("span.priceTag")]
            pricingCriteria = [criteria.find("em").text if criteria.find("em") else None for criteria in soup.select("span.priceTag")]
            cities = [url.split('/')[-2].replace('é', 'e').capitalize() for i in range(len(urls))]
            housingType = [url.split('/')[-1].replace('-for-sale', '').replace('-for-rent', '').replace('-', ' ').capitalize() for i in range(len(urls))]
            links.extend([[link, ids, price, currency, pricingCriteria, cities, housingType] for link, ids, price, currency, pricingCriteria, cities, housingType in zip(urls, prop_ids, prices, currencies, pricingCriteria, cities, housingType)])



excelLinks = ['https://www.mubawab.ma/en/st/casablanca/apartments-for-sale',
 'https://www.mubawab.ma/en/st/marrakech/apartments-for-sale',
 'https://www.mubawab.ma/en/st/tanger/apartments-for-sale',
 'https://www.mubawab.ma/en/st/rabat/apartments-for-sale',
 'https://www.mubawab.ma/en/st/salé/apartments-for-sale',
 'https://www.mubawab.ma/en/st/agadir/apartments-for-sale',
 'https://www.mubawab.ma/en/st/fès/apartments-for-sale',
 'https://www.mubawab.ma/en/st/temara/apartments-for-sale',
 'https://www.mubawab.ma/en/st/kénitra/apartments-for-sale',
 'https://www.mubawab.ma/en/st/mohammédia/apartments-for-sale',
 'https://www.mubawab.ma/en/st/marrakech/land-for-sale',
 'https://www.mubawab.ma/en/st/casablanca/land-for-sale',
 'https://www.mubawab.ma/en/st/tanger/land-for-sale',
 'https://www.mubawab.ma/en/st/rabat/land-for-sale',
 'https://www.mubawab.ma/en/st/dar-bouazza/land-for-sale',
 'https://www.mubawab.ma/en/st/bouskoura/land-for-sale',
 'https://www.mubawab.ma/en/st/agadir/land-for-sale',
 'https://www.mubawab.ma/en/st/fès/land-for-sale',
 'https://www.mubawab.ma/en/st/meknes/land-for-sale',
 'https://www.mubawab.ma/en/st/salé/land-for-sale',
 'https://www.mubawab.ma/en/st/marrakech/villas-and-luxury-homes-for-sale',
 'https://www.mubawab.ma/en/st/casablanca/villas-and-luxury-homes-for-sale',
 'https://www.mubawab.ma/en/st/rabat/villas-and-luxury-homes-for-sale',
 'https://www.mubawab.ma/en/st/dar-bouazza/villas-and-luxury-homes-for-sale',
 'https://www.mubawab.ma/en/st/bouskoura/villas-and-luxury-homes-for-sale',
 'https://www.mubawab.ma/en/st/tanger/villas-and-luxury-homes-for-sale',
 'https://www.mubawab.ma/en/st/agadir/villas-and-luxury-homes-for-sale',
 'https://www.mubawab.ma/en/st/essaouira/villas-and-luxury-homes-for-sale',
 'https://www.mubawab.ma/en/st/temara/villas-and-luxury-homes-for-sale',
 'https://www.mubawab.ma/en/st/casablanca/houses-for-sale',
 'https://www.mubawab.ma/en/st/marrakech/houses-for-sale',
 'https://www.mubawab.ma/en/st/tanger/houses-for-sale',
 'https://www.mubawab.ma/en/st/agadir/houses-for-sale',
 'https://www.mubawab.ma/en/st/salé/houses-for-sale',
 'https://www.mubawab.ma/en/st/oujda/houses-for-sale',
 'https://www.mubawab.ma/en/st/rabat/houses-for-sale',
 'https://www.mubawab.ma/en/st/meknes/houses-for-sale',
 'https://www.mubawab.ma/en/st/casablanca/commercial-property-for-sale',
 'https://www.mubawab.ma/en/st/marrakech/commercial-property-for-sale',
 'https://www.mubawab.ma/en/st/rabat/commercial-property-for-sale',
 'https://www.mubawab.ma/en/st/tanger/commercial-property-for-sale',
 'https://www.mubawab.ma/en/st/fès/commercial-property-for-sale',
 'https://www.mubawab.ma/en/st/agadir/commercial-property-for-sale',
 'https://www.mubawab.ma/en/st/marrakech/riads-for-sale',
 'https://www.mubawab.ma/en/st/casablanca/offices-for-sale',
 'https://www.mubawab.ma/en/st/marrakech/offices-for-sale',
 'https://www.mubawab.ma/en/st/casablanca/apartments-for-rent',
 'https://www.mubawab.ma/en/st/marrakech/apartments-for-rent',
 'https://www.mubawab.ma/en/st/rabat/apartments-for-rent',
 'https://www.mubawab.ma/en/st/tanger/apartments-for-rent',
 'https://www.mubawab.ma/en/st/agadir/apartments-for-rent',
 'https://www.mubawab.ma/en/st/bouskoura/apartments-for-rent',
 'https://www.mubawab.ma/en/st/dar-bouazza/apartments-for-rent',
 'https://www.mubawab.ma/en/st/kénitra/apartments-for-rent',
 'https://www.mubawab.ma/en/st/temara/apartments-for-rent',
 'https://www.mubawab.ma/en/st/mohammédia/apartments-for-rent',
 'https://www.mubawab.ma/en/st/casablanca/villas-and-luxury-homes-for-rent',
 'https://www.mubawab.ma/en/st/rabat/villas-and-luxury-homes-for-rent',
 'https://www.mubawab.ma/en/st/marrakech/villas-and-luxury-homes-for-rent',
 'https://www.mubawab.ma/en/st/bouskoura/villas-and-luxury-homes-for-rent',
 'https://www.mubawab.ma/en/st/dar-bouazza/villas-and-luxury-homes-for-rent',
 'https://www.mubawab.ma/en/st/tanger/villas-and-luxury-homes-for-rent',
 'https://www.mubawab.ma/en/st/casablanca/office-for-rent',
 'https://www.mubawab.ma/en/st/rabat/office-for-rent',
 'https://www.mubawab.ma/en/st/marrakech/office-for-rent',
 'https://www.mubawab.ma/en/st/tanger/office-for-rent',
 'https://www.mubawab.ma/en/st/casablanca/commercial-property-for-rent',
 'https://www.mubawab.ma/en/st/marrakech/commercial-property-for-rent',
 'https://www.mubawab.ma/en/st/tanger/commercial-property-for-rent',
 'https://www.mubawab.ma/en/st/rabat/commercial-property-for-rent',
 'https://www.mubawab.ma/en/st/casablanca/rooms-for-rent',
 'https://www.mubawab.ma/en/st/marrakech/apartments-vacational',
 'https://www.mubawab.ma/en/st/tanger/apartments-vacational',
 'https://www.mubawab.ma/en/st/agadir/apartments-vacational',
 'https://www.mubawab.ma/en/st/martil/apartments-vacational',
 'https://www.mubawab.ma/en/st/casablanca/apartments-vacational',
 'https://www.mubawab.ma/en/st/asilah/apartments-vacational',
 'https://www.mubawab.ma/en/st/essaouira/apartments-vacational',
 'https://www.mubawab.ma/en/st/rabat/apartments-vacational',
 "https://www.mubawab.ma/en/st/m'diq/apartments-vacational",
 'https://www.mubawab.ma/en/st/meknes/apartments-vacational',
 'https://www.mubawab.ma/en/st/marrakech/villas-and-luxury-homes-vacational']

databaseName = 'mubawab'

links = []
log.info('Gathering property links !')
with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
    futures, links = [], []
    for excelLink in excelLinks:
        futures.append(executor.submit(getExcel_links, excelLink))
    concurrent.futures.wait(futures)

sendData(links, ['url', 'propertyId', 'price', 'currency', 'pricingCriteria', 'city', 'housingType'], databaseName, 'propertyURLs')
log.info("URL extraction completed successfully.")
s3 = boto3.client("s3", region_name=aws_region_name)
s3.put_object(Body=log_stringio.getvalue(), Bucket=bucket_name, Key="logs/mubawab/url-extractor-logs.txt")
log.info("Logs transferred to s3 completed")
