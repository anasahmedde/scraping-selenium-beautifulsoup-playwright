import concurrent.futures
import requests
import pandas as pd
from pymongo import MongoClient
from lxml import html
from dotenv import load_dotenv
import os, io, logging, boto3, warnings, requests, time
import threading, re

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

log = logging.getLogger("zambianhome-detail-extractor")
log_stringio = io.StringIO()
handler = logging.StreamHandler(log_stringio)
handler.setFormatter(formatter)
log.addHandler(handler)

columnsURLs = ['url','propertyId','localPrice','propertyType','location','country','listingType','currency']
databaseName='zambianhome'
collectionNameURLs='propertyURLs'
collectionNameDetails='propertyDetails'
agents_dict = {}

def sendData(data, columns, databaseName, collectionName):
    try:
        log.info(f'Collected {len(data)} records!')
        df = pd.DataFrame(data, columns=columns)
        mongo_insert_data = df.to_dict('records')
        log.info('Sending Data to MongoDB!')

        def get_database():
            client = MongoClient(CONNECTION_STRING_MONGODB)
            return client[databaseName]

        dbname = get_database()
        collection_name = dbname[collectionName]

        for index, instance in enumerate(mongo_insert_data):
            collection_name.update_one({'propertyId': instance['propertyId']}, {'$set': instance}, upsert=True)

        log.info('Data sent to MongoDB successfully')

    except Exception as e:
        log.info('Some error occurred while sending data to MongoDB! Following is the error.')
        log.info(e)
        log.info('-----------------------------------------')

def extract_internal_area(area_str):
    if 'Zambia' in area_str:
        area_str = area_str.replace('Zambia','')
    
    area_split = area_str.split()
    if area_split == []:
        return None, None
    if any(char.isdigit() for char in area_split[-1]):
        area_unit = None
        area = area_str
    else:
        area_unit = area_split[-1]
        area = area_str.replace(area_split[-1],'').strip()

    return area, area_unit
    
def extract_currency_price_list(price_str):

    if not any(char.isdigit() for char in price_str):
        return None, None
    else:
        match = re.search(r'([A-Za-z]+)\s*([\d,]+)', price_str)
        
        if match:
            currency = match.group(1)
            price = match.group(2).replace(',', '') 
        else:
            return None, None
    
    return price, currency
 
def getData(databaseName, collectionNameURLs):
    print("Fetching Stored URLs.")
    client = MongoClient(CONNECTION_STRING_MONGODB)
    db = client[databaseName]
    collection = db[collectionNameURLs]
    data = collection.find()
    return list(data)

def continuous_connection(databaseName, collectionNameURLs):
    client = MongoClient(CONNECTION_STRING_MONGODB)
    db = client[databaseName]
    return db[collectionNameURLs]

columnsDetails = ['propertyId', 'country', 'localCurrency', 'propertyType', 'localPrice', 'location', 'housingType', 'url', 'propertyTitle', 'beds', 'baths', 'internalArea', 'internalAreaUnit','yearBuilt', 'agentContactPhone','agentContactMobile', 'agent', 'amenities', 'imgsUrls','city' ,'county', 'neighborhood', 'postalCode', 'description', 'propertySize', 'propertySizeUnit', 'agentURL', 'agentEmail', 'googleMapsURL']

def scrape_data(item):
    thread_id = threading.get_ident()
    if thread_id not in thread_results:
        thread_results[thread_id]=[]    

    if len(thread_results[thread_id])==list_pool_size:
        sendData(thread_results[thread_id], columnsDetails, databaseName, collectionNameDetails)
        thread_results[thread_id]=[]

    propertyId = item[0]
    country = item[1]
    localCurrency = item[2]
    propertyType = item[3] #type    
    localPrice = item[4]
    location = item[5]
    housingType = item[6]    
    url = item[7]

    retries = 3
    delay = 10
    while retries > 0:
        try:
            response = requests.get(url, timeout=180)
            tree = html.fromstring(response.content)
            propertyTitle = tree.xpath("//div[contains(@class, 'header-detail')]//h1/text()[1]")[0].strip()
            if tree.xpath("//div[@id='detail']//strong[contains(text(),'Bed')]/parent::li/text()"):
                try:
                    beds = float(tree.xpath("//div[@id='detail']//strong[contains(text(),'Bed')]/parent::li/text()")[0].strip())
                except:
                    beds = tree.xpath("//div[@id='detail']//strong[contains(text(),'Bed')]/parent::li/text()")[0].strip()
            else:
                beds = None
                
            if tree.xpath("//div[@id='detail']//strong[contains(text(),'Bath')]/parent::li/text()"):
                try:
                    baths = float(tree.xpath("//div[@id='detail']//strong[contains(text(),'Bath')]/parent::li/text()")[0].strip())
                except:
                    baths = tree.xpath("//div[@id='detail']//strong[contains(text(),'Bath')]/parent::li/text()")[0].strip()
            else:
                baths = None
                
            if tree.xpath("//div[@id='detail']//strong[contains(text(),'Land Area')]/parent::li/text()"):
                internalArea, internalAreaUnit = extract_internal_area(tree.xpath("//div[@id='detail']//strong[contains(text(),'Land Area')]/parent::li/text()")[0])
            else:
                internalArea = None
                internalAreaUnit = None
            if tree.xpath("//div[@id='detail']//strong[contains(text(),'Property Size')]/parent::li/text()"):
                propertySize, propertySizeUnit = extract_internal_area(tree.xpath("//div[@id='detail']//strong[contains(text(),'Property Size')]/parent::li/text()")[0])
            else:
                propertySize = None
                propertySizeUnit = None
            if tree.xpath("//div[@id='detail']//strong[contains(text(),'Year Built')]/parent::li/text()"):
                yearBuilt = tree.xpath("//div[@id='detail']//strong[contains(text(),'Year Built')]/parent::li/text()")[0]
            else:
                yearBuilt = None
            if tree.xpath("//div[@id='detail']//strong[contains(text(),'Price')]/parent::li/text()"):
                localPrice, localCurrency = extract_currency_price_list(tree.xpath("//div[@id='detail']//strong[contains(text(),'Price')]/parent::li/text()")[0])
                try:
                    localPrice = float(localPrice)
                except:
                    pass
            else:
                pass
            imgsUrls = [re.search(r'url\((https?://.+?)\)', img).group(1) for img in tree.xpath("//div[contains(@style, 'background-image')]/@style")]
            if tree.xpath("//i[contains(@class,'fa-phone')]/parent::span"):
                agentContactPhone = tree.xpath("//i[contains(@class,'fa-phone')]/parent::span/text()")[0]
            else:
                agentContactPhone = None
            if tree.xpath("//i[contains(@class,'fa-mobile')]/parent::span"):
                agentContactMobile = tree.xpath("//i[contains(@class,'fa-mobile')]/parent::span/text()")[0]
            else:
                agentContactMobile = None
            if tree.xpath("//i[contains(@class,'fa-user')]/parent::dd/text()"):
                agent = tree.xpath("//i[contains(@class,'fa-user')]/parent::dd/text()")[0]
            else:
                agent = None

            if tree.xpath("//div[contains(@class, 'agent-media')]/div/a/@href"):
                agentURL = tree.xpath("//div[contains(@class, 'agent-media')]/div/a/@href")[0]
                agentURLresp = requests.get(agentURL, timeout=180)
                agentURLtree = html.fromstring(agentURLresp.content)
                if agentURL not in agents_dict:
                    if agentURLtree.xpath("//input[@id='target_email']/@value"):
                        agentEmail = agentURLtree.xpath("//input[@id='target_email']/@value")[0]
                        agents_dict[agentURL] = agentEmail
                    else:
                        agentEmail = None
                        agents_dict[agentURL] = agentEmail
                else:
                    agentEmail = agents_dict[agentURL]
            else:
                agentURL = None
                agentEmail = None

            if tree.xpath("//div[@id='address']//a/@href"):
                googleMapsURL = tree.xpath("//div[@id='address']//a/@href")[0]
            else:
                googleMapsURL = None
            
            amenities = tree.xpath("//div[@id='features']//li/a/text()")

            if tree.xpath("//li[contains(@class,'detail-state')]/text()"):
                county = tree.xpath("//li[contains(@class,'detail-state')]/text()")[0].strip()
            else:
                county = None
            if tree.xpath("//li[contains(@class,'detail-city')]/text()"):
                city = tree.xpath("//li[contains(@class,'detail-city')]/text()")[0].strip()
            else:
                city = None
            if tree.xpath("//li[contains(@class,'detail-area')]/text()"):
                neighborhood = tree.xpath("//li[contains(@class,'detail-area')]/text()")[0].strip()
            else:
                neighborhood = None
            if tree.xpath("//li[contains(@class,'detail-zip')]/text()"):
                postalCode = tree.xpath("//li[contains(@class,'detail-zip')]/text()")[0].strip()
            else:
                postalCode = None
            if tree.xpath("//div[@id='description']"):
                description = tree.xpath("//div[@id='description']")[0].text_content().strip().replace('Description','').replace('Facebook Google+ LinkedIn WhatsApp SMS Messenger', '')
                description = ' '.join(description.split())
            else:
                description = None

        except (requests.exceptions.Timeout, requests.exceptions.SSLError):
            retries -= 1
            time.sleep(delay)

        except Exception as e:
            print(f"Failed to scrape data for {url} : {e}")
            retries -= 1
        finally:
            try:
                thread_results[thread_id].append([propertyId, country, localCurrency, propertyType, localPrice, location, housingType, url, propertyTitle, beds, baths, internalArea, internalAreaUnit,yearBuilt, agentContactPhone,agentContactMobile, agent, amenities, imgsUrls,city ,county, neighborhood, postalCode, description, propertySize, propertySizeUnit, agentURL, agentEmail, googleMapsURL])
                print(url, localPrice)
                return
            except Exception as e:
                log.info(f'Error while scraping url: {url} --> {e}')
                return
    print(f"Max retries reached. Could not scrape {url}")
    return


raw_data_URLs = getData(databaseName, collectionNameURLs)
property_data_URLs = [[value for value in list(data.values())[1:]] for data in raw_data_URLs]

with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
    executor.map(scrape_data, property_data_URLs)

for thread_id, result_list in thread_results.items():
    sendData(result_list, columnsDetails, databaseName, collectionNameDetails)  

log.info("Details extraction completed successfully.")
s3 = boto3.client("s3", region_name=aws_region_name)
s3.put_object(Body=log_stringio.getvalue(), Bucket=bucket_name, Key="logs/zambianhome/detail-extractor-logs.txt")
log.info("Logs transfered to s3 completed") 
