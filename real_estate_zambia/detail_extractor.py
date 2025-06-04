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

log = logging.getLogger("real-estate-zambia-detail-extractor")
log_stringio = io.StringIO()
handler = logging.StreamHandler(log_stringio)
handler.setFormatter(formatter)
log.addHandler(handler)

columnsURLs = ['url','propertyId','localPrice','propertyType','location','country','listingType','currency']
databaseName='real_estate_zambia'
collectionNameURLs='propertyURLs'
collectionNameDetails='propertyDetails'


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

columnsDetails = ['propertyId', 'country', 'localCurrency', 'propertyType', 'localPrice', 'location', 'housingType', 'url', 'propertyTitle', 'beds', 'baths', 'internalArea', 'internalAreaUnit','yearBuilt', 'agentContactPhone','agentContactMobile', 'agent', 'imgsUrls','city' ,'county', 'neighborhood', 'postalCode', 'description', 'dateAdded', 'availability', 'propertySize', 'propertySizeUnit', 'agentURL', 'agentEmail', 'googleMapsURL']
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}

agents_dict = {}

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

    retries = 5
    delay = 10
    while retries > 0:
        try:
            response = requests.get(url, timeout=180, headers=headers)
            if response.status_code == 403:
                print('Website blocked our IP for few minutes.')
                time.sleep(300)

                retries = retries -1
                continue
            tree = html.fromstring(response.content)
            propertyTitle = tree.xpath("//div[contains(@class, 'page-title')]//h1/text()[1]")[0].strip()
            if tree.xpath("//div[@id='property-detail-wrap']//strong[contains(text(),'Bed')]/following-sibling::span/text()"):
                try:
                    beds = float(tree.xpath("//div[@id='property-detail-wrap']//strong[contains(text(),'Bed')]/following-sibling::span/text()")[0].strip())
                except:
                    beds = tree.xpath("//div[@id='property-detail-wrap']//strong[contains(text(),'Bed')]/following-sibling::span/text()")[0]               
            else:
                beds = None
                
            if tree.xpath("//div[@id='property-detail-wrap']//strong[contains(text(),'Bath')]/following-sibling::span/text()"):
                try:
                    baths = float(tree.xpath("//div[@id='property-detail-wrap']//strong[contains(text(),'Bath')]/following-sibling::span/text()")[0].strip())
                except:
                    baths = tree.xpath("//div[@id='property-detail-wrap']//strong[contains(text(),'Bath')]/following-sibling::span/text()")[0].strip()
            else:
                baths = None
                
            if tree.xpath("//div[@id='property-detail-wrap']//strong[contains(text(),'Land Area')]/following-sibling::span/text()"):
                internalArea, internalAreaUnit = extract_internal_area(tree.xpath("//div[@id='property-detail-wrap']//strong[contains(text(),'Land Area')]/following-sibling::span/text()")[0])
            else:
                internalArea = None
                internalAreaUnit = None
            if tree.xpath("//div[@id='property-detail-wrap']//strong[contains(text(),'Property Size')]/following-sibling::span/text()"):
                propertySize, propertySizeUnit = extract_internal_area(tree.xpath("//div[@id='property-detail-wrap']//strong[contains(text(),'Property Size')]/following-sibling::span/text()")[0])
            else:
                propertySize = None
                propertySizeUnit = None
            if tree.xpath("//div[@id='property-detail-wrap']//strong[contains(text(),'Year Built')]/following-sibling::span/text()"):
                yearBuilt = tree.xpath("//div[@id='property-detail-wrap']//strong[contains(text(),'Year Built')]/following-sibling::span/text()")[0]
            else:
                yearBuilt = None
            if tree.xpath("//div[@id='property-detail-wrap']//strong[contains(text(),'Price')]/following-sibling::span/text()"):
                localPrice, localCurrency = extract_currency_price_list(tree.xpath("//div[@id='property-detail-wrap']//strong[contains(text(),'Price')]/following-sibling::span/text()")[0])
                localPrice = float(localPrice)
            else:
                pass
            imgsUrls = tree.xpath("//div[@class='lslide']/@data-thumb")
            if tree.xpath("//i[contains(@class,'icon-phone')]/following-sibling::span[1]/a/text()"):
                agentContactPhone = tree.xpath("//i[contains(@class,'icon-phone')]/following-sibling::span[1]/a/text()")[0]
            else:
                agentContactPhone = None
            if tree.xpath("//i[contains(@class,'icon-mobile-phone')]/following-sibling::span[1]/a/text()"):
                agentContactMobile = tree.xpath("//i[contains(@class,'icon-mobile-phone')]/following-sibling::span[1]/a/text()")[0]
            else:
                agentContactMobile = None
            if tree.xpath("(//ul[contains(@class,'agent-information')]//li[contains(@class, 'agent-name')])[1]/text()"):
                agent = tree.xpath("(//ul[contains(@class,'agent-information')]//li[contains(@class, 'agent-name')])[1]/text()")[0]
            else:
                agent = None

            if tree.xpath("//div[contains(@class, 'agent-image')]/a/@href"):
                try:
                    agentURL = tree.xpath("//div[contains(@class, 'agent-image')]/a/@href")[0]
                    agentURLresp = requests.get(agentURL, timeout=60, headers= headers)
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
                except Exception as e :
                    agentEmail = None

            else:
                agentURL = None
                agentEmail = None

            if tree.xpath("//div[@id='property-address-wrap']//a/@href"):
                googleMapsURL = tree.xpath("//div[@id='property-address-wrap']//a/@href")[0]
            else:
                googleMapsURL = None
        
            if tree.xpath("//li[contains(@class,'detail-state')]/span/text()"):
                county = tree.xpath("//li[contains(@class,'detail-state')]/span/text()")[0].strip()
            else:
                county = None
            if tree.xpath("//li[contains(@class,'detail-city')]/span/text()"):
                city = tree.xpath("//li[contains(@class,'detail-city')]/span/text()")[0].strip()
            else:
                city = None
            if tree.xpath("//li[contains(@class,'detail-area')]/span/text()"):
                neighborhood = tree.xpath("//li[contains(@class,'detail-area')]/span/text()")[0].strip()
            else:
                neighborhood = None
            if tree.xpath("//li[contains(@class,'detail-zip')]/span/text()"):
                postalCode = tree.xpath("//li[contains(@class,'detail-zip')]/span/text()")[0].strip()
            else:
                postalCode = None
            if tree.xpath("//div[@id='property-description-wrap']"):
                description = tree.xpath("//div[@id='property-description-wrap']")[0].text_content().strip().replace('Description','')
                description = ' '.join(description.split())
            else:
                description = None
            if tree.xpath("//div[contains(@class,'property-labels-wrap')]/a[contains(@href,'not-available')]"):
                availability = False
            else:
                availability = tree.xpath("//div[contains(@class,'property-labels-wrap')]/a/text()")[0].strip()
            if tree.xpath("//i[contains(@class,'icon-calendar-3')]/parent::span/text()"):
                dateAdded = tree.xpath("//i[contains(@class,'icon-calendar-3')]/parent::span/text()")[0]
            else:
                dateAdded = None
        except (requests.exceptions.Timeout, requests.exceptions.SSLError):
            retries -= 1
            time.sleep(delay)

        except Exception as e:
            print(f"Failed to scrape data for {url} : {e}")
            retries -= 1
        finally:
            try:
                thread_results[thread_id].append([propertyId, country, localCurrency, propertyType, localPrice, location, housingType, url, propertyTitle, beds, baths, internalArea, internalAreaUnit,yearBuilt, agentContactPhone,agentContactMobile, agent, imgsUrls,city ,county, neighborhood, postalCode, description, dateAdded, availability, propertySize, propertySizeUnit, agentURL, agentEmail, googleMapsURL])
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
s3.put_object(Body=log_stringio.getvalue(), Bucket=bucket_name, Key="logs/real-estate-zambia/detail-extractor-logs.txt")
log.info("Logs transfered to s3 completed") 
