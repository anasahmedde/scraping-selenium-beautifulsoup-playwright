import os, io, logging, boto3, warnings, requests
from pymongo import MongoClient
from dotenv import load_dotenv
import pandas as pd
from pymongo import MongoClient
from bs4 import BeautifulSoup

warnings.filterwarnings("ignore", category=DeprecationWarning) 
load_dotenv(override=True)

CONNECTION_STRING_MONGODB = os.getenv("CONNECTION_STRING")
aws_region_name = os.getenv("aws_region_name")
bucket_name = os.getenv("bucket_name")

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',datefmt='%Y-%m-%d %H:%M:%S')

log = logging.getLogger("seso-detail-extractor")
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
        log.info('Some error occured while sending data MongoDB! Following is the error.')
        log.error(e)

def continous_connection():
    clientC = MongoClient(CONNECTION_STRING_MONGODB)
    db = clientC['SeSo']
    return db['propertyURLs']

        
headers = {
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'en-GB,en-PK;q=0.9,en-US;q=0.8,en;q=0.7',
    'cache-control': 'no-cache',
    'pragma': 'no-cache',
    'sec-ch-ua': '"Google Chrome";v="111", "Not(A:Brand";v="8", "Chromium";v="111"',
    'sec-ch-ua-mobile': '?1',
    'sec-ch-ua-platform': '"Android"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-site'
}
# url = 'https://back.seso.global/property/search?countryId=83&countryId=160&location=1&location=6&location=4&location=21&location=20&location=19&location=17&location=22&location=2&location=3&location=16&location=5&suburbId=903&suburbId=22504&suburbId=7609&suburbId=895&suburbId=1144&suburbId=999&suburbId=23961&suburbId=26209&suburbId=1189&suburbId=2&suburbId=29092&suburbId=7032&suburbId=26210&suburbId=34328&suburbId=20755&suburbId=884&suburbId=7677&suburbId=1219&suburbId=6276&suburbId=1018&suburbId=32546&suburbId=742&suburbId=962&suburbId=34327&suburbId=34207&suburbId=26208&suburbId=34330&suburbId=33687&suburbId=29296&suburbId=30070&suburbId=26203&suburbId=5&suburbId=22339&suburbId=1221&suburbId=1265&suburbId=7654&suburbId=1266&suburbId=6977&suburbId=9&suburbId=10&suburbId=795&suburbId=34931&suburbId=33686&suburbId=21879&suburbId=6277&suburbId=852&suburbId=24159&suburbId=34326&suburbId=7331&suburbId=1190&suburbId=6284&suburbId=34331&suburbId=22335&suburbId=25337&suburbId=6976&suburbId=20754&suburbId=21367&suburbId=25341&suburbId=7138&suburbId=33806&suburbId=13'

url = 'https://back.seso.global/property/publish?&size=1000'

columns = ['url', 'propertyName', 'propertyId', 'area', 'currency', 'address', 'beds', 'baths', 'listingType', 'pricingCriteria', 'features', 'imgUrls', 'unitsAvailable', 'description', 'propertyStatus', 'price', 'priceStatus', 'priceDiff', 'priceChange', 'country', 'city', 'longitude', 'latitude', 'dateAdded', 'usdPrice']
databaseName = 'SeSo'

if __name__ == '__main__':

    log.info("Gathering property links!")
    response = requests.get(url, headers=headers)
    data = response.json()
    
    all_data = []
    
    singleItem = continous_connection()
    log.info("Extracting Details!")
    for prop in data['data']['properties']:
        propertyId = prop['id']
        url = f"https://app.seso.global/property-details/{propertyId}"
        propertyName = prop['propertyName']
        area = prop['propertySize']+' sqm' if prop['propertySize'] else None

        price = prop['propertyPrice']
        priceStatus, priceDiff, priceChange = None, None, None
        data = singleItem.find_one({"propertyId": propertyId})
        if price and data:
            oldPrice = data['price'] if data else None
            priceDiff = max(oldPrice, price) - min(oldPrice, price) if oldPrice else 0
            priceChange = True if (priceDiff > 0) else False
            if price != oldPrice:
                priceStatus = 'increased' if (price > oldPrice) else 'decreased'
            else:
                priceStatus = None

        currency = prop['currency']['currencyInitials']
        address = prop['address'].strip()
        beds = prop['propertiesFeatures']['numberOfBedrooms'] if prop['propertiesFeatures'] else None
        baths = prop['propertiesFeatures']['numberOfBathrooms'] if prop['propertiesFeatures'] else None
        listingType = prop['tag']['name']
        pricingCriteria = 'Month' if 'rent' in listingType else None
        keyFeatures = prop['propertiesFeatures']['keyFeatures'] if prop['propertiesFeatures'] else None
        additionalFeatures = prop['propertiesFeatures']['additionalFeatures'] if prop['propertiesFeatures'] else None
        features = None
        if (keyFeatures and additionalFeatures):
            features = [i.text.strip() for i in BeautifulSoup(keyFeatures, features="lxml").find_all('li')]
            features += [i.text.strip() for i in BeautifulSoup(additionalFeatures, features="lxml").find_all('li')]

        imgUrls = [img['imgUrl'] for img in prop['propertyImages']]
        unitsAvailable = prop['unitsAvailable']
        description = prop['propertyDescription']
        propertyStatus = prop['sesoPropertyType']['propertyTypeName']
        country = prop['country']['countryName']
        city = prop['city']['cityName']
        longitude = prop['longitude']
        latitude = prop['latitude']
        dateAdded = prop['createdOn']
        usdPrice = prop['usdPrice']

        all_data.append([url, propertyName, propertyId, area, currency, address, beds, baths, listingType, pricingCriteria, features, imgUrls, unitsAvailable, description, propertyStatus, price, priceStatus, priceDiff, priceChange, country, city, longitude, latitude, dateAdded, usdPrice])
        
    sendData(all_data, columns, databaseName, 'propertyDetails')
    log.info("Details extraction completed successfully.")
    s3 = boto3.client("s3", region_name=aws_region_name)
    s3.put_object(Body=log_stringio.getvalue(), Bucket=bucket_name, Key="logs/seso/detail-extractor-logs.txt")  
    log.info("Logs transfered to s3 completed")