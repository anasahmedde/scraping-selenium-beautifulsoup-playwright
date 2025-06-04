from pymongo import MongoClient
from dotenv import load_dotenv
import os

# Load .env file
load_dotenv(override=True)

CONNECTION_STRING_MONGODB = os.environ.get("CONNECTION_STRING")

def get_trading_data():
    client = MongoClient(CONNECTION_STRING_MONGODB)
    db = client['economicIndicators']
    collection1 = db['countries']
    collection2 = db['cities']
    data1 = collection1.find()
    data2 = collection2.find()
    return list(data1), list(data2)

def getCalcValue(feature, df):
    return [countryElem(item)[feature] if countryElem(item) is not None else None for item in df['Location: Country'].values]

def countryElem(countryName):
    return next((i for i in countriesDb if i['countryName'] == countryName), None)

def cityElem(cityName):
    return next((i for i in citiesDb if i['cityName'] == cityName), None)

countriesDb, citiesDb = get_trading_data()