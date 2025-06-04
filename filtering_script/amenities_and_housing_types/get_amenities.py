from pymongo import MongoClient
from dotenv import load_dotenv
import os
import pandas as pd

load_dotenv()
connection_string = os.getenv('CONNECTION_STRING')
db_name = os.getenv('MONGO_DB_NAME')
collection_name = os.getenv('MONGO_COLLECTION_NAME')

client = MongoClient(connection_string)
db = client[db_name]
collection = db[collection_name]

attributes = ["rehaniId", "amenities", "consolidatedCountry", "housingType", "price", "type"]

def fetch_and_store_documents(csv_filename):
    projection = {attr: 1 for attr in attributes}
    documents = collection.find({}, projection)
    df = pd.DataFrame(documents)
    df.to_csv(csv_filename, index=False, encoding='utf-8')


csv_filename = 'get_changes/Scrubber In/amenities.csv'
fetch_and_store_documents(csv_filename)
