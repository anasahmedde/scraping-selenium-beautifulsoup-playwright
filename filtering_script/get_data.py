from pymongo import MongoClient
from dotenv import load_dotenv
import os

# Load .env file
load_dotenv(override=True)

CONNECTION_STRING = os.environ.get('CONNECTION_STRING')

def get_data(databaseName):
    
    client = MongoClient(CONNECTION_STRING)
    return client[databaseName]