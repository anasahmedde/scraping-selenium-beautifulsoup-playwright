from pymongo import MongoClient
import pandas as pd
from dotenv import load_dotenv
import os

# Load .env file
load_dotenv(override=True)

CONNECTION_STRING = os.environ.get('CONNECTION_STRING')
DATABASE_NAME = 'rehaniAI'
COLLECTION_NAME = 'properties'

def get_data():
    client = MongoClient(CONNECTION_STRING)
    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]
    
    query = {
        "$and": [
            { "housingType": { "$regex": "land|apartment|condo", "$options": "i" } },
            { "internalArea": { "$in": [None, ""] } }
        ]
    }

    projection = {
        "_id": 0,
        "description_embedding": 0,
        "embedding": 0,
    }

    # data_mongo = list(collection.find(query, projection))

    # return pd.DataFrame(data_mongo) if data_mongo else pd.DataFrame()


    pipeline = [
        { "$match": query },
        { "$sample": { "size": 50 } },  # Get 100 random documents
        { "$project": projection }
    ]

    data_mongo = list(collection.aggregate(pipeline))

    return pd.DataFrame(data_mongo) if data_mongo else pd.DataFrame()
