from pymongo import MongoClient
import pandas as pd


MONGO_URI = "MONGO_URI"
DATABASE_NAME = "rehaniAI"
COLLECTION_NAME = "listings"


def get_filtered_records():
    client = MongoClient(MONGO_URI)
    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]

    query = {
        "$or": [
            {"amenities": {"$ne": []}},
            {"amenities": None}
        ]
    }
    projection = {
        "_id": 0,
        "rehaniId": 1,
        "amenities": 1,
        "consolidatedCountry": 1,
        "price": 1,
        "housingType": 1,
        "type": 1
    }

    # Fetch filtered records from the collection
    records = collection.find(query, projection)

    # Convert the cursor to a list (optional)
    filtered_records = list(records)

    # Close the MongoDB connection
    client.close()

    return filtered_records

records = get_filtered_records()


df = pd.DataFrame(records)
df = df.dropna(subset=['amenities'])
df = df[df['amenities'].str.strip().astype(bool)]
df.to_csv('amenities.csv', index=False)

