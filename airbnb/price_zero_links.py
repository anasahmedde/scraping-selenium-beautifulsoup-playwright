from pymongo import MongoClient
import json

CONNECTION_STRING = "mongodb://rehani-app:jha7657JKAHS_771huq@mongo-atlas-prod-public.rehanisoko-internal.com:27017/?authMechanism=SCRAM-SHA-256&directConnection=true"  # Put your connection string here
client = MongoClient(CONNECTION_STRING)
db = client['airbnb']
collection = db['propertyDetails']

docs_with_price_zero = collection.find(
    {"price": 0},
    {"url": 1, "propertyId": 1, "newEntry": 1, "city": 1, "discountedPrice": 1,
     "currency": 1, "pricingCriteria": 1, "isSuperhost": 1, "_id": 0}
)

links = []
for doc in docs_with_price_zero:
    links.append([
        doc.get("url"),
        doc.get("propertyId"),
        doc.get("newEntry", None),
        doc.get("city", None),
        doc.get("price", 0),
        doc.get("discountedPrice", None),
        doc.get("currency", None),
        doc.get("pricingCriteria", None),
        doc.get("isSuperhost", None)
    ])

with open("price_zero_links.json", "w") as f:
    json.dump(links, f, indent=2)

print(f"Saved {len(links)} records to price_zero_links.json")
