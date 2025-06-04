from pymongo import MongoClient, UpdateOne
import json, os
from dotenv import load_dotenv

with open('get_changes/Classifier Out/categorized_amenities_summary.json') as f:
    json_data = json.load(f)

load_dotenv()
connection_string = os.getenv('CONNECTION_STRING')
db_name = os.getenv('MONGO_DB_NAME')
collection_name = os.getenv('MONGO_COLLECTION_NAME')

client = MongoClient(connection_string)
db = client[db_name]
collection = db[collection_name]

def fetch_documents():
    return collection.find(
        {'amenities': {'$ne': None, '$ne': []}},
        {'_id': 1, 'locationCountry': 1, 'type': 1, 'amenities': 1, 'housingType':1}
    )

def categorize_amenity(amenity, country, type_, housingType):
    amenityFound = False
    for main_category, countries in json_data.items():
        if country not in countries or type_ not in countries[country]:
            continue
        amenities_data = countries[country][type_]

        if housingType and housingType in amenities_data:
            categories = amenities_data[housingType]
            for category, amenities_list in categories.items():
                if amenity in amenities_list:
                    amenityFound = True
                    return f"{main_category} - {category}"                 
            
        if amenityFound==False:
            for housing_type, categories in amenities_data.items():
                for category, amenities_list in categories.items():
                    if amenity in amenities_list:
                        return f"{main_category} - {category}"            

    
    return None

def update_documents(batch_size=1000):
    documents = fetch_documents()
    operations = []
    total_modified_count = 0
    
    for doc in documents:
        country = doc.get('locationCountry')
        type_ = doc.get('type')
        housingType = doc.get('housingType')        
        amenities = doc.get('amenities', [])

        if not amenities or type(amenities)!=list:
            continue

        categorized_amenities = set()
        
        for amenity in amenities:
            formatted_category = categorize_amenity(amenity, country, type_, housingType)
            if formatted_category:
                categorized_amenities.add(formatted_category)
        
        if categorized_amenities:
            operations.append(
                UpdateOne(
                    {'_id': doc['_id']},
                    {'$set': {'amenities': list(categorized_amenities)}}
                )
            )
        
        if len(operations) == batch_size:
            results = collection.bulk_write(operations, ordered=False)
            modified_count = results.modified_count
            total_modified_count += modified_count       
            print(f"Currently batch operation, Modified {modified_count} documents.")
            operations = []
    
    if operations:
        results = collection.bulk_write(operations, ordered=False)
        modified_count = results.modified_count
        total_modified_count += modified_count       
        print(f"Currently batch operation, Modified {modified_count} documents.")
    print(f"Overall Modified {total_modified_count} documents.")


update_documents()
