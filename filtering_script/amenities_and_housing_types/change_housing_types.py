from pymongo import MongoClient, UpdateOne
import json, os
from pymongo import UpdateOne
from dotenv import load_dotenv

with open('get_changes/Classifier Out/support files/FinalMappingsGPTPlusCSV.json', 'r') as file:
    mapping = json.load(file)

def capitalize_words(s):
    return ' '.join([word.capitalize() for word in s.split()])

load_dotenv()
connection_string = os.getenv('CONNECTION_STRING')
db_name = os.getenv('MONGO_DB_NAME')
collection_name = os.getenv('MONGO_COLLECTION_NAME')

client = MongoClient(connection_string)
db = client[db_name]
collection = db[collection_name]

batch_size = 1000  
total_modified_count = 0

for old_value, new_value in mapping.items():
    old_value_capitalized = capitalize_words(old_value)
    new_value_capitalized = capitalize_words(new_value)
    bulk_updates = []
    
    cursor = collection.find({'housingType': {'$regex': f'^{old_value}$', '$options': 'i'}})
    for document in cursor:
        bulk_updates.append(
            UpdateOne(
                {'_id': document['_id']},
                {'$set': {'housingType': new_value_capitalized}}
            )
        )
        if len(bulk_updates) == batch_size:
            result = collection.bulk_write(bulk_updates, ordered=False)
            modified_count = result.modified_count
            total_modified_count += modified_count
            bulk_updates = []
            print(f"Modified {modified_count} documents for housingType '{old_value_capitalized}' to '{new_value_capitalized}'.")

    if bulk_updates:
        result = collection.bulk_write(bulk_updates, ordered=False)
        modified_count = result.modified_count
        total_modified_count += modified_count
        print(f"Modified {modified_count} documents for housingType '{old_value_capitalized}' to '{new_value_capitalized}'.")

print(f"Total documents modified: {total_modified_count}")
