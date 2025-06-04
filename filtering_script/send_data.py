from pymongo import MongoClient, UpdateOne
from dotenv import load_dotenv
import numpy as np
import os

# Load .env file
load_dotenv(override=True)

CONNECTION_STRING = os.getenv("CONNECTION_STRING")

def send_data(df, databaseName, collectionName, log):
    try:
        log.info(f'Collected {len(df)} records!\n')
        log.info("Sending data to MongoDB!")
        df.replace({np.nan: None}, inplace=True)
        mongo_insert_data = df.to_dict('records')

        client = MongoClient(CONNECTION_STRING)
        dbname = client[databaseName]
        collection_name = dbname[collectionName]
        
        bulk_updates = []
        for instance in mongo_insert_data:
            try:
                priceDiff = instance.get('priceDiff')
                newPriceChange = 0 if priceDiff is None or not isinstance(priceDiff, (int, float)) else priceDiff

                update_operation = {
                    '$set': {**instance},
                    '$inc': {'cumulativePriceChange': newPriceChange}
                }

                bulk_updates.append(
                    UpdateOne(
                        {'rehaniId': instance['rehaniId']},
                        update_operation,
                        upsert=True
                    )
                )

            except Exception as e:
                log.info(e, instance)

        # Perform bulk update
        if bulk_updates:
            collection_name.bulk_write(bulk_updates, ordered=False)

        log.info('Data sent to MongoDB successfully')

    except Exception as e:
        log.info(instance)
        log.info('Some error occurred while sending data to MongoDB! Following is the error.')
        log.info(e)
        log.info('-----------------------------------------')
