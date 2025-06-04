import os
import sys
import json
import math
import io
import logging
import boto3
import warnings
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache
from datetime import datetime
from pymongo import MongoClient
from send_data import send_data
from add_more_calculations import add_more_calculations
from add_lat_long import add_lat_long
from add_embeddings import add_embeddings
from get_city_data import get_city_data
from dotenv import load_dotenv
import signal
import gc
import psutil

# Suppress deprecation warnings and load environment variables
warnings.filterwarnings("ignore", category=DeprecationWarning)
load_dotenv(override=True)

# AWS configuration
aws_region_name = os.getenv("aws_region_name")
bucket_name = os.getenv("bucket_name")

# Set up logging
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger("filtering-script")
log_stringio = io.StringIO()
handler = logging.StreamHandler(log_stringio)
handler.setFormatter(formatter)
log.addHandler(handler)

def graceful_shutdown(signum, frame):
    log.info(f"Received termination signal ({signum}). Initiating graceful shutdown.")
    try:
        s3 = boto3.client("s3", region_name=aws_region_name)
        s3.put_object(
            Body=log_stringio.getvalue(),
            Bucket=bucket_name,
            Key="logs/filtering-script-3/logs.txt",
            ACL="public-read"
        )
        log.info("Log file successfully uploaded to S3 during graceful shutdown.")
    except Exception as s3_error:
        log.error(f"Failed to upload log file to S3 during shutdown: {s3_error}", exc_info=True)
    sys.exit(0)

signal.signal(signal.SIGTERM, graceful_shutdown)
signal.signal(signal.SIGINT, graceful_shutdown)

# Import filters from different data sources
from filters.ethiopianProperties_filter import ethiopianProperties_filter
from filters.houseInRwanda_filter import houseInRwanda_filter
from filters.seso_filter import seso_filter
from filters.buyrentkenya_filter import buyrentkenya_filter
from filters.ghanaPropertyCentre_filter import ghanaPropertyCentre_filter
from filters.kenyaPropertyCentre_filter import kenyaPropertyCentre_filter
from filters.prophunt_filter import prophunt_filter
from filters.propertypro_co_ke_filter import propertypro_co_ke_filter
from filters.propertypro_co_ug_filter import propertypro_co_ug_filter
from filters.airbnb_filter import airbnb_filter
from filters.lamudi_filter import lamudi_filter
from filters.nigeriaPropertyCentre_filter import nigeriaPropertyCentre_filter
from filters.mubawab_filter import mubawab_filter
from filters.property24_filter import property24_filter
from filters.property24_co_ke_filter import property24_co_ke_filter
from filters.propertypro_co_zw_filter import propertypro_co_zw_filter
from filters.propertypro_ng_filter import propertypro_ng_filter
from filters.real_estate_tanzania_filter import real_estate_tanzania_filter
from filters.booking_filter import booking_filter
from filters.global_remax_filter import global_remax_filter
from filters.jiji_co_ke_filter import jiji_co_ke_filter
from filters.jiji_ug_filter import jiji_ug_filter
from filters.jiji_co_tz_filter import jiji_co_tz_filter
from filters.jiji_ng_filter import jiji_ng_filter
from filters.jiji_com_et_filter import jiji_com_et_filter
from filters.jiji_com_gh_filter import jiji_com_gh_filter

# Get the project root and add it to the Python path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

# Final database and collection names
finalDatabaseName = 'rehaniAI'
collectionName = 'properties'
with open(os.path.join(project_root, 'columns.json'), 'r') as json_file:
    column_dict = json.load(json_file)

# --- Additional Optimization: Parallel City Data Retrieval with Caching ---
@lru_cache(maxsize=256)
def cached_get_city_data(city):
    """Cached version to avoid duplicate calls."""
    return get_city_data(city)

def fetch_all_city_data(df, max_workers=16):
    """
    Fetch city data concurrently for all unique cities found in the DataFrame.
    Returns a dictionary mapping city names to their data.
    """
    unique_cities = df['consolidatedCity'].unique()
    city_results = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_city = {executor.submit(cached_get_city_data, city): city for city in unique_cities}
        for future in as_completed(future_to_city):
            city = future_to_city[future]
            try:
                city_results[city] = future.result()
            except Exception as e:
                log.error(f"Error retrieving city data for {city}: {e}")
                city_results[city] = None
    return city_results

def process_embeddings(df_chunk):
    return add_embeddings(df_chunk)

def send_data_thread(df_chunk):
    send_data(df_chunk, finalDatabaseName, collectionName, log)

# --- MongoDB Checkpointing Helpers ---
def is_record_processed(db, property_id):
    """
    Check in the 'processedDetails' collection whether a record with the given property_id
    has been marked as processed.
    """
    return db['processedDetails'].find_one({"propertyId": property_id, "status": "completed"}) is not None

def mark_records_processed(df_chunk, db):
    """
    Mark each record in the given DataFrame as processed by inserting or updating a document
    in the 'processedDetails' collection.
    """
    for _, row in df_chunk.iterrows():
        property_id = row['propertyId']
        url = row.get('url')
        try:
            result = db['processedDetails'].update_one(
                {"propertyId": property_id},
                {"$set": {"url": url, "status": "completed", "processedAt": datetime.utcnow()}},
                upsert=True
            )
            log.info(f"Marked record {property_id} as processed (matched: {result.matched_count}, modified: {result.modified_count})")
        except Exception as ex:
            log.error(f"Failed to mark record {property_id}: {ex}", exc_info=True)

def send_data_and_checkpoint(df_chunk, log, db):
    """
    Call the existing send_data function and, upon successful completion,
    mark each record in df_chunk as processed.
    """
    send_data(df_chunk, finalDatabaseName, collectionName, log)
    mark_records_processed(df_chunk, db)

def main():
    try:
        # Define filters to use for processing
        filters = [
            nigeriaPropertyCentre_filter,
            mubawab_filter,
            propertypro_co_zw_filter,
            propertypro_ng_filter,
            propertypro_co_ug_filter,
            global_remax_filter
        ]

        # Processing parameters
        chunk_size = 400
        batch_size = 50
        num_threads = 16  # Set threads as required

        # Create MongoDB client and get database handle
        client = MongoClient(os.getenv("CONNECTION_STRING"))
        db = client[finalDatabaseName]

        # --- Run each filter once and store its DataFrame ---
        filter_results = []
        for filter_func in filters:
            df = filter_func(log)
            filter_results.append(df)

        # Calculate total steps for progress reporting
        total_steps = sum(math.ceil(len(df) / chunk_size) for df in filter_results)
        completed_steps = 0

        # Process each DataFrame sequentially
        for idx, df in enumerate(filter_results, start=1):
            # Standardize the DataFrame by renaming columns and sorting
            df = df.rename(columns=column_dict)
            df = df.sort_values(by='locationCountry').reset_index(drop=True)
            log.info(f"Starting to process DataFrame {idx} serially!")

            # --- Checkpoint Filtering: Remove listings already processed as per 'processedDetails' ---
            processed_ids = db['processedDetails'].distinct('propertyId', {'status': 'completed'})
            original_count = len(df)
            df = df[~df['propertyId'].isin(processed_ids)]
            log.info(f"Skipping {original_count - len(df)} listings that are already processed in DataFrame {idx}.")
            log.info(f"Skipped {original_count - len(df)} listings; {len(df)} will be processed this run.")
            if df.empty:
                log.info(f"All records in DataFrame {idx} have already been processed. Skipping...")
                continue

            # --- Enrich DataFrame ---
            df = add_lat_long(df, log)
            log.info(f"Adding neighborhoods by processing latitude/longitude for DataFrame {idx}!")
            df = add_more_calculations(df, log)

            # --- Parallel City Data Retrieval ---
            city_results = fetch_all_city_data(df, max_workers=16)
            df['City GDP per Capita'] = df['consolidatedCity'].map(lambda x: (city_results.get(x) or {}).get('gdpPerCapita'))
            df['City Population'] = df['consolidatedCity'].map(lambda x: (city_results.get(x) or {}).get('population'))
            df['City Population Growth Rate'] = df['consolidatedCity'].map(lambda x: (city_results.get(x) or {}).get('populationGrowthRate'))

            # --- Chunk and Batch Processing with Checkpointing ---
            futures = []
            with ThreadPoolExecutor(max_workers=num_threads) as executor:
                for chunk_start in range(0, len(df), chunk_size):
                    chunk_end = min(chunk_start + chunk_size, len(df))
                    chunk = df.iloc[chunk_start:chunk_end].copy()

                    log.info(f"Adding embeddings for records {chunk_start + 1} to {chunk_end}")
                    chunk = add_embeddings(chunk)

                    # Split the chunk into smaller batches for sending data and checkpointing
                    for sub_start in range(0, len(chunk), batch_size):
                        sub_end = min(sub_start + batch_size, len(chunk))
                        sub_chunk = chunk.iloc[sub_start:sub_end]
                        # Submit the task that sends data and then marks the records as processed
                        futures.append(executor.submit(send_data_and_checkpoint, sub_chunk, log, db))

                    # Log memory usage
                    mem_usage = psutil.virtual_memory().percent
                    log.info(f"Current memory usage: {mem_usage}%")
                    
                    # Explicitly call garbage collection after each chunk
                    gc.collect()

                    completed_steps += 1
                    overall_progress = (completed_steps / total_steps) * 100
                    log.info(f"Overall Progress: {overall_progress:.2f}% completed")

                # Wait for all send_data tasks to complete
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        log.error(f"An error occurred while sending data: {e}")

        log.info("Script execution completed successfully! 100% done.")

    except Exception as e:
        log.error(f"Script failed due to an error: {e}", exc_info=True)

    finally:
        try:
            s3 = boto3.client("s3", region_name=aws_region_name)
            s3.put_object(Body=log_stringio.getvalue(), Bucket=bucket_name, Key="logs/filtering-script-3/logs.txt", ACL="public-read")
            log.info("Log file successfully uploaded to S3.")
        except Exception as s3_error:
            log.error(f"Failed to upload log file to S3: {s3_error}", exc_info=True)

if __name__ == "__main__":
    main()