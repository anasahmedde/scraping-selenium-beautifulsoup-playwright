#!/usr/bin/env python3
"""
Property24 Detail Extractor (ZenRows, Memory-Optimized)

Gathers property links and details from Property24 and stores them in MongoDB.
Transfers execution logs to S3.
"""

import concurrent.futures
import io
import logging
import os
import warnings
import datetime
import requests
import time

from bs4 import BeautifulSoup
from dotenv import load_dotenv
from pymongo import MongoClient
import boto3
import pandas as pd
import re

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
load_dotenv(override=True)

# MongoDB
MONGODB_URI = os.getenv("CONNECTION_STRING")
DATABASE_NAME = os.getenv("DATABASE_NAME", "property24")
COLLECTION_URLS = "propertyURLs"

# AWS S3
AWS_REGION = os.getenv("aws_region_name")
BUCKET_NAME = os.getenv("bucket_name")
LOG_OBJECT_KEY = "logs/property24/url-extractor-logs.txt"

# ZenRows
ZENROWS_API_KEY = os.getenv("ZENROWS_API_KEY")
ZENROWS_API_URL = "https://api.zenrows.com/v1/"

# Threading
THREAD_COUNT = int(os.getenv("threads", "4"))
LISTING_PAGES = range(1, 10)

# URLs
BASE_URL = "https://www.property24.com"
LINKS_URL_TEMPLATE = BASE_URL + "/for-sale/all-cities/gauteng/{page}"

# Output columns for URL data
COLUMNS = ["url", "propertyId", "listingType", "price"]

# -----------------------------------------------------------------------------
# Logger Setup
# -----------------------------------------------------------------------------
formatter = logging.Formatter(
    fmt='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logging.basicConfig(level=logging.INFO, format=formatter._fmt, datefmt=formatter.datefmt)
logger = logging.getLogger("property24-url-extractor")

# Capture logs in-memory for later upload
log_stream = io.StringIO()
stream_handler = logging.StreamHandler(log_stream)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

warnings.filterwarnings("ignore", category=DeprecationWarning)

# -----------------------------------------------------------------------------
# Database Helper
# -----------------------------------------------------------------------------

def get_database(uri: str):
    client = MongoClient(uri)
    return client[DATABASE_NAME]

# -----------------------------------------------------------------------------
# Data Insertion
# -----------------------------------------------------------------------------

def send_data(data: list, columns: list, collection_name: str):
    try:
        count = len(data)
        logger.info(f'Collected {count} records!')
        if count == 0:
            return

        df = pd.DataFrame(data, columns=columns)
        for col in df.select_dtypes(include=['datetime64']).columns:
            df[col] = df[col].fillna(pd.NaT).astype(str)

        records = df.to_dict('records')
        logger.info('Sending data to MongoDB...')

        db = get_database(MONGODB_URI)
        coll = db[collection_name]
        for rec in records:
            coll.update_one(
                {'propertyId': rec['propertyId']},
                {
                    '$set': {k: rec[k] for k in rec if k != 'propertyId'},
                    '$setOnInsert': {'date_listed': datetime.datetime.utcnow()},
                    '$currentDate': {'date_updated': True}
                },
                upsert=True
            )

        logger.info('Data sent successfully.')
    except Exception as exc:
        logger.error('Error sending data:', exc_info=exc)

# -----------------------------------------------------------------------------
# ZENROWS HTML FETCHER
# -----------------------------------------------------------------------------

def fetch_html_with_zenrows(url):
    params = {
        "apikey": ZENROWS_API_KEY,
        "url": url,
        # "js_render": "true",  # Uncomment only if necessary (JS rendering is slower)
        # "premium_proxy": "true", # Optional: increases success if you have it
    }
    for attempt in range(3):
        try:
            response = requests.get(ZENROWS_API_URL, params=params, timeout=60)
            response.raise_for_status()
            logger.info(f"[ZenRows] SUCCESS ({response.status_code}) for {url}")
            return response.text
        except Exception as e:
            logger.warning(f"[ZenRows] Attempt {attempt+1}/3 failed for {url}: {e}")
            time.sleep(2 + attempt*3)
    logger.error(f"[ZenRows] UNREACHABLE: Failed to fetch {url} after 3 attempts")
    return ""

def get_links(page: int) -> list:
    url = LINKS_URL_TEMPLATE.format(page=page)
    logger.info(f'Fetching listing page: {url}')
    html = fetch_html_with_zenrows(url)
    if not html:
        logger.error(f"Site unreachable or empty response for listing page: {url}")
        return []
    soup = BeautifulSoup(html, 'html.parser')
    if soup.select_one("div.p24_errorContent") or soup.find(string='No Items Found'):
        logger.warning(f"Error/content not found on page: {url}")
        return []
    links = [BASE_URL + a['href'] for a in soup.select('label.checkbox a[href]')]
    logger.info(f"Found {len(links)} area/city links on listing page {page}")
    return links

def get_property_urls(listing_url: str) -> list:
    logger.info(f"Fetching area/city page: {listing_url}")
    html = fetch_html_with_zenrows(listing_url)
    if not html:
        logger.error(f"Site unreachable or empty response for area/city page: {listing_url}")
        return []
    soup = BeautifulSoup(html, 'html.parser')
    if soup.find(string='No properties found') or soup.select_one("div.p24_errorContent"):
        logger.warning(f"No properties or error content found on area/city page: {listing_url}")
        return []

    # Use the pattern you found works in the browser:
    tiles = soup.select('div.p24_regularTile')
    results = []
    for tile in tiles:
        a = tile.find('a', href=True)
        if a:
            href = a['href']
            url = BASE_URL + href if href.startswith("/") else href

            match = re.search(r"/(\d+)(?:\?|$)", url)
            property_id = match.group(1) if match else None
            
            # Extract price if available (may need adjustment)
            price_span = tile.find('span', class_='p24_price')
            price = float(price_span.get('content', 0)) if price_span and price_span.has_attr('content') else None
            # Extract listing type if available
            tag_span = tile.find('span', class_='p24_tag')
            if tag_span and tag_span.text.strip():
                listing_type = tag_span.text.strip().lower()
            else:
                # Fallback: infer from URL if possible
                if "/for-sale/" in url:
                    listing_type = "sale"
                elif "/for-rent/" in url or "/to-rent/" in url:
                    listing_type = "rent"
                else:
                    listing_type = None

            logger.info(f'Property detail URL found: {url}')
            results.append([url, property_id, listing_type, price])
    logger.info(f"Found {len(results)} properties in {listing_url}")
    return results

# -----------------------------------------------------------------------------
# Main Execution (Memory-Optimized)
# -----------------------------------------------------------------------------

def main():
    logger.info('Starting ZenRows extraction...')
    # Step 1: Gather all listing URLs
    listing_urls = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=THREAD_COUNT) as pool:
        for urls in pool.map(get_links, LISTING_PAGES):
            listing_urls.extend(urls)
    # Include rent variants
    listing_urls += [u.replace('/for-sale/', '/for-rent/') for u in listing_urls]

    # Step 2: Fetch property detail URLs in batches as futures complete
    batch = []
    all_property_urls = set()
    with concurrent.futures.ThreadPoolExecutor(max_workers=THREAD_COUNT) as pool:
        futures = {pool.submit(get_property_urls, url): url for url in listing_urls}
        for future in concurrent.futures.as_completed(futures):
            details = future.result()
            if details:
                batch.extend(details)
                all_property_urls.update([d[0] for d in details])
            logger.info(f'Batch size is now {len(batch)}')
            if len(batch) >= 100:
                send_data(batch, COLUMNS, COLLECTION_URLS)
                batch.clear()
    if batch:
        send_data(batch, COLUMNS, COLLECTION_URLS)

    logger.info(f'Total unique property URLs fetched: {len(all_property_urls)}')
    for url in all_property_urls:
        logger.info(f'Unique property URL: {url}')

    logger.info('Extraction completed.')

    # Step 3: Upload logs
    s3 = boto3.client("s3", region_name=AWS_REGION)
    s3.put_object(Body=log_stream.getvalue(), Bucket=BUCKET_NAME, Key=LOG_OBJECT_KEY)
    logger.info('Logs uploaded to S3.')

if __name__ == "__main__":
    main()
