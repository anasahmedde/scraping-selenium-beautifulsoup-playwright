import re
import datetime
import concurrent.futures
from bs4 import BeautifulSoup
import os, io, logging, boto3, warnings, requests
from pymongo import MongoClient
from dotenv import load_dotenv
import pandas as pd

# ─── Configuration ─────────────────────────────────────────────────────────────
warnings.filterwarnings("ignore", category=DeprecationWarning)
load_dotenv(override=True)

# External services
CONNECTION_STRING_MONGODB = os.getenv("CONNECTION_STRING")
AWS_REGION_NAME          = os.getenv("aws_region_name")
BUCKET_NAME              = os.getenv("bucket_name")
ZENROWS_API_KEY          = os.getenv("ZENROWS_API_KEY")

# Scraper settings
BASE_URL      = "https://www.property24.co.mz"
THREADS       = int(os.getenv("threads", 10))
BATCH_SIZE    = 50                  # send to MongoDB every N pages

# MongoDB target
DATABASE_NAME   = "property24_co_mz"
COLLECTION_NAME = "propertyURLs"

# Data schema
COLUMNS = ["url", "propertyId", "listingType", "price"]

# Internal state
batch       = []
fetch_count = 0

# ─── Logging Setup ─────────────────────────────────────────────────────────────
formatter = logging.Formatter(
    '%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger("property24-mz-url-extractor")
log_stringio = io.StringIO()
handler = logging.StreamHandler(log_stringio)
handler.setFormatter(formatter)
log.addHandler(handler)

# ─── Helpers ───────────────────────────────────────────────────────────────────

def fetch_html(url):
    params = {"apikey": ZENROWS_API_KEY, "url": url}
    resp = requests.get("https://api.zenrows.com/v1/", params=params, timeout=120)
    resp.raise_for_status()
    return resp.text

def extract_links(url):
    html = fetch_html(url)
    soup = BeautifulSoup(html, "lxml")

    results = []
    href_re = re.compile(r"^/.+-(\d+)$")
    for a in soup.find_all("a", href=href_re):
        pid      = href_re.search(a["href"]).group(1)
        full_url = BASE_URL + a["href"]

        price_text = a.find_next(string=re.compile(r"MT\s?[\d\s,]+"))
        if price_text:
            num_str = re.sub(r"[^\d\.]", "", price_text)
            try:
                price = float(num_str)
            except ValueError:
                log.warning(f"Couldn’t parse price '{price_text}' on {full_url}")
                price = None
        else:
            price = None

        if "for-sale" in a["href"]:
            listing_type = "sale"
        elif "to-rent" in a["href"] or "for-rent" in a["href"]:
            listing_type = "rent"
        else:
            listing_type = "unknown"

        results.append([full_url, pid, listing_type, price])

    log.info(f"{url} → found {len(results)} listings")
    return results

def sendData(data):
    """Upsert a batch of records into MongoDB with date_listed & date_updated."""
    log.info(f"Sending {len(data)} records to MongoDB…")
    df      = pd.DataFrame(data, columns=COLUMNS)
    records = df.to_dict("records")

    client = MongoClient(CONNECTION_STRING_MONGODB)
    coll   = client[DATABASE_NAME][COLLECTION_NAME]

    for rec in records:
        coll.update_one(
            {"propertyId": rec["propertyId"]},
            {
                "$set": {
                    "url":         rec["url"],
                    "listingType": rec["listingType"],
                    "price":       rec["price"]
                },
                "$setOnInsert": {"date_listed":  datetime.datetime.utcnow()},
                "$currentDate": {"date_updated": True}
            },
            upsert=True
        )

    log.info("✅ Batch sent to MongoDB")

def maybe_flush():
    """Flush the batch to MongoDB every BATCH_SIZE fetches."""
    global batch, fetch_count
    if fetch_count and fetch_count % BATCH_SIZE == 0:
        sendData(batch)
        batch = []

# ─── Main Orchestration ────────────────────────────────────────────────────────

if __name__ == "__main__":
    # 1) Crawl `for-sale` pages until empty
    page = 1
    while True:
        url   = f"{BASE_URL}/property-for-sale?Page={page}"
        links = extract_links(url)
        if not links:
            break

        batch.extend(links)
        fetch_count += 1
        maybe_flush()
        page += 1

    # 2) Crawl `to-rent` pages until empty
    page = 1
    while True:
        url   = f"{BASE_URL}/property-to-rent?Page={page}"
        links = extract_links(url)
        if not links:
            break

        batch.extend(links)
        fetch_count += 1
        maybe_flush()
        page += 1

    # 3) Send any remaining records
    if batch:
        sendData(batch)

    # 4) Upload logs to S3
    s3 = boto3.client("s3", region_name=AWS_REGION_NAME)
    s3.put_object(
        Body=log_stringio.getvalue(),
        Bucket=BUCKET_NAME,
        Key="logs/property24-co-mz/url-extractor-logs.txt"
    )
    log.info("📁 Logs transferred to S3")
