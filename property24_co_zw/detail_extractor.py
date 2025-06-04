import re
import datetime
import threading
import concurrent.futures
from bs4 import BeautifulSoup, NavigableString
import os, io, logging, boto3, warnings, requests
from pymongo import MongoClient
from dotenv import load_dotenv
import pandas as pd
import numpy as np
import random
import time
from requests.exceptions import HTTPError, Timeout

# ─── Configuration ─────────────────────────────────────────────────────────────
warnings.filterwarnings("ignore", category=DeprecationWarning)
load_dotenv(override=True)

# External services
CONNECTION_STRING_MONGODB = os.getenv("CONNECTION_STRING")
AWS_REGION_NAME          = os.getenv("aws_region_name")
BUCKET_NAME              = os.getenv("bucket_name")
ZENROWS_API_KEY          = os.getenv("ZENROWS_API_KEY")

# Scraper settings
BASE_URL            = "https://www.property24.co.zw"
THREADS             = int(os.getenv("threads", 1))
LIST_POOL_SIZE      = int(os.getenv("list_pool_size", 50))
BATCH_SIZE_DETAILS  = LIST_POOL_SIZE

# MongoDB targets
URL_DB              = "property24_co_zw"
URL_COLLECTION      = "propertyURLs"
DETAILS_COLLECTION  = "propertyDetails"

# Data schema for details
DETAIL_COLUMNS = [
    'url', 'propertyId', 'listingType', 'title', 'location', 'agent', 'agentNumber',
    'price', 'currency', 'beds', 'baths', 'toilets', 'erfSize', 'amenities', 'imgUrls',
    'description', 'city', 'dateAdded', 'housingType', 'priceChange', 'priceStatus', 'priceDiff'
]

# Internal state
thread_results = {}

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
log = logging.getLogger("property24-zw-detail-extractor")
log_stringio = io.StringIO()
handler = logging.StreamHandler(log_stringio)
handler.setFormatter(formatter)
log.addHandler(handler)

# ─── Helpers ───────────────────────────────────────────────────────────────────

def fetch_html(url, max_retries=5):
    backoff = 1.0
    for attempt in range(1, max_retries+1):
        try:
            resp = requests.get(
                "https://api.zenrows.com/v1/",
                params={"apikey": ZENROWS_API_KEY, "url": url},
                timeout=120
            )
            # if ZenRows itself returns an error status, this will raise
            resp.raise_for_status()

            text = resp.text
            # detect the rate-limit message in the HTML
            if "Server is temporarily unavailable" in text:
                raise HTTPError("Rate limited by server message")

            return text

        except (HTTPError, Timeout) as e:
            log.warning(f"[fetch_html] attempt {attempt}/{max_retries} failed: {e}")
            if attempt == max_retries:
                log.error(f"Max fetch retries reached for {url}")
                raise
            # exponential backoff with jitter
            sleep_time = backoff + random.uniform(0, backoff)
            log.info(f"Sleeping {sleep_time:.1f}s before retrying...")
            time.sleep(sleep_time)
            backoff *= 2  # double the base backoff

    # Should never reach here
    raise RuntimeError(f"fetch_html failed for {url}")


def get_urls_from_db():
    client = MongoClient(CONNECTION_STRING_MONGODB)
    coll = client[URL_DB][URL_COLLECTION]
    return list(coll.find({}, { 'url':1, 'propertyId':1, 'listingType':1, 'price':1 }))


def send_detail_batch(batch):
    log.info(f"Sending {len(batch)} detailed records to MongoDB…")
    df = pd.DataFrame(batch, columns=DETAIL_COLUMNS)
    df.replace({np.nan: None}, inplace=True)
    records = df.to_dict('records')

    client = MongoClient(CONNECTION_STRING_MONGODB)
    coll = client[URL_DB][DETAILS_COLLECTION]
    for rec in records:
        coll.update_one(
            { 'propertyId': rec['propertyId'] },
            {
                '$set':         rec,
                '$setOnInsert': { 'date_listed':   datetime.datetime.utcnow() },
                '$currentDate': { 'date_updated': True }
            },
            upsert=True
        )
    log.info("✅ Detailed batch upsert complete")


def scrape_data(item):
    """Worker: scrape details for a single URL entry."""
    thread_id = threading.get_ident()
    if thread_id not in thread_results:
        thread_results[thread_id] = []

    url           = item['url']
    propertyId    = item['propertyId']
    listingType   = item.get('listingType', 'unknown')
    old_price     = item.get('price')
    currency      = 'USD'



    retries = 2
    delay   = 10
    while retries:
        try:
            html = fetch_html(url)
            soup = BeautifulSoup(html, 'lxml')

            # write out the raw HTML so you can inspect it later
            # with open("debug_page.html", "w", encoding="utf-8") as f:
            #     f.write(soup.prettify())

            # Example selectors - adjust to actual site structure
            # Title
            title_el = soup.select_one('div.sc_listingAddress h1')
            title    = title_el.get_text(strip=True) if title_el else None

            # Location
            loc_ps   = soup.select('div.p24_mBM p')
            location = loc_ps[0].get_text(strip=True) if loc_ps else None

            agent = agentPhone = None
            token_el = soup.select_one('input[name="ListingNumberToken"]')
            if token_el:
                token = token_el['value']
                ajax = requests.post(
                    "https://www.property24.co.zw/listing/listingfullcontact",
                    data={
                        "ListingNumberToken": token,
                        "ContactType":        "Telephone",
                        "Position":           "Top"
                    },
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                    timeout=30
                )
                text = BeautifulSoup(ajax.text, 'lxml')\
                        .get_text(separator='\n')\
                        .strip()
                lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
                if lines:
                    agent = lines[0]
                    agentPhone = lines[-1]

            features = {}
            for el in soup.select('span.p24_feature'):
                key = el.get_text(strip=True).rstrip(':')
                # get the text or element immediately following the label
                val_node = el.next_sibling
                # skip over empty text nodes
                while val_node and isinstance(val_node, str) and not val_node.strip():
                    val_node = val_node.next_sibling
                if hasattr(val_node, 'get_text'):
                    val = val_node.get_text(strip=True)
                else:
                    val = val_node.strip() if isinstance(val_node, str) else None
                features[key] = val

            # parse feature counts
            beds = None
            if features.get('Bedrooms') and features['Bedrooms'].isdigit():
                beds = int(features['Bedrooms'])
            baths = None
            if features.get('Bathrooms') and features['Bathrooms'].isdigit():
                baths = int(features['Bathrooms'])
            toilets = None
            if features.get('Toilets') and features['Toilets'].isdigit():
                toilets = int(features['Toilets'])

            erfSize = None
            for size_el in soup.select('.p24_size'):
                label = size_el.get('title', '').lower()
                val = size_el.get_text(strip=True)
                if 'erf' in label:
                    match = re.search(r'(\d[\d\s,.]*m²)', val)
                    if match:
                        erfSize = match.group(1).replace(' ', '')

            ignore_keys = {'Listing Number', 'Type of Property', 'List Date', 'Erf Size', 'Floor Area', 'Street Address', 'Rates and Taxes', 'Zoning', 'Levies', 'Size of farm', 'No Transfer Duty', 'Age', 'Price per m²'}
            rows = soup.select('#Property-Overview .p24_propertyOverviewRow') + soup.select('#External-Features .p24_propertyOverviewRow')
            amenities = []
            for row in rows:
                key = row.select_one('.p24_propertyOverviewKey').get_text(strip=True)
                if key and key not in ignore_keys:
                    amenities.append(key)
            
            # Images: extract full-resolution URLs from lightbox anchors or fallback to img attributes
            imgUrls = []
            for a in soup.select('a.js_lightboxImageSrc'):
                if a.has_attr('data-src'):
                    imgUrls.append(a['data-src'])
            # fallback if none found
            if not imgUrls:
                imgUrls = [img.get('lazy-src-background') or img.get('src')
                        for img in soup.select('#main-gallery-images-container img.js_lazyImageLoading')]

            # Description: concatenate text nodes and expanded span in order
            description = None
            container = soup.select_one('div.sc_listingDetailsText')
            if container:
                parts = []
                for node in container.contents:
                    # text nodes
                    if isinstance(node, NavigableString):
                        text = node.strip()
                        if text:
                            parts.append(text)
                    # the hidden expanded description span
                    elif getattr(node, 'name', None) and node.get('id') == 'readMoreDescription':
                        txt = node.get_text(strip=True)
                        if txt:
                            parts.append(txt)
                description = ' '.join(parts)

            # City / region
            crumbs = [a.get_text(strip=True) for a in soup.select('ul.breadcrumb li a')]
            city = crumbs[1] if len(crumbs) >= 2 else None

            dateAdded = datetime.datetime.utcnow()

            housingType = None
            for row in soup.select('#Property-Overview .p24_propertyOverviewRow'):
                key_el = row.select_one('.p24_propertyOverviewKey')
                if key_el and key_el.get_text(strip=True) == 'Type of Property':
                    val_el = row.select_one('.p24_info')
                    housingType = val_el.get_text(strip=True) if val_el else None
                    break

            # Price: use value from MongoDB to avoid re-scraping
            old_price = item.get('price')
            price = float(old_price) if old_price is not None else None

            # Price change logic
            old_price = item.get('price')
            priceDiff = abs((price or 0) - (old_price or 0))
            priceChange = priceDiff > 0
            priceStatus = ('increased' if price and old_price and price > old_price else
                        'decreased' if price and old_price and price < old_price else None)
            
            print("URL:", url)
            print("PropertyId:", propertyId)
            print("Type:", listingType)
            print("Title:", title)
            print("Location:", location)
            print("Agent:", agent)
            print("AgentPhone:", agentPhone)
            print("Price:", price)
            print("Currency:", currency)
            print("Beds:", beds)
            print("Baths:", baths)
            print("Toilets:", toilets)
            print("Erf Size:", erfSize)
            print("Amenities:", amenities)
            print("Images:", imgUrls)
            print("Description:", description)
            print("City:", city)
            print("DateAdded:", dateAdded)
            print("HousingType:", housingType)
            print("PriceChange:", priceChange)
            print("PriceStatus:", priceStatus)
            print("PriceDiff:", priceDiff)
            print()

            # Collect
            thread_results[thread_id].append([
                url, propertyId, listingType, title, location, agent, agentPhone,
                price, currency, beds, baths, toilets, erfSize, amenities, imgUrls,
                description, city, dateAdded, housingType, priceChange, priceStatus, priceDiff
            ])

            # Flush per-thread batch
            if len(thread_results[thread_id]) >= LIST_POOL_SIZE:
                send_detail_batch(thread_results[thread_id])
                thread_results[thread_id] = []

            return

        except Exception as e:
            log.warning(f"Error scraping {url}: {e}. Retries left: {retries-1}")
            retries -= 1
            time.sleep(delay)

    log.error(f"Failed to scrape after retries: {url}")


# ─── Main ──────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    # Fetch URL list
    entries = get_urls_from_db()

    # Start threads
    with concurrent.futures.ThreadPoolExecutor(max_workers=THREADS) as executor:
        executor.map(scrape_data, entries)

    # Flush remaining per-thread batches
    for batch in thread_results.values():
        if batch:
            send_detail_batch(batch)

    # Upload logs to S3
    s3 = boto3.client('s3', region_name=AWS_REGION_NAME)
    s3.put_object(
        Body=log_stringio.getvalue(),
        Bucket=BUCKET_NAME,
        Key='logs/property24-co-zw/detail-extractor-logs.txt'
    )
    log.info("📁 Detail extraction completed and logs uploaded.")
