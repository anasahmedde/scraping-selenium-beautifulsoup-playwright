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

warnings.filterwarnings("ignore", category=DeprecationWarning)
load_dotenv(override=True)

# ─── Config ─────────────────────────────────────
CONNECTION_STRING_MONGODB = os.getenv("CONNECTION_STRING")
AWS_REGION_NAME           = os.getenv("aws_region_name")
BUCKET_NAME               = os.getenv("bucket_name")
ZENROWS_API_KEY           = os.getenv("ZENROWS_API_KEY")

BASE_URL            = "https://www.property24.com"
THREADS             = int(os.getenv("threads", 8))
LIST_POOL_SIZE      = int(os.getenv("list_pool_size", 50))
BATCH_SIZE_DETAILS  = LIST_POOL_SIZE

URL_DB              = "property24"
URL_COLLECTION      = "propertyURLs"
DETAILS_COLLECTION  = "propertyDetails"
CHECKPOINT_COLLECTION  = "detailCheckpoint"

DETAIL_COLUMNS = [
    'url', 'propertyId', 'listingType', 'title', 'location', 'agent', 'agentPhone',
    'price', 'currency', 'beds', 'baths', 'parking', 'erfSize', 'floorSize', 'amenities', 'imgUrls',
    'description', 'city', 'province', 'dateAdded', 'housingType', 'priceChange', 'priceStatus', 'priceDiff'
]

thread_results = {}

formatter = logging.Formatter(
    '%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger("property24-detail-extractor")
log_stringio = io.StringIO()
handler = logging.StreamHandler(log_stringio)
handler.setFormatter(formatter)
log.addHandler(handler)


def check_mongo_connection(uri):
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        # ping the server
        client.admin.command('ping')
        log.info("✅ MongoDB connection OK")
        return True
    except Exception as e:
        log.error(f"❌ MongoDB connection failed: {e}")
        return False

# ─── Helpers ─────────────────────────────────────
def fetch_html(url, max_retries=4):
    backoff = 1.0
    for attempt in range(1, max_retries+1):
        try:
            resp = requests.get(
                "https://api.zenrows.com/v1/",
                params={"apikey": ZENROWS_API_KEY, "url": url},
                timeout=120
            )
            if resp.status_code == 404:
                log.info(f"Listing disappeared (404): {url}")
                return None
            resp.raise_for_status()
            text = resp.text
            if "Server is temporarily unavailable" in text:
                raise HTTPError("Rate limited by server message")
            return text
        except (HTTPError, Timeout) as e:
            log.warning(f"[fetch_html] attempt {attempt}/{max_retries} failed: {e}")
            if attempt == max_retries:
                log.error(f"Max fetch retries reached for {url}")
                raise
            sleep_time = backoff + random.uniform(0, backoff)
            log.info(f"Sleeping {sleep_time:.1f}s before retrying...")
            time.sleep(sleep_time)
            backoff *= 2
    raise RuntimeError(f"fetch_html failed for {url}")

def get_urls_from_db():
    client = MongoClient(CONNECTION_STRING_MONGODB)
    coll = client[URL_DB][URL_COLLECTION]
    return list(coll.find({}, { 'url':1, 'propertyId':1, 'listingType':1, 'price':1 }))

def get_pending_urls():
    client = MongoClient(CONNECTION_STRING_MONGODB)
    cp_coll = client[URL_DB][CHECKPOINT_COLLECTION]
    done_ids = {doc['propertyId'] for doc in cp_coll.find({}, {'propertyId':1})}
    all_urls = get_urls_from_db()
    return [u for u in all_urls if u['propertyId'] not in done_ids]

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
    # record checkpoints
    client = MongoClient(CONNECTION_STRING_MONGODB)
    cp_coll = client[URL_DB][CHECKPOINT_COLLECTION]
    for rec in records:
        cp_coll.update_one(
            {'propertyId': rec['propertyId']},
            {'$setOnInsert': {'checkpointedAt': datetime.datetime.utcnow()}},
            upsert=True
        )
    log.info(f"✅ Recorded {len(records)} checkpoints")

def scrape_data(item):
    thread_id = threading.get_ident()
    if thread_id not in thread_results:
        thread_results[thread_id] = []

    url         = item['url']
    propertyId  = item['propertyId']
    listingType = item.get('listingType', 'unknown')
    old_price   = item.get('price')
    currency    = 'ZAR'

    retries = 2
    delay   = 10
    while retries:
        try:
            html = fetch_html(url)
            soup = BeautifulSoup(html, 'lxml')

            # Title
            title_el = soup.select_one('h1')
            title = title_el.get_text(strip=True) if title_el else None

            # Location
            # location_el = soup.select_one('.p24_streetAddress')
            # location = location_el.get_text(strip=True) if location_el else None
            location = None

            # 1) Try .p24_streetAddress
            street_el = soup.select_one('.p24_streetAddress')
            if street_el and street_el.get_text(strip=True):
                location = street_el.get_text(strip=True)

            else:
                # 2) Fallback to .p24_mBM only if it’s “Place, Place” (no digits, has comma, short)
                fb_el = soup.select_one('.p24_mBM')
                if fb_el:
                    fb = fb_el.get_text(strip=True)
                    if ',' in fb and not re.search(r'\d', fb) and len(fb) < 50:
                        location = fb

                # 3) Last-resort: breadcrumbs
                if not location:
                    crumbs = [a.get_text(strip=True) for a in soup.select('#breadCrumbContainer a')]
                    if len(crumbs) >= 2:
                        location = f"{crumbs[-2]}, {crumbs[-1]}"

            # Price
            price_el = soup.select_one('.p24_price')
            price = None
            if price_el:
                ptxt = price_el.get_text(strip=True).replace('R', '').replace(' ', '').replace(',', '')
                try:
                    price = int(ptxt)
                except:
                    price = None

            # Features
            beds = baths = parking = None
            for feat in soup.select('.p24_featureDetails'):
                label = feat.get('title', '').lower()
                val = feat.get_text(strip=True)
                if 'bedroom' in label: beds = val
                elif 'bathroom' in label: baths = val
                elif 'parking' in label: parking = val

            erfSize = floorSize = None
            for size_el in soup.select('.p24_size'):
                label = size_el.get('title', '').lower()
                val = size_el.get_text(strip=True)
                if 'erf' in label:
                    match = re.search(r'(\d[\d\s,.]*m²)', val)
                    if match:
                        erfSize = match.group(1).replace(' ', '')

            # Floor Size from .p24_info, fallback if not found above
            for el in soup.select('.p24_propertyOverviewKey'):
                if el.get_text(strip=True).lower() == 'floor size':
                    parent = el.parent
                    info_div = parent.select_one('.p24_info')
                    if info_div:
                        match = re.search(r'(\d[\d\s,.]*m²)', info_div.get_text(strip=True))
                        if match:
                            floorSize = match.group(1).replace(' ', '')

            # Amenities extraction (all sections)
            ignore_keys = {
                'description',
                'lifestyle',
                'street address',
                'occupation date',
                'deposit requirements',
                'levies',
                'rates and taxes',
                'type of property',
                'floor size',
                'erf size',
                'listing number',
                'listing date',
                'price',
                'rates',
                'levy',
                'floor area',
                'stand size',

                # bedrooms & bathrooms (these are just counts, not amenities)
                'bedroom 1','bedroom 2','bedroom 3','bedroom 4',
                'bathroom 1','bathroom 2','bathroom 3',
                'bedrooms','bathrooms',

                # room-type things you don’t want
                'kitchen','kitchens',
                'lounge','dining room','family/tv room','entrance halls','office','reception rooms',

                # structural bits
                'wall','roof','floor','standalone building','outside toilets',

                # outbuildings & garages
                'outbuildings','outbuilding 1','outbuilding 2',
                'garage','garage 1','garage 2','garage 3','parking','parking 1','parking 2',

                # misc you don’t consider features
                'coverage','special features','special features','other','others','style','facing','window', 'no transfer duty'
            }
            amenities = []
            for row in soup.select('.p24_propertyOverviewRow'):
                key_el = row.select_one('.p24_propertyOverviewKey')
                if key_el:
                    key = key_el.get_text(strip=True)
                    if key and key.lower() not in ignore_keys:
                        amenities.append(key)

            # Images
            imgUrls = [
                div.get('data-image-url')
                for div in soup.select('.js_lightboxImageWrapper')
                if div.get('data-image-url')
            ]

            # Description
            desc_el = soup.select_one('.js_expandedText')
            desc_raw = desc_el.get_text() if desc_el else ''
            description = ' '.join(line.strip() for line in desc_raw.splitlines() if line.strip())

            # Agent (robust fallback)
            agent = None
            for selector in ['.p24_agent', '.p24_agentDetails', '.p24_agentInfo']:
                agent_el = soup.select_one(selector)
                if agent_el:
                    name = agent_el.get_text(strip=True, separator='\n').split('\n')[0]
                    if name and 'contact' not in name.lower():
                        agent = name
                        break

            # Agent Phone
            agentPhone = None
            phone_links = soup.select('.p24_showNumbers a[href^="tel:"]')
            if phone_links:
                agentPhone = ', '.join(a.get_text(strip=True) for a in phone_links if a.get_text(strip=True))
            else:
                show_numbers = soup.select_one('.p24_showNumbers')
                if show_numbers:
                    phone_matches = re.findall(r'(\+?\d[\d\s\-()]{7,})', show_numbers.get_text())
                    filtered = [ph for ph in phone_matches if 'show' not in ph.lower()]
                    if filtered:
                        agentPhone = ', '.join(filtered)

            # City, Province from breadcrumbs
            crumbs = [a.get_text(strip=True) for a in soup.select('#breadCrumbContainer a')]
            city = crumbs[-2] if len(crumbs) >= 2 else None
            province = crumbs[-3] if len(crumbs) >= 3 else None

            # Date Added (use scrape time)
            dateAdded = datetime.datetime.utcnow()

            # Housing Type
            housingType = None
            for el in soup.select('.p24_propertyOverviewKey'):
                if el.get_text(strip=True).lower() == 'type of property':
                    parent = el.parent
                    info_div = parent.select_one('.p24_info')
                    if info_div:
                        housingType = info_div.get_text(strip=True)
                    else:
                        sibling = parent.select_one('.col-6.noPadding')
                        housingType = sibling.get_text(strip=True) if sibling else None
                    break

            # Price Change Logic (if previous price exists)
            priceDiff = abs((price or 0) - (old_price or 0)) if old_price else None
            priceChange = priceDiff > 0 if priceDiff else False
            priceStatus = (
                'increased' if price and old_price and price > old_price else
                'decreased' if price and old_price and price < old_price else None
            )

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
            print("Parking:", parking)
            print("Erf Size:", erfSize)
            print("Floor Size:", floorSize)
            print("Amenities:", amenities)
            print("Images:", imgUrls)
            print("Description:", description)
            print("City:", city)
            print("Province:", province)
            print("DateAdded:", dateAdded)
            print("HousingType:", housingType)
            print("PriceChange:", priceChange)
            print("PriceStatus:", priceStatus)
            print("PriceDiff:", priceDiff)
            print()

            thread_results[thread_id].append([
                url, propertyId, listingType, title, location, agent, agentPhone,
                price, currency, beds, baths, parking, erfSize, floorSize, amenities, imgUrls,
                description, city, province, dateAdded, housingType, priceChange, priceStatus, priceDiff
            ])

            if len(thread_results[thread_id]) >= LIST_POOL_SIZE:
                send_detail_batch(thread_results[thread_id])
                thread_results[thread_id] = []

            return

        except Exception as e:
            log.warning(f"Error scraping {url}: {e}. Retries left: {retries-1}")
            retries -= 1
            time.sleep(delay)

    log.error(f"Failed to scrape after retries: {url}")

# ─── Main ─────────────────────────────────────
if __name__ == '__main__':
    if not check_mongo_connection(CONNECTION_STRING_MONGODB):
        log.error("Exiting: cannot reach MongoDB")
        raise SystemExit(1)
    
    entries = get_pending_urls()
    with concurrent.futures.ThreadPoolExecutor(max_workers=THREADS) as executor:
        executor.map(scrape_data, entries)
    for batch in thread_results.values():
        if batch:
            send_detail_batch(batch)
    s3 = boto3.client('s3', region_name=AWS_REGION_NAME)
    s3.put_object(
        Body=log_stringio.getvalue(),
        Bucket=BUCKET_NAME,
        Key='logs/property24/detail-extractor-logs.txt'
    )
    log.info("📁 Detail extraction completed and logs uploaded.")
