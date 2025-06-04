import pandas as pd
from selenium import webdriver
import os, datetime, re, requests, json, math, io, logging, boto3, threading
from bs4 import BeautifulSoup
from lxml import etree
from pymongo import MongoClient
from dotenv import load_dotenv
import time
import re
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

load_dotenv(override=True)

databaseName='airbnb'
collectionNameURLs='propertyURLs'
collectionNameDetails='propertyDetails'
CONNECTION_STRING_MONGODB = os.getenv("CONNECTION_STRING")
aws_region_name = os.getenv("aws_region_name")
bucket_name = os.getenv("bucket_name")
threads = int(os.getenv("gui_threads"))
list_pool_size = int(os.getenv("list_pool_size"))

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',datefmt='%Y-%m-%d %H:%M:%S')

log = logging.getLogger("airbnb-detail-extractor")
log_stringio = io.StringIO()
handler = logging.StreamHandler(log_stringio)
handler.setFormatter(formatter)
log.addHandler(handler)


def getDatabase(databaseName):
    CONNECTION_STRING = CONNECTION_STRING_MONGODB
    client = MongoClient(CONNECTION_STRING)
    return client[databaseName]

monthly_tourist_percentage = {
    "Kenya": {
        "January": 9,
        "February": 8,
        "March": 8,
        "April": 7,
        "May": 6,
        "June": 8,
        "July": 10,
        "August": 11,
        "September": 9,
        "October": 9,
        "November": 8,
        "December": 7
    },
    "South Africa": {
        "January": 10,
        "February": 8,
        "March": 9,
        "April": 8,
        "May": 7,
        "June": 8,
        "July": 10,
        "August": 9,
        "September": 8,
        "October": 8,
        "November": 7,
        "December": 9
    },
    "Egypt": {
        "January": 8,
        "February": 7,
        "March": 8,
        "April": 9,
        "May": 7,
        "June": 6,
        "July": 8,
        "August": 9,
        "September": 8,
        "October": 9,
        "November": 8,
        "December": 13
    },
    "Morocco": {
        "January": 6,
        "February": 6,
        "March": 7,
        "April": 8,
        "May": 9,
        "June": 10,
        "July": 12,
        "August": 13,
        "September": 9,
        "October": 8,
        "November": 6,
        "December": 6
    },
    "Tanzania": {
        "January": 8,
        "February": 7,
        "March": 6,
        "April": 5,
        "May": 4,
        "June": 6,
        "July": 10,
        "August": 12,
        "September": 11,
        "October": 10,
        "November": 9,
        "December": 12
    },
    "Uganda": {
        "January": 9,
        "February": 8,
        "March": 7,
        "April": 6,
        "May": 5,
        "June": 7,
        "July": 11,
        "August": 12,
        "September": 10,
        "October": 9,
        "November": 8,
        "December": 8
    },
    "Ghana": {
        "January": 7,
        "February": 6,
        "March": 7,
        "April": 8,
        "May": 7,
        "June": 8,
        "July": 9,
        "August": 10,
        "September": 9,
        "October": 9,
        "November": 10,
        "December": 10
    },
    "Senegal": {
        "January": 8,
        "February": 7,
        "March": 8,
        "April": 9,
        "May": 8,
        "June": 7,
        "July": 9,
        "August": 10,
        "September": 9,
        "October": 8,
        "November": 9,
        "December": 8
    },
    "Gambia": {
        "January": 9,
        "February": 8,
        "March": 9,
        "April": 8,
        "May": 7,
        "June": 6,
        "July": 7,
        "August": 8,
        "September": 9,
        "October": 10,
        "November": 11,
        "December": 10
    },
    "Rwanda": {
        "January": 8,
        "February": 7,
        "March": 7,
        "April": 6,
        "May": 5,
        "June": 6,
        "July": 9,
        "August": 10,
        "September": 9,
        "October": 8,
        "November": 7,
        "December": 8
    },
    "Ethiopia": {
        "January": 7,
        "February": 6,
        "March": 7,
        "April": 8,
        "May": 7,
        "June": 6,
        "July": 8,
        "August": 9,
        "September": 8,
        "October": 9,
        "November": 8,
        "December": 9
    },
    "Nigeria": {
        "January": 8,
        "February": 7,
        "March": 8,
        "April": 9,
        "May": 8,
        "June": 7,
        "July": 9,
        "August": 10,
        "September": 9,
        "October": 8,
        "November": 9,
        "December": 8
    },
    "Democratic Republic of the Congo": {
        "January": 7,
        "February": 6,
        "March": 7,
        "April": 8,
        "May": 7,
        "June": 6,
        "July": 8,
        "August": 9,
        "September": 8,
        "October": 9,
        "November": 8,
        "December": 9
    },
    "Zambia": {
        "January": 7,
        "February": 6,
        "March": 7,
        "April": 8,
        "May": 8,
        "June": 9,
        "July": 11,
        "August": 12,
        "September": 11,
        "October": 9,
        "November": 7,
        "December": 5
    },
    "Zimbabwe": {
        "January": 7,
        "February": 6,
        "March": 7,
        "April": 8,
        "May": 8,
        "June": 9,
        "July": 11,
        "August": 12,
        "September": 11,
        "October": 9,
        "November": 7,
        "December": 5
    }
}


######### This should be removed after work completed on json file #########

import json

with open("price_zero_links.json", "r") as f:
    links = json.load(f)

log.info(f"Total items to be entertained (retry): {len(links)}")

########## commenting only for getting urls from json file ##########

# log.info('Fetching URLs from airbnb.com database')
# dbname_1=getDatabase(databaseName)
# links=[]
# collection_name_1 = dbname_1[collectionNameURLs]
# data_mongo=list(collection_name_1.find({},{'_id':False}))
# for i in data_mongo:
#     links.append([i['url'],i['propertyId'],i['newEntry'],i['city'],i['price'],i['discountedPrice'],i['currency'],i['pricingCriteria'],i['isSuperhost']])
# log.info(f'Total items to be entertained: {len(links)}')

def sendData(data,columns,databaseName,collectionName):
    try:
        log.info(f'Collected {len(data)} records!')
        df=pd.DataFrame(data,columns=columns)
        mongo_insert_data=df.to_dict('records')
        log.info('Sending Data to MongoDB!')
        def get_database():
            CONNECTION_STRING = CONNECTION_STRING_MONGODB
            client = MongoClient(CONNECTION_STRING)
            return client[databaseName]
        dbname = get_database()
        collection_name = dbname[collectionName]
        # for index,instance in enumerate(mongo_insert_data):
        #     collection_name.update_one({'propertyId':instance['propertyId']},{'$set':instance},upsert=True)
        for instance in mongo_insert_data:
            query = {'url': instance['url']}
            existing_entry = collection_name.find_one(query)
            if existing_entry is None:
                instance['dateListed'] = datetime.datetime.today().strftime('%Y-%m-%d')
                collection_name.insert_one(instance)
            else:
                collection_name.update_one(query, {'$set': instance})
        log.info('Data sent to MongoDB successfully')
    except Exception as e:
        log.info('Some error occurred while sending data MongoDB! Following is the error.')
        log.error(e)


def get_thread_list(current_session_data,threads,itemsPerThread):
    current_session_data_thread=[]
    for thread_no in range(1,threads+1):
        initial_index=(thread_no-1)*itemsPerThread
        final_index=thread_no*itemsPerThread
        current_session_data_thread.append(current_session_data[initial_index:final_index])
    return current_session_data_thread


def start_driver():
    chromeOptions = webdriver.ChromeOptions()
    chromeOptions.add_argument("--disable-popup-blocking")
    chromeOptions.add_argument("--headless")
    chromeOptions.add_argument("--window-size=1920,1080")
    driver = webdriver.Chrome(options=chromeOptions)
    try:
        driver.minimize_window()
    except:
        pass
    try:
        driver.maximize_window()
    except:
        pass
    return driver
    
def maximize(driver):
    try:
        driver.minimize_window()
    except:
        pass
    try:
        driver.maximize_window()
    except:
        pass
    
def get_nested(dct, keys, default=None):
    for key in keys:
        try:
            dct = dct[key]
        except (KeyError, IndexError, TypeError):
            return default
    return dct

def get_first(lst, default=None):
    try:
        return lst[0]
    except (IndexError, TypeError):
        return default


import datetime

def append_dates_to_url(url):
    # Choose a future date to avoid booking limits or min-stay issues
    today = datetime.date.today()
    check_in = today + datetime.timedelta(days=45)  # 45 days from today
    check_out = check_in + datetime.timedelta(days=5)
    params = f"?check_in={check_in}&check_out={check_out}&adults=1&guests=1"
    if "?" in url:
        # Already has params, so append with &
        return url + "&" + params.lstrip("?")
    else:
        return url + params

def get_airbnb_price(driver, url, max_wait=7):
    driver.get(url)
    price_text = None

    # First, try data-testid selector
    try:
        price_elem = WebDriverWait(driver, max_wait).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'span[data-testid="book-it-default-bar-price-amount"]'))
        )
        price_text = price_elem.text.strip()
    except Exception:
        # Fallback: check for the obfuscated class (e.g. "umg93v9")
        soup = BeautifulSoup(driver.page_source, "lxml")
        price_span = soup.find("span", class_=lambda x: x and "umg93v9" in x)
        if price_span:
            price_text = price_span.get_text(strip=True)
        else:
            # Fallback: any span with $<digits>
            for span in soup.find_all("span"):
                txt = span.get_text(strip=True)
                if re.match(r"\$\d+", txt):
                    price_text = txt
                    break

    # Extract price as float
    price_val = None
    if price_text:
        price_match = re.findall(r"[\d,]+", price_text)
        if price_match:
            price_val = float(price_match[0].replace(",", ""))
    return price_val

def find_consecutive_available_dates(calendar_data, nights=5):
    months = calendar_data.get("data", {}).get("merlin", {}).get("pdpAvailabilityCalendar", {}).get("calendarMonths", [])
    all_days = []
    for month in months:
        all_days.extend(month.get("days", []))
    # Create a list of dates that are bookable
    bookable_days = [d["calendarDate"] for d in all_days if d.get("bookable")]
    # Find a run of N consecutive days
    for i in range(len(bookable_days) - (nights - 1)):
        window = bookable_days[i:i+nights]
        dt_objs = [datetime.datetime.strptime(d, "%Y-%m-%d") for d in window]
        consecutive = all((dt_objs[j+1] - dt_objs[j]).days == 1 for j in range(nights - 1))
        if consecutive:
            return window[0], window[-1]
    return None, None


def start_thread_process(driver_instance,chrome_instance,*working_list):
    collectionNameDetails='propertyDetails'
    databaseName='airbnb'
    columnsDetails=['url','propertyId','newEntry','city','price','discountedPrice','currency','pricingCriteria','isSuperhost',
                    'superHostName','superHostLink','accuracyRating','bedType',
                    'checkinRating','cleanlinessRating','communicationRating','guestSatisfactionOverall','locationRating','personCapacity','roomType',
                    'valueRating','ReviewCount','imgUrl','propertyType','title','location','instantBook','amenities','description','guest','bedroom',
                    'bed','bath','latitude','longitude','cancellationPolicy',
                'recentReview','recentReviewRating','recentReviewDate','reviewsPerMonth', 'calendarData', 'monthlyOccupation', 'country']

    working_list=working_list[0]
    all_data=[]
    log.info(f"Thread-{chrome_instance} initialized chrome tab successfully.")
    driver_instance.get('https://www.airbnb.com/api/v3/')
    driver_instance.implicitly_wait(30)
    
    for index,i in enumerate(working_list):
        if len(all_data)%list_pool_size==0 and len(all_data)!=0:
            sendData(all_data,columnsDetails,databaseName,collectionNameDetails)
            all_data=[]
        try:
            
            options={
            "url": working_list[index][0],
            "elements": [
                {
                    "selector": "html"
                }
            ],
            "gotoOptions": {
                "timeout": 20000,
                "waitUntil": "networkidle2"
                }
            }
            response=requests.post('https://chrome.browserless.io/scrape?token=1d95a9c1-e2a4-4148-9530-cdada482be70&stealth', json=options)
            if "We can't seem to find the page you're looking for." in response.json()['data'][0]['results'][0]['text']:
                log.info(f'Lisitng Removed: {working_list[index][0]}')
                continue
            soup = BeautifulSoup(response.json()['data'][0]['results'][0]['html'], features="lxml")
            dom = etree.HTML(str(soup))
            
            url=working_list[index][0]
            propertyId=working_list[index][1]

            calendarData = driver_instance.execute_script('''
                var datas;
                function sleep(ms) {
                  return new Promise(resolve => setTimeout(resolve, ms));
                }
                async function fetchData() {
                    var response, data;
                    var url = 'https://www.airbnb.com/api/v3/PdpAvailabilityCalendar?operationName=PdpAvailabilityCalendar&locale=en&currency=USD&variables={"request":{"count":12,"listingId":"''' + str(propertyId) + '''","month":''' + str(datetime.datetime.now().month) + ''',"year":''' + str(datetime.datetime.now().year) + '''}}&extensions={"persistedQuery":{"version":1,"sha256Hash":"8f08e03c7bd16fcad3c92a3592c19a8b559a0d0855a84028d1163d4733ed9ade"}}';
                    var options = {
                        "headers": {
                            "accept": "*/*",
                            "accept-language": "en-US,en;q=0.9",
                            "content-type": "application/json",
                            "device-memory": "8",
                            "dpr": "1",
                            "ect": "4g",
                            "sec-fetch-dest": "empty",
                            "sec-fetch-mode": "cors",
                            "sec-fetch-site": "same-origin",
                            "viewport-width": "1920",
                            "x-airbnb-api-key": "API_KEY",
                            "x-airbnb-graphql-platform": "web",
                            "x-airbnb-graphql-platform-client": "minimalist-niobe",
                            "x-airbnb-supports-airlock-v2": "true",
                            "x-client-request-id": "1r9dijd06hqr9k0eib5a21v781x4",
                            "x-csrf-token": "null",
                            "x-csrf-without-token": "1",
                            "x-niobe-short-circuited": "true"
                        },
                        "referrer": "https://www.airbnb.com/rooms/''' + str(propertyId) + '''",
                        "referrerPolicy": "strict-origin-when-cross-origin",
                        "body": null,
                        "method": "GET",
                        "mode": "cors",
                        "credentials": "include"
                    };
                    var maxRetries = 10;
                    var retryDelay = 5000; // 2 seconds
                    for (var i = 0; i < maxRetries; i++) {
                        response = await fetch(url, options);
                        if (response.status === 429) {
                            // Status code 429 indicates rate limit exceeded
                            await sleep(retryDelay);
                            continue;
                        } else if (response.ok) {
                            data = await response.json();
                            return data;
                        } else {
                            // Other errors
                            throw new Error('Fetch failed with status ' + response.status);
                        }
                    }
                    // If max retries exceeded
                    throw new Error('Max retries exceeded');
                }
                return await fetchData();
            ''')
            if "errors" in calendarData:
                log.info(f"{propertyId} is removed.")
                continue

            # --- Find 5 consecutive available nights ---
            check_in, check_out = find_consecutive_available_dates(calendarData, nights=5)
            if check_in and check_out:
                price_url = f"{url}?check_in={check_in}&check_out={check_out}&adults=1&guests=1"
                price = get_airbnb_price(driver_instance, price_url)
                log.info(f"Fetched price for detail extractor: {price} for {price_url}")
            else:
                price = working_list[index][4]  # fallback to old price if dates not found
                log.info(f"Could not find 5 consecutive available dates for {url}. Using fallback price.")

            if not price or price == 0:
                price = working_list[index][4]

            
            newEntry=working_list[index][2]
            city=working_list[index][3]
            # price=working_list[index][4]
            discountedPrice=working_list[index][5]
            currency=working_list[index][6]
            # pricingCriteria=working_list[index][7]
            pricingCriteria="for 5 nights"
            isSuperhost=working_list[index][8]
            
            if len(dom.xpath('//a[@aria-label="Go to Host full profile"]/@href'))>0:
                superHostLink=dom.xpath('//a[@aria-label="Go to Host full profile"]/@href')[0]
                if 'airbnb' not in superHostLink:
                    superHostLink='https://www.airbnb.com'+superHostLink
            else:
                superHostLink=None
            if len(dom.xpath('//div[@data-section-id="HOST_OVERVIEW_DEFAULT"]//div[contains(text(),"Hosted")]'))>0:
                superHostName=dom.xpath('//div[@data-section-id="HOST_OVERVIEW_DEFAULT"]//div[contains(text(),"Hosted")]')[0].text.replace('Hosted by','').strip()
            else:
                superHostName=None

            

            MAX_ATTEMPTS = 3
            data = None
            for attempt in range(1, MAX_ATTEMPTS + 1):
                if attempt > 1:
                    # Re-request the page from browserless on retry
                    response = requests.post(
                        'https://chrome.browserless.io/scrape?token=1d95a9c1-e2a4-4148-9530-cdada482be70&stealth', 
                        json=options
                    )
                    soup = BeautifulSoup(response.json()['data'][0]['results'][0]['html'], features="lxml")
                    dom = etree.HTML(str(soup))
                    time.sleep(1)  # Small delay between attempts

                script_elems = dom.xpath("//script[@id='data-deferred-state-0']")
                if script_elems:
                    try:
                        data = json.loads(script_elems[0].text)
                        break
                    except Exception as e:
                        log.error(f"Attempt {attempt}: JSON parse error at {url}: {e}")
                else:
                    log.warning(f"Attempt {attempt}: No JSON script found for URL: {url}")

                if attempt < MAX_ATTEMPTS:
                    log.info(f"Retrying for URL: {url} (attempt {attempt+1})")
                else:
                    log.error(f"Failed to get JSON after {MAX_ATTEMPTS} attempts for URL: {url}")

            if data is None:
                continue
            # if data is not None:
            #     print(json.dumps(data, indent=2))
            if not (isinstance(data, dict) and 
                    'niobeMinimalClientData' in data and 
                    isinstance(data['niobeMinimalClientData'], list) and 
                    len(data['niobeMinimalClientData']) > 0):
                log.error(f"Malformed JSON (niobeMinimalClientData missing or invalid) for URL: {url}")
                continue

            accuracyRating = get_nested(
                data, [
                    'niobeMinimalClientData', 0, 1, 'data', 'presentation',
                    'stayProductDetailPage', 'sections', 'metadata', 'loggingContext',
                    'eventDataLogging', 'accuracyRating'
                ]
            )

            bedType = get_nested(
                data, [
                    'niobeMinimalClientData', 0, 1, 'data', 'presentation',
                    'stayProductDetailPage', 'sections', 'metadata', 'loggingContext',
                    'eventDataLogging', 'bedType'
                ]
            )

            checkinRating = get_nested(
                data, [
                    'niobeMinimalClientData', 0, 1, 'data', 'presentation',
                    'stayProductDetailPage', 'sections', 'metadata', 'loggingContext',
                    'eventDataLogging', 'checkinRating'
                ]
            )

            cleanlinessRating = get_nested(
                data, [
                    'niobeMinimalClientData', 0, 1, 'data', 'presentation',
                    'stayProductDetailPage', 'sections', 'metadata', 'loggingContext',
                    'eventDataLogging', 'cleanlinessRating'
                ]
            )

            communicationRating = get_nested(
                data, [
                    'niobeMinimalClientData', 0, 1, 'data', 'presentation',
                    'stayProductDetailPage', 'sections', 'metadata', 'loggingContext',
                    'eventDataLogging', 'communicationRating'
                ]
            )

            guestSatisfactionOverall = get_nested(
                data, [
                    'niobeMinimalClientData', 0, 1, 'data', 'presentation',
                    'stayProductDetailPage', 'sections', 'metadata', 'loggingContext',
                    'eventDataLogging', 'guestSatisfactionOverall'
                ]
            )

            locationRating = get_nested(
                data, [
                    'niobeMinimalClientData', 0, 1, 'data', 'presentation',
                    'stayProductDetailPage', 'sections', 'metadata', 'loggingContext',
                    'eventDataLogging', 'locationRating'
                ]
            )

            personCapacity = get_nested(
                data, [
                    'niobeMinimalClientData', 0, 1, 'data', 'presentation',
                    'stayProductDetailPage', 'sections', 'metadata', 'loggingContext',
                    'eventDataLogging', 'personCapacity'
                ]
            )

            roomType = get_nested(
                data, [
                    'niobeMinimalClientData', 0, 1, 'data', 'presentation',
                    'stayProductDetailPage', 'sections', 'metadata', 'loggingContext',
                    'eventDataLogging', 'roomType'
                ]
            )

            valueRating = get_nested(
                data, [
                    'niobeMinimalClientData', 0, 1, 'data', 'presentation',
                    'stayProductDetailPage', 'sections', 'metadata', 'loggingContext',
                    'eventDataLogging', 'valueRating'
                ]
            )

            ReviewCount = get_nested(
                data, [
                    'niobeMinimalClientData', 0, 1, 'data', 'presentation',
                    'stayProductDetailPage', 'sections', 'metadata', 'loggingContext',
                    'eventDataLogging', 'visibleReviewCount'
                ]
            )

            imgUrl = get_nested(
                data, [
                    'niobeMinimalClientData', 0, 1, 'data', 'presentation',
                    'stayProductDetailPage', 'sections', 'metadata', 'sharingConfig',
                    'imageUrl'
                ]
            )

            propertyType = get_nested(
                data, [
                    'niobeMinimalClientData', 0, 1, 'data', 'presentation',
                    'stayProductDetailPage', 'sections', 'metadata', 'sharingConfig',
                    'propertyType'
                ]
            )

            title = get_nested(
                data, [
                    'niobeMinimalClientData', 0, 1, 'data', 'presentation',
                    'stayProductDetailPage', 'sections', 'metadata', 'seoFeatures',
                    'ogTags', 'ogTitle'
                ]
            )
            if not title:
                title = get_nested(
                    data, [
                        'niobeMinimalClientData', 0, 1, 'data', 'presentation',
                        'stayProductDetailPage', 'sections', 'metadata', 'sharingConfig',
                        'title'
                    ]
                )
            location = get_nested(
                data, 
                ['niobeMinimalClientData', 0, 1, 'data', 'presentation', 'stayProductDetailPage', 'sections', 'metadata', 'seoFeatures', 'breadcrumbDetails', 0, 'searchText']
            )

            if not location:
                location = get_nested(
                    data, 
                    ['niobeMinimalClientData', 0, 1, 'data', 'presentation', 'stayProductDetailPage', 'sections', 'metadata', 'sharingConfig', 'location']
                )

            if location:
                parts = [part.strip() for part in location.split(',')]
                if len(parts) >= 2:
                    city = parts[-2]
                else:
                    city = parts[0]
            else:
                city = None

            instantBook = get_nested(
                data, [
                    'niobeMinimalClientData', 0, 1, 'data', 'presentation',
                    'stayProductDetailPage', 'sections', 'metadata',
                    'bookingPrefetchData', 'canInstantBook'
                ]
            )
            sections = get_nested(
                data,
                ['niobeMinimalClientData', 0, 1, 'data', 'presentation', 'stayProductDetailPage', 'sections', 'sections'],
                []
            )
            amenities = []
            for instance in sections:
                if instance.get('sectionComponentType') == 'AMENITIES_DEFAULT':
                    for amenity in instance.get('section', {}).get('seeAllAmenitiesGroups', []):
                        if len(amenity.get('amenities', [])) == 0:
                            break
                        if amenity['amenities'][0].get('title') != "Not included":
                            for singleAmenity in amenity['amenities']:
                                amenities.append(singleAmenity.get('title'))
                    break

            description = ""
            for instance in sections:
                if instance.get('sectionComponentType') == "DESCRIPTION_DEFAULT":
                    htmlDescription = get_nested(instance, ['section', 'htmlDescription'], None)
                    if htmlDescription:
                        htmlText = htmlDescription.get('htmlText')
                        if htmlText:
                            pattern = re.compile('<.*?>')
                            description = re.sub(pattern, ' ', htmlText).replace('  ', ' ').strip()
                        else:
                            description = ""
                    else:
                        description = ""
                    break

            if len(dom.xpath("//li[contains(text(),'guest')]"))!=0:
                guest=dom.xpath("//li[contains(text(),'guest')]/text()")[0]
                guest=int(re.findall(r'\d+',guest)[0])
            else:
                guest=None

            if len(dom.xpath("//li[contains(text(),'bedroom')]"))!=0:
                bedroom=dom.xpath("//li[contains(text(),'bedroom')]/text()")[0]
                bedroom=int(re.findall(r'\d+',bedroom)[0])
            else:
                bedroom=None
            
            bed = None
            if len(dom.xpath("//li[contains(text(),'bed') and not(contains(text(),'room'))]"))!=0:
                bed=dom.xpath("//li[contains(text(),'bed') and not(contains(text(),'room'))]/text()")[0]
                try:
                    bath=int(re.findall(r'\d+',bath)[0])
                except:
                    bath = 1
            if len(dom.xpath("//li[contains(text(),'bath')]"))!=0:
                bath=dom.xpath("//li[contains(text(),'bath')]/text()")[0]
                try:
                    bath=int(re.findall(r'\d+',bath)[0])
                except:
                    bath = 1
            else:
                bath=None
            
            calendarData = driver_instance.execute_script('''
                var datas;
                function sleep(ms) {
                  return new Promise(resolve => setTimeout(resolve, ms));
                }
                async function fetchData() {
                    var response, data;
                    var url = 'https://www.airbnb.com/api/v3/PdpAvailabilityCalendar?operationName=PdpAvailabilityCalendar&locale=en&currency=USD&variables={"request":{"count":12,"listingId":"''' + str(propertyId) + '''","month":''' + str(datetime.datetime.now().month) + ''',"year":''' + str(datetime.datetime.now().year) + '''}}&extensions={"persistedQuery":{"version":1,"sha256Hash":"8f08e03c7bd16fcad3c92a3592c19a8b559a0d0855a84028d1163d4733ed9ade"}}';
                    var options = {
                        "headers": {
                            "accept": "*/*",
                            "accept-language": "en-US,en;q=0.9",
                            "content-type": "application/json",
                            "device-memory": "8",
                            "dpr": "1",
                            "ect": "4g",
                            "sec-fetch-dest": "empty",
                            "sec-fetch-mode": "cors",
                            "sec-fetch-site": "same-origin",
                            "viewport-width": "1920",
                            "x-airbnb-api-key": "API_KEY",
                            "x-airbnb-graphql-platform": "web",
                            "x-airbnb-graphql-platform-client": "minimalist-niobe",
                            "x-airbnb-supports-airlock-v2": "true",
                            "x-client-request-id": "1r9dijd06hqr9k0eib5a21v781x4",
                            "x-csrf-token": "null",
                            "x-csrf-without-token": "1",
                            "x-niobe-short-circuited": "true"
                        },
                        "referrer": "https://www.airbnb.com/rooms/''' + str(propertyId) + '''",
                        "referrerPolicy": "strict-origin-when-cross-origin",
                        "body": null,
                        "method": "GET",
                        "mode": "cors",
                        "credentials": "include"
                    };
                    var maxRetries = 10;
                    var retryDelay = 5000; // 2 seconds
                    for (var i = 0; i < maxRetries; i++) {
                        response = await fetch(url, options);
                        if (response.status === 429) {
                            // Status code 429 indicates rate limit exceeded
                            await sleep(retryDelay);
                            continue;
                        } else if (response.ok) {
                            data = await response.json();
                            return data;
                        } else {
                            // Other errors
                            throw new Error('Fetch failed with status ' + response.status);
                        }
                    }
                    // If max retries exceeded
                    throw new Error('Max retries exceeded');
                }
                return await fetchData();
            ''')
            if "errors" in calendarData:
                log.info(f"{propertyId} is removed.")
                continue
            country = location.split(',')[-1].strip()
            month_data = {}
            for month in calendarData['data']['merlin']['pdpAvailabilityCalendar']['calendarMonths']:
                year = month['year']
                m = month['month']
                month_key = f"{year}-{str(m).zfill(2)}"
                total_days = len(month['days'])
                booked_days = sum(not day['available'] for day in month['days'])
                month_data[month_key] = {"total_days": total_days, "booked_days": booked_days}    

            tourist_percentages = monthly_tourist_percentage[country]
            average_monthly_arrivals = sum(tourist_percentages.values()) / len(tourist_percentages)
            seasonality_index = {
                month: percentage / average_monthly_arrivals
                for month, percentage in tourist_percentages.items()
            }            

            first_month_key = next(iter(month_data))
            first_month_name = first_month_key.split('-')[1]
            first_month_name = {
                "01": "January", "02": "February", "03": "March", "04": "April",
                "05": "May", "06": "June", "07": "July", "08": "August",
                "09": "September", "10": "October", "11": "November", "12": "December"
            }[first_month_name]
            
            observed_occupancy_first_month = (
                (month_data[first_month_key]['booked_days'] / month_data[first_month_key]['total_days']) * 100
            )
            
            normalized_occupancy_rate = min(
                100, observed_occupancy_first_month / seasonality_index[first_month_name]
            )
            
            predicted_occupancy = {}
            for month_key in month_data:
                month_name = {
                    "01": "January", "02": "February", "03": "March", "04": "April",
                    "05": "May", "06": "June", "07": "July", "08": "August",
                    "09": "September", "10": "October", "11": "November", "12": "December"
                }[month_key.split('-')[1]]
                predicted_rate = normalized_occupancy_rate * seasonality_index[month_name]
                predicted_occupancy[month_key] = min(100, predicted_rate)

            latitude = get_nested(
                data, [
                    'niobeMinimalClientData', 0, 1, 'data', 'presentation',
                    'stayProductDetailPage', 'sections', 'metadata', 'loggingContext',
                    'eventDataLogging', 'listingLat'
                ]
            )

            longitude = get_nested(
                data, [
                    'niobeMinimalClientData', 0, 1, 'data', 'presentation',
                    'stayProductDetailPage', 'sections', 'metadata', 'loggingContext',
                    'eventDataLogging', 'listingLng'
                ]
            )

            cancellationPolicy = get_nested(
                data, [
                    'niobeMinimalClientData', 0, 1, 'data', 'presentation',
                    'stayProductDetailPage', 'sections', 'metadata', 'bookingPrefetchData',
                    'cancellationPolicies', 0, 'book_it_module_tooltip'
                ]
            )

            reviewsData=driver_instance.execute_script('''
            var datas;
            await fetch('https://www.airbnb.com/api/v3/PdpReviews?operationName=PdpReviews&locale=en&currency=USD&variables={"request":{"fieldSelector":"for_p3","limit":7,"listingId":"'''+str(propertyId)+'''"}}&extensions={"persistedQuery":{"version":1,"sha256Hash":"22574ca295dcddccca7b9c2e3ca3625a80eb82fbdffec34cb664694730622cab"}}', {
              "headers": {
                "accept": "*/*",
                "accept-language": "en-US,en;q=0.9,ar;q=0.8",
                "content-type": "application/json",
                "device-memory": "8",
                "dpr": "1",
                "ect": "4g",
                "sec-ch-ua": "Chromium;v=112, Google Chrome;v=112, Not:A-Brand;v=99",
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": "Windows",
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-origin",
                "viewport-width": "1920",
                "x-airbnb-api-key": "API_KEY",
                "x-airbnb-graphql-platform": "web",
                "x-airbnb-graphql-platform-client": "minimalist-niobe",
                "x-airbnb-supports-airlock-v2": "true",
                "x-client-request-id": "0g7ruas0oxavzo06wmghl0axpja7",
                "x-csrf-token": "null",
                "x-csrf-without-token": "1",
                "x-niobe-short-circuited": "true"
              },
              "referrer": "https://www.airbnb.com/rooms/'''+str(propertyId)+'''",
              "referrerPolicy": "strict-origin-when-cross-origin",
              "body": null,
              "method": "GET",
              "mode": "cors",
              "credentials": "include"
            }).then((response) => response.json())
                                .then((data) => (datas = data));

                 return datas.data.merlin.pdpReviews.reviews;
            ''')
            
            if len(reviewsData)>0:
                recentReview=reviewsData[0]['comments']
                recentReviewDate=reviewsData[0]['localizedDate']
                recentReviewRating=reviewsData[0]['rating']
                records={}
                for inst in reviewsData:
                    if (inst['localizedDate']) not in records:
                        records[(inst['localizedDate'])]=1
                    else:
                        records[(inst['localizedDate'])]=records[(inst['localizedDate'])]+1
                reviewsPerMonth=math.ceil(sum(records.values())/len(records.values()))
            else:
                recentReviewDate=None
                recentReview=None
                recentReviewRating=None
                reviewsPerMonth=None 
            all_data.append([url,propertyId,newEntry,city,price,discountedPrice,currency,pricingCriteria,isSuperhost,
                    superHostName,superHostLink,accuracyRating,bedType,
                    checkinRating,cleanlinessRating,communicationRating,guestSatisfactionOverall,locationRating,personCapacity,roomType,
                    valueRating,ReviewCount,imgUrl,propertyType,title,location,instantBook,amenities,description,guest,bedroom,
                    bed,bath,latitude,longitude,cancellationPolicy,recentReview,recentReviewRating,recentReviewDate,reviewsPerMonth,
                    month_data, predicted_occupancy, country
                    ])
            log.info(url)
            vars_dict = {
                "url": url,
                "propertyId": propertyId,
                "newEntry": newEntry,
                "city": city,
                "price": price,
                "discountedPrice": discountedPrice,
                "currency": currency,
                "pricingCriteria": pricingCriteria,
                "isSuperhost": isSuperhost,
                "superHostName": superHostName,
                "superHostLink": superHostLink,
                "accuracyRating": accuracyRating,
                "bedType": bedType,
                "checkinRating": checkinRating,
                "cleanlinessRating": cleanlinessRating,
                "communicationRating": communicationRating,
                "guestSatisfactionOverall": guestSatisfactionOverall,
                "locationRating": locationRating,
                "personCapacity": personCapacity,
                "roomType": roomType,
                "valueRating": valueRating,
                "ReviewCount": ReviewCount,
                "imgUrl": imgUrl,
                "propertyType": propertyType,
                "title": title,
                "location": location,
                "instantBook": instantBook,
                "amenities": amenities,
                "description": description,
                "guest": guest,
                "bedroom": bedroom,
                "bed": bed,
                "bath": bath,
                "latitude": latitude,
                "longitude": longitude,
                "cancellationPolicy": cancellationPolicy,
                "recentReview": recentReview,
                "recentReviewRating": recentReviewRating,
                "recentReviewDate": recentReviewDate,
                "reviewsPerMonth": reviewsPerMonth,
                "month_data": month_data,
                "predicted_occupancy": predicted_occupancy,
                "country": country
            }

            for k, v in vars_dict.items():
                print(f"{k}: {v}")

        except Exception as e:
            log.error(f'Thread-{chrome_instance}: {e}')
            log.error(f'Error raised while scraping URL: {url}')
            

    if len(all_data) <list_pool_size-1:
        sendData(all_data,columnsDetails,databaseName,collectionNameDetails)

    driver_instance.quit()
    log.info(f"Chrome: {chrome_instance}: Extraction Completed and Data Send successfully.")


itemsPerThread=math.ceil(len(links)/threads)
current_session_data_thread=get_thread_list(links,threads,itemsPerThread) #3d list

driver_opened=[]
for idx,thread_list in enumerate(current_session_data_thread):
    driver_opened.append(start_driver())

threads_item=[]
for idx,thread_list in enumerate(current_session_data_thread):
    t1 = threading.Thread(target=start_thread_process,args=(driver_opened[idx],idx,current_session_data_thread[idx]),daemon=True)
    threads_item.append(t1)
    threads_item[-1].start()

for thread in threads_item:
    thread.join()

log.info("Chrome tabs closed successfully!")
log.info("Details extraction completed successfully.")
s3 = boto3.client("s3", region_name=aws_region_name)
s3.put_object(Body=log_stringio.getvalue(), Bucket=bucket_name, Key="logs/airbnb/detail-extractor-logs.txt")  
