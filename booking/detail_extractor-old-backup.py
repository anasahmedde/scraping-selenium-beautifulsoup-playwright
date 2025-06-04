from selenium.webdriver.common.by import By
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from pymongo import MongoClient
import pandas as pd
import os, io, logging, boto3, warnings, time, re, math
import concurrent.futures
from dotenv import load_dotenv
import numpy as np
from datetime import datetime

warnings.filterwarnings("ignore", category=DeprecationWarning) 
load_dotenv(override=True)

CONNECTION_STRING_MONGODB = os.getenv("CONNECTION_STRING")
aws_region_name = os.getenv("aws_region_name")
bucket_name = os.getenv("bucket_name")
threads = int(os.getenv("gui_threads"))
list_pool_size = int(os.getenv("list_pool_size"))

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',datefmt='%Y-%m-%d %H:%M:%S')

log = logging.getLogger("booking-detail-extractor")
log_stringio = io.StringIO()
handler = logging.StreamHandler(log_stringio)
handler.setFormatter(formatter)
log.addHandler(handler)


def sendData(data,columns,databaseName,collectionName):
    try:
        log.info(f'Collected {len(data)} records!')
        df=pd.DataFrame(data,columns=columns)
        datetime_columns = df.select_dtypes(include=['datetime64']).columns
        for col in datetime_columns:
            df[col] = df[col].fillna(pd.NaT).astype(str)
        mongo_insert_data=df.to_dict('records')
        log.info('Sending Data to MongoDB!')
        def get_database():
            CONNECTION_STRING = CONNECTION_STRING_MONGODB
            client = MongoClient(CONNECTION_STRING)
            return client[databaseName]
        dbname = get_database()
        collection_name = dbname[collectionName]
        # for index,instance in enumerate(mongo_insert_data):
        #     collection_name.update_one({'variantId':instance['variantId']},{'$set':instance},upsert=True)
        for instance in mongo_insert_data:
            query = {'variantId': instance['variantId']}
            existing_entry = collection_name.find_one(query)
            if existing_entry is None:
                instance['dateListed'] = datetime.today().strftime('%Y-%m-%d')
                collection_name.insert_one(instance)
            else:
                collection_name.update_one(query, {'$set': instance})
        log.info('Data sent to MongoDB successfully')
    except Exception as e:
        log.info('Some error occurred while sending data MongoDB! Following is the error.')
        log.error(e)
        
def getData():
    log.info("Fetching Stored URLs.")
    client = MongoClient(CONNECTION_STRING_MONGODB)

    db = client['booking']
    collection = db['propertyURLs']
    data = collection.find()
    
    return list(data)

def continuous_connection():
    clientC = MongoClient(CONNECTION_STRING_MONGODB)
    db = clientC['booking']
    return db['propertyURLs']


def get_hotel_data(url_chunk):
    results = []
    driver = webdriver.Chrome()
    driver.maximize_window()
    driver.get("https://www.booking.com/?selected_currency=USD")
    
    for url in url_chunk:
        log.info(f"Processing URL: {url}")
        if len(results)%list_pool_size==0 and len(results)!=0:
            sendData(results, columns, databaseName, 'propertyDetails')
            results=[]

        roomType, beds = None, None
        try:
            driver.get(url)
            try:
                title = driver.find_element(By.XPATH, "//div[@id='hp_hotel_name']//h2").text
            except:
                continue

            try:
                log.info("Waiting for photo gallery to load...")
                WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, "//span[@class='bh-photo-grid-thumb-more-inner-2']"))).click()
                log.info("Photo gallery loaded.")
                driver.execute_script("document.querySelector('div.bh-photo-modal-thumbs-grid__below').scrollIntoView()")
                time.sleep(3)
                driver.find_element(By.XPATH, "//button[@title='Close']").click()
                imagesXpath = "//img[@class='bh-photo-modal-grid-image']"
            except:
                imagesXpath = "//div[@class='clearfix bh-photo-grid bh-photo-grid--space-down fix-score-hover-opacity']//img"

            propertyId = driver.find_element(By.XPATH, "//input[@name='hotel_id']").get_attribute('value')
            stars = len(driver.find_elements(By.XPATH, "//span[starts-with(@data-testid, 'rating-')]//span"))
            address = driver.find_element(By.XPATH, "//p[@id='showMap2']/span").text
            country, city = address.split(',')[-1].strip(), address.split(',')[-2].strip()
            images = [img.get_attribute('src') for img in driver.find_elements(By.XPATH, imagesXpath)]
            description = ''.join(driver.find_element(By.XPATH, "//div[@class='hp_desc_main_content']").text.split('\n')[1:])
            highlights = driver.find_element(By.XPATH, "//div[@class='property-highlights ph-icon-fill-color']").text.replace('Property Highlights\n', '').replace('\nReserve', '')
            categoryRating = [rating.text.replace('\n', ' : ') for rating in driver.find_elements(By.XPATH, '//div[@data-testid="PropertyReviewsRegionBlock"]//div[@data-testid="review-subscore"]')]

            closestAirports = driver.execute_script('''
                try{
                    var list = [];
                    var items = document.evaluate("//div[text()='Closest Airports']/../../../ul//li", 
                        document, null, XPathResult.ORDERED_NODE_SNAPSHOT_TYPE, null);
                    for (var i = 0; i < items.snapshotLength; i++) {
                        list.push(items.snapshotItem(i).textContent.replace('Airport', 'Airport : '));
                    }
                    return list;
                }
                catch{
                    return null;
                }
            ''')

            try:
                checkIn = driver.find_element(By.XPATH, "//div[@id='checkin_policy']//span[@class='timebar__caption']").text
            except:
                checkIn = driver.find_element(By.XPATH, "//div[@id='checkin_policy']").text.replace('Check-in', '').strip() 

            try:
                checkOut = driver.find_element(By.XPATH, "//div[@id='checkout_policy']//span[@class='timebar__caption']").text
            except:
                checkOut = driver.find_element(By.XPATH, "//div[@id='checkout_policy']").text.replace('Check-out', '').strip() 

            features = driver.find_element(By.XPATH, "//div[@data-testid='property-most-popular-facilities-wrapper']").text.split('\n')
            breakfastIncluded = True if 'Breakfast' in highlights else False
            areaInfo = [info.text.replace('\n', ' : ') for info in driver.find_elements(By.XPATH, '(//div[@data-testid="property-section--content"])[2]//li')]
            
            moreBtns = driver.find_elements(By.XPATH, "//div[@class='hprt-facilities-block']//a")
            for element in moreBtns:
                try:
                    WebDriverWait(driver, 10).until(EC.element_to_be_clickable(element)).click()
                except:
                    WebDriverWait(driver, 10).until(EC.element_to_be_clickable(element)).click()
            try:
                if driver.find_element(By.XPATH, '//div[@data-testid="review-score-right-component"]'):
                    rating = float(driver.find_element(By.XPATH, '//div[@data-testid="review-score-right-component"]').text.split('\n')[0])
                    reviews = int(driver.find_element(By.XPATH, '//div[@data-testid="review-score-right-component"]').text.split('\n')[-1].split(' ')[0].replace(',', ''))
            except:
                rating, reviews = None, None
            
            variants = driver.find_elements(By.XPATH, "//table[@id='hprt-table']//tbody//tr")
            totalPrice = 0
            for room in variants:
                try:
                    totalPrice += float(room.get_attribute("data-hotel-rounded-price"))
                except TypeError:
                    totalPrice += float(room.find_element(By.CSS_SELECTOR, "span.prco-valign-middle-helper").text.replace('US$', ''))

            avgPrice = totalPrice/len(variants)
                
            for i in variants:
                variantId = i.get_attribute("data-block-id")
                breakfast, discountPercent, savings, taxesIncluded, taxAmount, refundPolicy, prePayment, cancellationPolicy = None, None, None, None, None, None, None, None
                if 'Max. people: ' in i.find_elements(By.TAG_NAME, "td")[-4].text:
                    sleeps = i.find_elements(By.TAG_NAME, "td")[-4].text.split('Max. people: ')[-1]
                else:
                    sleeps = i.find_elements(By.TAG_NAME, "td")[-4].text.split(' - ')[1].split(' ')[0]

                opts = [opts.text.replace('•\n', '') for opts in i.find_elements(By.TAG_NAME, "td")[-2].find_elements(By.TAG_NAME, "li")]
                for opt in opts:
                    if 'breakfast' in opt.lower():
                        breakfast = opt
                    elif 'refund' in opt.lower():
                        refundPolicy = opt
                    elif 'prepayment' in opt.lower() or 'advance' in opt.lower():
                        prePayment = opt
                    elif 'cancel' in opt.lower():
                        cancellationPolicy = opt

                try:
                    price = float(i.get_attribute("data-hotel-rounded-price"))
                except TypeError:
                    price = float(room.find_element(By.CSS_SELECTOR, "span.prco-valign-middle-helper").text.replace('US$', ''))
                    
                priceOpts = i.find_elements(By.TAG_NAME, "td")[-3].text.split('\n')
                for priceOpt in priceOpts:
                    if '% off' in priceOpt:
                        discountPercent = priceOpt
                    elif "You're saving US$" in priceOpt:
                        savings = int(priceOpt.replace("You're saving US$", ''))
                    elif 'taxes' in priceOpt:
                        taxesIncluded = True if 'Includes' in priceOpt else False
                        taxAmount = None if taxesIncluded else float(re.search(r"\$\d+(,\d+)*", priceOpt).group(0).replace('$', '').replace(",", ""))

                if len(i.find_elements(By.TAG_NAME, "td")) == 5:

                    main = i.find_elements(By.TAG_NAME, "td")[0]
                    roomType = main.find_element(By.CSS_SELECTOR, "span.hprt-roomtype-icon-link").text
                    try:
                        beds = main.find_element(By.CSS_SELECTOR, "div.hprt-roomtype-bed").text.split('\n')
                    except:
                        beds = None
                    try:
                        roomAvailability = main.find_element(By.CSS_SELECTOR, "div.thisRoomAvailabilityNew").text
                    except:
                        roomAvailability = None

                    facilities = [facilities.text for facilities in main.find_elements(By.CSS_SELECTOR, "div.hprt-facilities-facility")]
                    otherFacilities = [otherFacilities.text for otherFacilities in main.find_elements(By.CSS_SELECTOR, "ul.hprt-facilities-others li")]
                    amenities = facilities + otherFacilities
                    size = [x for x in amenities if "m²" in x][0] if [x for x in amenities if "m²" in x] else None
                    balcony = True if [x for x in amenities if "balcony" in x.lower()] else False
                    views = [x for x in amenities if "view" in x.lower()] if [x for x in amenities if "view" in x.lower()] else None

                priceStatus, priceDiff, priceChange = None, None, None
                data = singleItem.find_one({"mainUrl": driver.current_url.split('?')[0]})
                oldPrice = data['price'] if data else None
                priceDiff = max(oldPrice, avgPrice) - min(oldPrice, avgPrice) if oldPrice else 0
                priceChange = True if (priceDiff > 0) else False
                if avgPrice != oldPrice:
                    priceStatus = 'increased' if (avgPrice > oldPrice) else 'decreased'
                else:
                    priceStatus = None

                results.append([variantId, driver.current_url.split('?')[0], driver.current_url, title, propertyId, stars, address, country, city, images, description, highlights, categoryRating, closestAirports, checkIn, checkOut, features, breakfastIncluded, rating, reviews, int(sleeps), breakfast, refundPolicy, prePayment, cancellationPolicy, price, discountPercent, savings, taxesIncluded, taxAmount, roomType, beds, roomAvailability, amenities, size, balcony, views, round(avgPrice, 2), priceStatus, round(priceDiff, 2), priceChange, "$", "1 night, 1 adult", areaInfo])

            log.info(url)

        except Exception as e:
            pass
        
    if len(results)<list_pool_size:
        sendData(results, columns, databaseName, 'propertyDetails')
        log.info(f"Scraped {len(results)} properties so far...")
    driver.quit()

databaseName = 'booking'
columns = ["variantId", "mainUrl", "url", "title", "propertyId", "stars", "address", "country", "city", "images", "description", "highlights", "categoryRating", "closestAirports", "checkIn", "checkOut", "features", "breakfastIncluded", "rating", "reviews", "sleeps", "breakfast", "refundPolicy", "prePayment", "cancellationPolicy", "price", "discountPercent", "savings", "taxesIncluded", "taxAmount", "roomType", "beds", "roomAvailability", "amenities", "size", "balcony", "views", "avgPrice", "priceStatus", "priceDiff", "priceChange", "currency", "pricingCriteria", "areaInfo"]

datas = getData()
print(f"Total records: {len(datas)}")
links = [data['url'].strip() for data in datas]
singleItem = continuous_connection()

urls_per_thread = math.ceil(len(links) / threads)
url_chunks = [links[i:i+urls_per_thread] for i in range(0, len(links), urls_per_thread)]

with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
    futures = [executor.submit(get_hotel_data, url_chunk) for url_chunk in url_chunks]


s3 = boto3.client("s3", region_name=aws_region_name)
s3.put_object(Body=log_stringio.getvalue(), Bucket=bucket_name, Key="logs/booking/detail-extractor-logs.txt")  
log.info("Details extraction completed successfully.")
