from selenium.webdriver.common.by import By
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from pymongo import MongoClient
import pandas as pd
import os, io, logging, boto3, warnings, time, re, math, json
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
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

log = logging.getLogger("booking-detail-extractor")
log_stringio = io.StringIO()
handler = logging.StreamHandler(log_stringio)
handler.setFormatter(formatter)
log.addHandler(handler)
logging.getLogger("urllib3").setLevel(logging.ERROR)

# Helper function for field extraction with logging.
def safe_extract(driver, by, locator, field_name, default_value=""):
    try:
        element = driver.find_element(by, locator)
        # If it is an input element, get its value; otherwise, its text.
        if element.tag_name.lower() == "input":
            value = element.get_attribute("value")
        else:
            value = element.text
        log.info(f"Extracted {field_name}: {value}")
        return value
    except Exception as e:
        log.error(f"Error extracting {field_name} using locator {locator}: {e}")
        return default_value

def sendData(data, columns, databaseName, collectionName):
    try:
        log.info(f'Collected {len(data)} records!')
        df = pd.DataFrame(data, columns=columns)
        datetime_columns = df.select_dtypes(include=['datetime64']).columns
        for col in datetime_columns:
            df[col] = df[col].fillna(pd.NaT).astype(str)
        mongo_insert_data = df.to_dict('records')
        log.info('Sending Data to MongoDB!')
        def get_database():
            CONNECTION_STRING = CONNECTION_STRING_MONGODB
            client = MongoClient(CONNECTION_STRING)
            return client[databaseName]
        dbname = get_database()
        collection_name = dbname[collectionName]
        # Update or insert each instance into the collection
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
    
    # Initialize URL counter to flush after every 10 URLs
    url_count = 0

    for url in url_chunk:
        url_count += 1
        log.info(f"Processing URL: {url}")
        # Set default values for fields so that even if one fails, the record is built.
        title = ""
        propertyId = ""
        stars = 0
        address = ""
        country = ""
        city = ""
        images = []
        description = ""
        highlights = ""
        categoryRating = []
        closestAirports = None
        checkIn = ""
        checkOut = ""
        features = []
        breakfastIncluded = False
        rating = None
        reviews = None
        sleeps = ""
        breakfast = ""
        refundPolicy = ""
        prePayment = ""
        cancellationPolicy = ""
        price = 0.0
        discountPercent = ""
        savings = ""
        taxesIncluded = False
        taxAmount = ""
        roomType = ""
        beds = ""
        roomAvailability = ""
        amenities = ""
        size = ""
        balcony = False
        views = ""
        avgPrice = 0.0
        priceStatus = ""
        priceDiff = 0.0
        priceChange = False
        currency = "$"
        pricingCriteria = "1 night, 1 adult"
        areaInfo = ""

        try:
            driver.get(url)
            # Extract title; if missing, record remains empty.
            # title = safe_extract(driver, By.XPATH, "//div[@id='hp_hotel_name']//h2", "title")
            #if not title:
            #    log.error("Title extraction failed; skipping this URL.")
            #    continue

            try:
                title = safe_extract(driver, By.XPATH, "//div[@id='hp_hotel_name']//h2", "title")
            except Exception as e:
                log.error("Exception occurred while extracting title: " + str(e), exc_info=False)
                title = ""

            if not title:
                log.error("Title extraction failed; skipping this URL.")
                continue

            # Photo gallery extraction with try/catch to avoid failing the script
            try:
                log.info("Waiting for photo gallery to load...")
                
                try:
                    gallery_button = WebDriverWait(driver, 5).until(
                        EC.element_to_be_clickable((By.XPATH, "//span[@class='bh-photo-grid-thumb-more-inner-2']"))
                    )
                    gallery_button.click()
                except Exception as e:
                    log.error(f"Error clicking gallery button: {e}", exc_info=False)
                try:
                    result = driver.execute_script("""
                        var elem = document.querySelector('div.bh-photo-modal-thumbs-grid__below');
                        if (elem) {
                            elem.scrollIntoView();
                            return "success";
                        } else {
                            return "element not found";
                        }
                    """)
                    if result != "success":
                        log.error("Element 'div.bh-photo-modal-thumbs-grid__below' not found while attempting to scroll into view.")
                except Exception as e:
                    log.error("Error executing scrollIntoView: " + str(e), exc_info=False)

                time.sleep(3)
                try:
                    driver.find_element(By.XPATH, "//button[@title='Close']").click()
                except Exception as e:
                    log.error(f"Error clicking close button: {e}", exc_info=False)
                # Use the refined XPath to extract only images without any class attribute
                imagesXpath = "//div[contains(@class, 'bh-photo-modal')]//img[not(@class) or normalize-space(@class)='']"
            except Exception as e:
                log.info("Photo gallery loading fallback triggered.")
                log.error("ERROR Exception " + str(e), exc_info=False)
                imagesXpath = "//div[@class='clearfix bh-photo-grid bh-photo-grid--space-down fix-score-hover-opacity']//img"

            # Extract hotel id from JavaScript (using execute_script)
            propertyId = driver.execute_script("return window.utag_data && window.utag_data.hotel_id ? window.utag_data.hotel_id : '';")
            if not propertyId:
                log.error("Hotel id not found in utag_data; skipping this URL.")
                continue

            # Extract stars count
            try:
                stars = len(driver.find_elements(By.XPATH, "//span[starts-with(@data-testid, 'rating-')]//span"))
                log.info(f"Extracted stars: {stars}")
            except Exception as e:
                log.error(f"Error extracting stars: {e}", exc_info=False)
                stars = 0

            # Modified Address Extraction from JSON-LD
            try:
                json_scripts = driver.find_elements(By.XPATH, "//script[@type='application/ld+json']")
                json_ld_data = None
                for script in json_scripts:
                    try:
                        data = json.loads(script.get_attribute("innerHTML"))
                        if isinstance(data, dict) and data.get("@type") == "Hotel":
                            json_ld_data = data
                            break
                    except Exception:
                        continue
                if json_ld_data and "address" in json_ld_data:
                    address_obj = json_ld_data["address"]
                    if isinstance(address_obj, dict):
                        full_address = address_obj.get("streetAddress", "")
                    else:
                        full_address = address_obj
                    address = full_address
                    parts = [part.strip() for part in full_address.split(",")]
                    country = parts[-1] if len(parts) >= 1 else ""
                    city = parts[-2] if len(parts) >= 2 else ""
                    log.info(f"Extracted address: {address}, city: {city}, country: {country}")
                else:
                    log.error("Address not found in JSON-LD data.")
            except Exception as e:
                log.error(f"Error extracting address from JSON-LD: {e}", exc_info=False)
                address = ""
                country = ""
                city = ""

            # Extract images using the refined XPath and getting currentSrc if available
            try:
                elements = driver.find_elements(By.XPATH, imagesXpath)
                log.info(f"Found {len(elements)} image elements using XPath: {imagesXpath}")
                images = []
                for img in elements:
                    image_url = img.get_attribute("currentSrc")
                    if not image_url:
                        image_url = img.get_attribute("src")
                    if "max200" in image_url:
                        image_url = image_url.replace("max200", "max1280x900")
                    images.append(image_url)
                log.info(f"Extracted {len(images)} images.")
            except Exception as e:
                log.error(f"Error extracting images: {e}", exc_info=False)
                images = []

            # Extract description
            description = safe_extract(driver, By.XPATH, "//div[@class='hp_desc_main_content']", "description")
            description = ''.join(description.split('\n')[1:])  # same processing as before

            # Extract highlights
            highlights = safe_extract(driver, By.XPATH, "//div[@class='property-highlights ph-icon-fill-color']", "highlights").replace('Property Highlights\n', '').replace('\nReserve', '')

            # Extract categoryRating
            try:
                categoryRating = [rating.text.replace('\n', ' : ') for rating in driver.find_elements(By.XPATH, '//div[@data-testid="PropertyReviewsRegionBlock"]//div[@data-testid="review-subscore"]')]
                log.info(f"Extracted categoryRating: {categoryRating}")
            except Exception as e:
                log.error(f"Error extracting categoryRating: {e}", exc_info=False)
                categoryRating = []

            # Extract closestAirports via JS
            closestAirports = driver.execute_script('''
                try{
                    var list = [];
                    var items = document.evaluate("//div[text()='Closest airports']/../../../ul//li", document, null, XPathResult.ORDERED_NODE_SNAPSHOT_TYPE, null);
                    for (var i = 0; i < items.snapshotLength; i++) {
                        list.push(items.snapshotItem(i).textContent.replace('Airport', 'Airport : '));
                    }
                    return list;
                }
                catch{
                    return null;
                }
            ''')

            # Check-in extraction
            # Extract checkIn using JavaScript to process the full policy block.
            try:
                checkIn = driver.execute_script("""
                    try {
                        // Locate the full policies section.
                        let checkInNode = document.evaluate(
                            "//section[@id='policies']//div[@data-testid='property-section--content']",
                            document, null,
                            XPathResult.FIRST_ORDERED_NODE_TYPE, null
                        ).singleNodeValue;
            
                        let fullPolicy = checkInNode ? checkInNode.textContent : "";
            
                        // Initialize checkIn variable.
                        let checkIn = "";
            
                        // If fullPolicy is found, extract only the check-in part.
                        if (fullPolicy) {
                            // If "Check-out" is present, assume the check-in policy is before that.
                            if (fullPolicy.indexOf("Check-out") !== -1) {
                                checkIn = fullPolicy.split("Check-out")[0];
                            } else {
                                checkIn = fullPolicy;
                            }
                            // Remove the "Check-in" label and any extra whitespace.
                            checkIn = checkIn.replace("Check-in", "").trim();
                            checkIn = checkIn.replace(/(\\d{1,2}:\\d{2})([A-Za-z])/g, "$1\\n$2");
                        }
            
                        return checkIn;
                    } catch(e) { 
                        return "";
                    }
                """)
                log.info("Extracted checkIn: " + checkIn)
            except Exception as e:
                log.error("Error extracting checkIn: " + str(e), exc_info=False)
                checkIn = ""


            # Check-out extraction
            # Extract checkOut using JavaScript in execute_script.
            try:
                checkOut = driver.execute_script("""
                    try {
                        // Locate the full policies section.
                        let policyNode = document.evaluate(
                            "//section[@id='policies']//div[@data-testid='property-section--content']",
                            document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null
                        ).singleNodeValue;
            
                        let fullPolicy = policyNode ? policyNode.textContent : "";
            
                        let checkOut = "";
                        if (fullPolicy) {
                            // Check if "Check-out" exists in the text.
                            if (fullPolicy.indexOf("Check-out") !== -1) {
                                // Get text after "Check-out".
                                checkOut = fullPolicy.split("Check-out")[1];
                                // Optionally, if there is a following label such as "Cancellation", cut text up to that.
                                if (checkOut.indexOf("Cancellation") !== -1) {
                                    checkOut = checkOut.split("Cancellation")[0];
                                }
                            }
                            checkOut = checkOut.trim();
                            // Insert a space after time if missing.
                            checkOut = checkOut.replace(/(\\d{1,2}:\\d{2})([A-Za-z])/g, "$1\\n$2");
                        }
                        return checkOut;
                    } catch(e) {
                        return "";
                    }
                """)
                log.info("Extracted checkOut: " + checkOut)
            except Exception as e:
                log.error("Error extracting checkOut: " + str(e), exc_info=False)
                checkOut = ""

            # Features extraction
            features = safe_extract(driver, By.XPATH, "//div[@data-testid='property-most-popular-facilities-wrapper']", "features").split('\n')

            # BreakfastIncluded flag
            breakfastIncluded = True if 'Breakfast' in highlights else False

            # Extract areaInfo
            try:
                areaInfo = [info.text.replace('\n', ' : ') for info in driver.find_elements(By.XPATH, '(//div[@data-testid="property-section--content"])[2]//li')]
                log.info(f"Extracted areaInfo: {areaInfo}")
            except Exception as e:
                log.error(f"Error extracting areaInfo: {e}", exc_info=False)
                areaInfo = []

            # Click extra facility buttons
            try:
                moreBtns = driver.find_elements(By.XPATH, "//div[@class='hprt-facilities-block']//a")
                for element in moreBtns:
                    try:
                        WebDriverWait(driver, 10).until(EC.element_to_be_clickable(element)).click()
                    except Exception:
                        WebDriverWait(driver, 10).until(EC.element_to_be_clickable(element)).click()
                log.info("Clicked additional facility buttons.")
            except Exception as e:
                log.error(f"Error clicking additional facility buttons: {e}", exc_info=False)

            # Extract rating and reviews
            try:
                rating_element = driver.find_element(By.XPATH, '//div[@data-testid="review-score-right-component"]')
                raw_text = rating_element.text
                log.info("Raw rating text: " + raw_text)
                
                # Use regex to extract the rating value (e.g., "8.0")
                rating_match = re.search(r"(\d\.\d+)", raw_text)
                rating = float(rating_match.group(1)) if rating_match else None
                
                # Use regex to extract the review count (e.g., "408 reviews")
                reviews_match = re.search(r"(\d+)\s+reviews", raw_text)
                reviews = int(reviews_match.group(1)) if reviews_match else None
                
                log.info(f"Extracted rating: {rating}, reviews: {reviews}")
            except Exception as e:
                log.error(f"Error extracting rating and reviews: {e}", exc_info=False)
                rating, reviews = None, None


            # Extract variants and price info
            variants = driver.find_elements(By.XPATH, "//table[@id='hprt-table']//tbody//tr")
            totalPrice = 0
            for room in variants:
                try:
                    totalPrice += float(room.get_attribute("data-hotel-rounded-price"))
                except TypeError:
                    try:
                        totalPrice += float(room.find_element(By.CSS_SELECTOR, "span.prco-valign-middle-helper").text.replace('US$', ''))
                    except Exception as e:
                        log.error(f"Error extracting room price: {e}", exc_info=False)
                        totalPrice += 0
            avgPrice = totalPrice / len(variants) if variants else 0

            # Iterate through each variant for detailed extraction
            for i in variants:
                variantId = i.get_attribute("data-block-id")
                breakfast, discountPercent, savings, taxesIncluded, taxAmount, refundPolicy, prePayment, cancellationPolicy = None, None, None, None, None, None, None, None

                try:
                    td_text = i.find_elements(By.TAG_NAME, "td")[-4].text
                    if 'Max. people: ' in td_text:
                        sleeps = td_text.split('Max. people: ')[-1]
                    else:
                        sleeps = td_text.split(' - ')[1].split(' ')[0]
                except Exception as e:
                    log.error(f"Error extracting sleeps: {e}", exc_info=False)
                    sleeps = ""

                try:
                    opts = [opt.text.replace('•\n', '') for opt in i.find_elements(By.TAG_NAME, "td")[-2].find_elements(By.TAG_NAME, "li")]
                    for opt in opts:
                        if 'breakfast' in opt.lower():
                            breakfast = opt
                        elif 'refund' in opt.lower():
                            refundPolicy = opt
                        elif 'prepayment' in opt.lower() or 'advance' in opt.lower():
                            prePayment = opt
                        elif 'cancel' in opt.lower():
                            cancellationPolicy = opt
                except Exception as e:
                    log.error(f"Error extracting option fields: {e}", exc_info=False)

                try:
                    try:
                        price = float(i.get_attribute("data-hotel-rounded-price"))
                    except TypeError:
                        price = float(i.find_element(By.CSS_SELECTOR, "span.prco-valign-middle-helper").text.replace('US$', ''))
                except Exception as e:
                    log.error(f"Error extracting price: {e}", exc_info=False)
                    price = 0.0
                    
                try:
                    priceOpts = i.find_elements(By.TAG_NAME, "td")[-3].text.split('\n')
                    for priceOpt in priceOpts:
                        if '% off' in priceOpt:
                            discountPercent = priceOpt
                        elif "You're saving US$" in priceOpt:
                            savings = int(priceOpt.replace("You're saving US$", ''))
                        elif 'taxes' in priceOpt:
                            taxesIncluded = True if 'Includes' in priceOpt else False
                            taxAmount = None if taxesIncluded else float(re.search(r"\$\d+(,\d+)*", priceOpt).group(0).replace('$', '').replace(",", ""))
                except Exception as e:
                    log.error(f"Error extracting price options: {e}", exc_info=False)
                
                # Extract room details if available
                if len(i.find_elements(By.TAG_NAME, "td")) == 5:
                    try:
                        main = i.find_elements(By.TAG_NAME, "td")[0]
                        roomType = safe_extract(main, By.CSS_SELECTOR, "span.hprt-roomtype-icon-link", "roomType")
                        try:
                            beds = safe_extract(main, By.CSS_SELECTOR, "div.hprt-roomtype-bed", "beds").split('\n')
                        except Exception as e:
                            log.error(f"Error extracting beds: {e}", exc_info=False)
                            beds = ""
                        try:
                            roomAvailability = safe_extract(main, By.CSS_SELECTOR, "div.thisRoomAvailabilityNew", "roomAvailability")
                        except Exception as e:
                            log.error(f"Error extracting roomAvailability: {e}", exc_info=False)
                            roomAvailability = ""
                        try:
                            facilities = [fac.text for fac in main.find_elements(By.CSS_SELECTOR, "div.hprt-facilities-facility")]
                            otherFacilities = [of.text for of in main.find_elements(By.CSS_SELECTOR, "ul.hprt-facilities-others li")]
                            amenities = facilities + otherFacilities
                        except Exception as e:
                            log.error(f"Error extracting amenities: {e}", exc_info=False)
                            amenities = ""
                        try:
                            size_list = [x for x in amenities if "m²" in x]
                            size = size_list[0] if size_list else ""
                        except Exception as e:
                            log.error(f"Error extracting size: {e}", exc_info=False)
                            size = ""
                        balcony = True if any("balcony" in x.lower() for x in amenities) else False
                        try:
                            views_list = [x for x in amenities if "view" in x.lower()]
                            views = views_list if views_list else ""
                        except Exception as e:
                            log.error(f"Error extracting views: {e}", exc_info=False)
                            views = ""
                    except Exception as e:
                        log.error(f"Error extracting room details: {e}", exc_info=False)

                try:
                    data = singleItem.find_one({"mainUrl": driver.current_url.split('?')[0]})
                    oldPrice = data['price'] if data and 'price' in data else None
                    if oldPrice is not None:
                        priceDiff = abs(avgPrice - oldPrice)
                        priceChange = (priceDiff > 0)
                        if avgPrice > oldPrice:
                            priceStatus = 'increased'
                        elif avgPrice < oldPrice:
                            priceStatus = 'decreased'
                        else:
                            priceStatus = None
                    else:
                        priceDiff = 0
                        priceChange = False
                        priceStatus = None
                except Exception as e:
                    log.error(f"Error comparing price with old data: {e}", exc_info=False)
                    priceDiff = 0
                    priceChange = False
                    priceStatus = ""

                results.append([
                    variantId,
                    driver.current_url.split('?')[0],
                    driver.current_url,
                    title,
                    propertyId,
                    stars,
                    address,
                    country,
                    city,
                    images,
                    description,
                    highlights,
                    categoryRating,
                    closestAirports,
                    checkIn,
                    checkOut,
                    features,
                    breakfastIncluded,
                    rating,
                    reviews,
                    int(sleeps) if sleeps.isdigit() else 0,
                    breakfast,
                    refundPolicy,
                    prePayment,
                    cancellationPolicy,
                    price,
                    discountPercent,
                    savings,
                    taxesIncluded,
                    taxAmount,
                    roomType,
                    beds,
                    roomAvailability,
                    amenities,
                    size,
                    balcony,
                    views,
                    round(avgPrice, 2),
                    priceStatus,
                    round(priceDiff, 2),
                    priceChange,
                    currency,
                    pricingCriteria,
                    areaInfo
                ])

            log.info(url)
            log.info(f"URL count: {url_count}, results length: {len(results)}")

        except Exception as e:
            log.error(f"An error occurred: {e}", exc_info=False)
            # Continue processing other URLs
            pass

        # Flush results after every 10 processed URLs
        if url_count % 10 == 0:
            if results:
                sendData(results, columns, databaseName, 'propertyDetails')
                results = []  # Clear memory after sending the batch
                log.info("Flushed results after processing 10 URLs.")

    # Send any remaining data after processing the last chunk of URLs.
    if results:
        sendData(results, columns, databaseName, 'propertyDetails')
        log.info(f"Flushed final batch with {len(results)} records.")

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
