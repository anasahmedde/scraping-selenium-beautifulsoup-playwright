import pandas as pd
from selenium import webdriver
import os, datetime, re, math, io, logging, boto3, threading
from pymongo import MongoClient
from dotenv import load_dotenv
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
import math
import re

load_dotenv(override=True)

databaseName='knightfrank_ug'
collectionNameURLs='propertyURLs'
collectionNameDetails='propertyDetails'
CONNECTION_STRING_MONGODB = os.getenv("CONNECTION_STRING")
aws_region_name = os.getenv("aws_region_name")
bucket_name = os.getenv("bucket_name")
threads = int(os.getenv("gui_threads"))
list_pool_size = int(os.getenv("list_pool_size"))

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',datefmt='%Y-%m-%d %H:%M:%S')

log = logging.getLogger("knightfrank-detail-extractor")
log_stringio = io.StringIO()
handler = logging.StreamHandler(log_stringio)
handler.setFormatter(formatter)
log.addHandler(handler)


def getDatabase(databaseName):
    CONNECTION_STRING = CONNECTION_STRING_MONGODB
    client = MongoClient(CONNECTION_STRING)
    return client[databaseName]


log.info('Fetching URLs from knightfrank database')
dbname_1=getDatabase(databaseName)
links=[]
collection_name_1 = dbname_1[collectionNameURLs]
data_mongo=list(collection_name_1.find({},{'_id':False}))
for i in data_mongo:
    links.append([i['url'],i['propertyId'],i['price'],i['currency'],i['pricingCriteria'],i['listingType']])
log.info(f'Total items to be entertained: {len(links)}')

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
        log.info('Some error occured while sending data MongoDB! Following is the error.')
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
    chromeOptions.add_argument("--headless")  # Run in headless mode
    chromeOptions.add_argument("--disable-dev-shm-usage")  # Overcome shared memory issues
    chromeOptions.add_argument("--no-sandbox")  # Disable sandbox (required for root users)
    chromeOptions.add_argument("--disable-gpu")  # Disable GPU (optional for headless)
    chromeOptions.add_argument("--remote-debugging-port=9222")  # Avoid port conflicts
    chromeOptions.add_argument("--disable-blink-features=AutomationControlled")  # Mask Selenium usage
    chromeOptions.add_argument("--window-size=1920,1080")  # Set a standard window size

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
    
def get_last_number(value):

    if not isinstance(value, str):
        return value 
    numbers = re.findall(r'\d+', value)
    return int(numbers[-1]) if numbers else None

def start_thread_process(driver_instance,chrome_instance,*working_list):
    databaseName='knightfrank_ug'
    collectionNameDetails='propertyDetails'
    columnsDetails = ['url','propertyId','price','currency','pricingCriteria','listingType','title', 'housingType', 'imgUrls', 'amenities', 'beds', 'baths', 'reception', 'parking', 'address', 'city', 'description', 'tenure', 'floorArea', 'land', 'agentName', 'agentContact']

    working_list=working_list[0]
    all_data=[]
    log.info(f"Thread-{chrome_instance} initialized chrome tab successfully.")
            
    for index,i in enumerate(working_list):

        if len(all_data)%list_pool_size==0 and len(all_data)!=0:
            sendData(all_data,columnsDetails,databaseName,collectionNameDetails)
            all_data=[]
        
        url=working_list[index][0]
        propertyId=working_list[index][1]
        price=working_list[index][2]
        currency=working_list[index][3]
        pricingCriteria=working_list[index][4]
        listingType=working_list[index][5]

        driver_instance.get(url)
        driver_instance.implicitly_wait(30)

              
        try:
            WebDriverWait(driver_instance, 20).until(
            EC.presence_of_element_located((By.XPATH,'/html/body/section/kf-search/ng-component/property-details/div[1]/div[1]/property-header/div/property-title/h1')))
        except TimeoutException:
            pass 

        try:   
            title = driver_instance.execute_script(''' return document.querySelector('.top-section h1').innerText''')
        except:
            title = None
        
        try:
            WebDriverWait(driver_instance, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR,'.pdp-carousel-open')))
        
            try:
                view_more_button = driver_instance.execute_script(
                    '''return document.querySelector(".pdp-carousel-open")'''
                )
                driver_instance.execute_script("arguments[0].scrollIntoView({block: 'center'});", view_more_button)
            
                # Click the button directly via JavaScript to bypass interception
                driver_instance.execute_script("arguments[0].click();", view_more_button)
                print("Clicked the 'View More' button.")
                imgUrls = driver_instance.execute_script(''' return Array.from(document.querySelectorAll(".slide-item img")).map(img => img.getAttribute('src') || img.getAttribute('data-lazy'))''')
                
            except Exception as e:
                imgUrls = driver_instance.execute_script('''
                return Array.from(document.querySelectorAll("[class^='pdp-image-'] img"))
                            .map(img => img.src);
                ''')
                print(f"Error clicking next button: {e}")
        
        except TimeoutException:
            print("button not found")
            try:  
                imgUrls = driver_instance.execute_script('''
                return Array.from(document.querySelectorAll("[class^='pdp-image-'] img"))
                            .map(img => img.src);
                ''')
            except:
                imgUrls = None
        
        try:   
            amenities = driver_instance.execute_script('''
                return Array.from(document.querySelectorAll('.all-amenities .amenity'))
                            .map(amenity => amenity.innerText);
            ''')
        except:
            amenities = None

        try: 
            housingType = driver_instance.execute_script(''' 
            const parentDiv = Array.from(document.querySelectorAll('.pdp-amenity')).find(div => 
            div.querySelector('.features-title')?.innerText.trim() === 'Property type'
            );
            return parentDiv.querySelector('span').innerText
            ''')
        except:
            housingType = None

        try:  
            tenure = driver_instance.execute_script(''' 
            const parentDiv = Array.from(document.querySelectorAll('.pdp-amenity')).find(div => 
            div.querySelector('.features-title')?.innerText.trim() === 'Tenure'
            );
            return parentDiv.querySelector('span').innerText
            ''')
        except:
            tenure = None

        try:  
            floorArea = driver_instance.execute_script(''' 
            const parentDiv = Array.from(document.querySelectorAll('.pdp-amenity')).find(div => 
            div.querySelector('.features-title')?.innerText.trim() === 'Floor area'
            );
            return parentDiv.querySelector('span').innerText
            ''')
        except:
            floorArea = None

        try:  
            land = driver_instance.execute_script(''' 
            const parentDiv = Array.from(document.querySelectorAll('.pdp-amenity')).find(div => 
            div.querySelector('.features-title')?.innerText.trim() === 'Land'
            );
            return parentDiv.querySelector('span').innerText
            ''')
        except:
            land = None
        
        
        try:
            beds = get_last_number(driver_instance.execute_script(''' return document.querySelector('.property-features .bed').innerText'''))
        except:
            if "land" not in housingType.lower():         
                continue
            beds = None
        
        try:
            baths = get_last_number(driver_instance.execute_script(''' return document.querySelector('.property-features .bath').innerText'''))   
        except:   
            if  "hotel" in housingType.lower():
                baths = beds
            elif "land" not in housingType.lower():
                continue
            else:
                baths = None
        
        try:
            reception = driver_instance.execute_script(''' return document.querySelector('.property-features .reception').innerText''')
        except:
            reception = None
        
        try:
            parking = driver_instance.execute_script(''' return document.querySelector('.property-features .parking').innerText''')
        except:
            parking = None
        
        try:
            address = driver_instance.execute_script(''' return document.querySelector('property-address-title .pdp-property-address').innerText + ', ' + document.querySelector('property-address-title .country').innerText''')
        except:
            address = None

        try:
            city = driver_instance.execute_script(''' return document.querySelector('ul[data-selenium-id="relatedAreas-list-locationSearch"] li a').innerText''')
        except:
            city = None

        try:
            description = driver_instance.execute_script(''' return document.querySelector(".main-description div").innerText''')
        except:
            description = None
        
        try:
            WebDriverWait(driver_instance, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR,'ul.ng-star-inserted > li:last-child button')))
        
            try:
                next_button = driver_instance.execute_script(
                    '''return document.querySelector('ul.ng-star-inserted > li:last-child button')'''
                )
          
                driver_instance.execute_script("arguments[0].scrollIntoView({block: 'center'});", next_button)
            
       
                driver_instance.execute_script("arguments[0].click();", next_button)
                print("Clicked the 'Next' button.")
                agentName = driver_instance.execute_script(''' return document.querySelector(".contact-name div").innerText''')
                agentContact = driver_instance.execute_script(''' return document.querySelector(".contact-phone div").innerText''')
            except Exception as e:
                agentName = ''
                agentContact = ''
                print(f"Error clicking next button: {e}")
                
        except TimeoutException:
            print("button not found") 
        
        all_data.append([url, propertyId, price, currency, pricingCriteria, listingType, title, housingType, imgUrls, amenities, beds, baths, reception, parking, address, city, description, tenure, floorArea, land, agentName, agentContact])

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
s3.put_object(Body=log_stringio.getvalue(), Bucket=bucket_name, Key="logs/knightfrank/detail-extractor-logs.txt")  
