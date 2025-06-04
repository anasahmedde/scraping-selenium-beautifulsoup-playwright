import pandas as pd
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
import time, os, re, ast, io, logging, boto3, warnings
from pymongo import MongoClient
from dotenv import load_dotenv
import math

warnings.filterwarnings("ignore", category=DeprecationWarning) 
load_dotenv(override=True)

CONNECTION_STRING_MONGODB = os.getenv("CONNECTION_STRING")
aws_region_name = os.getenv("aws_region_name")
bucket_name = os.getenv("bucket_name")

allURLs = {
    "sale": ['https://www.knightfrank.ug/properties/residential/for-sale/uganda/all-types/all-beds',
            'https://www.knightfrank.ug/properties/commercial/for-sale/uganda/all-types/all-beds'
        ],
    "rent": ['https://www.knightfrank.ug/properties/residential/to-let/uganda/all-types/all-beds','https://www.knightfrank.ug/properties/commercial/to-let/uganda/all-types/all-beds'
        ]
}



formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',datefmt='%Y-%m-%d %H:%M:%S')

log = logging.getLogger("knightfrank-url-extractor")
log_stringio = io.StringIO()
handler = logging.StreamHandler(log_stringio)
handler.setFormatter(formatter)
log.addHandler(handler)


def sendData(data,columns,databaseName,collectionName):
    try:
        log.info(f'Collected {len(data)} records!')
        print(f'Sending {len(data)} records to url collection.')
        df=pd.DataFrame(data,columns=columns)
        mongo_insert_data=df.to_dict('records')
        log.info('Sending Data to MongoDB!')
        def get_database():
            CONNECTION_STRING = CONNECTION_STRING_MONGODB
            client = MongoClient(CONNECTION_STRING)
            return client[databaseName]
        dbname = get_database()
        collection_name = dbname[collectionName]
        for index,instance in enumerate(mongo_insert_data):
            collection_name.update_one({'propertyId':instance['propertyId']},{'$set':instance},upsert=True)
        log.info('Data sent to MongoDB successfully')
    except Exception as e:
        log.info('Some error occured while sending data MongoDB! Following is the error.')
        log.error(e)

def removeDuplicates(lst):
    returnList=[]
    for instance in lst:
        if instance not in returnList:
            returnList.append(instance)
    return returnList

links=[]
columns=['url','propertyId','price','currency','pricingCriteria','listingType','housingType']
databaseName='knightfrank_ug'
collectionName='propertyURLs'


def start_chrome_tab():
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
    log.info("Successfully started chrome tab.")
    return driver

driver = start_chrome_tab()

for listingType in allURLs:
    for url in allURLs[listingType]:
        try:
            driver.get(url)
            driver.implicitly_wait(30)
    
            try:
                element = WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.XPATH,'//*[@id="results-top"]/div[1]/header-results-bar/div/div/div/div[1]/div/span')))
            except TimeoutException:
                pass 
    
            paginationText = driver.execute_script(''' return document.querySelector('.results-top-left .row .lead .ng-star-inserted').innerText''')
            numOfPages = int(paginationText.split(' ')[-1])
            listingsPerPage = int(paginationText.split(' ')[0].split('-')[-1])
            numberOfIterations = math.ceil(numOfPages/listingsPerPage)

    
            for iteration in range(numberOfIterations):
                log.info(f"Scraping iteration {iteration + 1}...")
    
                listingsOnCurrentPage = driver.execute_script(''' return document.querySelectorAll('.grid-view-switch-container .properties-item ')''')
               
                for listing in listingsOnCurrentPage:
        
                    try:
                        url = driver.execute_script(
                            '''return arguments[0].querySelector('.inner-wrapper > a')''', 
                            listing
                        ).get_attribute('href')
                        
                    except Exception as e:
                        continue
        
                    try:
                        propertyId = url.split('/')[-1]
                        
                    except Exception as e:
                        propertyId = ''
        
        
                    try:
                        price = driver.execute_script(
                            '''return parseFloat(arguments[0].querySelector('.inner-wrapper .grid-price').getAttribute('data-price-numerical'))''', 
                            listing
                        )
                    except Exception as e:
                        price = '' 
        
                    try:
                        currencyText = driver.execute_script(
                            '''return arguments[0].querySelector('.inner-wrapper .grid-price span').innerText''', 
                            listing
                        )
                        
                        currency_match = re.search(r'[^0-9,]+', currencyText)
                        if currency_match:
                            currency = currency_match.group(0).strip()
                        else:
                            currency = ''
                            
                    except Exception as e:
                        currency = ''
        
                    try:
                        housingType = driver.execute_script(
                            '''return arguments[0].querySelector('.inner-wrapper .grid-type').innerText''', 
                            listing
                        )
                        
                    except Exception as e:
                        housingType = ''
        
                    try:
                        if(listingType == 'sale'):
                            pricingCriteria = ''
                        else:
                            pricingCriteria = driver.execute_script(
                                '''return arguments[0].querySelector('.inner-wrapper .grid-prefix').innerText''', 
                                listing
                            )
                        
                    except Exception as e:
                        pricingCriteria = ''
    

                    links.append([url, propertyId, price, currency, pricingCriteria, listingType, housingType])
    
                # Click the "Next" button if more iterations remain
                if iteration < numberOfIterations - 1:
                    try:
                        next_button = driver.execute_script(
                            '''return document.querySelector('.pagination > li:last-child > a')'''
                        )
                        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", next_button)
    
                        # Click the button directly via JavaScript to bypass interception
                        driver.execute_script("arguments[0].click();", next_button)
                        log.info("Clicked the 'Next' button.")
            
                        # Wait for the next page to load
                        WebDriverWait(driver, 10).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, '.grid-view-switch-container .properties-item'))
                        )
                    except Exception as e:
                        log.error(f"Error clicking next button: {e}")
                        break
        
        except Exception as e:
            log.info("Error occured at URL: ",url)
            log.error(e)
            log.info("Restarting the Chrome tab!")
            driver.quit()
            driver = start_chrome_tab()
        
        finally:
            links=removeDuplicates(links)     
            sendData(links,columns,databaseName,collectionName) 
            links=[]

log.info("Removing duplicates from the collected records.")
links=removeDuplicates(links)       
sendData(links,columns,databaseName,collectionName) 
s3 = boto3.client("s3", region_name=aws_region_name)
s3.put_object(Body=log_stringio.getvalue(), Bucket=bucket_name, Key="logs/airbnb/url-extractor-logs.txt")  
driver.quit()
log.info("Chrome tab closed successfully!")
log.info("URL extraction completed successfully.")
