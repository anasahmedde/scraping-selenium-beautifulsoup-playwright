from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import datetime
import re

def get_calendar_data(driver, property_id):
    now = datetime.datetime.now()
    calendar_js = f'''
        var done = arguments[0];
        var url = 'https://www.airbnb.com/api/v3/PdpAvailabilityCalendar?operationName=PdpAvailabilityCalendar&locale=en&currency=USD&variables={{"request":{{"count":12,"listingId":"{property_id}","month":{now.month},"year":{now.year}}}}}&extensions={{"persistedQuery":{{"version":1,"sha256Hash":"8f08e03c7bd16fcad3c92a3592c19a8b559a0d0855a84028d1163d4733ed9ade"}}}}';
        fetch(url, {{
            "headers": {{
                "accept": "*/*",
                "accept-language": "en-US,en;q=0.9",
                "content-type": "application/json",
                "x-airbnb-api-key": "API_KEY"
            }},
            "method": "GET",
            "credentials": "include"
        }}).then(resp => resp.json()).then(data => done(data)).catch(err => done(null));
    '''
    return driver.execute_async_script(calendar_js)

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
        # Check that the dates are consecutive
        dt_objs = [datetime.datetime.strptime(d, "%Y-%m-%d") for d in window]
        consecutive = all((dt_objs[j+1] - dt_objs[j]).days == 1 for j in range(nights - 1))
        if consecutive:
            return window[0], window[-1]
    return None, None

def get_airbnb_price(driver, url, max_wait=8):
    driver.get(url)
    price_text = None
    try:
        price_elem = WebDriverWait(driver, max_wait).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'span[data-testid="book-it-default-bar-price-amount"]'))
        )
        price_text = price_elem.text.strip()
    except Exception:
        # Fallbacks
        soup = BeautifulSoup(driver.page_source, "lxml")
        price_span = soup.find("span", class_=lambda x: x and "umg93v9" in x)
        if price_span:
            price_text = price_span.get_text(strip=True)
        else:
            for span in soup.find_all("span"):
                txt = span.get_text(strip=True)
                if re.match(r"\$\d+", txt):
                    price_text = txt
                    break

    price_val = None
    if price_text:
        price_match = re.findall(r"[\d,]+", price_text)
        if price_match:
            price_val = float(price_match[0].replace(",", ""))
    return price_val

if __name__ == "__main__":
    property_id = "1349923075352979083"
    base_url = f"https://www.airbnb.com/rooms/{property_id}"

    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")

    driver = webdriver.Chrome(options=chrome_options)
    driver.get(base_url)

    calendar_data = get_calendar_data(driver, property_id)
    check_in, check_out = find_consecutive_available_dates(calendar_data, nights=5)
    print("5 consecutive bookable dates found:", check_in, check_out)

    if check_in and check_out:
        price_url = f"{base_url}?check_in={check_in}&check_out={check_out}&adults=1&guests=1"
        price = get_airbnb_price(driver, price_url)
        print(f"Price found for {price_url}: {price}")
    else:
        print("Could not find 5 consecutive available dates.")

    driver.quit()
