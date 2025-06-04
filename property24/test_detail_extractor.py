import requests
from bs4 import BeautifulSoup
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv(override=True)
ZENROWS_API_KEY = os.getenv("ZENROWS_API_KEY")

# Replace with any current, live property24.com ZA property detail URL
URL = "https://www.property24.com/for-sale/dalview/brakpan/gauteng/2271/115970263"

def fetch_html(url):
    resp = requests.get(
        "https://api.zenrows.com/v1/",
        params={"apikey": ZENROWS_API_KEY, "url": url},
        timeout=120
    )
    if resp.status_code == 404:
        print("404 - Listing disappeared.")
        return None
    resp.raise_for_status()
    return resp.text

def extract_property24_components(html):
    soup = BeautifulSoup(html, 'lxml')

    # Title
    title_el = soup.select_one('h1')
    title = title_el.get_text(strip=True) if title_el else None

    # # Location
    # location_el = soup.select_one('.p24_streetAddress')
    # location = location_el.get_text(strip=True) if location_el else None

    # Location (streetAddress → p24_mBM → breadcrumbs)

    import re

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
    price = price_el.get_text(strip=True).replace('R', '').replace(' ', '').replace(',', '') if price_el else None

    # Features
    beds = baths = parking = None
    for feat in soup.select('.p24_featureDetails'):
        label = feat.get('title', '').lower()
        val = feat.get_text(strip=True)
        if 'bedroom' in label: beds = val
        elif 'bathroom' in label: baths = val
        elif 'parking' in label: parking = val

    import re

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
        'street address', 'occupation date', 'deposit requirements',
        'levies', 'rates and taxes', 'type of property', 'floor size',
        'erf size', 'listing number', 'rates', 'levy', 'floor area',
        'stand size', 'listing date', 'price', 'bedrooms', 'bathrooms',
        'wall', 'roof', 'kitchens', 'office', 'reception rooms', 'floor', 'special feature'
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

    agent = None
    for selector in ['.p24_agent', '.p24_agentDetails', '.p24_agentInfo']:
        agent_el = soup.select_one(selector)
        if agent_el:
            # Take only the first line and ignore "Show Contact Number"
            name = agent_el.get_text(strip=True, separator='\n').split('\n')[0]
            if name and 'contact' not in name.lower():
                agent = name
                break

    agentPhone = None
    phone_links = soup.select('.p24_showNumbers a[href^="tel:"]')
    if phone_links:
        agentPhone = ', '.join(a.get_text(strip=True) for a in phone_links if a.get_text(strip=True))
    else:
        show_numbers = soup.select_one('.p24_showNumbers')
        if show_numbers:
            import re
            phone_matches = re.findall(r'(\+?\d[\d\s\-()]{7,})', show_numbers.get_text())
            filtered = [ph for ph in phone_matches if 'show' not in ph.lower()]
            if filtered:
                agentPhone = ', '.join(filtered)

    # City, Province
    crumbs = [a.get_text(strip=True) for a in soup.select('#breadCrumbContainer a')]
    city = crumbs[-2] if len(crumbs) >= 2 else None
    province = crumbs[-3] if len(crumbs) >= 3 else None

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

    # Print results for debug
    print("Title:", title)
    print("Location:", location)
    print("Price:", price)
    print("Beds:", beds)
    print("Baths:", baths)
    print("Parking:", parking)
    print("Erf Size:", erfSize)
    print("Floor Size:", floorSize)
    print("Amenities:", amenities)
    print("Images:", imgUrls)
    print("Description:", description)
    print("Agent:", agent)
    print("Agent Phone:", agentPhone)
    print("City:", city)
    print("Province:", province)
    print("Housing Type:", housingType)

if __name__ == "__main__":
    html = fetch_html(URL)
    if html:
        extract_property24_components(html)
