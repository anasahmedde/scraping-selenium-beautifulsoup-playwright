import requests
import time

CACHE = {}
CACHE_EXPIRY = 86400  # 1 day in seconds

def currency_converter(selected_currency, base_currency):
    cache_key = f"{selected_currency}_{base_currency}"
    
    # Check if value exists in cache and is not expired
    if cache_key in CACHE:
        value, expiry = CACHE[cache_key]
        if time.time() < expiry:
            return float(value)
        else:
            del CACHE[cache_key]  # Remove expired cache item
    
    try:
        if selected_currency == base_currency:
            return 1
        
        result = convert(selected_currency, base_currency)
        if 'error' in result and result['error'] in ['base_currency not found', 'No valid target currencies found']:
            result1 = convert(base_currency, 'USD')
            result2 = convert(selected_currency, 'USD')
            converted_value = result2.get('data', {}).get(selected_currency, {}).get('value', 1) / \
                              result1.get('data', {}).get(base_currency, {}).get('value', 1)
        else:
            converted_value = result.get('data', {}).get(selected_currency, {}).get('value', 1)
        
        # Store in cache with an expiry time
        CACHE[cache_key] = (converted_value, time.time() + CACHE_EXPIRY)
        return converted_value
    except Exception as e:
        return 1

def convert(selected_currency, base_currency):
    url = "https://general-api.rehanisoko-internal.com/rehani/convert-currencies"
    headers = {"Content-Type": "application/json"}
    payload = {
        "target_currency": [selected_currency],
        "base_currency": base_currency,
    }
    
    response = requests.post(url, headers=headers, json=payload)
    print(response.json(), '*'*50)
    return response.json()
