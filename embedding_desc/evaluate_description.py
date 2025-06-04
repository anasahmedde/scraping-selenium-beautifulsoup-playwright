from openai import OpenAI
import json, os, requests, re
import pandas as pd
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_fixed

load_dotenv(override=True)
openai_req_timeout = 15
openai_key = os.getenv("openai_key")
base_model = 'gpt-3.5-turbo-0125'

def extract_numeric_value(value):
    # Strip any non-numeric characters (like currency symbols, commas, etc.)
    match = re.sub(r'[^\d.]', '', str(value))
    return float(match) if match else 0

def evaluate_description(df):
    results = []
    for index, row in df.iterrows():
        print(index, '----')
        try:
            result = generate_new_entries(row)
            results.extend(result)
        except Exception as e:
            print(e)
    return pd.DataFrame(results)

@retry(stop=stop_after_attempt(2), wait=wait_fixed(1))
def generate_new_entries(entry):

    client = OpenAI(api_key=openai_key, timeout=openai_req_timeout)
    systemMessage="""You are an intelligent QA bot who answers the questions asked by the user. You are provided with the apartments description and you need to answer all the questions from the description if possible else reply 'This information is not yet available.'\nThe response should be in JSON format. Example format of response: {'apartments_count': 3, 'apartments': [{'unit_name':'some string', 'baths': positive integer value, 'beds': positive integer value, 'size_sqft':'size in positive number in square feet (convert to sqft if in some other unit)', 'price': 'positive integer value', 'currency':'currency string in standard 3 digit format like USD, KES etc (not in symbols).'}, {...},{...}],'apartments_features':['spa','gym',...],'apartments_contact':[{'contact_name':'name in string','contact_email':'email of the person','contact_number':'03122029205'},{...},{...}]}.\nMake sure the answer in value must be in str type or list type and dont include linebreaks or tab characters in the response. If the contact number or email is not present for any person then it should be empty string. Attributes extracted that are size, price, beds and baths should be positive integers and do not convert or mention currencies in price attribute. If any query is not answerable then return the empty string response against that query, but make sure to answer in json always."""

    content_user=f"""This is the property title {entry['title']} and property description:\n{entry['description']}\nThese are the queries of the user:\n """+"""{
        "Unit_count": "How many individual units (apartments, plots of land, individual houses) are present in the description?",
        "Units": "Categorize the individual units in terms of unit_name, beds, bath, size (note the unit of measurement for dwellings vs. land), currency and price.",
        "features": "List any special features or amenities mentioned in the description",
        "contact": "Bring back any contact information"
    """

    ret = client.chat.completions.create(
        messages=[
            {
                "role": "system",
                "content": systemMessage,
            },
            {
                "role": "user",
                "content": content_user,
            },
        ],
        temperature=0.0,
        presence_penalty=1,
        frequency_penalty=1,
        max_tokens=1500,
        model=base_model,
        response_format =  { "type": "json_object" },
        timeout=10
    )

    res = json.loads(ret.choices[0].message.content)
    results = []

    if any([res.get('Units', []), res.get('apartments', [])]):
        units = res.get('Units', [])
        apartments = res.get('apartments', [])
        all_apartments = units + apartments
        if len(all_apartments) > 1:
            for apt_index, apt in enumerate(all_apartments):
                cleaned_item = {key.replace(' ', '').strip(): value for key, value in apt.items()}
                try:
                    conversion_factor = 1
                    if cleaned_item['currency'].replace(' ', '').strip() != 'USD' and cleaned_item['currency'].replace(' ', '').strip() != '':
                        url = "https://general-api.rehanisoko-internal.com/rehani/convert-currencies"

                        payload = json.dumps({
                            "target_currency": [
                                "USD"
                            ],
                            "base_currency": cleaned_item['currency'].replace(' ', '').strip()
                        })
                        headers = {
                            'Content-Type': 'application/json'
                        }

                        response = requests.request("POST", url, headers=headers, data=payload)

                        if response.status_code == 200:
                            res = response.json()
                            conversion_factor = res['data']['USD']['value']
                        else:
                            print(f"Currency conversion failed for entry: {cleaned_item}")

                    local_price = extract_numeric_value(cleaned_item.get('price', '0'))

                    new_entry = {
                        **entry,
                        'title': cleaned_item.get('unit_name'),
                        'baths': cleaned_item.get('baths'),
                        'beds': cleaned_item.get('beds'),
                        'localCurrency': (cleaned_item.get('currency') or '').replace(' ', '').strip(),
                        'localPrice': local_price,
                        'price': local_price * conversion_factor,
                        'internalArea': extract_numeric_value(cleaned_item.get('size_sqft', '0')),
                        'propertyId': f"{entry.get('propertyId')}_{apt_index}",
                        'rehaniId': f"{entry.get('rehaniId')}_{apt_index}",
                        'amenities': (entry.get('features', []) if isinstance(entry.get('features', []), list) else [entry.get('features', [])]) +
                            (entry.get('apartments_features', []) if isinstance(entry.get('apartments_features', []), list) else [entry.get('apartments_features', [])]) +
                            (entry.get('apartment_features', []) if isinstance(entry.get('apartment_features', []), list) else [entry.get('apartment_features', [])]) +
                            (cleaned_item.get('amenities', []) if isinstance(cleaned_item.get('amenities', []), list) else [cleaned_item.get('amenities', [])]),
                        'agent': entry.get('agent') or cleaned_item.get('contact', [{}])[0].get('contact_name', ''),
                        'agentContact': entry.get('agentContact') or cleaned_item.get('contact', [{}])[0].get('contact_number', ''),
                        'agentEmailAddress': entry.get('agentEmailAddress') or cleaned_item.get('contact', [{}])[0].get('contact_email', ''),
                    }
                    results.append(new_entry)
                except Exception as e:
                    print(e, '---------------------------------------------->', apt)

    return results