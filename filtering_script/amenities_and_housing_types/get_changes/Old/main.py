from pymongo import MongoClient
import pandas as pd
from thefuzz import process
from thefuzz import fuzz
import ast
import json
from openai import OpenAI

api_key = 'API_KEY'
base_model = 'gpt-4-turbo'

df = pd.read_csv('amenities.csv')
#df = df.iloc[:1000]
df = df.dropna(subset=['amenities'])


invalid_categories = {
    "financial terms": ["mortgage", "loan", "interest rate", "finance option", "credit score"],
    "legal terms": ["lease agreement", "tenancy", "contract", "liability", "insurance"],
    "technical specifications": ["voltage", "wattage", "amperage", "technical support", "specification"],
    "operational details": ["maintenance schedule", "operating hours", "management policies", "staffing"],
    "construction details": ["foundation", "structural integrity", "load-bearing", "building codes"],
    "utility billing information": ["billing cycle", "meter reading", "utility rates", "payment plans"],
    "ownership information": ["ownership transfer", "title deed", "property tax", "equity"],
    "historical data": ["built in", "renovated in", "previous owners", "historical significance"],
    "marketing slogans": ["dream home", "once in a lifetime", "investment opportunity", "turnkey"],
    "community guidelines": ["HOA rules", "community regulations", "neighborhood watch", "curfew"],
    "basic rooms": ["bedroom", "bedrooms", "bathroom", "kitchen", "living room", "dining room"],
    "standard furniture": ["bed", "sofa", "chair", "table", "desk"],
    "common appliances": ["refrigerator", "stove", "oven", "microwave", "dishwasher"],
    "basic fixtures": ["sink", "toilet", "shower", "bathtub", "lighting"],
    "standard storage": ["closet", "cabinet", "shelf", "drawer", "wardrobe"],
    "decorative items": ["ornamental fountain", "decorative vase"],
    "non-functional features": ["fake fireplace", "non-functional chimney"],
    "excessive landscaping": ["elaborate garden design", "overgrown bushes"],
    "temporary structures": ["inflatable pool", "portable gazebo"],
    "ambiguous amenities": ["modern amenities", "contemporary features"],
    "generic descriptions": ["beautiful views", "stunning architecture"],
    "subjective features": ["charming atmosphere", "cozy ambiance"],
    "overused terms": ["luxury living", "premium amenities"],
    "community amenities": ["near", "nearby", "church nearby", "supermarket nearby", "mosques nearby", "hospital"],
    "infrastructure features": ["street lights"],
    "Internal measurements": ["Feet", "Sq.M", "Meters", "Metres", "Sq. Ft.", "Square", "Square Meters"]
}

all_amenities = []
for item in list(invalid_categories.values()):
    all_amenities.extend(item)


def is_match(amenity, all_amenities, threshold=70):
    for item in all_amenities:
        if fuzz.ratio(amenity.lower(), item.lower()) >= threshold:
            return True, item
    return False, None

def get_similar_amenities(df, all_amenities):
    amenity_category_dict = {}
    similar_amenities_dict = {}
    for _, row in df.iterrows():
        rehaniId = row['rehaniId']
        amenities = ast.literal_eval(row['amenities'])
        similar_amenities = []
        for amenity in amenities:
            match, all_amenity_item = is_match(amenity, all_amenities)
            if match:
                similar_amenities.append(amenity)
                if amenity not in amenity_category_dict:
                    amenity_category_dict[amenity] = all_amenity_item
        if similar_amenities:
            similar_amenities_dict[rehaniId] = similar_amenities
    return similar_amenities_dict, amenity_category_dict

similar_amenities_dict, amenity_category_dict = get_similar_amenities(df, all_amenities)
amenities_to_be_removed = []
for item in list(similar_amenities_dict.values()):
    amenities_to_be_removed.extend(item)

amenity_record = {}
for item in amenities_to_be_removed:
    if item in amenity_record:
        amenity_record[item] +=1
    else:
        amenity_record[item] = 1

amenities_to_be_removed = list(set(amenities_to_be_removed))
client = OpenAI(api_key=api_key, timeout=60)

message_content = (
    "Please confirm if the following amenities are valid real estate amenities. "
    "A residential amenity refers to any feature or facility located exclusively on the property, beyond the basic essentials of a home, that enhances the living experience and quality of life for residents. "
    "These amenities can include both indoor and outdoor facilities, such as:\n"
    "Indoor Amenities: Fitness centers, swimming pools, clubhouses, laundry facilities, and communal kitchens.\n"
    "Outdoor Amenities: Parks, playgrounds, gardens, walking paths, sports courts, and barbecue areas.\n"
    "Services and Conveniences: 24-hour security, maintenance services, concierge services, and on-site parking.\n\n"
    "Residential amenities are designed to provide convenience, comfort, recreation, and social opportunities, making the living environment more attractive and desirable. "
    "Buildings with such amenities are typically more expensive to buy into or rent. Therefore, the definition of an amenity should exclude basic elements expected in any home (e.g., bedrooms, living rooms, building materials), features not exclusively located on the property (e.g., hospitals, community centers), the size of the place, and building materials. "
    "The focus should instead be on features that add extra value and desirability to the property.\n\n"
    "For each amenity, provide 'Yes' or 'No' based on whether it should be considered a valid real estate amenity.\n\n"
    "Amenities:\n" + "\n".join([f"{item}" for item in amenities_to_be_removed]) +"\n Make sure response should be in JSON format for example {'amenity_name':True or False}"
)

ret = client.chat.completions.create(
    model=base_model,
    messages=[
        {
            "role": "user",
            "content": message_content
        }
    ],
    response_format =  { "type": "json_object" }
)

gptResponse = json.loads(ret.choices[0].message.content.strip())

def remove_amenities_from_df(df, similar_amenities_dict, gptResponse):
    final_df = df.copy()
    for rehaniId, amenities in similar_amenities_dict.items():
        row_index = final_df[final_df['rehaniId'] == rehaniId].index
        for idx in row_index:
            current_amenities = final_df.at[idx, 'amenities']
            current_amenities_list = ast.literal_eval(current_amenities)
            updated_amenities_list = [item for item in current_amenities_list if item not in amenities_to_be_removed or gptResponse.get(item)==True]
            final_df.at[idx, 'amenities'] = str(updated_amenities_list)
    return final_df

final_df = remove_amenities_from_df(df, similar_amenities_dict, gptResponse)
final_df['amenities'] = final_df['amenities'].apply(ast.literal_eval)

final_df.to_json('cleanedAmenityData.json', orient='records', indent=4)
with open('removedAmenities.json', 'w') as file:
    json.dump(amenities_to_be_removed, file, indent=4)

with open('amenityRecord.json', 'w') as file:
    json.dump(amenity_record, file, indent=4)

with open('amenityRemoveCategory.json', 'w') as file:
    json.dump(amenity_category_dict, file, indent=4)

with open('similarAmenitiesEveryRecord.json', 'w') as file:
    json.dump(similar_amenities_dict, file, indent=4)

with open('gptResponse.json', 'w') as file:
    json.dump(gptResponse, file, indent=4)
