import json
import pandas as pd
import matplotlib.pyplot as plt
import os
from openai import OpenAI
import numpy as np
from thefuzz import process
from thefuzz import fuzz

# Ensure the directories exist
os.makedirs("Scrubber Out", exist_ok=True)
os.makedirs("Classifier Out", exist_ok=True)
os.makedirs("Classifier Out/support files", exist_ok=True)  # Create support files subfolder

# Global counter for warnings
warning_count = 0

# Threshold for data credibility
min_records_threshold = 30  # Example threshold for minimum number of records

# Exchange rate for Ugandan Shillings to USD
UGXtoUSD = 1  # Adjust this to the actual exchange rate if known

# Define a mapping for listing types to normalize them
listing_type_map = {
    'rent': 'Rent',
    'rent,leased': 'Rent',
    'sale': 'Sale',
    'sold': 'Sale'
}

def load_api_keys(file_path):
    with open(file_path) as file:
        try:
            return json.load(file)
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON from file {file_path}: {e}")
            return {}

def load_data(file_path):
    with open(file_path) as file:
        try:
            data = json.load(file)
            return data
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON from file {file_path}: {e}")
            return {}

def normalize_listing_type(listing_type):
    # Remove spaces and make lowercase
    listing_type_clean = listing_type.replace(' ', '').lower()
    # Map to normalized listing type
    return listing_type_map.get(listing_type_clean, listing_type_clean.capitalize())

def flatten_data(data):
    flat_data = []
    for amenity, countries in data.items():
        for country, property_types in countries.items():
            if country.lower().replace(" ", "") == "southafrica":
                country = "South Africa"
            for property_type, listing_types in property_types.items():
                for listing_type, price in listing_types.items():
                    if country == "Uganda":
                        price = price / UGXtoUSD  # Convert UGX to USD
                    # Normalize listing type
                    normalized_listing_type = normalize_listing_type(listing_type)
                    flat_data.append({
                        "Amenity": amenity,
                        "Country": country,
                        "Property_Type": property_type,
                        "Listing_Type": normalized_listing_type,
                        "Price": price  # Use the processed value directly
                    })
    return pd.DataFrame(flat_data)

def categorize_amenities(amenities, openai_api_key):
    message_content = (
        "Please categorize the following amenities into the specified categories. "
        "A residential amenity refers to any feature or facility located exclusively on the property, beyond the basic essentials of a home, that enhances the living experience and quality of life for residents. "
        "These amenities can include both indoor and outdoor facilities, such as:\n"
        "Indoor Amenities: Fitness centers, swimming pools, clubhouses, laundry facilities, and communal kitchens.\n"
        "Outdoor Amenities: Parks, playgrounds, gardens, walking paths, sports courts, and barbecue areas.\n"
        "Services and Conveniences: 24-hour security, maintenance services, concierge services, and on-site parking.\n\n"
        "Residential amenities are designed to provide convenience, comfort, recreation, and social opportunities, making the living environment more attractive and desirable. "
        "Buildings with such amenities are typically more expensive to buy into or rent. Therefore, the definition of an amenity should exclude basic elements expected in any home (e.g., bedrooms, living rooms, building materials), features not exclusively located on the property (e.g., hospitals, community centers), the size of the place, and building materials. "
        "The focus should instead be on features that add extra value and desirability to the property.\n\n"
        "General List of residential amenities:\n"
        "Safety & Security\n"
        "Power & Utilities\n"
        "Recreation & Leisure\n"
        "Technology & Connectivity\n"
        "Services and Conveniences\n"
        "Outdoor Amenities\n"
        "Parking & Transport\n"
        "General Residential Amenity (Please give a specific category in brackets)\n"
        "Not a residential amenity\n\n"
        "Amenities:\n" + "\n".join([f"{item}" for item in amenities]) +
        "\nPlease categorize each amenity in JSON format, for example: {'amenity_name': 'category'}"
    )

    # Save the outgoing amenities to a file called 0GPT.json
    with open('Classifier Out/support files/0GPT.json', 'w') as file:
        json.dump({amenity: "Unknown" for amenity in amenities}, file, indent=2)
    
    client = OpenAI(api_key=openai_api_key, timeout=600)  # Create client object

    try:
        response = client.chat.completions.create(
            model="gpt-4",
            messages=[
                {
                    "role": "user",
                    "content": message_content
                }
            ]
        )

        response_content = response.choices[0].message.content.strip()  # Correctly access message content

        # Replace single quotes with double quotes
        response_content_clean = response_content.replace("'", '"').replace('\n', ' ').replace('\r', '')

        # Check for incomplete JSON and attempt to fix
        if not response_content_clean.endswith('}'):
            print("Warning: JSON string is incomplete. Attempting to fix.")
            last_comma_index = response_content_clean.rfind(',')
            response_content_clean = response_content_clean[:last_comma_index] + ' }'

        # Save the response content to a file called 1GPT.json
        with open('Classifier Out/support files/1GPT.json', 'w') as file:
            json.dump(json.loads(response_content_clean), file)

        try:
            # Parse the cleaned-up response content
            categorized_amenities = json.loads(response_content_clean)
            return categorized_amenities
        except json.JSONDecodeError as e:
            print(f"Failed to decode JSON: {e}")
            return {}
    except Exception as e:
        print("Error here: ", e)
        return {}

def categorize_housing_types(housing_types, openai_api_key):
    message_content = (
        "Please categorize the following housing types into the specified categories. "
        "The categories are as follows:\n"
        "1-Bedroom Apartment, 2-Bedroom Apartment, 3-Bedroom Apartment, 4+ Bedroom Apartment, Studio Apartment, Penthouse, Apartment, Loft, Tiny home, "
        "1-Bedroom Condo, 2-Bedroom Condo, 3-Bedroom Condo, 4+ Bedroom Condo, Studio Condo, 1-Bedroom Villa, 2-Bedroom Villa, 3-Bedroom Villa, 4+ Bedroom Villa, "
        "Luxury Villa, 1-Bedroom Suite, 2-Bedroom Suite, Presidential Suite, Executive Suite, Junior Suite, Studio Suite, 1-Bedroom House, 2-Bedroom House, "
        "3-Bedroom House, 4+ Bedroom House, Single Family Home, Townhouse, 1-Bedroom Duplex, 2-Bedroom Duplex, 3-Bedroom Duplex, 4+ Bedroom Duplex, Multiplex, "
        "4-Bed Dormitory, 6-Bed Dormitory, 8-Bed Dormitory, Single Bed Dormitory, 1-Bedroom Bungalow, 2-Bedroom Bungalow, 3-Bedroom Bungalow, 4+ Bedroom Bungalow, "
        "Luxury Bungalow, 1-Bedroom Cottage, 2-Bedroom Cottage, 3-Bedroom Cottage, 4+ Bedroom Cottage, Luxury Cottage, 1-Bedroom Chalet, 2-Bedroom Chalet, "
        "3-Bedroom Chalet, 4+ Bedroom Chalet, Luxury Chalet, Single Room, Double Room, Queen Room, Twin Room, King Room, Agricultural Land, Farmhouse, Residential Land, "
        "Commercial Land, Mixed-Use Land, Shop, Office Space, Warehouse, Restaurant, Hotel Room, Guest House Room, Hostel Room, Bed and Breakfast Room, Entire Home, "
        "Entire Apartment, Entire Villa, Entire Cottage, Apartment Building, Block of Flats, Office Building, Standard Tent, Deluxe Tent, Luxury Tent, Camping Tent, "
        "Shared Room in Apartment, Shared Room in House, Shared Room in Hostel, Bus, Church, Co-working Space, Hall, Parking, Virtual Office\n\n"
        "Housing Types:\n" + "\n".join([f"{item}" for item in housing_types]) +
        "\nPlease categorize each housing type in JSON format, for example: {'housing_type_name': 'category'}"
    )

    # Save the outgoing housing types to a file called 0GPT_Housing.json
    with open('Classifier Out/support files/0GPT_Housing.json', 'w') as file:
        json.dump({housing_type: "Unknown" for housing_type in housing_types}, file, indent=2)
    
    client = OpenAI(api_key=openai_api_key, timeout=600)  # Create client object

    try:
        response = client.chat.completions.create(
            model="gpt-4",
            messages=[
                {
                    "role": "user",
                    "content": message_content
                }
            ]
        )

        response_content = response.choices[0].message.content.strip()  # Correctly access message content

        # Replace single quotes with double quotes
        response_content_clean = response_content.replace("'", '"').replace('\n', ' ').replace('\r', '')

        # Check for incomplete JSON and attempt to fix
        if not response_content_clean.endswith('}'):
            print("Warning: JSON string is incomplete. Attempting to fix.")
            last_comma_index = response_content_clean.rfind(',')
            response_content_clean = response_content_clean[:last_comma_index] + ' }'

        # Save the response content to a file called 1GPT_Housing.json
        with open('Classifier Out/support files/1GPT_Housing.json', 'w') as file:
            json.dump(json.loads(response_content_clean), file)

        try:
            # Parse the cleaned-up response content
            categorized_housing_types = json.loads(response_content_clean)
            return categorized_housing_types
        except json.JSONDecodeError as e:
            print(f"Failed to decode JSON: {e}")
            return {}
    except Exception as e:
        print("Error here: ", e)
        return {}

def reformat_categorized_amenities(categorized_amenities, detailed_data, country_filter, listing_type_filter, amenity_price_avg):
    reformatted_data = {}
    
    for amenity, category in categorized_amenities.items():
        if category not in reformatted_data:
            reformatted_data[category] = {}
        
        for country, property_types in detailed_data.get(amenity, {}).items():
            if country_filter != 'all' and country.lower() != country_filter.lower():
                continue
            
            if country not in reformatted_data[category]:
                reformatted_data[category][country] = {}
            
            for property_type, listing_types in property_types.items():
                for listing_type, price in listing_types.items():
                    if listing_type_filter != 'all' and listing_type.lower() != listing_type_filter.lower():
                        continue

                    if listing_type not in reformatted_data[category][country]:
                        reformatted_data[category][country][listing_type] = {}
                    
                    if property_type not in reformatted_data[category][country][listing_type]:
                        reformatted_data[category][country][listing_type][property_type] = {}
                    
                    categ = amenity_price_avg.loc[amenity_price_avg['Amenity'] == amenity, 'Category'].values[0] if amenity in amenity_price_avg['Amenity'].values else "Unclassified"

                    if categ not in reformatted_data[category][country][listing_type][property_type]:
                        reformatted_data[category][country][listing_type][property_type][categ] = []
                    reformatted_data[category][country][listing_type][property_type][categ].append(amenity)

    return reformatted_data

def reformat_categorized_housing_types(categorized_housing_types, df):
    for idx, row in df.iterrows():
        original_housing_type = row['Property_Type']
        if original_housing_type in categorized_housing_types:
            df.at[idx, 'Property_Type'] = categorized_housing_types[original_housing_type]
    return df

def summarize_data(df):
    summary = {
        "Country": [],
        "Listing_Type": [],
        "Total_Records": [],
        "Avg_Price_Total": [],
        "Non_Empty_Amenities_Records": [],
        "Avg_Price_Non_Empty": [],
        "Empty_Amenities_Records": [],
        "Avg_Price_Empty": []
    }
    
    grouped = df.groupby(['Country', 'Listing_Type'])
    
    for (country, listing_type), group in grouped:
        total_records = len(group)
        avg_price_total = group['Price'].mean() if total_records > 0 else 0
        
        non_empty_amenities = group[group['Amenity'].str.len() > 0]
        non_empty_records = len(non_empty_amenities)
        avg_price_non_empty = non_empty_amenities['Price'].mean() if non_empty_records > 0 else 0
        
        empty_amenities = group[group['Amenity'].str.len() == 0]
        empty_records = len(empty_amenities)
        avg_price_empty = empty_amenities['Price'].mean() if empty_records > 0 else 0
        
        summary["Country"].append(country)
        summary["Listing_Type"].append(listing_type)
        summary["Total_Records"].append(total_records)
        summary["Avg_Price_Total"].append(avg_price_total)
        summary["Non_Empty_Amenities_Records"].append(non_empty_records)
        summary["Avg_Price_Non_Empty"].append(avg_price_non_empty)
        summary["Empty_Amenities_Records"].append(empty_records)
        summary["Avg_Price_Empty"].append(avg_price_empty)
    
    return pd.DataFrame(summary)

def summarize_cleaned_data(file_path):
    # Load the cleaned data
    data = load_data(file_path)
    
    # Flatten data similar to flatten_data function
    flat_data = []
    for entry in data:
        country = entry.get('consolidatedCountry', '').replace("SouthAfrica", "South Africa").replace("South Africa", "South Africa")
        housing_type = entry.get('housingType', '')
        rent_type = entry.get('type', '')
        price = entry.get('price', 0) or 0
        amenities = entry.get('amenities', [])

        if country == "Uganda":
            price = price / UGXtoUSD  # Convert UGX to USD

        # Normalize listing type
        normalized_listing_type = normalize_listing_type(rent_type)
        flat_data.append({
            "Amenity": amenities,
            "Country": country,
            "Property_Type": housing_type,
            "Listing_Type": normalized_listing_type,
            "Price": price
        })

    df = pd.DataFrame(flat_data)

    # Summarize using the summarize_data logic
    return summarize_data(df)

def filter_data(df, country, listing_type):
    if country.lower() != 'all':
        df = df[df['Country'].str.lower() == country.lower()]
    if listing_type.lower() != 'all':
        df = df[df['Listing_Type'].str.lower() == listing_type.lower()]
    return df

def classify_amenities(df):
    if df.empty:
        return df

    # Group by Amenity and calculate average price
    amenity_price_avg = df.groupby('Amenity')['Price'].mean().reset_index()

    # Calculate IQR
    Q1 = amenity_price_avg['Price'].quantile(0.25)
    Q3 = amenity_price_avg['Price'].quantile(0.75)
    IQR = Q3 - Q1

    # Identify and filter outliers
    outlier_filter = (amenity_price_avg['Price'] >= (Q1 - 1.5 * IQR)) & (amenity_price_avg['Price'] <= (Q3 + 1.5 * IQR))
    filtered_amenity_price_avg = amenity_price_avg[outlier_filter]

    # Recalculate quartiles on filtered data
    basic_threshold = filtered_amenity_price_avg['Price'].quantile(0.25)
    standard_threshold = filtered_amenity_price_avg['Price'].quantile(0.5)
    luxury_threshold = filtered_amenity_price_avg['Price'].quantile(0.75)

    # Classify amenities
    def classify_amenity(price):
        if price <= basic_threshold:
            return 'Basic'
        elif price <= luxury_threshold:
            return 'Standard'
        else:
            return 'Luxury'

    filtered_amenity_price_avg['Category'] = filtered_amenity_price_avg['Price'].apply(classify_amenity)
    return filtered_amenity_price_avg

def check_and_correct_lists(df):
    for col in df.columns:
        for i, val in enumerate(df[col]):
            if isinstance(val, list):
                df.at[i, col] = val[0] if len(val) > 0 else None


def classify_housing_types(df):
    if df.empty:
        return df
    check_and_correct_lists(df)
    df = df.dropna(subset=['Property_Type', 'Price'])
    df['Property_Type'] = df['Property_Type'].astype(str)
    df['Price'] = pd.to_numeric(df['Price'], errors='coerce')    
    housing_type_price_avg = df.groupby('Property_Type')['Price'].mean().reset_index()

    # Calculate IQR
    Q1 = housing_type_price_avg['Price'].quantile(0.25)
    Q3 = housing_type_price_avg['Price'].quantile(0.75)
    IQR = Q3 - Q1

    # Identify and filter outliers
    outlier_filter = (housing_type_price_avg['Price'] >= (Q1 - 1.5 * IQR)) & (housing_type_price_avg['Price'] <= (Q3 + 1.5 * IQR))
    filtered_housing_type_price_avg = housing_type_price_avg[outlier_filter]

    # Recalculate quartiles on filtered data
    basic_threshold = filtered_housing_type_price_avg['Price'].quantile(0.25)
    standard_threshold = filtered_housing_type_price_avg['Price'].quantile(0.5)
    luxury_threshold = filtered_housing_type_price_avg['Price'].quantile(0.75)

    # Classify housing types
    def classify_housing_type(price):
        if price <= basic_threshold:
            return 'Basic'
        elif price <= luxury_threshold:
            return 'Standard'
        else:
            return 'Luxury'

    filtered_housing_type_price_avg['Category'] = filtered_housing_type_price_avg['Price'].apply(classify_housing_type)
    return filtered_housing_type_price_avg

def visualize_data(amenity_price_avg, housing_type_price_avg, country, listing_type, combined_plots, combined_plots_data, unit_name):
    if amenity_price_avg.empty or housing_type_price_avg.empty:
        print(f"No data available for {country} - {listing_type}.")
        return combined_plots_data

    if combined_plots:
        combined_plots_data[country] = (amenity_price_avg, housing_type_price_avg)
        return combined_plots_data
    
    output_filename = f'Classifier Out/support files/analysis_{country}_{listing_type}.png'
    
    plt.figure(figsize=(18, 6))

    plt.subplot(1, 3, 1)
    plt.boxplot(amenity_price_avg['Price'])
    plt.title(f'{country} - Amenities')
    plt.ylabel(f'Price (in {unit_name})')
    plt.xticks([1], ['Prices'])

    plt.subplot(1, 3, 2)
    amenity_price_avg.groupby('Category')['Price'].mean().plot(kind='bar', color=['blue', 'orange', 'green'])
    plt.title(f'{country} - Amenities')
    plt.ylabel(f'Average Price (in {unit_name})')
    plt.xlabel('Category')

    plt.subplot(1, 3, 3)
    plt.hist(amenity_price_avg['Price'], bins=10, color='skyblue', edgecolor='black')
    plt.title(f'{country} - Amenities')
    plt.xlabel(f'Price (in {unit_name})')
    plt.ylabel('Frequency')

    for ax in plt.gcf().axes:
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'{x:,.1f}'))

    plt.tight_layout()
    plt.savefig(output_filename)
    plt.close()

    return combined_plots_data

def combine_and_save_plots(combined_plots_data, listing_type, unit_name):
    if not combined_plots_data:
        print("No data available for combined plots.")
        return
    
    num_countries = len(combined_plots_data)
    
    plt.figure(figsize=(18, 6))
    for idx, (country, (amenity_df, housing_type_df)) in enumerate(combined_plots_data.items()):
        plt.subplot(2, num_countries, idx + 1)
        plt.hist(amenity_df['Price'], bins=10, color='skyblue', edgecolor='black')
        plt.title(f'{country} - Amenities')
        plt.xlabel(f'Price (in {unit_name})')
        plt.ylabel('Frequency')

        plt.subplot(2, num_countries, idx + num_countries + 1)
        plt.hist(housing_type_df['Price'], bins=10, color='lightgreen', edgecolor='black')
        plt.title(f'{country} - Housing Types')
        plt.xlabel(f'Price (in {unit_name})')
        plt.ylabel('Frequency')

    plt.tight_layout()
    for ax in plt.gcf().axes:
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'{x:,.1f}'))
    plt.savefig(f'Classifier Out/support files/combined_histogram_{listing_type}.png')
    plt.close()

def process_data(data, unit_scale):
    global warning_count
    result = {}
    housing_types = set()
    for entry in data:
        amenities = entry.get('amenities', None)
        
        if isinstance(amenities, list):
            amenity_list = amenities
        elif isinstance(amenities, dict):
            amenity_list = []
            for key in amenities.keys():
                if isinstance(amenities[key], list):
                    amenity_list.extend(amenities[key])
        else:
            warning_count += 1
            continue

        if not amenity_list:
            warning_count += 1
            continue

        country = entry.get('consolidatedCountry', '').replace("SouthAfrica", "South Africa").replace("South Africa", "South Africa")
        housing_type = entry.get('housingType', '')
        housing_types.add(housing_type)
        rent_type = entry.get('type', '')
        price = entry.get('price', 0) or 0

        if country == "Uganda":
            price = price / UGXtoUSD

        normalized_listing_type = normalize_listing_type(rent_type)

        for amenity in amenity_list:
            if amenity not in result:
                result[amenity] = {}
            if country not in result[amenity]:
                result[amenity][country] = {}
            if housing_type not in result[amenity][country]:
                result[amenity][country][housing_type] = {}
            result[amenity][country][housing_type][normalized_listing_type] = price / unit_scale

    return result, list(housing_types)

def check_data_sufficiency(df, country, listing_type):
    record_count = len(df)
    if record_count < min_records_threshold:
        return {
            "Credible": "No",
            "Count": record_count,
            "Mean": df['Price'].mean() if record_count > 0 else None,
            "Std": df['Price'].std() if record_count > 0 else None,
            "Min": df['Price'].min() if record_count > 0 else None,
            "25%": df['Price'].quantile(0.25) if record_count > 0 else None,
            "50%": df['Price'].median() if record_count > 0 else None,
            "75%": df['Price'].quantile(0.75) if record_count > 0 else None,
            "Max": df['Price'].max() if record_count > 0 else None
        }
    else:
        return {
            "Credible": "Yes",
            "Count": record_count,
            "Mean": df['Price'].mean(),
            "Std": df['Price'].std(),
            "Min": df['Price'].min(),
            "25%": df['Price'].quantile(0.25),
            "50%": df['Price'].median(),
            "75%": df['Price'].quantile(0.75),
            "Max": df['Price'].max()
        }

def main():
    global warning_count

    api_keys = load_api_keys('API Keys/HaniAiKeys.json')
    openai_api_key = api_keys.get("openAI", {}).get("apiKey", "")

    if not openai_api_key:
        print("OpenAI API key is missing or invalid.")
        return

    input_initial_json_path = 'Scrubber Out/cleanedAmenityData.json'
    input_housing_csv = 'Scrubber In/amenities.csv'
    output_json_base_path = 'Classifier Out/support files/amenityDist'
    output_csv_base_path = 'Classifier Out/support files/classification'
    output_plot_base_path = 'Classifier Out/support files/analysis'
    output_summary_path = 'Classifier Out/support files/data_sufficiency_summary.csv'
    output_records_summary_path = 'Classifier Out/support files/country_listing_summary.csv'
    output_categorized_amenities_path = 'Classifier Out/categorized_amenities_summary.json'

    # unit_input = input("Enter the unit to display prices in (units, thousands, or millions): ").strip().lower()
    unit_input = "units"
    if unit_input == 'units':
        unit_scale = 1
        unit_name = 'units'
    elif unit_input == 'thousands':
        unit_scale = 1e3
        unit_name = 'thousands'
    elif unit_input == 'millions':
        unit_scale = 1e6
        unit_name = 'millions'
    else:
        print("Invalid input. Defaulting to thousands.")
        unit_scale = 1e3
        unit_name = 'thousands'


    df = pd.read_csv(input_housing_csv)
    df = df.dropna(subset=['housingType']).reset_index(drop=True)
    df = df.replace({np.nan: None})
    df['housingType'] = df['housingType'].str.lower()    
    housing_mappings_df = pd.read_csv("inputHousingTypesCriteria.csv")
    housing_mappings = {}
    housing_mappings_df['Housing Type Category'] = housing_mappings_df['Housing Type Category'].str.lower()
    housing_mappings_df['Keywords'] = housing_mappings_df['Keywords'].str.lower()
    for idx, column in enumerate(housing_mappings_df['Housing Type Category']):
        housing_mappings[column] = [item.strip() for item in housing_mappings_df['Keywords'][idx].split(',')]      

    reverse_housing_mappings = {v: k for k, vs in housing_mappings.items() for v in vs}

    def is_match(housingType, housingTypes, threshold=80):
        best_match = None
        highest_score = threshold

        for item in housingTypes:
            score = fuzz.ratio(housingType.lower(), item.lower())
            if score > highest_score:
                highest_score = score
                best_match = item

        if best_match:
            return True, best_match
        else:
            return False, None

    specific_housing_type_mapping = {}

    for housingType in df['housingType'].unique():
        
        if housingType in reverse_housing_mappings:
            specific_housing_type_mapping[housingType] = reverse_housing_mappings[housingType]
        else:
            match, matched_item = is_match(housingType, list(set(reverse_housing_mappings.values())))
            specific_housing_type_mapping[housingType] = matched_item

    with open('Classifier Out\support files\mappingsFromCSV.json', 'w') as file:
        json.dump(specific_housing_type_mapping, file, indent=4)    

    unmapped_housing_types = []
    mapped_housing_types = []

    for key, value in specific_housing_type_mapping.items():
        if value is None:
            unmapped_housing_types.append(key)
        else:
            mapped_housing_types.append(key)

    specific_housing_type_mapping = {k: v for k, v in specific_housing_type_mapping.items() if v is not None}



    gptResponses = {}
    client = OpenAI(api_key=openai_api_key, timeout=600)
    for iter in range(0, len(unmapped_housing_types),50):
        message_content = (
            f"Following are the unmapped housing types coming from some external site.\n{unmapped_housing_types[iter:iter+50]}\nMap these unmapped housing types to the following given housing types exactly:\n {mapped_housing_types}"+
            "\n Make sure response should be in JSON format for example:  {'Commerce': 'Commercial Property, .....}. Make sure key should be same string as in unmapped housing type list given. Skip those which can not be mapped."
        )
        
        ret = client.chat.completions.create(
            model='gpt-4-turbo',
            messages=[
                {
                    "role": "user",
                    "content": message_content
                }
            ],
            response_format =  { "type": "json_object" }
        )
        gptResponse = json.loads(ret.choices[0].message.content.strip())
        gptResponses.update(gptResponse)   

    while True: 
        unmapped_housing_types_still = [item for item in unmapped_housing_types if item not in gptResponses]
        print(unmapped_housing_types_still)
        if unmapped_housing_types_still == []:
            break
        else:
            for iter in range(0, len(unmapped_housing_types_still),50):
                message_content = (
                    f"Following are the unmapped housing types coming from some external site.\n{unmapped_housing_types_still[iter:iter+50]}\nMap these unmapped housing types to the following given housing types exactly:\n {mapped_housing_types}"+
                    "\n Make sure response should be in JSON format for example:  {'Commerce': 'Commercial Property, .....}. Make sure key should be same string as in unmapped housing type list given. Skip those which can not be mapped."
                )
                
                ret = client.chat.completions.create(
                    model='gpt-4-turbo',
                    messages=[
                        {
                            "role": "user",
                            "content": message_content
                        }
                    ],
                    response_format =  { "type": "json_object" }
                )
                gptResponse = json.loads(ret.choices[0].message.content.strip())
                gptResponses.update(gptResponse)             


    with open('Classifier Out\support files\gptMappings.json', 'w') as file:
        json.dump(gptResponses, file, indent=4)    

    for key in list(gptResponses.keys()):
        if key in specific_housing_type_mapping:
            del gptResponses[key]

    for key, value in gptResponses.items():
        if value in specific_housing_type_mapping:
            gptResponses[key] = specific_housing_type_mapping[value]        

    specific_housing_type_mapping.update(gptResponses)
    
    with open('Classifier Out\support files\FinalMappingsGPTPlusCSV.json', 'w') as file:
        json.dump(specific_housing_type_mapping, file, indent=4)    
        
    initial_data = load_data(input_initial_json_path)
    if not initial_data:
        print("Initial data is missing or could not be loaded.")
        return
    
    for idx,item in enumerate(initial_data):
        if item['housingType']!=None and item['housingType'].lower() in specific_housing_type_mapping:
            initial_data[idx]['housingType'] = specific_housing_type_mapping[item['housingType'].lower()].capitalize()

    processed_data, housing_types = process_data(initial_data, unit_scale)

    output_json_path = f'{output_json_base_path}.json'
    with open(output_json_path, 'w') as file:
        json.dump(processed_data, file, indent=2)
    
    print(f"Processed data saved to {output_json_path}")

    loaded_data = load_data(output_json_path)
    if not loaded_data:
        print("Processed data could not be loaded.")
        return

    df = flatten_data(loaded_data)

    records_summary_df = summarize_data(df)
    records_summary_df.to_csv(output_records_summary_path, index=False)
    print(f"Country and listing type summary (processed data) saved to {output_records_summary_path}")

    cleaned_summary_df = summarize_cleaned_data(input_initial_json_path)
    with open(output_records_summary_path, 'a') as f:
        f.write("\n\nSummary for Cleaned Data\n")
    cleaned_summary_df.to_csv(output_records_summary_path, index=False, mode='a', header=True)
    print(f"Country and listing type summary (cleaned data) saved to {output_records_summary_path}")

    # country_input = input("Enter country (or 'all' for all countries, or 'each' to process each country individually): ").strip().lower()
    # listing_type_input = input("Enter listing type (Sale, Rent, Vacation, or 'all' for all listing types): ").strip().lower()
    country_input = 'all'
    listing_type_input = 'all'
    countries_to_process = df['Country'].unique().tolist()
    if country_input == 'all':
        pass
    elif country_input != 'each':
        countries_to_process = [country_input]

    listing_types_to_process = df['Listing_Type'].unique().tolist()
    if listing_type_input == 'all':
        listing_types_to_process = listing_types_to_process
    else:
        listing_types_to_process = [listing_type_input]

    summary_data = {}

    for listing_type in listing_types_to_process:
        print(listing_type)
        combined_plots_data = {}
        for country in countries_to_process:
            print(country)
            df_filtered = filter_data(df, country, listing_type)

            filtered_amenities = df_filtered['Amenity'].unique().tolist()
            with open('Classifier Out/support files/0GPT.json', 'w') as file:
                json.dump({amenity: "Unknown" for amenity in filtered_amenities}, file, indent=2)

            summary = check_data_sufficiency(df_filtered, country, listing_type)
            summary_data[f"{country}_{listing_type}"] = summary

            amenity_price_avg = classify_amenities(df_filtered)

            country_suffix = country if country != 'all' else 'all'
            listing_type_suffix = listing_type if listing_type != 'all' else 'all'
            country_listing_suffix = f'_{country_suffix}_{listing_type_suffix}'

            output_csv_path = f'{output_csv_base_path}{country_listing_suffix}.csv'

            if not amenity_price_avg.empty:
                amenity_price_avg.to_csv(output_csv_path, index=False)
                print(f"Classified amenities saved to {output_csv_path}")

            housing_types = df_filtered['Property_Type'].unique().tolist()
            categorized_housing_types = categorize_housing_types(housing_types, openai_api_key)
            df_filtered = reformat_categorized_housing_types(categorized_housing_types, df_filtered)

            housing_type_price_avg = classify_housing_types(df_filtered)
            combined_plots_data = visualize_data(amenity_price_avg, housing_type_price_avg, country, listing_type, True, combined_plots_data, unit_name)

        combine_and_save_plots(combined_plots_data, listing_type, unit_name)

    summary_df = pd.DataFrame(summary_data).T
    summary_df.to_csv(output_summary_path)
    print(f"Data sufficiency summary saved to {output_summary_path}")

    unique_amenities = df_filtered['Amenity'].unique().tolist()

    categorized_amenities = categorize_amenities(unique_amenities, openai_api_key)

    reformatted_amenities = reformat_categorized_amenities(categorized_amenities, loaded_data, country_input, listing_type_input, amenity_price_avg)

    with open(output_categorized_amenities_path, 'w') as file:
        json.dump(reformatted_amenities, file, indent=2)
    print(f"Categorized amenities summary saved to {output_categorized_amenities_path}")

    print(f"Total number of skipped entries due to missing or malformed amenities: {warning_count}")

if __name__ == "__main__":
    main()
