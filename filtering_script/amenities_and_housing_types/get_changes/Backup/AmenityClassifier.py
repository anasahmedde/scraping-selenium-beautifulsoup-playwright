import json
import pandas as pd
import matplotlib.pyplot as plt
import os
from openai import OpenAI
import numpy as np

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

def visualize_data(amenity_price_avg, country, listing_type, combined_plots, combined_plots_data, unit_name):
    if amenity_price_avg.empty:
        print(f"No data available for {country} - {listing_type}.")
        return combined_plots_data

    if combined_plots:
        # Append the data to combined plots data
        combined_plots_data[country] = amenity_price_avg
        return combined_plots_data
    
    # Single country chart
    output_filename = f'Classifier Out/support files/amenity_analysis_{country}_{listing_type}.png'
    
    plt.figure(figsize=(18, 6))

    # Box plot for price distribution
    plt.subplot(1, 3, 1)
    plt.boxplot(amenity_price_avg['Price'])
    plt.title(f'{country}')
    plt.ylabel(f'Price (in {unit_name})')
    plt.xticks([1], ['Prices'])

    # Bar plot for average prices per category
    plt.subplot(1, 3, 2)
    amenity_price_avg.groupby('Category')['Price'].mean().plot(kind='bar', color=['blue', 'orange', 'green'])
    plt.title(f'{country}')
    plt.ylabel(f'Average Price (in {unit_name})')
    plt.xlabel('Category')

    # Histogram for price distribution
    plt.subplot(1, 3, 3)
    plt.hist(amenity_price_avg['Price'], bins=10, color='skyblue', edgecolor='black')
    plt.title(f'{country}')
    plt.xlabel(f'Price (in {unit_name})')
    plt.ylabel('Frequency')

    # Format y-axis labels to one decimal place
    for ax in plt.gcf().axes:
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'{x:,.1f}'))

    # Save the figure without showing it
    plt.tight_layout()
    plt.savefig(output_filename)
    plt.close()  # Close the plot instead of showing it
    return combined_plots_data

def combine_and_save_plots(combined_plots_data, listing_type, unit_name):
    if not combined_plots_data:
        print("No data available for combined plots.")
        return
    
    num_countries = len(combined_plots_data)
    
    # Combined Histogram
    plt.figure(figsize=(18, 6))
    for idx, (country, df) in enumerate(combined_plots_data.items()):
        plt.subplot(1, num_countries, idx + 1)
        plt.hist(df['Price'], bins=10, color='skyblue', edgecolor='black')
        plt.title(f'{country}')
        plt.xlabel(f'Price (in {unit_name})')
        plt.ylabel('Frequency')
    plt.tight_layout()
    for ax in plt.gcf().axes:
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'{x:,.1f}'))
    plt.savefig(f'Classifier Out/support files/combined_histogram_{listing_type}.png')
    plt.close()

    # Combined Bar Plot
    plt.figure(figsize=(18, 6))
    for idx, (country, df) in enumerate(combined_plots_data.items()):
        plt.subplot(1, num_countries, idx + 1)
        df.groupby('Category')['Price'].mean().plot(kind='bar', color=['blue', 'orange', 'green'])
        plt.title(f'{country}')
        plt.ylabel(f'Average Price (in {unit_name})')
        plt.xlabel('Category')
    plt.tight_layout()
    for ax in plt.gcf().axes:
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'{x:,.1f}'))
    plt.savefig(f'Classifier Out/support files/combined_barplot_{listing_type}.png')
    plt.close()

    # Combined Box Plot
    plt.figure(figsize=(18, 6))
    for idx, (country, df) in enumerate(combined_plots_data.items()):
        plt.subplot(1, num_countries, idx + 1)
        plt.boxplot(df['Price'])
        plt.title(f'{country}')
        plt.ylabel(f'Price (in {unit_name})')
        plt.xticks([1], ['Prices'])
    plt.tight_layout()
    for ax in plt.gcf().axes:
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'{x:,.1f}'))
    plt.savefig(f'Classifier Out/support files/combined_boxplot_{listing_type}.png')
    plt.close()

def process_amenities(data, unit_scale):
    global warning_count
    result = {}
    for entry in data:
        amenities = entry.get('amenities', None)
        
        # Check if amenities is a list
        if isinstance(amenities, list):
            amenity_list = amenities
        # Check if amenities is a dictionary
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
        rent_type = entry.get('type', '')
        price = entry.get('price', 0) or 0  # Ensure price is not None and use 0 as default

        if country == "Uganda":
            price = price / UGXtoUSD  # Convert UGX to USD

        # Normalize listing type
        normalized_listing_type = normalize_listing_type(rent_type)

        for amenity in amenity_list:
            if amenity not in result:
                result[amenity] = {}
            if country not in result[amenity]:
                result[amenity][country] = {}
            if housing_type not in result[amenity][country]:
                result[amenity][country][housing_type] = {}
            result[amenity][country][housing_type][normalized_listing_type] = price / unit_scale  # Correct division for chosen unit
    
    return result

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

    # Load API keys
    api_keys = load_api_keys('API Keys/HaniAiKeys.json')
    openai_api_key = api_keys.get("openAI", {}).get("apiKey", "")

    if not openai_api_key:
        print("OpenAI API key is missing or invalid.")
        return

    # Define file paths
    input_initial_json_path = 'Scrubber Out/cleanedAmenityData.json'  # Initial data file should be placed here
    output_json_base_path = 'Classifier Out/support files/amenityDist'
    output_csv_base_path = 'Classifier Out/support files/amenity_classification'
    output_plot_base_path = 'Classifier Out/support files/amenity_analysis'
    output_summary_path = 'Classifier Out/support files/data_sufficiency_summary.csv'
    output_records_summary_path = 'Classifier Out/support files/country_listing_summary.csv'
    output_categorized_amenities_path = 'Classifier Out/categorized_amenities_summary.json'

    # Get user input for price unit
    unit_input = input("Enter the unit to display prices in (units, thousands, or millions): ").strip().lower()
    if unit_input == 'units':
        unit_scale = 1
        unit_name = 'units'
    elif unit_input == 'thousands':
        unit_scale = 1e3  # Correct division for thousands
        unit_name = 'thousands'
    elif unit_input == 'millions':
        unit_scale = 1e6  # Correct division for millions
        unit_name = 'millions'
    else:
        print("Invalid input. Defaulting to thousands.")
        unit_scale = 1e3
        unit_name = 'thousands'

    # Load initial data
    initial_data = load_data(input_initial_json_path)
    if not initial_data:
        print("Initial data is missing or could not be loaded.")
        return

    processed_data = process_amenities(initial_data, unit_scale)

    # Save processed data to amenityDist.json
    output_json_path = f'{output_json_base_path}.json'
    with open(output_json_path, 'w') as file:
        json.dump(processed_data, file, indent=2)
    
    print(f"Processed data saved to {output_json_path}")

    # Load the processed data for further processing
    loaded_data = load_data(output_json_path)
    if not loaded_data:
        print("Processed data could not be loaded.")
        return

    df = flatten_data(loaded_data)

    # Summarize records by country and listing type
    records_summary_df = summarize_data(df)
    records_summary_df.to_csv(output_records_summary_path, index=False)
    print(f"Country and listing type summary (processed data) saved to {output_records_summary_path}")

    # Append the summary for cleaned data to the same CSV file
    cleaned_summary_df = summarize_cleaned_data(input_initial_json_path)
    with open(output_records_summary_path, 'a') as f:
        f.write("\n\nSummary for Cleaned Data\n")
    cleaned_summary_df.to_csv(output_records_summary_path, index=False, mode='a', header=True)
    print(f"Country and listing type summary (cleaned data) saved to {output_records_summary_path}")

    # Get user input for country and listing type
    country_input = input("Enter country (or 'all' for all countries, or 'each' to process each country individually): ").strip().lower()
    listing_type_input = input("Enter listing type (Sale, Rent, Vacation, or 'all' for all listing types): ").strip().lower()

    # Define the countries to process
    countries_to_process = df['Country'].unique().tolist()
    if country_input == 'all':
        countries_to_process = ['all']
    elif country_input != 'each':
        countries_to_process = [country_input]

    # Get unique listing types
    listing_types_to_process = df['Listing_Type'].unique().tolist()
    if listing_type_input == 'all':
        listing_types_to_process = listing_types_to_process
    else:
        listing_types_to_process = [listing_type_input]

    summary_data = {}

    for listing_type in listing_types_to_process:
        combined_plots_data = {}  # Reset for each listing type
        for country in countries_to_process:
            # Filter the data based on country and listing type
            df_filtered = filter_data(df, country, listing_type)

            # Extract and save filtered amenities
            filtered_amenities = df_filtered['Amenity'].unique().tolist()
            with open('Classifier Out/support files/0GPT.json', 'w') as file:
                json.dump({amenity: "Unknown" for amenity in filtered_amenities}, file, indent=2)

            # Check data sufficiency and store summary
            summary = check_data_sufficiency(df_filtered, country, listing_type)
            summary_data[f"{country}_{listing_type}"] = summary

            # Classify amenities
            amenity_price_avg = classify_amenities(df_filtered)

            # Modify output file paths to include country and listing type
            country_suffix = country if country != 'all' else 'all'
            listing_type_suffix = listing_type if listing_type != 'all' else 'all'
            country_listing_suffix = f'_{country_suffix}_{listing_type_suffix}'

            output_csv_path = f'{output_csv_base_path}{country_listing_suffix}.csv'

            # Save the classified amenities to a CSV file
            if not amenity_price_avg.empty:
                amenity_price_avg.to_csv(output_csv_path, index=False)
                print(f"Classified amenities saved to {output_csv_path}")

            # Collect data for combined plots
            combined_plots_data = visualize_data(amenity_price_avg, country, listing_type, True, combined_plots_data, unit_name)

        # Save combined plots for each listing type
        combine_and_save_plots(combined_plots_data, listing_type, unit_name)

    # Save the summary table
    summary_df = pd.DataFrame(summary_data).T  # Transpose to have countries as rows
    summary_df.to_csv(output_summary_path)
    print(f"Data sufficiency summary saved to {output_summary_path}")

    # Extract and categorize amenities
    unique_amenities = df_filtered['Amenity'].unique().tolist()  # Use filtered amenities

    categorized_amenities = categorize_amenities(unique_amenities, openai_api_key)

    # Save the categorized amenities summary
    with open(output_categorized_amenities_path, 'w') as file:
        json.dump(categorized_amenities, file, indent=2)
    print(f"Categorized amenities summary saved to {output_categorized_amenities_path}")

    # Display total number of warnings
    print(f"Total number of skipped entries due to missing or malformed amenities: {warning_count}")

if __name__ == "__main__":
    main()
