import json
import random
from openai import OpenAI
import os
from datetime import datetime

api_key = 'API_KEY'
base_model = 'gpt-4'

# Define input and output file paths
input_file = 'AmenityData.json'
output_file = 'CleanedAmenityData.json'
removed_output_file = 'removedAmenities.json'

# Ask the user if they would like to produce debug files
produce_debug_files = input("Would you like to produce debug files? (y/n): ").strip().lower() == 'y'

# Create a debug subfolder if needed
debug_folder = 'debug'
if produce_debug_files:
    os.makedirs(debug_folder, exist_ok=True)

# Get the current date and time for file naming
current_time = datetime.now().strftime("%Y%m%d_%H%M%S")

# Define the categories of invalid amenities
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

# Function to save debug output to a file
def save_debug_info(filename, content):
    if produce_debug_files:
        filepath = os.path.join(debug_folder, f"{current_time}_{filename}")
        with open(filepath, 'w') as f:
            f.write(content)

# Function to categorize amenities (case insensitive)
def categorize_amenities(amenities):
    categorized = {}
    debug_lines = []
    for amenity in amenities:
        matched = False
        for category, keywords in invalid_categories.items():
            # Add debug log for each comparison
            debug_lines.append(f"Checking '{amenity}' against category '{category}' with keywords {keywords}")
            if any(keyword.lower() in amenity.lower() for keyword in keywords):
                debug_lines.append(f"Flagging '{amenity}' as invalid under category '{category}'")
                if amenity.lower() in categorized:
                    categorized[amenity.lower()].append(category)
                else:
                    categorized[amenity.lower()] = [category]
                matched = True
                break  # Stop checking other categories once a match is found
        if matched:
            debug_lines.append(f"'{amenity}' is flagged as invalid.")
        else:
            debug_lines.append(f"'{amenity}' is considered valid.")

    save_debug_info("AmenityCheckDebug.txt", "\n".join(debug_lines))
    return categorized

# Function to confirm invalid amenities using OpenAI GPT-4
def confirm_invalid_amenities(categorized_amenities):
    client = OpenAI(api_key=api_key, timeout=60)
    amenities_to_confirm = [{"amenity": amenity, "categories": categories} for amenity, categories in categorized_amenities.items()]
    
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
        "Amenities:\n" + "\n".join([f"{item['amenity']} (Categories: {', '.join(item['categories'])})" for item in amenities_to_confirm])
    )

    ret = client.chat.completions.create(
        model=base_model,
        messages=[
            {
                "role": "user",
                "content": message_content
            }
        ],
    )
    
    debug_content = f"Message sent to GPT:\n{message_content}\n\nGPT Response:\n{ret.choices[0].message.content.strip()}"
    save_debug_info("GPTCallDebug.txt", debug_content)
    
    responses = ret.choices[0].message.content.strip().split('\n')
    confirmations = {}
    for response in responses:
        if ':' in response:
            amenity, decision = response.split(':')
            confirmations[amenity.strip().lower()] = decision.strip().lower()
    return confirmations

# Function to filter amenities based on GPT confirmation
def filter_amenities(confirmations, amenities):
    filtered_amenities = [amenity for amenity in amenities if confirmations.get(amenity.lower(), 'yes') == 'no']
    return filtered_amenities

try:
    # Read the input JSON data from the file
    with open(input_file, 'r') as file:
        data = json.load(file)
    
    save_debug_info("DataReadDebug.txt", f"Data read from file:\n{json.dumps(data, indent=2)}")

    # Ensure the data is a list of dictionaries
    if not isinstance(data, list):
        raise TypeError("The JSON data must be a list of dictionaries")

    # Filter out entries with empty amenities
    non_empty_data = [item for item in data if item.get("amenities")]
    
    save_debug_info("NonEmptyDataDebug.txt", f"Non-empty data:\n{json.dumps(non_empty_data, indent=2)}")

    # Ask the user for the number of records to process
    user_input = input("Enter the number of records to process (or type 'All' to process the entire list): ").strip().lower()

    # Determine the number of records to process
    if user_input == 'all':
        selected_data = non_empty_data
    else:
        num_records_to_send = int(user_input)
        selected_data = random.sample(non_empty_data, num_records_to_send)
    
    save_debug_info("SelectedDataDebug.txt", f"Selected data:\n{json.dumps(selected_data, indent=2)}")

    cleaned_data = []
    removed_amenities = {}
    invalid_amenities = {}
    categorized_amenities_list = []  # To track categorized amenities for debugging
    scrpt2GPT_debug = []  # Track amenities with statuses for debugging

    # Step 1: Categorize amenities and determine initial script status
    for item in selected_data:
        try:
            amenities = item.get("amenities", [])
            categorized_amenities = categorize_amenities(amenities)
            categorized_amenities_list.append(categorized_amenities)
            
            for amenity in amenities:
                script_status = "no" if amenity.lower() in categorized_amenities else "yes"
                gpt_status = "NS"  # Not Sent by default
                final_status = script_status

                if script_status == "no":
                    invalid_amenities[amenity.lower()] = categorized_amenities[amenity]

                scrpt2GPT_debug.append({
                    "amenity": amenity,
                    "script_status": script_status,
                    "gpt_status": gpt_status,
                    "final_status": final_status
                })
            item["amenities"] = amenities
            cleaned_data.append(item)
        except Exception as e:
            save_debug_info(f"SkippingItemDebug_{item.get('id', 'unknown')}.txt", f"Skipping item: {json.dumps(item, indent=2)}\nError: {str(e)}")
            continue

    save_debug_info("CategorizedAmenitiesListDebug.txt", f"Categorized amenities list:\n{json.dumps(categorized_amenities_list, indent=2)}")
    save_debug_info("InvalidAmenitiesDebug.txt", f"Invalid amenities to confirm:\n{json.dumps(invalid_amenities, indent=2)}")

    # Step 2: Send invalid amenities to GPT and update statuses
    if invalid_amenities:
        confirmations = confirm_invalid_amenities(invalid_amenities)
        save_debug_info("ConfirmationsDebug.txt", f"Confirmations received:\n{json.dumps(confirmations, indent=2)}")

        for debug_entry in scrpt2GPT_debug:
            amenity = debug_entry["amenity"].lower()
            if debug_entry["script_status"] == "no":
                if amenity in confirmations:
                    gpt_status = confirmations[amenity]
                    final_status = gpt_status
                else:
                    gpt_status = "NS"
                    final_status = debug_entry["script_status"]

                debug_entry["gpt_status"] = gpt_status
                debug_entry["final_status"] = final_status

    save_debug_info("scrpt2GPT.txt", f"Amenities statuses:\n{json.dumps(scrpt2GPT_debug, indent=2)}")

    final_cleaned_data = []
    final_removed_amenities = {}

    # Step 3: Finalize cleaned and removed data based on final statuses
    for item in cleaned_data:
        item_amenities = item.get("amenities", [])
        valid_amenities = [amenity for amenity in item_amenities if any(debug["final_status"] == "yes" for debug in scrpt2GPT_debug if debug["amenity"].lower() == amenity.lower())]
        invalid_amenities = [amenity for amenity in item_amenities if any(debug["final_status"] == "no" for debug in scrpt2GPT_debug if debug["amenity"].lower() == amenity.lower())]

        item["amenities"] = valid_amenities

        if valid_amenities:
            final_cleaned_data.append(item)
        else:
            save_debug_info(f"RemovedItemDebug_{item.get('id', 'unknown')}.txt", f"Removed item due to empty amenities: {json.dumps(item, indent=2)}")

        for amenity in invalid_amenities:
            categories = categorized_amenities_list[selected_data.index(item)].get(amenity, [])
            for category in categories:
                if category not in final_removed_amenities:
                    final_removed_amenities[category] = []
                final_removed_amenities[category].append(f"{amenity} ({categories.count(category)})")

    save_debug_info("FinalCleanedDataDebug.txt", f"Final cleaned data:\n{json.dumps(final_cleaned_data, indent=2)}")
    save_debug_info("FinalRemovedAmenitiesDebug.txt", f"Final removed amenities:\n{json.dumps(final_removed_amenities, indent=2)}")

    # Write the final cleaned data to the output file
    with open(output_file, 'w') as file:
        json.dump(final_cleaned_data, file, indent=2)

    # Write the final removed amenities to the removed output file
    with open(removed_output_file, 'w') as file:
        json.dump(final_removed_amenities, file, indent=2)

    print(f"Cleaned data has been saved to {output_file}")
    print(f"Removed amenities have been saved to {removed_output_file}")

except FileNotFoundError:
    print(f"The file {input_file} does not exist.")
except json.JSONDecodeError:
    print(f"The file {input_file} is not a valid JSON file.")
except TypeError as e:
    print(e)
except ValueError:
    print("Invalid input. Please enter a valid number or 'All'.")
except Exception as e:
    print(e)

# Force creation of missing debug files if they do not exist
if produce_debug_files:
    for filename in ["SkippingItemDebug.txt", "CleanedDataDebug.txt", "RemovedAmenitiesDebug.txt"]:
        if not os.path.exists(os.path.join(debug_folder, f"{current_time}_{filename}")):
            save_debug_info(filename, "Debug information not generated.\n")
