import json
import csv
import os

# Define the file paths
input_file_path = os.path.join('Classifier Out', 'categorized_amenities_summary.json')
output_file_path = 'housing_type_summary.csv'

def extract_housing_types(data):
    housing_types = set()

    def traverse(data):
        if isinstance(data, dict):
            for key, value in data.items():
                if key not in ["Standard", "Basic", "Luxury", "Unclassified"]:
                    if key.isalpha():
                        housing_types.add(key)
                    traverse(value)
        elif isinstance(data, list):
            for item in data:
                traverse(item)

    traverse(data)
    return sorted(housing_types)

# Read the JSON data from the input file
with open(input_file_path, 'r') as json_file:
    data = json.load(json_file)

# Extract the housing types
housing_types = extract_housing_types(data)

# Write the housing types to a CSV file
with open(output_file_path, 'w', newline='') as csv_file:
    writer = csv.writer(csv_file)
    writer.writerow(['Housing Type'])
    for housing_type in housing_types:
        writer.writerow([housing_type])

print(f"Housing types summary saved to {output_file_path}")
