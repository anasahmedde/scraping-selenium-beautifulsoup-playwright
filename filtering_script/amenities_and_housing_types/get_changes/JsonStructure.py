import json
import os

def summarize_json_structure(json_data, level=0):
    summary_lines = []
    indent = '  ' * level

    if isinstance(json_data, dict):
        for key, value in json_data.items():
            if isinstance(value, dict):
                summary_lines.append(f"{indent}{key}: Object")
                summary_lines.extend(summarize_json_structure(value, level + 1))
            elif isinstance(value, list):
                summary_lines.append(f"{indent}{key}: Array")
                if value:
                    summary_lines.extend(summarize_json_structure(value[0], level + 1))
            else:
                summary_lines.append(f"{indent}{key}: {type(value).__name__}")
    elif isinstance(json_data, list):
        if json_data:
            summary_lines.extend(summarize_json_structure(json_data[0], level))
    else:
        summary_lines.append(f"{indent}Value: {type(json_data).__name__}")

    return summary_lines

def summarize_json_file(file_path):
    with open(file_path, 'r') as file:
        json_data = json.load(file)

    summary_lines = summarize_json_structure(json_data)
    summary_text = "\n".join(summary_lines)

    summary_file_path = os.path.join(os.path.dirname(file_path), 'AmenityData_Structure_Summary.txt')
    with open(summary_file_path, 'w') as summary_file:
        summary_file.write(summary_text)

    print(f"Summary saved to {summary_file_path}")

# Assuming the script is in the same directory as the AmenityData.json file
json_file_path = 'AmenityData.json'
summarize_json_file(json_file_path)
