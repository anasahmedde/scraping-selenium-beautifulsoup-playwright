from shapely.geometry import shape, Point
from concurrent.futures import ThreadPoolExecutor
from shapely.validation import make_valid
import os, json
from datetime import datetime
import pandas as pd
import threading
from tenacity import retry, stop_after_attempt, wait_fixed
from pathlib import Path

#threads = int(os.getenv("gui_threads"))
threads = 1

countries = {
    'Ethiopia': 'ETH',
    'Rwanda': 'RWA',
    'Ghana': 'GHA',
    'Kenya': 'KEN',
    'Uganda': 'UGN',
    'Nigeria': 'NIG',
    'Tanzania': 'TZA',
    'Senegal': 'SEN',
    'Egypt': 'EGY',
    'Gambia': 'GAM',
    'Morocco': 'MOR',
    'South Africa': 'SAR',
    'Democratic Republic of the Congo': 'DRC',
    'Zimbabwe': 'ZIM',
    'Namibia': 'NAM',
    'Angola': 'ANG',
    'Mozambique': 'MOZ',
    'Malawi': 'MAL',
    'Zambia': 'ZAM',
    'Ivory Coast': 'CDI',
    'Burundi': 'BUR',
    'South Sudan': 'SSD',
    'Botswana': 'BOT'
}

levels = {
  'neighborhood': {
    'NAME_2': ['Gambia', 'Ghana', 'Zambia'],
    'NAME_3': ['Ethiopia', 'Kenya', 'Morocco', 'Tanzania'],
    'NAME_4': ['Rwanda', 'Uganda'],
    'wardname': ['Nigeria']
  },
  'city': {
    'NAME_1': ['Kenya', 'Gambia', 'Ghana', 'Rwanda', 'Tanzania', 'Zambia'],
    'NAME_2': ['Ethiopia', 'Morocco', 'Uganda'],
    'lganame': ['Nigeria'] 
  }
}

# name2Countries = ('Democratic Republic of the Congo', 'Zambia', 'Zimbabwe', 'Morocco', 'Egypt', 'Botswana', 'Gambia', 'Namibia')
# name4Countries = ('Rwanda', 'Uganda')
# name2CityCountries = ('Uganda')

thread_local = threading.local()

def process_geojson_feature(feature, point, result):
    try:
        geometry = shape(feature['geometry'])
        
        if not geometry.is_valid:
            geometry = make_valid(geometry)

        if geometry.contains(point):
            properties = feature['properties']
            consolidatedCountry = properties.get('COUNTRY', result['locationCountry'])
            
            neighborhood_level_key = next((key for key, countries in levels['neighborhood'].items() if consolidatedCountry in countries), None)
            city_level_key = next((key for key, countries in levels['city'].items() if consolidatedCountry in countries), None)

            consolidatedCity = properties[city_level_key]
            consolidatedNeighbourhood = properties[neighborhood_level_key]
            consolidatedState = properties['statename'] if consolidatedCountry == 'Nigeria' else None
        
            return geometry, consolidatedCountry, consolidatedCity, consolidatedNeighbourhood, consolidatedState
        else:
            return None, None, None, None, None
    except Exception as e:
        return None, None, None, None, None

def assignDefaultValues(df_concat, ind):
    location_neighbourhood = df_concat.at[ind, 'locationNeighbourhood']
    location_city = df_concat.at[ind, 'locationCity']
    location_country = df_concat.at[ind, 'locationCountry']

    df_concat.at[ind, 'location'] = ", ".join(filter(lambda x: x is not None and x != '' and x != 'None', [str(part) for part in [location_neighbourhood, location_city, location_country]]))
    df_concat.at[ind, 'consolidatedCountry'] = location_country
    df_concat.at[ind, 'consolidatedCity'] = location_city
    df_concat.at[ind, 'consolidatedNeighbourhood'] = location_neighbourhood
    df_concat.at[ind, 'consolidatedState'] = None

def add_more_calculations(df_concat, log):
    result_dfs = []  # Define a list to store the DataFrames from each thread

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
    def process_row(df_concat, ind, result):
        nonlocal result_dfs  # Declare result_dfs as nonlocal to modify it within the inner function

        df_concat.at[ind, 'lastUpdated'] = datetime.now().date().strftime("%Y-%m-%d")

        if not hasattr(thread_local, 'mainCountry'):
            thread_local.mainCountry = "Ethiopia"

            # Get the current script's directory
            script_dir = Path(__file__).resolve().parent

            # Define the folder and file paths
            thread_local.folder_path = script_dir / "shape_files"
            file_path = thread_local.folder_path / "ETH.json"

            print(file_path, script_dir)  # Debugging output

            try:
                with file_path.open('r', encoding='utf-8') as file:
                    thread_local.json_data = json.load(file)
            except FileNotFoundError:
                print(f"Error: {file_path} not found")
            except json.JSONDecodeError:
                print("Error: Failed to parse JSON file")

        # if not hasattr(thread_local, 'mainCountry'):
        #     thread_local.mainCountry = "Ethiopia"
        #     current_directory = os.getcwd()
        #     folder_name = 'shape_files'
        #     thread_local.folder_path = os.path.join(current_directory, folder_name)
        #     file_path = os.path.join(thread_local.folder_path, 'ETH.json')
        #     print(file_path,  os.getcwd())
        #     with open(file_path, 'r', encoding='utf-8') as file:
        #         thread_local.json_data = json.load(file)

        if result.get("locationLat") or result.get("locationLon"):
            if thread_local.mainCountry != result["locationCountry"]:
                thread_local.mainCountry = result["locationCountry"]

                country_code = countries.get(result["locationCountry"], result["locationCountry"])
                file_path = Path(thread_local.folder_path) / f"{country_code}.json"

                try:
                    with file_path.open('r', encoding='utf-8') as file:
                        thread_local.json_data = json.load(file)
                except FileNotFoundError:
                    assignDefaultValues(df_concat, ind)
                    return

            longitude, latitude = str(result["locationLon"]).replace(',', ''), str(result["locationLat"]).replace(',', '')
            point = Point(longitude, latitude)

            for item in thread_local.json_data.get('features', []):
                geometry, consolidatedCountry, consolidatedCity, consolidatedNeighbourhood, consolidatedState = process_geojson_feature(item, point, result)

                if geometry is not None:
                    df_concat.at[ind, 'consolidatedCountry'] = consolidatedCountry
                    df_concat.at[ind, 'consolidatedCity'] = consolidatedCity
                    df_concat.at[ind, 'consolidatedNeighbourhood'] = consolidatedNeighbourhood
                    df_concat.at[ind, 'consolidatedState'] = consolidatedState
                    df_concat.at[ind, 'location'] = ", ".join(filter(lambda x: x is not None and x != '' and x != 'None', [str(part) for part in [consolidatedNeighbourhood, consolidatedCity, consolidatedCountry]]))
                    break
            else:
                assignDefaultValues(df_concat, ind)
        else:
            assignDefaultValues(df_concat, ind)

        print(df_concat.at[ind, 'location'])
        return
    
    # Execute the process_row function concurrently
    with ThreadPoolExecutor(max_workers=threads) as executor1:
        futures1 = []
        for ind, result in df_concat.iterrows():
            futures1.append(executor1.submit(process_row, df_concat, ind, result))

        total_records = len(df_concat)
        processed_records = 0
        for future in futures1:
            future.result()
            processed_records += 1
            print(f'Adding neighborhoods by processing lat/long: Processed {processed_records}/{total_records} listings.')

    # Concatenate all DataFrames from result_dfs into a single DataFrame
    if result_dfs:
        df_result = pd.concat(result_dfs)
        concatenated_df = pd.concat([df_concat, df_result])
    else:
        concatenated_df = df_concat.copy()

    return concatenated_df
