from pymongo import MongoClient
from dotenv import load_dotenv
from thefuzz import process
import os

# Load environment variables
load_dotenv(override=True)
CONNECTION_STRING_MONGODB = os.environ.get("CONNECTION_STRING")

def get_trading_data():
    """Fetch all city data from MongoDB."""
    client = MongoClient(CONNECTION_STRING_MONGODB)
    db = client['economicIndicators']
    collection = db['cities']
    return list(collection.find({}, {"_id": 0, "cityName": 1, "gdpPerCapita": 1, "population": 1, "populationGrowthRate": 1}))

# Load city data from MongoDB
city_data = get_trading_data()
city_names = [city["cityName"] for city in city_data]  # Extract city names for fuzzy matching

def normalize_city_name(city_name):
    """Normalize city names (e.g., remove extra spaces, lower case)."""
    return city_name.strip().lower()

city_names = [normalize_city_name(city["cityName"]) for city in city_data]

def get_city_data(city_name):
    """Find the best match for the city and return its data."""
    if not city_name:
        return None

    normalized_city_name = normalize_city_name(city_name)  # Normalize input city name
    best_match, score = process.extractOne(normalized_city_name, city_names)

    if score > 75:
        # Find the full city data for the best match
        return next(city for city in city_data if normalize_city_name(city["cityName"]) == best_match)

    return None  # No good match found
