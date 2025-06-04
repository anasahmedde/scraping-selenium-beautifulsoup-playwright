from langchain.embeddings import OpenAIEmbeddings
import os
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_fixed

openai_key = os.getenv("openai_key")
client_embedding = OpenAIEmbeddings(api_key=openai_key)

def generate_embedded_description(row):
    description_embedding = "Listing summary:\n\n"

    if not pd.isnull(row.get('housingType')):
        description_embedding += f"Listing type: {row['housingType']}\n"
    
    if not pd.isnull(row.get('type')):
        description_embedding += f"Listing status: for {row['type']}\n"
    
    if not pd.isnull(row.get('consolidatedNeighbourhood')) and not pd.isnull(row.get('consolidatedCity')) and not pd.isnull(row.get('consolidatedState')) and not pd.isnull(row.get('consolidatedCountry')):
        description_embedding += f"Location: {row['consolidatedNeighbourhood']}, {row['consolidatedCity']}, {row['consolidatedState']}, {row['consolidatedCountry']}\n"
    elif not pd.isnull(row.get('consolidatedNeighbourhood')) and not pd.isnull(row.get('consolidatedCity')) and not pd.isnull(row.get('consolidatedCountry')):
        description_embedding += f"Location: {row['consolidatedNeighbourhood']}, {row['consolidatedCity']}, {row['consolidatedCountry']}\n"
    elif not pd.isnull(row.get('consolidatedCity')) and not pd.isnull(row.get('consolidatedCountry')):
        description_embedding += f"Location: {row['consolidatedCity']}, {row['consolidatedCountry']}\n"
    elif not pd.isnull(row.get('locationAddress')):
        description_embedding += f"Location: {row['locationAddress']}\n"

    if not pd.isnull(row.get('beds')) and not pd.isnull(row.get('baths')):
        try:
            description_embedding += f"Beds & baths: {int(row['beds'])} Beds, {int(row['baths'])} Baths\n"
        except ValueError:
            description_embedding += f"Beds & baths: {row['beds']} Beds, {row['baths']} Baths\n"
    
    if 'internalArea' in row and not pd.isnull(row['internalArea']):
        description_embedding += f"Size: {row['internalArea']} square feet\n"
    
    if not pd.isnull(row.get('price')):
        description_embedding += f"Price: {int(row['price'])} USD\n"
    
    if not pd.isnull(row.get('pricePerSf')):
        description_embedding += f"Price per square ft: {row['pricePerSf']}\n"
    
    if not pd.isnull(row.get('cumulativePriceChange')):
        description_embedding += f"Cumulative Price change: {row['cumulativePriceChange']}\n"
    
    if not pd.isnull(row.get('priceStatus')) and not pd.isnull(row.get('priceDiff')):
        description_embedding += f"Latest Price change: {row['priceStatus']} by {row['priceDiff']}\n"
    
    if not pd.isnull(row.get('daysOnMarket')):
        description_embedding += f"Days on market: {row['daysOnMarket']} days\n"
    
    # if row.get('amenities') is not None and row['amenities']:
    #     description_embedding += f"Amenities: {', '.join(row['amenities'])}."

    if row.get('amenities') is not None and row['amenities']:
        description_embedding += f"Amenities: {', '.join(map(str, row['amenities']))}."

    if not pd.isnull(row.get('description')):
        description_embedding += f"\n\nListing description:\n\n{row['description']}"
    
    return description_embedding.strip()


def generate_embedding(description):
    response = client_embedding.embed_query(description)
    return response


# @retry(stop=stop_after_attempt(4), wait=wait_fixed(6*3600))
def add_embeddings(df):
    df['description_embedding'] = df.apply(generate_embedded_description, axis=1)
    df['embedding'] = df['description_embedding'].apply(generate_embedding)
    return df
