from get_data import get_data
import pandas as pd
from datetime import datetime
from hash_url import hash_url

def global_remax_filter(log):

    databaseName='global_remax'
    dbname_1=get_data(databaseName)

    collection_name_1 = dbname_1['propertyDetails']
    log.info(f'{"*"*40}\nCollecting data of {databaseName}')
    data_mongo=list(collection_name_1.find({},{'_id':False}))
    df20=pd.DataFrame(data_mongo,columns=data_mongo[0].keys())
    log.info(f'Filtering data of {databaseName}\n{"*"*40}')

    df20['Website']='global.remax.com'
    df20['rehaniId'] = df20['url'].apply(lambda x: hash_url(x))
    df20.rename(columns={'propertyTitle':'Title'}, inplace=True)
    df20.rename(columns={'beds':'Beds'}, inplace=True)
    df20.rename(columns={'baths':'Baths'}, inplace=True)
    df20['propertyType']=df20['propertyType'].str.replace('buy','Sale')
    df20['propertyType']=df20['propertyType'].str.replace('rent','Rent')
    df20.rename(columns={'propertyType':'Type (Rent, Sale, Vacation)'}, inplace=True)
    df20['Location: City'] = None
    df20['Location: Country'] = df20['country']
    df20['Location: Lat']=None
    df20['Location: Lon']=None
    df20['Location: Neighbourhood']=None
    df20.rename(columns={'address':'Location: Address'}, inplace=True)
    today = pd.Timestamp(datetime.now().strftime('%Y-%m-%d'))
    dates = pd.to_datetime(df20['dateListed'])
    diff_days = (today - dates).dt.days
    df20['Days on Market'] = diff_days
    df20.rename(columns={'dateListed':'dateAdded'}, inplace=True)
    # df20.rename(columns={'usdPrice':'price'}, inplace=True)

    parking=[]
    for item in df20['parkingSpaces']:
        if str(item)=='nan':
            parking.append(False)
        elif item==None:
            parking.append(False)
        else:
            parking.append(True)
    df20['Parking']=parking

    df20.rename(columns={'housingType':'Housing Type'}, inplace=True)
    df20['Number of amenities'] = [len(item) for item in df20['amenities']]
    df20['Price criteria']=None

    highEndAmenities = [
        sum(1 for amenity in item if amenity.lower() in ['pool', 'gym', 'spa'])
        for item in df20['amenities']
    ]
    df20['Number of high end amenities (pool, gym, spa)'] = highEndAmenities

    df20["internalArea"] = (
        df20["internalArea"]
        .astype(str)          # Convert to string to handle `.replace()`
        .str.replace(",", "") # Remove commas
        .apply(pd.to_numeric, errors="coerce")  # Convert to numeric (NaN for invalid)
        .mul(10.764)          # Convert sqm → sqft
        .round(2)             # Round to 2 decimal places
    )

    df20.loc[df20["Type (Rent, Sale, Vacation)"] == "Rent", "Price criteria"] = "Month"

    df20.drop(['parkingSpaces', 'rooms', 'builtArea', 'builtAreaUnit', 'lotSize', 'lotSizeUnit', 'floors', 'usdPrice', 'yearBuilt', 'dateAvailable', 'country', 'internalAreaUnit'], axis=1,inplace=True)
    df20 = df20.reindex(sorted(df20.columns), axis=1)

    return df20