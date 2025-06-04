from convert_currency import currency_converter
from get_data import get_data
import pandas as pd
from datetime import datetime
from hash_url import hash_url

def jiji_com_gh_filter(log):

    databaseName='jiji_com_gh'
    dbname_1=get_data(databaseName)
    collection_name_1 = dbname_1['propertyDetails']
    log.info(f'{"*"*40}\nCollecting data of {databaseName}')
    data_mongo=list(collection_name_1.find({},{'_id':False}))
    df24=pd.DataFrame(data_mongo,columns=data_mongo[0].keys())
    log.info(f'Filtering data of {databaseName}\n{"*"*40}')

    df24['Website']='jiji.com.gh'
    df24['rehaniId'] = df24['url'].apply(lambda x: hash_url(x))
    df24.rename(columns={'propertyTitle':'Title'}, inplace=True)
    df24.rename(columns={'agentNumber':'Agent Contact'}, inplace=True)
    df24.rename(columns={'agent':'Agent'}, inplace=True)
    df24['Agent Email Address']=None
    df24.rename(columns={'beds':'Beds'}, inplace=True)
    df24.rename(columns={'baths':'Baths'}, inplace=True)
    df24.rename(columns={'propertyType':'Housing Type'}, inplace=True)
    df24.rename(columns={'listingType':'Type (Rent, Sale, Vacation)'}, inplace=True)
    df24.rename(columns={'addedOn':'dateAdded'}, inplace=True)
    today = pd.Timestamp(datetime.now().strftime('%Y-%m-%d'))
    dates = pd.to_datetime(df24['dateAdded'])
    diff_days = (today - dates).dt.days
    df24['Days on Market'] = diff_days
    df24['Occupancy']=None
    df24['Number of Guests']=None

    df24['Number of amenities'] = [len(item) for item in df24.get('amenities', [])]

    highEndAmenities = [
        sum(1 for amenity in item if amenity.lower() in ['pool', 'gym', 'spa'])
        for item in df24['amenities']
    ]
    df24['Number of high end amenities (pool, gym, spa)'] = highEndAmenities

    parking=[]
    for item in df24['parking']:
        if str(item)=='nan':
            parking.append(False)
        elif item==None:
            parking.append(False)
        else:
            parking.append(True)
    df24['Parking']=parking 

    df24['localPrice'] = df24['price']
    df24['localCurrency'] = df24['currency']
    
    ghsToUsd = currency_converter('USD', 'GHS')
    prices=[]
    for idx,item in enumerate(df24['price']):
        if df24['currency'][idx]=='GHS':
            item=item*ghsToUsd
        prices.append(item)
    df24['price']=prices
    df24.rename(columns={'price':'Price'}, inplace=True)
    prices=[]
    for idx,item in enumerate(df24['priceDiff']):
        if df24['currency'][idx]=='GHS':
            item=item*ghsToUsd
        prices.append(item)
    df24['priceDiff']=prices

    df24['pricingCriteria'] = df24['pricingCriteria'].apply(
        lambda x: (
            '6 Months' if x and ('six' in x.lower() or '6' in x.lower()) else
            'Month' if x and 'month' in x.lower() else
            'Day' if x and 'day' in x.lower() else
            'Week' if x and 'week' in x.lower() else
            'Year' if x and ('annum' in x.lower() or 'annual' in x.lower()) else None
        )
    )
    df24.rename(columns={'pricingCriteria': 'Price criteria'}, inplace=True)


    conversion_factors = {
        'sqm': 10.7639,     # Square meters to square feet
        'ac': 43560,        # Acres to square feet
        'hectares': 107639, # Hectares to square feet
        'sf': 1             # Square feet remains as is
    }

    df24['Internal Area (s.f)'] = df24.apply(
        lambda row: float(row['size']) * conversion_factors.get(row['sizeUnit'], None)
        if row['size'] and row['sizeUnit'] in conversion_factors else None, axis=1
    )

    df24['Price per s.f.']=df24['Price']/df24['Internal Area (s.f)']
    df24['Fees (commissions, cleaning etc)']=None
    df24['Location: District']=None    
    df24['Location: Country']='Ghana'
    df24.rename(columns={'address':'Location: Address'}, inplace=True)
    df24['Location: Lat']=None
    df24['Location: Lon']=None

    df24.rename(columns={'city':'Location: City'}, inplace=True)
    df24.rename(columns={'neighborhood':'Location: Neighbourhood'}, inplace=True)

    df24.drop(['currency','toilets', 'size', 'sizeUnit'], axis=1,inplace=True)
    df24 = df24.reindex(sorted(df24.columns), axis=1)

    return df24