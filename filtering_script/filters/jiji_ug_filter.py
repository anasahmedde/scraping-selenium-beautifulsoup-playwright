from convert_currency import currency_converter
from get_data import get_data
import pandas as pd
from datetime import datetime
from hash_url import hash_url

def jiji_ug_filter(log):

    databaseName='jiji_ug'
    dbname_1=get_data(databaseName)
    collection_name_1 = dbname_1['propertyDetails']
    log.info(f'{"*"*40}\nCollecting data of {databaseName}')
    data_mongo=list(collection_name_1.find({},{'_id':False}))
    df26=pd.DataFrame(data_mongo,columns=data_mongo[0].keys())
    log.info(f'Filtering data of {databaseName}\n{"*"*40}')

    df26['Website']='jiji.ug'
    df26['rehaniId'] = df26['url'].apply(lambda x: hash_url(x))
    df26.rename(columns={'propertyTitle':'Title'}, inplace=True)
    df26.rename(columns={'agentNumber':'Agent Contact'}, inplace=True)
    df26.rename(columns={'agent':'Agent'}, inplace=True)
    df26['Agent Email Address']=None
    df26.rename(columns={'beds':'Beds'}, inplace=True)
    df26.rename(columns={'baths':'Baths'}, inplace=True)
    df26.rename(columns={'propertyType':'Housing Type'}, inplace=True)
    df26.rename(columns={'listingType':'Type (Rent, Sale, Vacation)'}, inplace=True)
    df26.rename(columns={'addedOn':'dateAdded'}, inplace=True)
    today = pd.Timestamp(datetime.now().strftime('%Y-%m-%d'))
    dates = pd.to_datetime(df26['dateAdded'])
    diff_days = (today - dates).dt.days
    df26['Days on Market'] = diff_days
    df26['Occupancy']=None
    df26['Number of Guests']=None

    df26['Number of amenities'] = [len(item) for item in df26.get('amenities', [])]

    highEndAmenities = [
        sum(1 for amenity in item if amenity.lower() in ['pool', 'gym', 'spa'])
        for item in df26['amenities']
    ]
    df26['Number of high end amenities (pool, gym, spa)'] = highEndAmenities

    parking=[]
    for item in df26['parking']:
        if str(item)=='nan':
            parking.append(False)
        elif item==None:
            parking.append(False)
        else:
            parking.append(True)
    df26['Parking']=parking 

    df26['localPrice'] = df26['price']
    df26['localCurrency'] = df26['currency']
    
    ugxToUsd = currency_converter('USD', 'UGX')
    prices=[]
    for idx,item in enumerate(df26['price']):
        if df26['currency'][idx]=='UGX':
            item=item*ugxToUsd
        prices.append(item)
    df26['price']=prices
    df26.rename(columns={'price':'Price'}, inplace=True)
    prices=[]
    for idx,item in enumerate(df26['priceDiff']):
        if df26['currency'][idx]=='UGX':
            item=item*ugxToUsd
        prices.append(item)
    df26['priceDiff']=prices

    df26['pricingCriteria'] = df26['pricingCriteria'].apply(
        lambda x: (
            '6 Months' if x and ('six' in x.lower() or '6' in x.lower()) else
            'Month' if x and 'month' in x.lower() else
            'Day' if x and 'day' in x.lower() else
            'Week' if x and 'week' in x.lower() else
            'Year' if x and ('annum' in x.lower() or 'annual' in x.lower()) else None
        )
    )
    df26.rename(columns={'pricingCriteria': 'Price criteria'}, inplace=True)


    conversion_factors = {
        'sqm': 10.7639,     # Square meters to square feet
        'ac': 43560,        # Acres to square feet
        'hectares': 107639, # Hectares to square feet
        'sf': 1             # Square feet remains as is
    }

    df26['Internal Area (s.f)'] = df26.apply(
        lambda row: float(row['size']) * conversion_factors.get(row['sizeUnit'], None)
        if row['size'] and row['sizeUnit'] in conversion_factors else None, axis=1
    )

    df26['Price per s.f.']=df26['Price']/df26['Internal Area (s.f)']
    df26['Fees (commissions, cleaning etc)']=None
    df26['Location: District']=None    
    df26['Location: Country']='Uganda'
    df26.rename(columns={'address':'Location: Address'}, inplace=True)
    df26['Location: Lat']=None
    df26['Location: Lon']=None

    df26.rename(columns={'city':'Location: City'}, inplace=True)
    df26.rename(columns={'neighborhood':'Location: Neighbourhood'}, inplace=True)

    df26.drop(['currency','toilets', 'size', 'sizeUnit'], axis=1,inplace=True)
    df26 = df26.reindex(sorted(df26.columns), axis=1)

    return df26