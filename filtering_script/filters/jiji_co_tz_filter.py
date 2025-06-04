from convert_currency import currency_converter
from get_data import get_data
import pandas as pd
from datetime import datetime
from hash_url import hash_url

def jiji_co_tz_filter(log):

    databaseName='jiji_co_tz'
    dbname_1=get_data(databaseName)
    collection_name_1 = dbname_1['propertyDetails']
    log.info(f'{"*"*40}\nCollecting data of {databaseName}')
    data_mongo=list(collection_name_1.find({},{'_id':False}))
    df22=pd.DataFrame(data_mongo,columns=data_mongo[0].keys())
    log.info(f'Filtering data of {databaseName}\n{"*"*40}')

    df22['Website']='jiji.co.tz'
    df22['rehaniId'] = df22['url'].apply(lambda x: hash_url(x))
    df22.rename(columns={'propertyTitle':'Title'}, inplace=True)
    df22.rename(columns={'agentNumber':'Agent Contact'}, inplace=True)
    df22.rename(columns={'agent':'Agent'}, inplace=True)
    df22['Agent Email Address']=None
    df22.rename(columns={'beds':'Beds'}, inplace=True)
    df22.rename(columns={'baths':'Baths'}, inplace=True)
    df22.rename(columns={'propertyType':'Housing Type'}, inplace=True)
    df22.rename(columns={'listingType':'Type (Rent, Sale, Vacation)'}, inplace=True)
    df22.rename(columns={'addedOn':'dateAdded'}, inplace=True)
    today = pd.Timestamp(datetime.now().strftime('%Y-%m-%d'))
    dates = pd.to_datetime(df22['dateAdded'])
    diff_days = (today - dates).dt.days
    df22['Days on Market'] = diff_days
    df22['Occupancy']=None
    df22['Number of Guests']=None

    df22['Number of amenities'] = [len(item) for item in df22.get('amenities', [])]

    highEndAmenities = [
        sum(1 for amenity in item if amenity.lower() in ['pool', 'gym', 'spa'])
        for item in df22['amenities']
    ]
    df22['Number of high end amenities (pool, gym, spa)'] = highEndAmenities

    parking=[]
    for item in df22['parking']:
        if str(item)=='nan':
            parking.append(False)
        elif item==None:
            parking.append(False)
        else:
            parking.append(True)
    df22['Parking']=parking 

    df22['localPrice'] = df22['price']
    df22['localCurrency'] = df22['currency']
    
    kshToUsd = currency_converter('USD', 'TZS')
    prices=[]
    for idx,item in enumerate(df22['price']):
        if df22['currency'][idx]=='TZS':
            item=item*kshToUsd
        prices.append(item)
    df22['price']=prices
    df22.rename(columns={'price':'Price'}, inplace=True)
    prices=[]
    for idx,item in enumerate(df22['priceDiff']):
        if df22['currency'][idx]=='TZS':
            item=item*kshToUsd
        prices.append(item)
    df22['priceDiff']=prices

    df22['pricingCriteria'] = df22['pricingCriteria'].apply(
        lambda x: (
            '6 Months' if x and ('six' in x.lower() or '6' in x.lower()) else
            'Month' if x and 'month' in x.lower() else
            'Day' if x and 'day' in x.lower() else
            'Week' if x and 'week' in x.lower() else
            'Year' if x and ('annum' in x.lower() or 'annual' in x.lower()) else None
        )
    )
    df22.rename(columns={'pricingCriteria': 'Price criteria'}, inplace=True)


    conversion_factors = {
        'sqm': 10.7639,     # Square meters to square feet
        'ac': 43560,        # Acres to square feet
        'hectares': 107639, # Hectares to square feet
        'sf': 1             # Square feet remains as is
    }

    df22['Internal Area (s.f)'] = df22.apply(
        lambda row: float(row['size']) * conversion_factors.get(row['sizeUnit'], None)
        if row['size'] and row['sizeUnit'] in conversion_factors else None, axis=1
    )

    df22['Price per s.f.']=df22['Price']/df22['Internal Area (s.f)']
    df22['Fees (commissions, cleaning etc)']=None
    df22['Location: District']=None    
    df22['Location: Country']='Tanzania'
    df22.rename(columns={'address':'Location: Address'}, inplace=True)
    df22['Location: Lat']=None
    df22['Location: Lon']=None

    df22.rename(columns={'city':'Location: City'}, inplace=True)
    df22.rename(columns={'neighborhood':'Location: Neighbourhood'}, inplace=True)

    df22.drop(['currency','toilets', 'size', 'sizeUnit'], axis=1,inplace=True)
    df22 = df22.reindex(sorted(df22.columns), axis=1)

    return df22