from convert_currency import currency_converter
from get_data import get_data
import pandas as pd
from datetime import datetime
from hash_url import hash_url

def jiji_co_ke_filter(log):

    databaseName='jiji_co_ke'
    dbname_1=get_data(databaseName)
    collection_name_1 = dbname_1['propertyDetails']
    log.info(f'{"*"*40}\nCollecting data of {databaseName}')
    data_mongo=list(collection_name_1.find({},{'_id':False}))
    df21=pd.DataFrame(data_mongo,columns=data_mongo[0].keys())
    log.info(f'Filtering data of {databaseName}\n{"*"*40}')

    df21['Website']='jiji.co.ke'
    df21['rehaniId'] = df21['url'].apply(lambda x: hash_url(x))
    df21.rename(columns={'propertyTitle':'Title'}, inplace=True)
    df21.rename(columns={'agentNumber':'Agent Contact'}, inplace=True)
    df21.rename(columns={'agent':'Agent'}, inplace=True)
    df21['Agent Email Address']=None
    df21.rename(columns={'beds':'Beds'}, inplace=True)
    df21.rename(columns={'baths':'Baths'}, inplace=True)
    df21.rename(columns={'propertyType':'Housing Type'}, inplace=True)
    df21.rename(columns={'listingType':'Type (Rent, Sale, Vacation)'}, inplace=True)
    df21.rename(columns={'addedOn':'dateAdded'}, inplace=True)
    today = pd.Timestamp(datetime.now().strftime('%Y-%m-%d'))
    dates = pd.to_datetime(df21['dateAdded'])
    diff_days = (today - dates).dt.days
    df21['Days on Market'] = diff_days
    df21['Occupancy']=None
    df21['Number of Guests']=None

    df21['Number of amenities'] = [len(item) for item in df21.get('amenities', [])]

    highEndAmenities = [
        sum(1 for amenity in item if amenity.lower() in ['pool', 'gym', 'spa'])
        for item in df21['amenities']
    ]
    df21['Number of high end amenities (pool, gym, spa)'] = highEndAmenities

    parking=[]
    for item in df21['parking']:
        if str(item)=='nan':
            parking.append(False)
        elif item==None:
            parking.append(False)
        else:
            parking.append(True)
    df21['Parking']=parking 

    df21['localPrice'] = df21['price']
    df21['localCurrency'] = df21['currency']
    
    kshToUsd = currency_converter('USD', 'KES')
    prices=[]
    for idx,item in enumerate(df21['price']):
        if df21['currency'][idx]=='KES':
            item=item*kshToUsd
        prices.append(item)
    df21['price']=prices
    df21.rename(columns={'price':'Price'}, inplace=True)
    prices=[]
    for idx,item in enumerate(df21['priceDiff']):
        if df21['currency'][idx]=='KES':
            item=item*kshToUsd
        prices.append(item)
    df21['priceDiff']=prices

    df21['pricingCriteria'] = df21['pricingCriteria'].apply(
        lambda x: (
            '6 Months' if x and ('six' in x.lower() or '6' in x.lower()) else
            'Month' if x and 'month' in x.lower() else
            'Day' if x and 'day' in x.lower() else
            'Week' if x and 'week' in x.lower() else
            'Year' if x and ('annum' in x.lower() or 'annual' in x.lower()) else None
        )
    )
    df21.rename(columns={'pricingCriteria': 'Price criteria'}, inplace=True)


    conversion_factors = {
        'sqm': 10.7639,     # Square meters to square feet
        'ac': 43560,        # Acres to square feet
        'hectares': 107639, # Hectares to square feet
        'sf': 1             # Square feet remains as is
    }

    df21['Internal Area (s.f)'] = df21.apply(
        lambda row: float(row['size']) * conversion_factors.get(row['sizeUnit'], None)
        if row['size'] and row['sizeUnit'] in conversion_factors else None, axis=1
    )

    df21['Price per s.f.']=df21['Price']/df21['Internal Area (s.f)']
    df21['Fees (commissions, cleaning etc)']=None
    df21['Location: District']=None    
    df21['Location: Country']='Kenya'
    df21.rename(columns={'address':'Location: Address'}, inplace=True)
    df21['Location: Lat']=None
    df21['Location: Lon']=None

    df21.rename(columns={'city':'Location: City'}, inplace=True)
    df21.rename(columns={'neighborhood':'Location: Neighbourhood'}, inplace=True)

    df21.drop(['currency','toilets', 'size', 'sizeUnit'], axis=1,inplace=True)
    df21 = df21.reindex(sorted(df21.columns), axis=1)

    return df21