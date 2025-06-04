from convert_currency import currency_converter
from get_data import get_data
import pandas as pd
from datetime import datetime
from hash_url import hash_url

def jiji_com_et_filter(log):

    databaseName='jiji_com_et'
    dbname_1=get_data(databaseName)
    collection_name_1 = dbname_1['propertyDetails']
    log.info(f'{"*"*40}\nCollecting data of {databaseName}')
    data_mongo=list(collection_name_1.find({},{'_id':False}))
    df23=pd.DataFrame(data_mongo,columns=data_mongo[0].keys())
    log.info(f'Filtering data of {databaseName}\n{"*"*40}')

    df23['Website']='jiji.com.et'
    df23['rehaniId'] = df23['url'].apply(lambda x: hash_url(x))
    df23.rename(columns={'propertyTitle':'Title'}, inplace=True)
    df23.rename(columns={'agentNumber':'Agent Contact'}, inplace=True)
    df23.rename(columns={'agent':'Agent'}, inplace=True)
    df23['Agent Email Address']=None
    df23.rename(columns={'beds':'Beds'}, inplace=True)
    df23.rename(columns={'baths':'Baths'}, inplace=True)
    df23.rename(columns={'propertyType':'Housing Type'}, inplace=True)
    df23.rename(columns={'listingType':'Type (Rent, Sale, Vacation)'}, inplace=True)
    df23.rename(columns={'addedOn':'dateAdded'}, inplace=True)
    today = pd.Timestamp(datetime.now().strftime('%Y-%m-%d'))
    dates = pd.to_datetime(df23['dateAdded'])
    diff_days = (today - dates).dt.days
    df23['Days on Market'] = diff_days
    df23['Occupancy']=None
    df23['Number of Guests']=None

    df23['Number of amenities'] = [len(item) for item in df23.get('amenities', [])]

    highEndAmenities = [
        sum(1 for amenity in item if amenity.lower() in ['pool', 'gym', 'spa'])
        for item in df23['amenities']
    ]
    df23['Number of high end amenities (pool, gym, spa)'] = highEndAmenities

    parking=[]
    for item in df23['parking']:
        if str(item)=='nan':
            parking.append(False)
        elif item==None:
            parking.append(False)
        else:
            parking.append(True)
    df23['Parking']=parking 

    df23['localPrice'] = df23['price']
    df23['localCurrency'] = df23['currency']
    
    etbToUsd = currency_converter('USD', 'ETB')
    prices=[]
    for idx,item in enumerate(df23['price']):
        if df23['currency'][idx]=='ETB':
            item=item*etbToUsd
        prices.append(item)
    df23['price']=prices
    df23.rename(columns={'price':'Price'}, inplace=True)
    prices=[]
    for idx,item in enumerate(df23['priceDiff']):
        if df23['currency'][idx]=='ETB':
            item=item*etbToUsd
        prices.append(item)
    df23['priceDiff']=prices

    df23['pricingCriteria'] = df23['pricingCriteria'].apply(
        lambda x: (
            '6 Months' if x and ('six' in x.lower() or '6' in x.lower()) else
            'Month' if x and 'month' in x.lower() else
            'Day' if x and 'day' in x.lower() else
            'Week' if x and 'week' in x.lower() else
            'Year' if x and ('annum' in x.lower() or 'annual' in x.lower()) else None
        )
    )
    df23.rename(columns={'pricingCriteria': 'Price criteria'}, inplace=True)


    conversion_factors = {
        'sqm': 10.7639,     # Square meters to square feet
        'ac': 43560,        # Acres to square feet
        'hectares': 107639, # Hectares to square feet
        'sf': 1             # Square feet remains as is
    }

    df23['Internal Area (s.f)'] = df23.apply(
        lambda row: float(row['size']) * conversion_factors.get(row['sizeUnit'], None)
        if row['size'] and row['sizeUnit'] in conversion_factors else None, axis=1
    )

    df23['Price per s.f.']=df23['Price']/df23['Internal Area (s.f)']
    df23['Fees (commissions, cleaning etc)']=None
    df23['Location: District']=None    
    df23['Location: Country']='Ethiopia'
    df23.rename(columns={'address':'Location: Address'}, inplace=True)
    df23['Location: Lat']=None
    df23['Location: Lon']=None

    df23.rename(columns={'city':'Location: City'}, inplace=True)
    df23.rename(columns={'neighborhood':'Location: Neighbourhood'}, inplace=True)

    df23.drop(['currency','toilets', 'size', 'sizeUnit'], axis=1,inplace=True)
    df23 = df23.reindex(sorted(df23.columns), axis=1)

    return df23