from convert_currency import currency_converter
from get_data import get_data
import pandas as pd
from datetime import datetime
from hash_url import hash_url

def jiji_ng_filter(log):

    databaseName='jiji_ng'
    dbname_1=get_data(databaseName)
    collection_name_1 = dbname_1['propertyDetails']
    log.info(f'{"*"*40}\nCollecting data of {databaseName}')
    data_mongo=list(collection_name_1.find({},{'_id':False}))
    df25=pd.DataFrame(data_mongo,columns=data_mongo[0].keys())
    log.info(f'Filtering data of {databaseName}\n{"*"*40}')

    df25['Website']='jiji.ng'
    df25['rehaniId'] = df25['url'].apply(lambda x: hash_url(x))
    df25.rename(columns={'propertyTitle':'Title'}, inplace=True)
    df25.rename(columns={'agentNumber':'Agent Contact'}, inplace=True)
    df25.rename(columns={'agent':'Agent'}, inplace=True)
    df25['Agent Email Address']=None
    df25.rename(columns={'beds':'Beds'}, inplace=True)
    df25.rename(columns={'baths':'Baths'}, inplace=True)
    df25.rename(columns={'propertyType':'Housing Type'}, inplace=True)
    df25.rename(columns={'listingType':'Type (Rent, Sale, Vacation)'}, inplace=True)
    df25.rename(columns={'addedOn':'dateAdded'}, inplace=True)
    today = pd.Timestamp(datetime.now().strftime('%Y-%m-%d'))
    dates = pd.to_datetime(df25['dateAdded'])
    diff_days = (today - dates).dt.days
    df25['Days on Market'] = diff_days
    df25['Occupancy']=None
    df25['Number of Guests']=None

    df25['Number of amenities'] = [len(item) for item in df25.get('amenities', [])]

    highEndAmenities = [
        sum(1 for amenity in item if amenity.lower() in ['pool', 'gym', 'spa'])
        for item in df25['amenities']
    ]
    df25['Number of high end amenities (pool, gym, spa)'] = highEndAmenities

    parking=[]
    for item in df25['parking']:
        if str(item)=='nan':
            parking.append(False)
        elif item==None:
            parking.append(False)
        else:
            parking.append(True)
    df25['Parking']=parking 

    df25['localPrice'] = df25['price']
    df25['localCurrency'] = df25['currency']
    
    ngnToUsd = currency_converter('USD', 'NGN')
    prices=[]
    for idx,item in enumerate(df25['price']):
        if df25['currency'][idx]=='NGN':
            item=item*ngnToUsd
        prices.append(item)
    df25['price']=prices
    df25.rename(columns={'price':'Price'}, inplace=True)
    prices=[]
    for idx,item in enumerate(df25['priceDiff']):
        if df25['currency'][idx]=='NGN':
            item=item*ngnToUsd
        prices.append(item)
    df25['priceDiff']=prices

    df25['pricingCriteria'] = df25['pricingCriteria'].apply(
        lambda x: (
            '6 Months' if x and ('six' in x.lower() or '6' in x.lower()) else
            'Month' if x and 'month' in x.lower() else
            'Day' if x and 'day' in x.lower() else
            'Week' if x and 'week' in x.lower() else
            'Year' if x and ('annum' in x.lower() or 'annual' in x.lower()) else None
        )
    )
    df25.rename(columns={'pricingCriteria': 'Price criteria'}, inplace=True)


    conversion_factors = {
        'sqm': 10.7639,     # Square meters to square feet
        'ac': 43560,        # Acres to square feet
        'hectares': 107639, # Hectares to square feet
        'sf': 1             # Square feet remains as is
    }

    df25['Internal Area (s.f)'] = df25.apply(
        lambda row: float(row['size']) * conversion_factors.get(row['sizeUnit'], None)
        if row['size'] and row['sizeUnit'] in conversion_factors else None, axis=1
    )

    df25['Price per s.f.']=df25['Price']/df25['Internal Area (s.f)']
    df25['Fees (commissions, cleaning etc)']=None
    df25['Location: District']=None    
    df25['Location: Country']='Nigeria'
    df25.rename(columns={'address':'Location: Address'}, inplace=True)
    df25['Location: Lat']=None
    df25['Location: Lon']=None

    df25.rename(columns={'city':'Location: City'}, inplace=True)
    df25.rename(columns={'neighborhood':'Location: Neighbourhood'}, inplace=True)

    df25.drop(['currency','toilets', 'size', 'sizeUnit'], axis=1,inplace=True)
    df25 = df25.reindex(sorted(df25.columns), axis=1)

    return df25