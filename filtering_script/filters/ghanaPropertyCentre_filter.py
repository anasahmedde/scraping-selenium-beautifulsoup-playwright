from get_data import get_data
import pandas as pd
import re
from convert_currency import currency_converter
from datetime import datetime
from hash_url import hash_url

def ghanaPropertyCentre_filter(log):

    databaseName='ghanaPropertyCentre'
    dbname_1=get_data(databaseName)
    collection_name_1 = dbname_1['propertyDetails']
    log.info(f'{"*"*40}\nCollecting data of {databaseName}')
    data_mongo=list(collection_name_1.find({},{'_id':False}))
    df5=pd.DataFrame(data_mongo,columns=data_mongo[0].keys())
    log.info(f'Filtering data of {databaseName}\n{"*"*40}')

    df5['Website']='ghanapropertycentre.com'
    df5['rehaniId'] = df5['url'].apply(lambda x: hash_url(x))
    df5.rename(columns={'propertyTitle':'Title'}, inplace=True)
    df5.rename(columns={'agentNumber':'Agent Contact'}, inplace=True)
    df5.rename(columns={'agent':'Agent'}, inplace=True)
    df5['Agent Email Address']=None
    df5.rename(columns={'beds':'Beds'}, inplace=True)
    df5.rename(columns={'baths':'Baths'}, inplace=True)
    df5.rename(columns={'propertyType':'Housing Type'}, inplace=True)
    df5['listingType']=df5['listingType'].str.replace('For ','')
    df5['listingType']=df5['listingType'].where(pd.notnull(df5['listingType']), None)
    df5.rename(columns={'listingType':'Type (Rent, Sale, Vacation)'}, inplace=True)
    df5.rename(columns={'addedOn':'dateAdded'}, inplace=True)
    today = pd.Timestamp(datetime.now().strftime('%Y-%m-%d'))
    dates = pd.to_datetime(df5['dateAdded'])
    diff_days = (today - dates).dt.days
    df5['Days on Market'] = diff_days
    df5['Occupancy']=None
    df5['Number of Guests']=None
    df5['pricingCriteria'] = df5['pricingCriteria'].apply(
        lambda x: '6 Months' if 'six' or '6' in x.lower() else (
            'Month' if 'month' in x.lower() else (
            'Day' if 'day' in x.lower() else (
            'Week' if 'week' in x.lower() else (
            'Year' if 'annum' in x.lower() or 'annual' in x.lower() else None))))
    )
    df5.rename(columns={'pricingCriteria':'Price criteria'}, inplace=True)
    df5['amenities'] = None
    df5['Number of amenities']=None
    df5['Number of high end amenities (pool, gym, spa)']=None
    parking=[]
    for item in df5['parkingSpaces']:
        if str(item)=='nan':
            parking.append(False)
        elif item==None:
            parking.append(False)
        else:
            parking.append(True)
    df5['Parking']=parking 
    df5['localPrice'] = df5['price']
    df5['localCurrency'] = df5['currency']

    gcToUsd = currency_converter('USD', 'GHS')
    prices=[]
    for idx,item in enumerate(df5['price']):
        if df5['currency'][idx]=='GH₵':
            item=item*gcToUsd
        prices.append(item)
    df5['price']=prices
    df5.rename(columns={'price':'Price'}, inplace=True)
    prices=[]
    for idx,item in enumerate(df5['priceDiff']):
        if df5['currency'][idx]=='GH₵':
            item=item*gcToUsd
        prices.append(item)
    df5['priceDiff']=prices
    df5.rename(columns={'priceDiff':'Price Change'}, inplace=True)
    internalArea=[]
    for item in df5['size']:
        if item==None:
            internalArea.append(None)
        else:
            if 'sqm' in item:
                internalArea.append(float(''.join(re.findall(r'\d+', item)))* 10.7639)
            else:
                internalArea.append(None)
    df5['size']=internalArea
    df5.rename(columns={'size':'Internal Area (s.f)'}, inplace=True)
    df5['Price per s.f.']=df5['Price']/df5['Internal Area (s.f)']
    df5['Fees (commissions, cleaning etc)']=None
    df5['Location: City']='Accra'
    district=[]
    for item in df5['address']:
        district.append(item.split(',')[-2].strip())
    df5['Location: District']=district    
    df5['Location: Country']='Ghana'
    df5.rename(columns={'address':'Location: Address'}, inplace=True)
    df5['Location: Lat']=None
    df5['Location: Lon']=None
    df5['Location: Neighbourhood']=None

    df5.drop(['currency','marketStatus','parkingSpaces','toilets', 'lastUpdated', 'plotSize'], axis=1,inplace=True)
    df5 = df5.reindex(sorted(df5.columns), axis=1)

    return df5