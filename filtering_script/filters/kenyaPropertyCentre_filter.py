from get_data import get_data
import pandas as pd
import re
from convert_currency import currency_converter
from datetime import datetime
from hash_url import hash_url

def kenyaPropertyCentre_filter(log):

    databaseName='kenyaPropertyCentre'
    dbname_1=get_data(databaseName)
    collection_name_1 = dbname_1['propertyDetails']
    log.info(f'{"*"*40}\nCollecting data of {databaseName}')
    data_mongo=list(collection_name_1.find({},{'_id':False}))
    df6=pd.DataFrame(data_mongo,columns=data_mongo[0].keys())
    log.info(f'Filtering data of {databaseName}\n{"*"*40}')

    df6['Website']='kenyapropertycentre.com'
    df6['rehaniId'] = df6['url'].apply(lambda x: hash_url(x))
    df6.rename(columns={'propertyTitle':'Title'}, inplace=True)
    df6.rename(columns={'agentNumber':'Agent Contact'}, inplace=True)
    df6.rename(columns={'agent':'Agent'}, inplace=True)
    df6['Agent Email Address']=None
    df6.rename(columns={'beds':'Beds'}, inplace=True)
    df6.rename(columns={'baths':'Baths'}, inplace=True)
    df6.rename(columns={'propertyType':'Housing Type'}, inplace=True)
    df6['listingType']=df6['listingType'].str.replace('For ','')
    df6['listingType']=df6['listingType'].where(pd.notnull(df6['listingType']), None)
    df6.rename(columns={'listingType':'Type (Rent, Sale, Vacation)'}, inplace=True)
    df6.rename(columns={'addedOn':'dateAdded'}, inplace=True)
    today = pd.Timestamp(datetime.now().strftime('%Y-%m-%d'))
    dates = pd.to_datetime(df6['dateAdded'])
    diff_days = (today - dates).dt.days
    df6['Days on Market'] = diff_days
    df6['Occupancy']=None
    df6['Number of Guests']=None  
    df6['Number of amenities']=None
    df6['Number of high end amenities (pool, gym, spa)']=None
    parking=[]
    for item in df6['parkingSpaces']:
        if str(item)=='nan':
            parking.append(False)
        elif item==None:
            parking.append(False)
        else:
            parking.append(True)
    df6['Parking']=parking 
    df6['localPrice'] = df6['price']
    df6['localCurrency'] = df6['currency']
    kshToUsd = currency_converter('USD', 'KES')
    prices=[]
    for idx,item in enumerate(df6['price']):
        if df6['currency'][idx]=='KSh':
            item=item*kshToUsd
        prices.append(item)
    df6['price']=prices
    df6.rename(columns={'price':'Price'}, inplace=True)
    prices=[]
    for idx,item in enumerate(df6['priceDiff']):
        if df6['currency'][idx]=='KSh':
            item=item*kshToUsd
        prices.append(item)
    df6['pricingCriteria'] = df6['pricingCriteria'].apply(
        lambda x: '6 Months' if 'six' or '6' in x.lower() else (
            'Month' if 'month' in x.lower() else (
            'Day' if 'day' in x.lower() else (
            'Week' if 'week' in x.lower() else (
            'Year' if 'annum' in x.lower() or 'annual' in x.lower() else None))))
    )
    df6.rename(columns={'pricingCriteria':'Price criteria'}, inplace=True)
    df6['priceDiff']=prices
    internalArea=[]
    for item in df6['size']:
        if item==None:
            internalArea.append(None)
        else:
            if 'sqm' in item:
                internalArea.append(float(''.join(re.findall(r'\d+', item)))* 10.7639)
            else:
                internalArea.append(None)
    df6['size']=internalArea
    df6.rename(columns={'size':'Internal Area (s.f)'}, inplace=True)
    df6['Price per s.f.']=df6['Price']/df6['Internal Area (s.f)']
    df6['Fees (commissions, cleaning etc)']=None
    cities=[]
    for item in df6['address']:
        cities.append(item.split(',')[-1].strip())
    df6['Location: City']=cities
    district=[]
    for item in df6['address']:
        district.append(item.split(',')[-2].strip())
    df6['Location: District']=district    
    df6['Location: Country']='Kenya'
    df6.rename(columns={'address':'Location: Address'}, inplace=True)
    df6['Location: Lat']=None
    df6['Location: Lon']=None
    df6['Location: Neighbourhood']=None

    df6['amenities']=None
    df6.drop(['currency','marketStatus','parkingSpaces','toilets', 'lastUpdated', 'plotSize'], axis=1,inplace=True)
    df6 = df6.reindex(sorted(df6.columns), axis=1)

    return df6