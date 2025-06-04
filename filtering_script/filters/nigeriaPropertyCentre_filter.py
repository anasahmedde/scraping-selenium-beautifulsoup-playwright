from get_data import get_data
import pandas as pd
import re
from convert_currency import currency_converter
from datetime import datetime
from hash_url import hash_url

def nigeriaPropertyCentre_filter(log):

    databaseName='nigeriaPropertyCentre'
    dbname_1=get_data(databaseName)

    collection_name_1 = dbname_1['propertyDetails']
    log.info(f'{"*"*40}\nCollecting data of {databaseName}')
    data_mongo=list(collection_name_1.find({},{'_id':False}))
    df12=pd.DataFrame(data_mongo,columns=data_mongo[0].keys())
    log.info(f'Filtering data of {databaseName}\n{"*"*40}')

    df12['Website'] = "nigeriapropertycenter.com"
    df12['rehaniId'] = df12['url'].apply(lambda x: hash_url(x))
    df12.rename(columns={'propertyTitle':'Title'}, inplace=True)
    df12.rename(columns={'agentNumber':'Agent Contact'}, inplace=True)
    df12.rename(columns={'agent':'Agent'}, inplace=True)
    df12['Agent Email Address']=None
    df12.rename(columns={'beds':'Beds'}, inplace=True)
    df12.rename(columns={'baths':'Baths'}, inplace=True)
    df12.rename(columns={'propertyType':'Housing Type'}, inplace=True)
    df12['listingType']=df12['listingType'].str.replace('For ','')
    df12['listingType']=df12['listingType'].where(pd.notnull(df12['listingType']), None)
    df12.rename(columns={'listingType':'Type (Rent, Sale, Vacation)'}, inplace=True)
    df12.rename(columns={'addedOn':'dateAdded'}, inplace=True)
    today = pd.Timestamp(datetime.now().strftime('%Y-%m-%d'))
    dates = pd.to_datetime(df12['dateAdded'])
    diff_days = (today - dates).dt.days
    df12['Days on Market'] = diff_days
    df12['Occupancy']=None
    df12['Number of Guests']=None
    df12['Number of amenities']=None
    df12['Number of high end amenities (pool, gym, spa)']=None
    parking=[]
    for item in df12['parkingSpaces']:
        if str(item)=='nan':
            parking.append(False)
        elif item==None:
            parking.append(False)
        else:
            parking.append(True)
    df12['Parking']=parking 

    ngnToUsd = currency_converter('USD', 'NGN')
    df12['localPrice'] = df12['price']
    df12['localCurrency'] = df12['currency']
    prices=[]
    for idx,item in enumerate(df12['price']):
        if df12['currency'][idx]=='₦':
            item=item*ngnToUsd
        prices.append(item)
    df12['price']=prices
    df12.rename(columns={'price':'Price'}, inplace=True)

    prices=[]
    for idx,item in enumerate(df12['priceDiff']):
        if df12['currency'][idx]=='₦':
            item=item*ngnToUsd
        prices.append(item)

    df12['pricingCriteria'] = df12['pricingCriteria'].apply(
        lambda x: '6 Months' if 'six' or '6' in x.lower() else (
            'Month' if 'month' in x.lower() else (
            'Day' if 'day' in x.lower() else (
            'Week' if 'week' in x.lower() else (
            'Year' if 'annum' in x.lower() or 'annual' in x.lower() else None))))
    )
    df12.rename(columns={'pricingCriteria':'Price criteria'}, inplace=True)
    df12['priceDiff']=prices
    internalArea=[]
    for item in df12['size']:
        if item==None:
            internalArea.append(None)
        else:
            if 'sqm' in item:
                internalArea.append(float(''.join(re.findall(r'\d+', item)))* 10.7639)
            else:
                internalArea.append(None)
    df12['size']=internalArea
    df12.rename(columns={'size':'Internal Area (s.f)'}, inplace=True)
    df12['Price per s.f.']=df12['Price']/df12['Internal Area (s.f)']
    df12['Fees (commissions, cleaning etc)']=None
    cities=[]
    for item in df12['address']:
        cities.append(item.split(',')[-1].strip())
    df12['Location: City']=cities


    district=[]
    for item in df12['address']:
        district.append(item.split(',')[-2].strip())
    df12['Location: District']=district    
    df12['Location: Country']='Nigeria'
    df12.rename(columns={'address':'Location: Address'}, inplace=True)
    df12['Location: Lat']=None
    df12['Location: Lon']=None
    df12['Location: Neighbourhood']=None

    df12['Location: City']=df12['Location: City'].str.replace('\d+', '')
    df12['Location: City']=df12['Location: City'].str.strip()
    df12['Location: City']=df12['Location: City'].str.replace('County','')

    df12['amenities']=None
    df12.drop(['currency','marketStatus','parkingSpaces','toilets', 'lastUpdated', 'plotSize'], axis=1,inplace=True)
    df12 = df12.reindex(sorted(df12.columns), axis=1)

    return df12