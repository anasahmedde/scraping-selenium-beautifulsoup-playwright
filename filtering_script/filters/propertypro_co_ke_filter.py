from get_data import get_data
import pandas as pd
from convert_currency import currency_converter
from datetime import datetime
from hash_url import hash_url

def propertypro_co_ke_filter(log):

    databaseName='propertypro_co_ke'
    dbname_1=get_data(databaseName)

    collection_name_1 = dbname_1['propertyDetails']
    log.info(f'{"*"*40}\nCollecting data of {databaseName}')
    data_mongo=list(collection_name_1.find({},{'_id':False}))
    df8=pd.DataFrame(data_mongo,columns=data_mongo[0].keys())
    log.info(f'Filtering data of {databaseName}\n{"*"*40}')

    df8['Website']='propertypro.co.ke'
    df8['rehaniId'] = df8['url'].apply(lambda x: hash_url(x))
    df8.rename(columns={'title':'Title'}, inplace=True)
    df8.rename(columns={'agentNumber':'Agent Contact'}, inplace=True)
    df8.rename(columns={'agent':'Agent'}, inplace=True)
    df8['Agent Email Address']=None
    df8.rename(columns={'beds':'Beds'}, inplace=True)
    df8.rename(columns={'baths':'Baths'}, inplace=True)
    df8['Fees (commissions, cleaning etc)']=None

    df8.rename(columns={'housingType':'Housing Type'}, inplace=True)
    amenities=[]
    for item in df8['amenities']:   
        amenities.append(len(item))
    df8['Number of amenities']=amenities
    highEndAmenities=[]
    for item in df8['amenities']:
        counter=0
        if 'pool' in str(item).lower():
            counter=counter+1
        if 'gym' in str(item).lower(): 
            counter=counter+1
        if 'spa' in str(item).lower():
            counter=counter+1
        highEndAmenities.append(counter)
    df8['Number of high end amenities (pool, gym, spa)']=highEndAmenities
    df8['Number of Guests']=None
    df8['Occupancy']=None
    df8['Parking']=None
    df8['listingType']=df8['listingType'].str.replace('rent','Rent')
    df8['listingType']=df8['listingType'].str.replace('shortlet','Rent')
    df8['listingType']=df8['listingType'].str.replace('sale','Sale')
    df8.rename(columns={'listingType':'Type (Rent, Sale, Vacation)'}, inplace=True)
    dates = pd.to_datetime(df8['dateAdded'], format='%d %b, %Y')
    today = pd.Timestamp(datetime.now().strftime('%Y-%m-%d'))
    diff_days = [(today - date).days for date in dates]
    df8['Days on Market']=diff_days
    df8['localPrice'] = df8['price']
    df8['localCurrency'] = df8['currency']
    kesToUsd = currency_converter('USD', 'KES')
    prices=[]
    for idx,item in enumerate(df8['price']):
        if df8['currency'][idx]=='KES':
            item=item*kesToUsd if item else None
        prices.append(item)
    df8['price']=prices
    df8.rename(columns={'price':'Price'}, inplace=True)
    prices=[]
    for idx,item in enumerate(df8['priceDiff']):
        if df8['currency'][idx]=='KES':
            item=item*kesToUsd if item else None
        prices.append(item)
    df8['priceDiff']=prices
    df8.rename(columns={'priceDiff':'Price Change'}, inplace=True)
    pricingCriteria=[]
    for item in df8['Type (Rent, Sale, Vacation)']:
        if item=='Rent':
            pricingCriteria.append('Month')
        else:
            pricingCriteria.append(None)
    df8['Price criteria']=pricingCriteria
    df8['Price per s.f.']=None
    df8['Internal Area (s.f)']=None
    df8['Location: Lat']=None
    df8['Location: Lon']=None
    cities=[]
    for item in df8['location']:
        if item:
            cities.append(item.split()[-1])
        else:
            cities.append(None)
    df8['Location: City']=cities
    df8['Location: District']=None
    df8['Location: Neighbourhood']=None
    df8['Location: Country']='Kenya'
    df8.rename(columns={'location':'Location: Address'}, inplace=True)
    df8['Location: City']=df8['Location: City'].str.replace('Gishu','Uasin Gishu')
    df8['Location: City']=df8['Location: City'].str.replace('Bay','Homa Bay')
    df8['Location: City']=df8['Location: City'].str.replace("Murang'a",'Muranga')

    df8.drop(['currency','toilets', 'city'], axis=1,inplace=True)
    df8 = df8.reindex(sorted(df8.columns), axis=1)

    return df8