from get_data import get_data
import pandas as pd
from convert_currency import currency_converter
from datetime import datetime
from hash_url import hash_url

def propertypro_co_ug_filter(log):

    databaseName='propertypro_co_ug'
    dbname_1=get_data(databaseName)
    collection_name_1 = dbname_1['propertyDetails']
    log.info(f'{"*"*40}\nCollecting data of {databaseName}')
    data_mongo=list(collection_name_1.find({},{'_id':False}))
    df9=pd.DataFrame(data_mongo,columns=data_mongo[0].keys())
    log.info(f'Filtering data of {databaseName}\n{"*"*40}')

    df9['Website']='propertypro.co.ug'
    df9['rehaniId'] = df9['url'].apply(lambda x: hash_url(x))
    df9.rename(columns={'title':'Title'}, inplace=True)
    df9.rename(columns={'agentNumber':'Agent Contact'}, inplace=True)
    df9.rename(columns={'agent':'Agent'}, inplace=True)
    df9['Agent Email Address']=None
    df9.rename(columns={'beds':'Beds'}, inplace=True)
    df9.rename(columns={'baths':'Baths'}, inplace=True)
    df9['Fees (commissions, cleaning etc)']=None

    df9.rename(columns={'housingType':'Housing Type'}, inplace=True)
    amenities=[]
    for item in df9['amenities']:   
        amenities.append(len(item))
    df9['Number of amenities']=amenities
    highEndAmenities=[]
    for item in df9['amenities']:
        counter=0
        if 'pool' in str(item).lower():
            counter=counter+1
        if 'gym' in str(item).lower(): 
            counter=counter+1
        if 'spa' in str(item).lower():
            counter=counter+1
        highEndAmenities.append(counter)
    df9['Number of high end amenities (pool, gym, spa)']=highEndAmenities
    df9['Number of Guests']=None
    df9['Occupancy']=None
    df9['Parking']=None
    df9['listingType']=df9['listingType'].str.replace('rent','Rent')
    df9['listingType']=df9['listingType'].str.replace('shortlet','Rent')
    df9['listingType']=df9['listingType'].str.replace('sale','Sale')
    df9.rename(columns={'listingType':'Type (Rent, Sale, Vacation)'}, inplace=True)

    dates = pd.to_datetime(df9['dateAdded'], format='%d %b, %Y')
    today = pd.Timestamp(datetime.now().strftime('%Y-%m-%d'))
    diff_days = [(today - date).days for date in dates]
    df9['Days on Market']=diff_days
    df9['localPrice'] = df9['price']
    df9['localCurrency'] = df9['currency']
    
    ugxToUsd = currency_converter('USD', 'UGX')
    prices=[]
    for idx,item in enumerate(df9['price']):
        if df9['currency'][idx]=='UGX':
            item=item*ugxToUsd
        prices.append(item)
    df9['price']=prices
    df9.rename(columns={'price':'Price'}, inplace=True)
    prices=[]
    for idx,item in enumerate(df9['priceDiff']):
        if df9['currency'][idx]=='UGX':
            item=item*ugxToUsd
        prices.append(item)
    df9['priceDiff']=prices
    df9.rename(columns={'priceDiff':'Price Change'}, inplace=True)
    pricingCriteria=[]
    for item in df9['Type (Rent, Sale, Vacation)']:
        if item=='Rent':
            pricingCriteria.append('Month')
        else:
            pricingCriteria.append(None)
    df9['Price criteria']=pricingCriteria
    df9['Price per s.f.']=None
    df9['Internal Area (s.f)']=None
    df9['Location: Lat']=None
    df9['Location: Lon']=None
    cities=[]
    for item in df9['location']:
        cities.append(item.split()[-2])
    df9['Location: City']=cities   
    df9['Location: Country']='Uganda'
    df9.rename(columns={'location':'Location: Address'}, inplace=True)
    df9['Location: District']=None
    df9['Location: Neighbourhood']=None

    df9.drop(['currency','toilets', 'city'], axis=1,inplace=True)
    df9 = df9.reindex(sorted(df9.columns), axis=1)

    return df9
