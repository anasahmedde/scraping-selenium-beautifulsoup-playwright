from get_data import get_data
import pandas as pd
from datetime import datetime
from hash_url import hash_url

def propertypro_co_zw_filter(log):

    databaseName='propertypro_co_zw'
    dbname_1=get_data(databaseName)

    collection_name_1 = dbname_1['propertyDetails']
    log.info(f'{"*"*40}\nCollecting data of {databaseName}')
    data_mongo=list(collection_name_1.find({},{'_id':False}))
    df16=pd.DataFrame(data_mongo,columns=data_mongo[0].keys())
    log.info(f'Filtering data of {databaseName}\n{"*"*40}')

    df16['Website']='propertypro.co.zw'
    df16['rehaniId'] = df16['url'].apply(lambda x: hash_url(x))
    df16.rename(columns={'title':'Title'}, inplace=True)
    df16.rename(columns={'agentNumber':'Agent Contact'}, inplace=True)
    df16.rename(columns={'agent':'Agent'}, inplace=True)
    df16['Agent Email Address']=None
    df16.rename(columns={'beds':'Beds'}, inplace=True)
    df16.rename(columns={'baths':'Baths'}, inplace=True)
    df16['Fees (commissions, cleaning etc)']=None

    df16.rename(columns={'housingType':'Housing Type'}, inplace=True)
    amenities=[]
    for item in df16['amenities']:   
        amenities.append(len(item))
    df16['Number of amenities']=amenities
    highEndAmenities=[]
    for item in df16['amenities']:
        counter=0
        if 'pool' in str(item).lower():
            counter=counter+1
        if 'gym' in str(item).lower(): 
            counter=counter+1
        if 'spa' in str(item).lower():
            counter=counter+1
        highEndAmenities.append(counter)
    df16['Number of high end amenities (pool, gym, spa)']=highEndAmenities
    df16['Number of Guests']=None
    df16['Occupancy']=None
    df16['Parking']=None
    df16['listingType']=df16['listingType'].str.replace('rent','Rent')
    df16['listingType']=df16['listingType'].str.replace('shortlet','Rent')
    df16['listingType']=df16['listingType'].str.replace('sale','Sale')
    df16.rename(columns={'listingType':'Type (Rent, Sale, Vacation)'}, inplace=True)
    dates = pd.to_datetime(df16['dateAdded'], format='%d %b, %Y')
    today = pd.Timestamp(datetime.now().strftime('%Y-%m-%d'))
    diff_days = [(today - date).days for date in dates]
    df16['Days on Market']=diff_days

    df16['localPrice'] = df16['price']
    df16['localCurrency'] = df16['currency']
    zidToUsd=0.0027631943 
    prices=[]
    for idx,item in enumerate(df16['price']):
        if df16['currency'][idx]=='ZID':
            item=item*zidToUsd
        prices.append(item)
    df16['price']=prices
    df16.rename(columns={'price':'Price'}, inplace=True)

    prices=[]
    for idx,item in enumerate(df16['priceDiff']):
        if df16['currency'][idx]=='ZID':
            item=item*zidToUsd
        prices.append(item)
    df16['priceDiff']=prices
    df16.rename(columns={'priceDiff':'Price Change'}, inplace=True)
    pricingCriteria=[]
    for item in df16['Type (Rent, Sale, Vacation)']:
        if item=='Rent':
            pricingCriteria.append('Month')
        else:
            pricingCriteria.append(None)
    df16['Price criteria']=pricingCriteria
    df16['Price per s.f.']=None
    df16['Location: Lat']=None
    df16['Location: Lon']=None
    cities=[]
    for item in df16['location']:
        if item==None:
            cities.append(None)
        else:
            cities.append(item.split()[-1])
    df16['Location: City']=cities
    df16['Location: District']=None
    df16['Location: Neighbourhood']=None
    df16['Location: Country']='Zimbabwe'
    df16.rename(columns={'location':'Location: Address'}, inplace=True)

    df16['Location: City']=df16['Location: City'].str.replace('\d+', '')
    df16['Location: City']=df16['Location: City'].str.strip()
    df16['Location: City']=df16['Location: City'].str.replace('County','')

    df16.drop(['currency','toilets','city'], axis=1,inplace=True)
    df16 = df16.reindex(sorted(df16.columns), axis=1)

    return df16