from get_data import get_data
import pandas as pd
from convert_currency import currency_converter
from datetime import datetime
from hash_url import hash_url

def propertypro_ng_filter(log):

    databaseName='propertypro_ng'
    dbname_1=get_data(databaseName)

    collection_name_1 = dbname_1['propertyDetails']
    log.info(f'{"*"*40}\nCollecting data of {databaseName}')
    data_mongo=list(collection_name_1.find({},{'_id':False}))
    df17=pd.DataFrame(data_mongo,columns=data_mongo[0].keys())
    log.info(f'Filtering data of {databaseName}\n{"*"*40}')

    df17['Website']='propertypro.ng'
    df17['rehaniId'] = df17['url'].apply(lambda x: hash_url(x))
    df17.rename(columns={'title':'Title'}, inplace=True)
    df17.rename(columns={'agentNumber':'Agent Contact'}, inplace=True)
    df17.rename(columns={'agent':'Agent'}, inplace=True)
    df17['Agent Email Address']=None
    df17.rename(columns={'beds':'Beds'}, inplace=True)
    df17.rename(columns={'baths':'Baths'}, inplace=True)
    df17['Fees (commissions, cleaning etc)']=None

    df17.rename(columns={'housingType':'Housing Type'}, inplace=True)
    amenities=[]
    for item in df17['amenities']:   
        amenities.append(len(item))
    df17['Number of amenities']=amenities
    highEndAmenities=[]
    for item in df17['amenities']:
        counter=0
        if 'pool' in str(item).lower():
            counter=counter+1
        if 'gym' in str(item).lower(): 
            counter=counter+1
        if 'spa' in str(item).lower():
            counter=counter+1
        highEndAmenities.append(counter)
    df17['Number of high end amenities (pool, gym, spa)']=highEndAmenities
    df17['Number of Guests']=None
    df17['Occupancy']=None
    df17['Parking']=None
    df17['listingType']=df17['listingType'].str.replace('rent','Rent')
    df17['listingType']=df17['listingType'].str.replace('shortlet','Rent')
    df17['listingType']=df17['listingType'].str.replace('sale','Sale')
    df17.rename(columns={'listingType':'Type (Rent, Sale, Vacation)'}, inplace=True)
    dates = pd.to_datetime(df17['dateAdded'], format='%d %b, %Y')
    today = pd.Timestamp(datetime.now().strftime('%Y-%m-%d'))
    diff_days = [(today - date).days for date in dates]
    df17['Days on Market']=diff_days
    df17['localPrice'] = df17['price']
    df17['localCurrency'] = df17['currency']
    ngnToUsd = currency_converter('USD', 'NGN')
    prices=[]
    for idx,item in enumerate(df17['price']):
        if df17['currency'][idx]=='NGN':
            item=item*ngnToUsd
        prices.append(item)
    df17['price']=prices
    df17.rename(columns={'price':'Price'}, inplace=True)

    prices=[]
    for idx,item in enumerate(df17['priceDiff']):
        if df17['currency'][idx]=='NGN':
            item=item*ngnToUsd
        prices.append(item)
    df17['priceDiff']=prices
    df17.rename(columns={'priceDiff':'Price Change'}, inplace=True)
    pricingCriteria=[]
    for item in df17['Type (Rent, Sale, Vacation)']:
        if item=='Rent':
            pricingCriteria.append('Month')
        else:
            pricingCriteria.append(None)
    df17['Price criteria']=pricingCriteria
    df17['Price per s.f.']=None
    df17['Internal Area (s.f)']=None
    df17['Location: Lat']=None
    df17['Location: Lon']=None     
    cities=[]
    for item in df17['location']:
        if item==None:
            cities.append(None)
        else:
            cities.append(item.split()[-1])
    df17['Location: City']=cities
    df17['Location: District']=None
    df17['Location: Neighbourhood']=None
    df17['Location: Country']='Nigeria'
    df17.rename(columns={'location':'Location: Address'}, inplace=True)

    df17['Location: City']=df17['Location: City'].str.replace('\d+', '')
    df17['Location: City']=df17['Location: City'].str.strip()
    df17['Location: City']=df17['Location: City'].str.replace('County','')

    df17.drop(['currency','toilets', 'lastUpdated', 'city'], axis=1,inplace=True)
    df17 = df17.reindex(sorted(df17.columns), axis=1)

    return df17