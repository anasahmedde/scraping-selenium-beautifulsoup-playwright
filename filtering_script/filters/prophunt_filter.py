from get_data import get_data
import pandas as pd
from convert_currency import currency_converter
from datetime import datetime
from hash_url import hash_url

def prophunt_filter(log):

    databaseName='prophunt'
    dbname_1=get_data(databaseName)
    collection_name_1 = dbname_1['propertyDetails']
    log.info(f'{"*"*40}\nCollecting data of {databaseName}')
    data_mongo=list(collection_name_1.find({},{'_id':False}))
    df7=pd.DataFrame(data_mongo,columns=data_mongo[0].keys())
    log.info(f'Filtering data of {databaseName}\n{"*"*40}')

    df7['Website']='prophuntgh.com'
    df7['rehaniId'] = df7['url'].apply(lambda x: hash_url(x))
    df7.rename(columns={'title':'Title'}, inplace=True)
    df7.rename(columns={'agentNumber':'Agent Contact'}, inplace=True)
    df7.rename(columns={'agent':'Agent'}, inplace=True)
    df7['Agent Email Address']=None
    df7.rename(columns={'beds':'Beds'}, inplace=True)
    df7.rename(columns={'baths':'Baths'}, inplace=True)
    df7['Fees (commissions, cleaning etc)']=None

    df7['Housing Type']=df7['Title']
    amenities=[]
    for item in df7['amenities']:   
        amenities.append(len(item))
    df7['Number of amenities']=amenities
    highEndAmenities=[]
    for item in df7['amenities']:
        counter=0
        if 'pool' in str(item).lower():
            counter=counter+1
        if 'gym' in str(item).lower(): 
            counter=counter+1
        if 'spa' in str(item).lower():
            counter=counter+1
        highEndAmenities.append(counter)
    df7['Number of high end amenities (pool, gym, spa)']=highEndAmenities
    df7['Number of Guests']=None
    df7['Occupancy']=None
    df7.rename(columns={'parking':'Parking'}, inplace=True)
    df7['Housing Type']=None
    df7['listingType']=df7['listingType'].str.replace('rent','Rent')
    df7['listingType']=df7['listingType'].str.replace('development','Sale')
    df7['listingType']=df7['listingType'].str.replace('sale','Sale')
    df7.rename(columns={'listingType':'Type (Rent, Sale, Vacation)'}, inplace=True)
    dates = pd.to_datetime(df7['dateListed'], format='%d %b, %Y')
    today = pd.Timestamp(datetime.now().strftime('%Y-%m-%d'))
    diff_days = [(today - date).days for date in dates]
    df7['Days on Market']=diff_days
    df7.rename(columns={'dateListed':'dateAdded'}, inplace=True)
    df7.rename(columns={'longitude':'Location: Lon'}, inplace=True)
    df7.rename(columns={'latitude':'Location: Lat'}, inplace=True)
    df7['Price per s.f.']=None
    df7['localPrice'] = df7['price']
    df7['localCurrency'] = df7['currency']

    gcToUsd = currency_converter('USD', 'GHS')
    prices=[]
    for idx,item in enumerate(df7['price']):
        if df7['currency'][idx]=='gh₵':
            item=item*gcToUsd
        prices.append(item)
    df7['price']=prices
    df7.rename(columns={'price':'Price'}, inplace=True)
    prices=[]
    for idx,item in enumerate(df7['priceDiff']):
        if df7['currency'][idx]=='gh₵':
            item=item*gcToUsd
        prices.append(item)
    df7['priceDiff']=prices
    df7.rename(columns={'priceDiff':'Price Change'}, inplace=True)
    pricingCriteria=[]
    for item in df7['pricingCriteria']:
        if item==None:
            pricingCriteria.append(None)
        elif item=='Month':
            pricingCriteria.append('Month')
        else:
            pricingCriteria.append(None)
    df7.rename(columns={'pricingCriteria':'Price criteria'}, inplace=True)  
    df7['Location: City']=None
    df7['Location: District']=None
    df7['Location: Neighbourhood']=None
    df7['locationAddress'] = None

    df7['Location: Country']='Ghana'
    df7.drop(['currency','parkingVehicles'], axis=1,inplace=True)
    df7 = df7.reindex(sorted(df7.columns), axis=1)

    return df7