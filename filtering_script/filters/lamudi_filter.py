from get_data import get_data
import pandas as pd
import re
from convert_currency import currency_converter
from datetime import datetime
from hash_url import hash_url

def lamudi_filter(log):

    databaseName='lamudi'
    dbname_1=get_data(databaseName)
    collection_name_1 = dbname_1['propertyDetails']
    log.info(f'{"*"*40}\nCollecting data of {databaseName}')
    data_mongo=list(collection_name_1.find({},{'_id':False}))
    df11=pd.DataFrame(data_mongo,columns=data_mongo[0].keys())
    log.info(f'Filtering data of {databaseName}\n{"*"*40}')

    df11['Website'] = "lamudi"
    df11['rehaniId'] = df11['url'].apply(lambda x: hash_url(x))
    df11.rename(columns={'propertyTitle':'Title'}, inplace=True)
    df11.rename(columns={'agentNumber':'Agent Contact'}, inplace=True)
    df11.rename(columns={'agent':'Agent'}, inplace=True)
    df11.rename(columns={'agentEmail':'Agent Email Address'}, inplace=True)
    df11.rename(columns={'beds':'Beds'}, inplace=True)
    df11.rename(columns={'baths':'Baths'}, inplace=True)
    df11['Fees (commissions, cleaning etc)']=None
    amenities=[]
    for item in df11['amenities']:   
        amenities.append(len(item))
    df11['Number of amenities']=amenities
    highEndAmenities=[]
    for item in df11['amenities']:
        counter=0
        if 'pool' in str(item).lower():
            counter=counter+1
        if 'gym' in str(item).lower(): 
            counter=counter+1
        if 'spa' in str(item).lower():
            counter=counter+1
        highEndAmenities.append(counter)
    df11['Number of high end amenities (pool, gym, spa)']=highEndAmenities
    df11.rename(columns={'category':'Housing Type'}, inplace=True)
    df11['Number of Guests']=None
    df11['Occupancy']=None
    df11['Parking']=None

    df11['listingType']=df11['listingType'].str.replace('For sale','Sale')
    df11['listingType']=df11['listingType'].str.replace('For rent','Rent')
    df11.rename(columns={'listingType':'Type (Rent, Sale, Vacation)'}, inplace=True)
    df11.rename(columns={'dateListed':'dateAdded'}, inplace=True)
    today = pd.Timestamp(datetime.now().strftime('%Y-%m-%d'))
    dates = pd.to_datetime(df11['dateAdded'])
    diff_days = (today - dates).dt.days
    df11['Days on Market'] = diff_days
    
    ugxToUsd = currency_converter('USD', 'UGX')
    prices=[]
    for idx,item in enumerate(df11['price']):
        if df11['currency'][idx]=='Ugx':
            item=item*ugxToUsd
        prices.append(item)

    df11['localPrice'] = df11['price']
    df11['localCurrency'] = df11['currency']
    df11['price']=prices
    df11.rename(columns={'price':'Price'}, inplace=True)

    prices=[]
    for idx,item in enumerate(df11['priceDiff']):
        if df11['currency'][idx]=='Ugx':
            item=item*ugxToUsd
        prices.append(item)
    df11['priceDiff']=prices
    df11.rename(columns={'priceDiff':'Price Change'}, inplace=True)
    priceCriteria=[]
    for item in (df11['Type (Rent, Sale, Vacation)']):
        if item=='Rent':
            priceCriteria.append('Month')
        else:
            priceCriteria.append(None)
    df11['Price criteria']=priceCriteria  
    unitsConv={
        'Decimals':435.56,
        'Sq Meters':10.7639,
        'Sq Miles':27880000,
        'Acres':43560,
        'Units':1,
        'Sq Feet':1,
    }
    converted_sizes=[]
    for item in df11['size']:
        if item==None:
            converted_sizes.append(None)
        else:
            try:
                converted_sizes.append(float(item.split()[0])*unitsConv[re.sub('\d', '', item.replace('.','')).strip()])
            except:
                converted_sizes.append(None)
    df11['Internal Area (s.f)']=converted_sizes
    df11['Price per s.f.']=df11['Price']/df11['Internal Area (s.f)']
    df11['Location: Country']='Uganda'
    df11['Location: Address']=df11['location']
    df11.rename(columns={'location':'Location: District'}, inplace=True)
    df11.rename(columns={'district':'Location: City'}, inplace=True)
    df11['Location: Lat']=None
    df11['Location: Lon']=None
    df11['Location: Neighbourhood']=None
    df11['Location: City']=df11['Location: City'].str.replace('\d+', '')
    df11['Location: City']=df11['Location: City'].str.strip()
    df11['Location: City']=df11['Location: City'].str.replace('County','')

    df11.drop(["currency","size","tenure"], axis=1,inplace=True)
    df11 = df11.reindex(sorted(df11.columns), axis=1)

    return df11