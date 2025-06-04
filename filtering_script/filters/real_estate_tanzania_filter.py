from get_data import get_data
import pandas as pd
import re
from convert_currency import currency_converter
from datetime import datetime
from hash_url import hash_url

def real_estate_tanzania_filter(log):

    databaseName='realEstateTanzania'
    dbname_1=get_data(databaseName)

    collection_name_1 = dbname_1['propertyDetails']
    log.info(f'{"*"*40}\nCollecting data of {databaseName}')
    data_mongo=list(collection_name_1.find({},{'_id':False}))
    df18=pd.DataFrame(data_mongo,columns=data_mongo[0].keys())
    log.info(f'Filtering data of {databaseName}\n{"*"*40}')

    df18['Website']='realestatetanzania.com'
    df18['rehaniId'] = df18['url'].apply(lambda x: hash_url(x))
    df18.rename(columns={'propertyTitle':'Title'}, inplace=True)
    df18['agentNumber']=df18['agentNumber'].str.replace(' ','')
    df18.rename(columns={'agentNumber':'Agent Contact'}, inplace=True)
    df18.rename(columns={'agent':'Agent'}, inplace=True)
    df18['Agent Email Address']=None
    df18.rename(columns={'beds':'Beds'}, inplace=True)
    df18.rename(columns={'baths':'Baths'}, inplace=True)
    df18['Fees (commissions, cleaning etc)']=None
    df18['Number of amenities']=None
    df18['Number of high end amenities (pool, gym, spa)']=None
    df18['Number of Guests']=None
    df18['Occupancy']=None
    df18['Parking']=None
    df18['listingType']=df18['listingType'].str.replace('For ','')
    df18['listingType']=df18['listingType'].str.replace('Sold ','Sale')
    df18['listingType']=df18['listingType'].str.replace('Rented ','Rent')
    df18['listingType']=df18['listingType'].replace('Not Available ',None)
    df18.rename(columns={'listingType':'Type (Rent, Sale, Vacation)'}, inplace=True)
    df18.rename(columns={'propertyType':'Housing Type'}, inplace=True)

    tzsToUsd = currency_converter('USD', 'TZS')
    df18.rename(columns={'dateUpdated':'dateAdded'}, inplace=True)
    today = pd.Timestamp(datetime.now().strftime('%Y-%m-%d'))
    dates = pd.to_datetime(df18['dateAdded'])
    diff_days = (today - dates).dt.days
    df18['Days on Market'] = diff_days

    df18['localPrice'] = df18['price']
    df18['localCurrency'] = df18['currency']
    prices=[]
    for idx,item in enumerate(df18['price']):
        if df18['currency'][idx]=='TZS' or df18['currency'][idx]=='TSZ':
            item=item*tzsToUsd     
        prices.append(item)
    df18['price']=prices
    df18.rename(columns={'price':'Price'}, inplace=True)
    prices=[]
    for idx,item in enumerate(df18['priceDiff']):
        if df18['currency'][idx]=='TZS' or df18['currency'][idx]=='TSZ':
            item=item*tzsToUsd     
        prices.append(item)
    df18['priceDiff']=prices

    converted_size=[]
    for i in df18['size']:
        if str(i)=='nan' or i==None:
            converted_size.append(None)
            continue
        i=i.lower().replace(' ','').replace(',','')
        if 'sqm' in i:
            if len(re.findall(r"[-+]?(?:\d*\.*\d+)",i.split('sqmt')[0]))!=0:
                size=float(re.findall(r"[-+]?(?:\d*\.*\d+)",i.split('sqmt')[0])[0])*10.7639
            else:
                size=None
            converted_size.append(size)
        elif 'acre' in i:
            if len(re.findall(r"[-+]?(?:\d*\.*\d+)",i.split('acre')[0]))!=0:
                size=float(re.findall(r"[-+]?(?:\d*\.*\d+)",i.split('acre')[0])[0])*43560
            else:
                size=None        
            converted_size.append(size)
        elif 'sqft' in i:
            if len(re.findall(r"[-+]?(?:\d*\.*\d+)",i.split('sqft')[0]))!=0:
                size=float(re.findall(r"[-+]?(?:\d*\.*\d+)",i.split('sqft')[0])[0])
            else:
                size=None
            converted_size.append(size)
        elif 'm²' in i:
            if len(re.findall(r"[-+]?(?:\d*\.*\d+)",i.split('m²')[0]))!=0:
                size=float(re.findall(r"[-+]?(?:\d*\.*\d+)",i.split('m²')[0])[0])*10.7639
            else:
                size=None
            converted_size.append(size)
        else:
            converted_size.append(None)
        
    df18['Internal Area (s.f)']=converted_size       
    df18['Price per s.f.']=df18['Price']/df18['Internal Area (s.f)']

    df18['pricingCriteria'] = df18['pricingCriteria'].apply(
        lambda x: '6 Months' if 'six' or '6' in x.lower() else (
            'Month' if 'month' in x.lower() else (
            'Day' if 'day' in x.lower() else (
            'Week' if 'week' in x.lower() else (
            'Year' if 'annum' in x.lower() or 'annual' in x.lower() else None))))
    )

    df18['Location: Country']='Tanzania'
    df18['Location: Lat']=None
    df18['Location: Lon']=None
    df18.rename(columns={'address':'Location: Address'}, inplace=True)
    df18.rename(columns={'city':'Location: City'}, inplace=True)
    df18['Location: District']=None
    df18['Location: Neighbourhood']=None
    df18['Location: City']=df18['Location: City'].str.replace('\d+', '')
    df18['Location: City']=df18['Location: City'].str.strip()
    df18['Location: City']=df18['Location: City'].str.replace('County','')

    df18['amenities']=None
    df18.drop(["currency","location","pricingCriteria","size","state"], axis=1,inplace=True)
    df18 = df18.reindex(sorted(df18.columns), axis=1)

    return df18