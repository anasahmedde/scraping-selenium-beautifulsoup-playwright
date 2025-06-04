from get_data import get_data
import pandas as pd
from convert_currency import currency_converter

def property24_co_ke_filter(log):

    databaseName='property24_co_ke'
    dbname_1=get_data(databaseName)

    collection_name_1 = dbname_1['propertyDetails']
    log.info(f'{"*"*40}\nCollecting data of {databaseName}')
    data_mongo=list(collection_name_1.find({},{'_id':False}))
    df15=pd.DataFrame(data_mongo,columns=data_mongo[0].keys())
    log.info(f'Filtering data of {databaseName}\n{"*"*40}')

    df15['Website'] = "property24.co.ke"
    unitsDict={
        'm²':10.7639,
        'ha':107639,
        'acres':43560,
        'acre':43560,
    }
    converted_size=[]
    for idx,item in enumerate(df15['erfSize']):
        if str(item)=='nan' or item==None:
            if str(df15['floorSize'][idx])=='nan' or df15['floorSize'][idx]==None:
                converted_size.append(None)
            else:
                item=df15['floorSize'][idx].replace(',','')
                converted_size.append(float(item.split()[0])*unitsDict[df15['floorSize'][idx].split()[-1]])
        else:
            item=item.replace(',','')
            converted_size.append(float(item.split()[0])*unitsDict[item.split()[-1]])
            
    df15['Internal Area (s.f)']=converted_size  
    df15.rename(columns={'title':'Title'}, inplace=True)
    df15.rename(columns={'beds':'Beds'}, inplace=True)
    df15.rename(columns={'baths':'Baths'}, inplace=True)
    df15['Agent Email Address']=None
    df15['Agent Contact']=None        
    df15.rename(columns={'agent':'Agent'}, inplace=True)
    df15.rename(columns={'imgUrl':'imgUrls'}, inplace=True)
    df15['Days on Market']=None
    df15['Number of Guests']=None
    df15['Number of amenities']=None
    df15['Number of high end amenities (pool, gym, spa)']=None
    df15['Fees (commissions, cleaning etc)']=None
    df15['Housing Type']=None
    df15['priceChange']=None
    df15['priceStatus']=None
    df15['Price Change']=None
    df15['Occupancy']=None
    parkingStatuses=[]
    for i in df15['parking']:
        if str(i)=='nan':
            parkingStatuses.append(False)
        else:
            parkingStatuses.append(True)
    df15['parking']=parkingStatuses
    df15.rename(columns={'parking':'Parking'}, inplace=True)  
    df15['listingType']=df15['listingType'].str.replace('sale','Sale')
    df15['listingType']=df15['listingType'].str.replace('rent','Rent')
    df15.rename(columns={'listingType':'Type (Rent, Sale, Vacation)'}, inplace=True)
    df15['pricingCriteria']=df15['pricingCriteria'].str.replace('Per ','')
    df15['pricingCriteria']=df15['pricingCriteria'].replace('m²',None)
    df15.rename(columns={'pricingCriteria':'Price criteria'}, inplace=True)
    
    df15['localPrice'] = df15['price']
    df15['localCurrency'] = df15['currency']
    kshToUsd = currency_converter('USD', 'KES')
    prices=[]
    for idx,item in enumerate(df15['price']):
        if df15['currency'][idx]=='KSh':
            item=item*kshToUsd   
        prices.append(item)
    df15['price']=prices
    df15.rename(columns={'price':'Price'}, inplace=True)
    df15['Price per s.f.']=df15['Price']/df15['Internal Area (s.f)']

    cities=[]
    for item in df15['address']:
        if item==None or str(item)=='nan':
            cities.append(None)
            continue
        if item.split(',')[-1].strip()!='':
            cities.append(item.split(',')[-1].strip())
        else:
            cities.append(None)
            
    df15['Location: City']=cities
    df15.rename(columns={'address':'Location: Address'}, inplace=True)
    df15['Location: District']=None
    df15['Location: Lat']=None
    df15['Location: Lon']=None
    df15['Location: Neighbourhood']=None
    df15['Location: Country']='Kenya'
    df15['Location: City']=df15['Location: City'].str.replace('\d+', '')
    df15['Location: City']=df15['Location: City'].str.strip()
    df15['Location: City']=df15['Location: City'].str.replace('County','')

    df15['variantId']=None
    df15.drop(['city','country','currency','district','erfSize','floorSize'], axis=1,inplace=True)
    df15 = df15.reindex(sorted(df15.columns), axis=1)

    return df15