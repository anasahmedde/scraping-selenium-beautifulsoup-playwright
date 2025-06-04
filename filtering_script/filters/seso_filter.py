from get_data import get_data
import pandas as pd
from convert_currency import currency_converter
from hash_url import hash_url
from datetime import datetime

def seso_filter(log):
    
    databaseName='SeSo'
    dbname_1=get_data(databaseName)
    collection_name_1 = dbname_1['propertyDetails']
    log.info(f'{"*"*40}\nCollecting data of {databaseName}')
    data_mongo=list(collection_name_1.find({},{'_id':False}))
    df3=pd.DataFrame(data_mongo,columns=data_mongo[0].keys())
    log.info(f'Filtering data of {databaseName}\n{"*"*40}')

    df3['Website']='seso.global'
    df3['rehaniId'] = df3['url'].apply(lambda x: hash_url(x))
    df3.rename(columns={'propertyName':'Title'}, inplace=True)
    df3['Agent']=None
    df3['Agent Contact']=None
    df3['Agent Email Address']=None
    df3['localPrice'] = df3['price']
    df3['localCurrency'] = df3['currency']
    df3.rename(columns={'beds':'Beds'}, inplace=True)
    df3.rename(columns={'baths':'Baths'}, inplace=True)
    df3.rename(columns={'price':'Price'}, inplace=True)
    df3.rename(columns={'longitude':'Location: Lon'}, inplace=True)
    df3.rename(columns={'latitude':'Location: Lat'}, inplace=True)
    df3.rename(columns={'listingType':'Type (Rent, Sale, Vacation)'}, inplace=True)
    df3['Type (Rent, Sale, Vacation)'] = df3['Type (Rent, Sale, Vacation)'].str.replace('SALE','Sale')
    df3['Type (Rent, Sale, Vacation)'] = df3['Type (Rent, Sale, Vacation)'].str.replace('SOLD OUT','Sold')

    usdToGhs = currency_converter('USD', 'GHS')
    usdToNgn = currency_converter('USD', 'NGN')
    prices = []

    for idx, item in enumerate(df3['Price'].values):
        if item == None:
            prices.append(None)
        else:
            if df3['usdPrice'].values[idx] == 0:
                if df3['currency'].values[idx] == 'GHS':
                    item = item * usdToGhs
                elif df3['currency'].values[idx] == 'NGN':
                    item = item * usdToNgn
            else:
                item = df3['usdPrice'].values[idx]
            prices.append(item)

    df3['Price'] = prices

    areaList=[]
    for i, area in enumerate(df3['area']):
        if area is None:
            areaList.append(None)
            continue
        area=area.replace('x ','x')
        parts = area.split()
        try:
            if 'x' in area:
                numbers=parts[0].split('x')
                areaList.append(float(numbers[0])*float(numbers[1].strip())* 10.7639)
            elif len(parts) == 2 and parts[1] == 'sqm':
                areaList.append(float(parts[0]) * 10.7639)
            else:
                areaList.append(None)
        except:
            areaList.append(None)
    df3['Internal Area (s.f)']=areaList
    df3['Price per s.f.']=df3['Price']/df3['Internal Area (s.f)']
    
    df3['amenities'] = df3['features']
    amenities=[]
    for item in df3['features']:
        if item==None:
            amenities.append(0)    
        else:
            amenities.append(len(item))
    df3['Number of amenities']=amenities
    highEndAmenities=[]
    for item in df3['features']:
        counter=0
        if 'pool' in str(item).lower():
            counter=counter+1
        if 'gym' in str(item).lower(): 
            counter=counter+1
        if 'spa' in str(item).lower():
            counter=counter+1
        highEndAmenities.append(counter)

    df3['Number of high end amenities (pool, gym, spa)']=highEndAmenities
    df3['Number of Guests']=None
    df3['Occupancy']=None
    df3['Parking']=None
    df3.rename(columns={'pricingCriteria':'Price criteria'}, inplace=True)
    df3['Fees (commissions, cleaning etc)']=None
    #df3.rename(columns={'propertyStatus':'Housing Type'}, inplace=True)
    df3['Housing Type']=df3['Title']
    today = pd.Timestamp(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    dates = pd.to_datetime(df3['dateAdded'])
    diff_days = (today - dates).dt.days
    df3['Days on Market'] = diff_days
    df3.rename(columns={'address':'Location: Address'}, inplace=True)
    df3['Location: Neighbourhood']=None
    df3['Location: District']=None
    df3.rename(columns={'city':'Location: City'}, inplace=True)
    df3.rename(columns={'country':'Location: Country'}, inplace=True)

    df3.drop(['currency','area','unitsAvailable','usdPrice', 'propertyStatus', 'features'], axis=1,inplace=True)
    df3 = df3.reindex(sorted(df3.columns), axis=1)

    return df3