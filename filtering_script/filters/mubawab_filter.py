from convert_currency import currency_converter
from get_data import get_data
import pandas as pd
from datetime import datetime
from hash_url import hash_url

def mubawab_filter(log):

    databaseName='mubawab'
    dbname_1=get_data(databaseName)
    collection_name_1 = dbname_1['propertyDetails']
    log.info(f'{"*"*40}\nCollecting data of {databaseName}')
    data_mongo=list(collection_name_1.find({},{'_id':False}))
    df13=pd.DataFrame(data_mongo,columns=data_mongo[0].keys())
    log.info(f'Filtering data of {databaseName}\n{"*"*40}')

    def convert_to_sqft(area):
        if area is None:
            return None
        sqm = float(area[:-3])  # Extract numerical value as float
        sqft = sqm * 10.7639  # Convert square meters to square feet
        return round(sqft, 2)

    df13['Website'] = "mubawab.ma"
    df13['rehaniId'] = df13['url'].apply(lambda x: hash_url(x))
    df13.rename(columns={'title':'Title'}, inplace=True)
    df13.rename(columns={'agent':'Agent'}, inplace=True)
    df13['Agent Email Address']=None
    df13.rename(columns={'agentNumber':'Agent Contact'}, inplace=True)
    df13['Agent Contact']=df13['Agent Contact'].str.replace(' ','')
    df13['baths']=df13['baths'].str.replace('Bathrooms','')
    df13['baths']=df13['baths'].str.replace('Bathroom','')
    df13['beds']=df13['beds'].str.replace('Rooms','')
    df13['beds']=df13['beds'].str.replace('Room','')

    df13.rename(columns={'beds':'Beds'}, inplace=True)
    df13.rename(columns={'baths':'Baths'}, inplace=True)
    df13.rename(columns={'housingType':'Housing Type'}, inplace=True)
    
    df13['listingType']=df13['listingType'].where(pd.notnull(df13['listingType']), None)
    df13['listingType']=df13['listingType'].str.replace('sale','Sale')
    df13['listingType']=df13['listingType'].str.replace('rent','Rent')
    df13.rename(columns={'listingType':'Type (Rent, Sale, Vacation)'}, inplace=True)
    today = pd.Timestamp(datetime.now().strftime('%Y-%m-%d'))
    dates = pd.to_datetime(df13['dateListed'])
    diff_days = (today - dates).dt.days
    df13['Days on Market'] = diff_days
    df13['Number of Guests']=None
    df13.rename(columns={'dateListed':'dateAdded'}, inplace=True)
    amenities=[]
    for item in df13['amenities']:   
        amenities.append(len(item))
    df13['Number of amenities']=amenities
    highEndAmenities=[]
    for item in df13['amenities']:
        counter=0
        if 'pool' in str(item).lower():
            counter=counter+1
        if 'gym' in str(item).lower(): 
            counter=counter+1
        if 'spa' in str(item).lower():
            counter=counter+1
        highEndAmenities.append(counter)
    df13['Number of high end amenities (pool, gym, spa)']=highEndAmenities
    df13['Occupancy']=None
    df13['Parking']=None
    df13['Fees (commissions, cleaning etc)']=None
    converted_list= list(map(convert_to_sqft, df13['size'].values))
    df13['size']=converted_list
    df13.rename(columns={'size':'Internal Area (s.f)'}, inplace=True)
    df13.rename(columns={'city':'Location: City'}, inplace=True)
    df13.rename(columns={'district':'Location: District'}, inplace=True)
    df13['Location: Lat']=None
    df13['Location: Lon']=None
    df13['Location: Neighbourhood']=None
    df13['Location: Country']='Morocco'
    df13['Location: City']=df13['Location: City'].replace('and',None)
    df13['Location: City']=df13['Location: City'].replace('Property',None)
    df13.rename(columns={'address':'Location: Address'}, inplace=True)

    df13['pricingCriteria'] = df13['pricingCriteria'].apply(
        lambda x: '6 Months' if 'six' or '6' in x.lower() else (
            'Month' if 'month' in x.lower() else (
            'Day' if 'day' in x.lower() else (
            'Week' if 'week' in x.lower() else (
            'Year' if 'annum' in x.lower() or 'annual' in x.lower() else None))))
        )
    df13.rename(columns={'pricingCriteria':'Price criteria'}, inplace=True)
    df13['Location: Lon'] = df13['longitude']
    df13['Location: Lat'] = df13['latitude']

    df13['localPrice'] = df13['price']
    df13['localCurrency'] = df13['currency']

    DhToUsd = currency_converter('USD', 'AED')
    EurToUsd = currency_converter('USD', 'EUR')

    prices=[]
    for idx,item in enumerate(df13['price']):
        if df13['currency'][idx]=='DH':
            item=item*DhToUsd
        elif df13['currency'][idx]=='EUR':
            item=item*EurToUsd        
        prices.append(item)
    df13['price']=prices
    df13.rename(columns={'price':'Price'}, inplace=True)

    prices=[]
    for idx,item in enumerate(df13['priceDiff']):
        if df13['currency'][idx]=='DH':
            item=item*DhToUsd
        elif df13['currency'][idx]=='EUR':
            item=item*EurToUsd
        prices.append(item)
    df13['priceDiff']=prices

    df13.rename(columns={'priceDiff':'Price Change'}, inplace=True)
    df13['Price per s.f.']=df13['Price']/df13['Internal Area (s.f)']

    df13['Location: City']=df13['Location: City'].replace('Dar',None)
    df13['Location: City']=df13['Location: City'].str.replace('F%c%as','Fez')
    df13['Location: City']=df13['Location: City'].str.replace('Salé','Sale')
    df13['Location: City']=df13['Location: City'].str.replace('F%c%as','Fez')
    df13['Location: City']=df13['Location: City'].str.replace('\d+', '')
    df13['Location: City']=df13['Location: City'].str.strip()
    df13['Location: City']=df13['Location: City'].str.replace('County','')

    df13.drop(['currency', 'longitude', 'latitude'], axis=1,inplace=True)
    df13 = df13.reindex(sorted(df13.columns), axis=1)

    return df13