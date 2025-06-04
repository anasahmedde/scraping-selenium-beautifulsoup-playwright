from get_data import get_data
import pandas as pd
from convert_currency import currency_converter
from hash_url import hash_url
from datetime import datetime

def houseInRwanda_filter(log):
    
    databaseName='HouseInRwanda'
    dbname_1=get_data(databaseName)
    collection_name_1 = dbname_1['propertyDetails']
    log.info(f'{"*"*40}\nCollecting data of {databaseName}')
    data_mongo=list(collection_name_1.find({},{'_id':False}))
    df2=pd.DataFrame(data_mongo,columns=data_mongo[0].keys())
    log.info(f'Filtering data of {databaseName}\n{"*"*40}')

    df2['Website']='houseinrwanda.com'
    df2['rehaniId'] = df2['url'].apply(lambda x: hash_url(x))
    df2.rename(columns={'propertyTitle':'Title'}, inplace=True)
    df2.rename(columns={'advertType':'Type (Rent, Sale, Vacation)'}, inplace=True)

    typeOfListing=[]
    for item in df2['Type (Rent, Sale, Vacation)']:
        if item==None:
            typeOfListing.append(None)
        elif'Rent'in item:
            typeOfListing.append('Rent')
        elif item.strip()=='Sale':
            typeOfListing.append('Sale')
        elif item.strip()=='Auction':
            typeOfListing.append('Sale')
    df2['Type (Rent, Sale, Vacation)']=typeOfListing   
    df2['localPrice'] = df2['price']
    df2['localCurrency'] = df2['currency']

    df2.rename(columns={'agentCellPhone':'Agent Contact'}, inplace=True)
    df2.rename(columns={'agentEmailAddress':'Agent Email Address'}, inplace=True)
    df2.rename(columns={'agentName':'Agent'}, inplace=True)
    df2.rename(columns={'beds':'Beds'}, inplace=True)
    df2.rename(columns={'baths':'Baths'}, inplace=True)
    df2.rename(columns={'price':'Price'}, inplace=True)
    #df2.rename(columns={'priceDiff':'Price Change'}, inplace=True)
    df2['Location: Lat']=None
    df2['Location: Lon']=None
    df2.rename(columns={'propertyType':'Housing Type'}, inplace=True)
    df2['Fees (commissions, cleaning etc)']=None
    df2['Occupancy']=None
    df2['Number of Guests']=None

    usdToRwf = currency_converter('USD', 'RWF')
    prices=[]
    for idx,item in enumerate(df2['Price'].values):
        if item==None:
            prices.append(None)    
        elif item=='Price on request ':
            prices.append(None)
        else:
            if df2['currency'].values[idx]=='Rwf':
                item=item*usdToRwf
            prices.append(item)
    df2['Price']=prices
    def convert_to_sqft(area):
        if area is None:
            return None
        sqm = float(area[:-3])  # Extract numerical value as float
        sqft = sqm * 10.7639  # Convert square meters to square feet
        return round(sqft, 2)
    converted_list= list(map(convert_to_sqft, df2['plotSize'].values))
    df2['plotSize']=converted_list
    df2['Price per s.f.']=df2['Price']/df2['plotSize']
    df2.rename(columns={'plotSize':'Internal Area (s.f)'}, inplace=True)
    
    amenities=[]
    for idx,item in enumerate(df2['amenities']):
        if df2['furnished'].values[idx]=='Yes':
            amenities.append(len(item)+1)
        else:
            amenities.append(len(item))
    df2['Number of amenities']=amenities
    highEndAmenities=[]
    for item in df2['amenities']:
        counter=0
        if 'pool' in str(item).lower():
            counter=counter+1
        if 'gym' in str(item).lower(): 
            counter=counter+1
        if 'spa' in str(item).lower():
            counter=counter+1
        highEndAmenities.append(counter)
    df2['Number of high end amenities (pool, gym, spa)']=highEndAmenities
    df2['Location: Country']='Rwanda'
    cities=[]
    for item in df2['address'].values:
        if type(item)==str:
            cities.append(item.split(',')[0].strip())
        else:
            cities.append(None)
    district=[]
    for item in df2['address'].values:
        if type(item)==str:
            district.append(item.split(',')[1].strip())
        else:
            district.append(None)
    neigbourhood=[]
    for item in df2['address'].values:
        if type(item)==str:
            neigbourhood.append(item.split(',')[2].strip())
        else:
            neigbourhood.append(None)        
    df2['Location: City']=cities
    df2['Location: District']=district
    df2['Location: Neighbourhood']=neigbourhood
    df2.rename(columns={'address':'Location: Address'}, inplace=True)
    df2['Fees (commissions, cleaning etc)']=None
    df2['Parking']=None
    df2['Days on Market']=None
    df2.rename(columns={'pricingCriteria':'Price criteria'}, inplace=True)

    today = pd.Timestamp(datetime.now().strftime('%Y-%m-%d'))
    dates = pd.to_datetime(df2['dateListed'])
    diff_days = (today - dates).dt.days
    df2['Days on Market'] = diff_days
    df2.rename(columns={'dateListed':'dateAdded'}, inplace=True)

    df2.drop(['currency','expiryDate','furnished','totalFloors'], axis=1,inplace=True)

    df2 = df2.reindex(sorted(df2.columns), axis=1)

    return df2
