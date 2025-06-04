from get_data import get_data
import pandas as pd
import re
from datetime import datetime
from hash_url import hash_url

def booking_filter(log):

    databaseName='booking'
    dbname_1=get_data(databaseName)

    collection_name_1 = dbname_1['propertyDetails']
    log.info(f'{"*"*40}\nCollecting data of {databaseName}')
    data_mongo=list(collection_name_1.find({},{'_id':False}))
    df19=pd.DataFrame(data_mongo,columns=data_mongo[0].keys())
    log.info(f'Filtering data of {databaseName}\n{"*"*40}')

    df19['Website'] = "booking.com"
    df19['rehaniId'] = df19['variantId'].apply(lambda x: hash_url(x))
    df19.rename(columns={'title':'Title'}, inplace=True)
    df19['Agent']=None
    df19['Agent Contact']=None
    df19['Agent Email Address']=None
    bedsQuantity=[]
    for item in df19['beds']:
        if str(item)=='nan' or item==None:
            bedsQuantity.append(None)
            continue
        bedsQuantity.append(len(item))
    df19['Beds']=bedsQuantity
    bathrooms=[]
    for idx,item in enumerate(df19['amenities']):
        if item==None or str(item)=='nan':
            bathrooms.append(None)
            continue
        if 'Attached bathroom' in item:        
            if df19['beds'][idx]==None or str(df19['beds'][idx])=='nan':
                bathrooms.append(None)
            else:
                counter=0
                for rooms in df19['beds'][idx]:
                    if 'bed' in rooms.lower():
                        counter=counter+1
                bathrooms.append(counter)
        else:
            bathrooms.append(None)            
    df19['Baths']=bathrooms
    df19.rename(columns={'images':'imgUrls'}, inplace=True)
    df19['localPrice'] = df19['price']
    df19['localCurrency'] = df19['currency']
    df19.rename(columns={'price':'Price'}, inplace=True)
    df19.rename(columns={'roomType':'Housing Type'}, inplace=True)
    df19['Type (Rent, Sale, Vacation)']='Vacation'
    today = pd.Timestamp(datetime.now().strftime('%Y-%m-%d'))
    dates = pd.to_datetime(df19['dateListed'])
    diff_days = (today - dates).dt.days
    df19['Days on Market'] = diff_days
    df19.rename(columns={'dateListed':'dateAdded'}, inplace=True)
    df19['Fees (commissions, cleaning etc)']=None
    df19.rename(columns={'sleeps':'Number of Guests'}, inplace=True)
    df19['Occupancy']=None
    df19['Parking']=None
    converted_size=[]
    for i in df19['size']:
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
          
    df19['Internal Area (s.f)']=converted_size
    df19.rename(columns={'priceDiff':'Price Change'}, inplace=True)
    df19['Price per s.f.']=df19['Price']/df19['Internal Area (s.f)']
    df19.rename(columns={'pricingCriteria':'Price criteria'}, inplace=True)
    amenities=[]
    for item in df19['amenities']:
        if item==None:
            amenities.append(0)    
        else:
            amenities.append(len(item))
    df19['Number of amenities']=amenities
    highEndAmenities=[]
    for item in df19['amenities']:
        counter=0
        if 'pool' in str(item).lower():
            counter=counter+1
        if 'gym' in str(item).lower(): 
            counter=counter+1
        if 'spa' in str(item).lower():
            counter=counter+1
        highEndAmenities.append(counter)
    df19['Number of high end amenities (pool, gym, spa)']=highEndAmenities
    df19['Location: Lat']=None
    df19['Location: Lon']=None
    df19.rename(columns={'city':'Location: City'}, inplace=True)
    df19.rename(columns={'country':'Location: Country'}, inplace=True)
    df19.rename(columns={'address':'Location: Address'}, inplace=True)

    df19['Location: District']=None
    df19['Location: Neighbourhood']=None
    df19["Location: City"] = (
        df19["Location: City"]
        .str.replace(r"\d+", "", regex=True)  # Remove digits
        .str.replace(r"County", "", regex=True)  # Remove "County"
        .str.replace(r"[^a-zA-Z\s]", "", regex=True)  # Remove non-alphabetic chars
        .str.strip()  # Trim spaces
    )

    df19.drop(["variantId", "avgPrice","areaInfo","balcony","beds","breakfast","breakfastIncluded","cancellationPolicy","categoryRating","checkIn","checkOut","closestAirports","currency","discountPercent","features","highlights","mainUrl","prePayment","rating","refundPolicy","reviews","roomAvailability","savings","size","stars","taxAmount","taxesIncluded","views"], axis=1,inplace=True)
    df19 = df19.reindex(sorted(df19.columns), axis=1)

    return df19