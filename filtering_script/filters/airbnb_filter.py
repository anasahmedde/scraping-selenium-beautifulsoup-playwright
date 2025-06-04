from get_data import get_data
import pandas as pd
from datetime import datetime
from hash_url import hash_url

def airbnb_filter(log):

    databaseName='airbnb'
    dbname_1=get_data(databaseName)
    collection_name_1 = dbname_1['propertyDetails']
    log.info(f'{"*"*40}\nCollecting data of {databaseName}')
    data_mongo=list(collection_name_1.find({},{'_id':False}))
    df10=pd.DataFrame(data_mongo,columns=data_mongo[0].keys())
    log.info(f'Filtering data of {databaseName}\n{"*"*40}')

    df10['Website'] = "airbnb.com"

    df10['rehaniId'] = df10['url'].apply(lambda x: hash_url(x))
    df10['localPrice'] = df10['price']
    df10['localCurrency'] = df10['currency']
    df10.rename(columns={'title':'Title'}, inplace=True)
    df10.rename(columns={'superHostLink':'Agent Contact'}, inplace=True)
    df10.rename(columns={'superHostName':'Agent'}, inplace=True)
    df10['Agent Email Address']=None
    df10.rename(columns={'bed':'Beds'}, inplace=True)
    df10.rename(columns={'bath':'Baths'}, inplace=True)
    df10['Fees (commissions, cleaning etc)']=None
    df10.rename(columns={'imgUrl':'imgUrls'}, inplace=True)
    df10.rename(columns={'guest':'Number of Guests'}, inplace=True)
    amenities=[]
    for item in df10['amenities']:   
        amenities.append(len(item))
    df10['Number of amenities']=amenities
    highEndAmenities=[]
    for item in df10['amenities']:
        counter=0
        if 'pool' in str(item).lower():
            counter=counter+1
        if 'gym' in str(item).lower(): 
            counter=counter+1
        if 'spa' in str(item).lower():
            counter=counter+1
        highEndAmenities.append(counter)
    df10['Number of high end amenities (pool, gym, spa)']=highEndAmenities
    df10['Type (Rent, Sale, Vacation)']='Vacation'
    df10.rename(columns={'price':'Price'}, inplace=True)
    df10.rename(columns={'propertyType':'Housing Type'}, inplace=True)
    df10.rename(columns={'pricingCriteria':'Price criteria'}, inplace=True)
    df10['priceStatus']=None
    df10['priceChange']=None
    df10['Price per s.f.']=None
    df10['Price Change']=None
    df10['Parking']=None
    df10['Internal Area (s.f)']=None
    today = pd.Timestamp(datetime.now().strftime('%Y-%m-%d'))
    dates = pd.to_datetime(df10['dateListed'])
    diff_days = (today - dates).dt.days
    df10['Days on Market'] = diff_days
    df10.rename(columns={'city':'Location: City'}, inplace=True)
    df10.rename(columns={'location':'Location: Address'}, inplace=True)
    df10['Location: District']=None
    df10['Location: Neighbourhood']=None
    df10.rename(columns={'longitude':'Location: Lon'}, inplace=True)
    df10.rename(columns={'latitude':'Location: Lat'}, inplace=True)
    countries=[]
    for i in df10['Location: Address']:
        countries.append(i.split(',')[-1].strip())
    df10['Location: Country']=countries

    df10.rename(columns={'guestSatisfactionOverall':'rating'}, inplace=True)

    df10.drop(["ReviewCount","accuracyRating","bedType","bedroom","cancellationPolicy","checkinRating","cleanlinessRating","communicationRating","currency","discountedPrice","instantBook","isSuperhost","locationRating","newEntry","personCapacity","recentReview","recentReviewDate","recentReviewRating","reviewsPerMonth","roomType","valueRating"], axis=1,inplace=True)

    df10 = df10.reindex(sorted(df10.columns), axis=1)

    return df10
