from get_data import get_data
import pandas as pd
from convert_currency import currency_converter
from hash_url import hash_url
from datetime import datetime

def buyrentkenya_filter(log):

    databaseName='buyrentkenya'
    dbname_1=get_data(databaseName)

    collection_name_1 = dbname_1['propertyDetails']
    log.info(f'{"*"*40}\nCollecting data of {databaseName}')
    data_mongo=list(collection_name_1.find({},{'_id':False}))
    df4=pd.DataFrame(data_mongo,columns=data_mongo[0].keys())
    log.info(f'Filtering data of {databaseName}\n{"*"*40}')

    df4['Website']='buyrentkenya.com'
    df4['rehaniId'] = df4['url'].apply(lambda x: hash_url(x))
    df4.rename(columns={'title':'Title'}, inplace=True)
    df4.rename(columns={'agentNumber':'Agent Contact'}, inplace=True)
    df4.rename(columns={'agent':'Agent'}, inplace=True)
    df4['Agent Email Address']=None
    df4.rename(columns={'beds':'Beds'}, inplace=True)
    df4.rename(columns={'baths':'Baths'}, inplace=True)
    df4['Location: Lat']=None
    df4['Location: Lon']=None
    df4['Number of Guests']=None
    df4['Occupancy']=None
    df4.rename(columns={'listingType':'Type (Rent, Sale, Vacation)'}, inplace=True)
    df4['Type (Rent, Sale, Vacation)'] = df4['Type (Rent, Sale, Vacation)'].str.replace('sale','Sale')
    df4['Type (Rent, Sale, Vacation)'] = df4['Type (Rent, Sale, Vacation)'].str.replace('rent','Rent')
    df4.rename(columns={'parking':'Parking'}, inplace=True)
    df4['Fees (commissions, cleaning etc)']=None
    df4.rename(columns={'housingType':'Housing Type'}, inplace=True)
    amenities=[]
    for item in df4['amenities']:   
        amenities.append(len(item))
    df4['Number of amenities']=amenities
    highEndAmenities=[]
    for item in df4['amenities']:
        counter=0
        if 'pool' in str(item).lower():
            counter=counter+1
        if 'gym' in str(item).lower(): 
            counter=counter+1
        if 'spa' in str(item).lower():
            counter=counter+1
        highEndAmenities.append(counter)
    df4['Number of high end amenities (pool, gym, spa)']=highEndAmenities
    df4.rename(columns={'city':'Location: City'}, inplace=True)
    df4['Location: Country']='Kenya'
    
    df4.rename(columns={'location':'Location: Address'}, inplace=True)
    replacements = {
        ', Area': '',
        'Area, ': '',
        ', Island': '',
        'Island, ': '',
        ', Zimmermann': '',
        'Sukari, ': '',
    }

    def custom_replace(address):
        if 'Mathenge' in address:
            return 'General Mathenge, Westlands, Nairobi'
        elif 'Konza' in address:
            return 'Konza City, Machakos'
        elif 'Kamulu' in address:
            return 'Kamulu, Nairobi'
        elif 'Joska' in address:
            return 'Joska, Nairobi'
        elif 'Kahawa' in address:
            return 'Kahawa, Roysambu, Nairobi'
        elif 'State' in address and 'House' in address:
            return 'State House, Nairobi'
        elif 'Cbd' in address:
            return 'Nairobi Central Business District'
        elif 'Dagoretti' in address and 'Corner' in address:
            return 'Dagoretti Corner, Nairobi'
        
        elif 'Valley' in address and 'Arcade' in address:
            return 'Valley Arcade, Nairobi'
        elif 'Valley' in address and 'Spring' in address:
            return 'Spring Valley, Westlands, Nairobi'

        elif 'Ngumo' in address and 'Estate' in address:
            return 'Ngumo Estate, Nairobi'
        elif 'Hill' in address and 'Upper' in address:
            return 'Upper Hill, Nairobi'
        elif 'Lower' in address and 'Kabete' in address:
            return 'Lower Kabete, Westlands, Nairobi'
        
        elif 'Thika' in address and 'Road' in address:
            return 'Thika Road, Nairobi'
        elif 'Naivasha' in address and 'Road' in address:
            return 'Naivasha Road, Nairobi'
        elif 'Kenyatta' in address and 'Road' in address:
            return 'Kenyatta Road, Nairobi'
        elif 'Rhapta' in address and 'Road' in address:
            return 'Rhapta Road, Nairobi'
        elif 'Kiambu' in address and 'Road' in address:
            return 'Kiambu Road, Nairobi'
        
        elif 'Tatu' in address and 'Ruiru' in address:
            return 'Tatu City, Ruiru, Kiambu'
        elif 'Gikambura' in address and 'Kikuyu' in address:
            return 'Gikambura, Kikuyu, Kiambu'
        elif 'Rironi' in address and 'Limuru' in address:
            return 'Rironi, Limuru, Kiambu'
        elif 'Garden' in address and 'Estate' in address and 'Roysambu' in address:
            return 'Garden Estate, Roysambu, Nairobi'
        elif 'Ngumo' in address and 'Estate' in address:
            return 'Ngumo Estate, Nairobi'
        elif 'Mirema' in address and 'Roysambu' in address:
            return 'Mirema, Roysambu, Nairobi'
        elif 'Windsor' in address and 'Roysambu':
            return 'Windsor, Roysambu, Nairobi'
        
        elif 'Lake' in address and 'View' in address:
            return 'Lake View, Nairobi'
        elif 'Mountain' in address and 'View' in address:
            return 'Mountain View, Westlands, Nairobi'
        
        elif 'Brookside' in address and 'Westlands' in address:
            return 'Brookside, Westlands, Nairobi'
        elif 'Parklands' in address and 'Westlands' in address:
            return 'Parklands, Westlands, Nairobi'
        elif 'Hill' in address and 'View' in address:
            return 'Hill, Westlands, Nairobi'

        return address

    df4['Location: Address'] = df4['Location: Address'].replace(replacements, regex=True)
    df4['Location: Address'] = df4['Location: Address'].apply(custom_replace)

    df4['Location: District']=None
    df4['localPrice'] = df4['price']
    df4['localCurrency'] = df4['currency']

    df4.rename(columns={'size_sqft':'Internal Area (s.f)'}, inplace=True)
    kesToUsd = currency_converter('USD', 'KES')
    df4['price']=df4['price']*kesToUsd
    df4.rename(columns={'price':'Price'}, inplace=True)
    df4['Price per s.f.']=df4['Price']/df4['Internal Area (s.f)']
    df4['priceDiff']=df4['priceDiff']*kesToUsd
    df4['Price criteria']=None
    df4.rename(columns={'suburb':'Location: Neighbourhood'}, inplace=True)
    df4.rename(columns={'dateListed':'dateAdded'}, inplace=True)

    today = pd.Timestamp(datetime.now().strftime('%Y-%m-%d'))
    dates = pd.to_datetime(df4['dateAdded'])
    diff_days = (today - dates).dt.days
    df4['Days on Market'] = diff_days

    df4['Location: City'] = df4['Location: City'].str.replace('Nyali','Mombasa')
    df4['Location: City'] = df4['Location: City'].str.replace('Westlands','Nairobi')
    df4['Location: City'] = df4['Location: City'].str.replace('Mombasa Island','Mombasa')
    df4['Location: City'] = df4['Location: City'].str.replace('Roysambu','Nairobi')
    df4['Location: City'] = df4['Location: City'].str.replace('Ruiru','Kiambu County')
    df4['Location: City'] = df4['Location: City'].str.replace('Limuru','Nairobi')
    df4['Location: City'] = df4['Location: City'].str.replace('Kikuyu','Kiambu County')
    df4['Location: City'] = df4['Location: City'].str.replace('Eldoret','Uasin Gishu County')
    df4['Location: City'] = df4['Location: City'].str.replace("Murang'a County",'Muranga County')
    df4['Location: City'] = df4['Location: City'].str.replace('Apartments', '').replace('Villas', '').replace('Houses', '').replace('Residential Land', '').replace('Commercial Land', '').replace('Commercial Property', '').replace('Bedsitters', '').replace('Townhouses', '').replace('Offices', '').replace('Warehouses', '').replace('Shops', '').replace('Land', '')

    df4.drop(['currency', 'size', 'size_unit'], axis=1, errors='ignore', inplace=True)
    df4 = df4.reindex(sorted(df4.columns), axis=1)

    return df4
