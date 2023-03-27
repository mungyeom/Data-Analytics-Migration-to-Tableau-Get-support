import os
path = os.getcwd()
print(path)
os.chdir('/Users/gyeomi/Desktop/Aicore/Data_Analytics_Migration_to_Tableau')


import pandas as pd
import numpy as np
from tqdm import tqdm
tqdm.pandas()
import math

# distance

## scraping data
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import chromedriver_autoinstaller
import time
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from geopy import distance

pd.set_option('display.max_rows',1000)
pd.set_option('display.max_column',1000)

# load files
df_87 = pd.read_csv('1987.csv')
df_89 = pd.read_csv('1989.csv')
df_90 = pd.read_csv('1990.csv')
df_91 = pd.read_csv('1991.csv')
df_92 = pd.read_csv('1992.csv')
df_93 = pd.read_csv('1993.csv')
df_94 = pd.read_csv('1994.csv')
df_95 = pd.read_csv('1995.csv')
df_96 = pd.read_csv('1996.csv')

# 1987 data cleaning
df_87[df_87['DepTime']==3.0]
df_87.shape
df_87.info()
# how much missing data 
df_87.isnull().sum()/len(df_87) *100

# cancelled
df_87['Cancelled'].unique()
df_87[df_87['Cancelled']==1]

# TailNum
df_87['TailNum'] = df_87['TailNum'].fillna(0)
df_87['TailNum'] = df_87['TailNum'].astype(int)

# time
def Time(t):
    if len(t) == 4:
        if t == '2400':
            t = '0000'
        t = t + '00'
        return t[:2] + ':' + t[2:4] + ':' + t[4:]
    elif len(t) == 3:
        t = '0' + t +'00'
        return t[:2] + ':' + t[2:4] + ':' + t[4:]
    elif len(t) == 2:
        t = '00' + t + '00'
        return t[:2] + ':' + t[2:4] + ':' + t[4:]
    else:
        t = '000' + t + '00'
        return t[:2] + ':' + t[2:4] + ':' + t[4:]


# clean CRSDepTime
df_87['CRSDepTime'] = df_87['CRSDepTime'].apply(lambda x: str(int(x)) if not pd.isnull(x) else x)
df_87['CRSDepTime'] = df_87['CRSDepTime'].map(Time, na_action = 'ignore')
df_87['CRSDepTime']


# remove DepTime null values and convert the type
df_87['DepTime'] = df_87['DepTime'].apply(lambda x: str(int(x)) if not pd.isnull(x) else x)
df_87['DepTime'] = df_87['DepTime'].map(Time, na_action = 'ignore')
df_87['DepTime']
df_87[df_87['DepTime'].isnull()] # check the null values
df_87['DepTime'] = df_87['DepTime'].fillna(0) # fill them
df_87[df_87['DepTime']==0] #check 


# ArrTime
df_87['ArrTime'] = df_87['ArrTime'].apply(lambda x: str(int(x)) if not pd.isnull(x) else x)
df_87['ArrTime'] = df_87['ArrTime'].map(Time, na_action= 'ignore')
df_87['ArrTime']
df_87['ArrTime'].isnull().sum() # check the null values
df_87['ArrTime'] = df_87['ArrTime'].fillna(0)
df_87[(df_87['ArrTime']==0) & (df_87['DepTime']=='00:00:00')]


# CRSArrTime
df_87['CRSArrTime'] = df_87['CRSArrTime'].apply(lambda x: str(int(x)) if not pd.isnull(x) else x)
df_87['CRSArrTime'] = df_87['CRSArrTime'].map(Time, na_action= 'ignore')
df_87['CRSArrTime']
df_87.isnull().sum()


# UniqueCarrier
df_87['UniqueCarrier'] = df_87['UniqueCarrier'].astype('category')

# ActualElapsedTime
df_87['ActualElapsedTime'].isnull().sum()
df_87['ActualElapsedTime'] = df_87['ActualElapsedTime'].fillna(0)
df_87['ActualElapsedTime'] = df_87['ActualElapsedTime'].astype(int)

df_87.head()
# CRSElapsedTime
df_87['CRSElapsedTime'].isnull().sum()
# AirTime
df_87['AirTime'] = df_87['AirTime'].fillna(0)
df_87['AirTime'] = df_87['AirTime'].astype(int)
# ArrDelay
df_87['ArrDelay'] = df_87['ArrDelay'].fillna(0)
df_87['ArrDelay'] = df_87['ArrDelay'].astype(int)
# DepDelay
df_87['DepDelay'] = df_87['DepDelay'].fillna(0)
df_87['DepDelay'] = df_87['DepDelay'].astype(int)

# distance
import time
import pyautogui


df_87_distance = df_87.loc[:,['Origin', 'Dest']]
df_87_distance.drop_duplicates(['Origin', 'Dest'],inplace=True)
df_87_distance = df_87_distance.reset_index(drop=True)
origin_names_87 = []
dest_names_87 = []
distances_be_87 = []

chrome_ver = chromedriver_autoinstaller.get_chrome_version().split(',')[0]
print(chrome_ver)

try:
    crawler = webdriver.Chrome(f'./{chrome_ver}/chromedriver.exe')
except:
    chromedriver_autoinstaller.install(True)
    crawler = webdriver.Chrome(f'./{chrome_ver}/chromedriver.exe')
    
for i,j in zip(df_87_distance['Origin'],df_87_distance['Dest']):

    crawler.get('https://www.airportdistancecalculator.com/flight-{i}-to-{j}.html'.format(i=i, j=j))
    
    while True:
        bh = crawler.execute_script('return document.body.scrollHeight') # the top of a brower (before height)
        print(bh)
        crawler.implicitly_wait(20) # wait for 10 secs
        crawler.execute_script('window.scrollTo(0, document.body.scrollHeight)') # scrolling down
        crawler.implicitly_wait(20) # wait for 10 secs
        ah = crawler.execute_script('return document.body.scrollHeight') # after height
        if ah == bh:
            break
        bh = ah
    cities = crawler.find_elements(By.CSS_SELECTOR, '.yellowbox')
    for city in cities:
        try:
            origin_name = city.find_element(By.XPATH, '/html/body/div[2]/div/div[1]/div[1]/div[1]/div[1]/strong[2]').text
            origin_names_87.append(origin_name)
            dest_name = city.find_element(By.XPATH, '/html/body/div[2]/div/div[1]/div[1]/div[1]/div[1]/strong[3]').text
            dest_names_87.append(dest_name)
            distance_be = city.find_element(By.XPATH, '/html/body/div[2]/div/div[1]/div[1]/div[1]/div[1]/strong[1]').text
            distances_be_87.append(distance_be)
        except:
            origin_names_87.append('exception')
            dest_names_87.append('exception')
            distances_be_87.append('exception')
crawler.close()

dis_df_87 = pd.DataFrame({'Origin airport':origin_names_87, 'Destination airport':dest_names_87, 'Distance_km':distances_be_87})
dis_df_87

dis_df_87['Origin airport'] = dis_df_87['Origin airport'].str.replace('distance from','').str.replace(',','')
grouped = dis_df_87.groupby(['Origin airport', 'Destination airport'])
result = grouped.filter(lambda x: len(x) > 1)
print(result)
dis_df_87.loc[1208]

# find the same values between origin and dest
df_87_distance = df_87_distance.reset_index()
df_87_distance = df_87_distance.drop(['level_0','index'],axis=1)
result_2 = df_87_distance[df_87_distance['Origin'] == df_87_distance['Dest']]
result_2
df_87_distance.loc[1208]
df_87_distance.loc[1366]
df_87_distance.loc[1403]
df_87_distance.loc[2781]
df_87_distance.loc[2802]
df_87_distance

# add missing rows

new_row_1 = pd.DataFrame({'Origin airport': 'Fort Lauderdale–Hollywood International Airport',\
                         'Destination airport':'Fort Lauderdale–Hollywood International Airport', 'Distance_km':'0'}, index=[1207.5])
dis_df_87= pd.concat([dis_df_87.loc[:1207],new_row_1,dis_df_87[1208:]]).reset_index(drop=True)
dis_df_87.loc[1208]

dis_df_87.loc[1366]
new_row_2 = pd.DataFrame({'Origin airport': 'Harry Reid International Airport',\
                         'Destination airport':'Harry Reid International Airport', 'Distance_km':'0'}, index=[1365.5])
dis_df_87= pd.concat([dis_df_87.loc[:1365],new_row_2,dis_df_87[1366:]]).reset_index(drop=True)

dis_df_87.loc[1403]
new_row_3 = pd.DataFrame({'Origin airport': 'Phoenix Sky Harbor International Airport',\
                         'Destination airport':'Phoenix Sky Harbor International Airport', 'Distance_km':'0'}, index=[1402.5])
dis_df_87= pd.concat([dis_df_87.loc[:1402],new_row_3,dis_df_87[1403:]]).reset_index(drop=True)

dis_df_87.loc[2781]
new_row_4 = pd.DataFrame({'Origin airport': 'LaGuardia Airport',\
                         'Destination airport':'LaGuardia Airport', 'Distance_km':'0'}, index=[2780.5])
dis_df_87= pd.concat([dis_df_87.loc[:2780],new_row_4,dis_df_87[2781:]]).reset_index(drop=True)

dis_df_87.loc[2802]
new_row_5 = pd.DataFrame({'Origin airport': 'John F. Kennedy International Airport',\
                         'Destination airport':'John F. Kennedy International Airport', 'Distance_km':'0'}, index=[2801.5])
dis_df_87= pd.concat([dis_df_87.loc[:2801],new_row_5,dis_df_87[2802:]]).reset_index(drop=True)
dis_df_87

## fill exception values
df_87_distance.loc[1482]
dis_df_87.loc[643,'Origin airport']= 'Seattle/Tacoma International Airport'
dis_df_87.loc[643,'Destination airport']= 'Dallas/Ft. Worth International Airport'
dis_df_87.loc[643,'Distance_km']= '2669'

dis_df_87.loc[824,'Origin airport']= 'Blue Grass Airport'
dis_df_87.loc[824,'Destination airport']= "O'Hare International Airport"
dis_df_87.loc[824,'Distance_km']= '521'

dis_df_87.loc[1482,'Origin airport']= 'Wayne County Airport'
dis_df_87.loc[1482,'Destination airport']= 'Port Columbus International Airport'
dis_df_87.loc[1482,'Distance_km']= '250'

dis_df_87['Distance_km'] =dis_df_87['Distance_km'].str.replace('kilometers','').astype(int)

dis_df_87.to_csv('1987_distance.csv',index=False)
dis_df_87 = pd.read_csv('1987_distance.csv')
dis_df_87 = dis_df_87.drop('Unnamed: 0', axis=1)
dis_df_87['Origin airport'] = dis_df_87['Origin airport'].str.replace('distance from','').str.replace(',','')
dis_df_87['Distance_km'] =dis_df_87['Distance_km'].str.replace('kilometers','').astype(int)
dis_df_87.head()

# sam = pd.DataFrame({
#     'name': ['Alice', 'Bob', 'Charlie'],
#     'age': [25, 30, 35]
# })
# sam
# new_row = pd.DataFrame({'name': 'Dave', 'age': 40}, index=[0.5])
# sam = pd.concat([sam.loc[:0], new_row, sam.loc[1:]]).reset_index(drop=True) # 넣고 싶은 인덱스 -1 , 앞 인덱스 + 0.5, 내가 되고 싶은 인덱스
# print(sam)


# convert the unit
def km_to_mile(km):
    miles = km * 0.621371
    return miles

dis_df_87['Distance_mile'] = dis_df_87['Distance_km'].progress_apply(km_to_mile)
dis_df_87['Distance_mile'] = dis_df_87['Distance_mile'].round()
dis_df_87['Distance_mile'] = dis_df_87['Distance_mile'].astype(int)

# merge 
dis_df_87.isnull().sum()
df_87_distance = df_87_distance.reset_index(drop=True)
dis_df_87
df_87_distance
merged_df_87 = pd.concat([dis_df_87,df_87_distance],axis=1)
merged_df_87['Origin'].unique
merged_df_87.isnull().sum()
merged_df_87

# for origin, dest in zip(df_87['Origin'], df_87['Dest']):
#     mask = (merged_df_87['Origin'] == origin) & (merged_df_87['Dest'] == dest)
#     if mask.any():
#         df_87.loc[(df_87['Origin'] == origin) & (df_87['Dest'] == dest), 'Distance'] = merged_df_87.loc[mask, 'Distance_mile'].iloc[0]

merged_df_87
df_87
merged_87 = pd.merge(df_87,merged_df_87, how= 'outer', on= ['Origin','Dest'])
merged_87
merged_87 = merged_87.drop('Distance', axis=1)

# convert null to zero
merged_87['TaxiIn'] = merged_87['TaxiIn'].fillna(0)
merged_87['TaxiIn'] = merged_87['TaxiIn'].astype(int)

merged_87['TaxiOut'] = merged_87['TaxiOut'].fillna(0)
merged_87['TaxiOut'] = merged_87['TaxiOut'].astype(int)

merged_87['CancellationCode'] = merged_87['CancellationCode'].fillna(0).astype(int)

merged_87['CarrierDelay'] = merged_87['CarrierDelay'].fillna(0).astype(int)

merged_87['WeatherDelay'] = merged_87['WeatherDelay'].fillna(0).astype(int)

merged_87['NASDelay'] = merged_87['NASDelay'].fillna(0).astype(int)

merged_87['SecurityDelay'] = merged_87['SecurityDelay'].fillna(0).astype(int)

merged_87['LateAircraftDelay'] = merged_87['LateAircraftDelay'].fillna(0).astype(int)

merged_87['WeatherDelay'] = merged_87['WeatherDelay'].fillna(0).astype(int)
merged_87['TailNum'] = merged_87['TailNum'].fillna(0).astype(int)

# bool 
merged_87['Cancelled'] = merged_87['Cancelled'].astype('bool')
merged_87['Cancelled'].unique()
merged_87['Diverted'] = merged_87['Diverted'].astype('bool')

merged_87.head()
merged_87 = merged_87.drop('Distance', axis=1)
merged_87.to_csv('df_87.csv',index=False)
     
# 1989 data cleaning
df_89 = pd.read_csv('1989.csv')
df_89.shape
df_89.info()
# how much missing data 
df_89.isnull().sum()/len(df_89) *100

# cancelled
df_89['Cancelled'].unique()
df_89[df_89['Cancelled']==1]

# TailNum
df_89['TailNum'] = df_89['TailNum'].fillna(0).astype(int)


# time
def Time(t):
    if len(t) == 4:
        if t == '2400':
            t = '0000'
        t = t + '00'
        return t[:2] + ':' + t[2:4] + ':' + t[4:]
    elif len(t) == 3:
        t = '0' + t +'00'
        return t[:2] + ':' + t[2:4] + ':' + t[4:]
    elif len(t) == 2:
        t = '00' + t + '00'
        return t[:2] + ':' + t[2:4] + ':' + t[4:]
    else:
        t = '000' + t + '00'
        return t[:2] + ':' + t[2:4] + ':' + t[4:]


# clean CRSDepTime
df_89['CRSDepTime'] = df_89['CRSDepTime'].apply(lambda x: str(int(x)) if not pd.isnull(x) else x)
df_89['CRSDepTime'] = df_89['CRSDepTime'].map(Time, na_action = 'ignore')
df_89['CRSDepTime']


# remove DepTime null values and convert the type
df_89['DepTime'] = df_89['DepTime'].apply(lambda x: str(int(x)) if not pd.isnull(x) else x)
df_89['DepTime'] = df_89['DepTime'].map(Time, na_action = 'ignore')
df_89['DepTime']
df_89[df_89['DepTime'].isnull()] # check the null values
df_89['DepTime'] = df_89['DepTime'].fillna(0) # fill them
df_89[df_89['DepTime']==0] #check 


# ArrTime
df_89['ArrTime'] = df_89['ArrTime'].apply(lambda x: str(int(x)) if not pd.isnull(x) else x)
df_89['ArrTime'] = df_89['ArrTime'].map(Time, na_action= 'ignore')
df_89['ArrTime']
df_89['ArrTime'].isnull().sum() # check the null values
df_89['ArrTime'] = df_89['ArrTime'].fillna(0)
df_89[(df_89['ArrTime']==0) & (df_89['DepTime']=='00:00:00')]


# CRSArrTime
df_89['CRSArrTime'] = df_89['CRSArrTime'].apply(lambda x: str(int(x)) if not pd.isnull(x) else x)
df_89['CRSArrTime'] = df_89['CRSArrTime'].map(Time, na_action= 'ignore')
df_89['CRSArrTime']
df_89.isnull().sum()

# UniqueCarrier
df_89['UniqueCarrier'] = df_89['UniqueCarrier'].astype('category')

# ActualElapsedTime
df_89['ActualElapsedTime'].isnull().sum()
df_89['ActualElapsedTime'] = df_89['ActualElapsedTime'].fillna(0).astype(int)


df_89.head()
# CRSElapsedTime
df_89['CRSElapsedTime'].isnull().sum()
# AirTime
df_89['AirTime'] = df_89['AirTime'].fillna(0).astype(int)

# ArrDelay
df_89['ArrDelay'] = df_89['ArrDelay'].fillna(0).astype(int)

# DepDelay
df_89['DepDelay'] = df_89['DepDelay'].fillna(0).astype(int)

# distance
df_89_distance = df_89.loc[:,['Origin', 'Dest']]
df_89_distance.drop_duplicates(['Origin', 'Dest'],inplace=True)
df_89_distance.reset_index(drop=True)
df_89_distance
origin_names_89 = []
dest_names_89 = []
distances_be_89 = []

chrome_ver = chromedriver_autoinstaller.get_chrome_version().split(',')[0]
print(chrome_ver)

try:
    crawler = webdriver.Chrome(f'./{chrome_ver}/chromedriver.exe')
except:
    chromedriver_autoinstaller.install(True)
    crawler = webdriver.Chrome(f'./{chrome_ver}/chromedriver.exe')
    
for i,j in zip(df_89_distance['Origin'],df_89_distance['Dest']):

    crawler.get('https://www.airportdistancecalculator.com/flight-{i}-to-{j}.html'.format(i=i, j=j))
    
    while True:
        bh = crawler.execute_script('return document.body.scrollHeight') # the top of a brower (before height)
        print(bh)
        time.sleep(4) # wait for 20 secs
        crawler.execute_script('window.scrollTo(0, document.body.scrollHeight)') # scrolling down
        time.sleep(2) # wait for 20 secs
        ah = crawler.execute_script('return document.body.scrollHeight') # after height
        if ah == bh:
            break
        bh = ah
    cities = crawler.find_elements(By.CSS_SELECTOR, '.yellowbox')
    for city in cities:
        try:
            origin_name = city.find_element(By.XPATH, '/html/body/div[2]/div/div[1]/div[1]/div[1]/div[1]/strong[2]').text
            origin_names_89.append(origin_name)
            dest_name = city.find_element(By.XPATH, '/html/body/div[2]/div/div[1]/div[1]/div[1]/div[1]/strong[3]').text
            dest_names_89.append(dest_name)
            distance_be = city.find_element(By.XPATH, '/html/body/div[2]/div/div[1]/div[1]/div[1]/div[1]/strong[1]').text
            distances_be_89.append(distance_be)
        except:
            origin_names_89.append('exception')
            dest_names_89.append('exception')
            distances_be_89.append('exception')
crawler.close()


dis_df_89 = pd.DataFrame({'Origin airport':origin_names_89, 'Destination airport':dest_names_89, 'Distance_km':distances_be_89})
dis_df_89.to_csv('dis_df_89.csv',index=False)

# convert the unit
def km_to_mile(km):
    miles = km * 0.621371
    return miles

dis_df_89['Distance_km'] = dis_df_89['Distance_km'].str.replace(' kilometers','')
dis_df_89['Distance_km'] = dis_df_89['Distance_km'].astype(int)
dis_df_89['Origin airport'] = dis_df_89['Origin airport'].str.replace('distance from', '').str.replace(',','')

dis_df_89['Distance_mile'] = dis_df_89['Distance_km'].progress_apply(km_to_mile)
dis_df_89['Distance_mile'] = dis_df_89['Distance_mile'].round()
dis_df_89['Distance_mile'] = dis_df_89['Distance_mile'].astype(int)

# merge 
dis_df_89.isnull().sum()
dis_df_89
df_89_distance= df_89_distance.reset_index(drop=True)
df_89_distance[df_89_distance['Origin']==df_89_distance['Dest']]
df_89_distance.loc[3610]

dis_df_89.loc[3596]
new_row_89 = pd.DataFrame({'Origin airport': 'Los Angeles International Airport',\
                         'Destination airport':'Los Angeles International Airport', 'Distance_km':'0'}, index=[3595.5])
dis_df_89= pd.concat([dis_df_89.loc[:3595],new_row_89,dis_df_89[3596:]]).reset_index(drop=True)

dis_df_89.loc[3610]
new_row_89_1 = pd.DataFrame({'Origin airport': "O'Hare International Airport",\
                         'Destination airport':"O'Hare International Airport", 'Distance_km':'0'}, index=[3609.5])
dis_df_89= pd.concat([dis_df_89.loc[:3609],new_row_89_1,dis_df_89[3610:]]).reset_index(drop=True)

merged_df_89 = pd.concat([dis_df_89,df_89_distance],axis=1)
merged_df_89['Origin'].unique
merged_df_89.isnull().sum()
merged_df_89['Distance_mile'] = merged_df_89['Distance_mile'].fillna(0).astype(int)
merged_df_89

merged_89 = pd.merge(df_89,merged_df_89, how= 'outer', on= ['Origin','Dest'])
merged_89
merged_89 = merged_89.drop('Distance', axis=1)

# convert null to zero
merged_89['TaxiIn'] = merged_89['TaxiIn'].fillna(0)
merged_89['TaxiIn'] = merged_89['TaxiIn'].astype(int)

merged_89['TaxiOut'] = merged_89['TaxiIn'].fillna(0)
merged_89['TaxiOut'] = merged_89['TaxiOut'].astype(int)

merged_89['CancellationCode'] = merged_89['CancellationCode'].fillna(0).astype(int)

merged_89['CarrierDelay'] = merged_89['CarrierDelay'].fillna(0).astype(int)

merged_89['WeatherDelay'] = merged_89['WeatherDelay'].fillna(0).astype(int)

merged_89['NASDelay'] = merged_89['NASDelay'].fillna(0).astype(int)

merged_89['SecurityDelay'] = merged_89['SecurityDelay'].fillna(0).astype(int)

merged_89['LateAircraftDelay'] = merged_89['LateAircraftDelay'].fillna(0).astype(int)

# bool 
merged_89['Cancelled'] = merged_89['Cancelled'].astype('bool')
merged_89['Cancelled'].unique()
merged_89['Diverted'] = merged_89['Diverted'].astype('bool')

merged_89.info()
merged_89.to_csv('df_89.csv',index=False)


df_89.info()

# convert null to zero
df_89['TaxiIn'] = df_89['TaxiIn'].fillna(0).astype(int)

df_89['TaxiOut'] = df_89['TaxiIn'].fillna(0).astype(int)

df_89['CancellationCode'] = df_89['CancellationCode'].fillna(0).astype(int)

df_89['CarrierDelay'] = df_89['CarrierDelay'].fillna(0).astype(int)

df_89['WeatherDelay'] = df_89['WeatherDelay'].fillna(0).astype(int)

df_89['NASDelay'] = df_89['NASDelay'].fillna(0).astype(int)

df_89['SecurityDelay'] = df_89['SecurityDelay'].fillna(0).astype(int)

df_89['LateAircraftDelay'] = df_89['LateAircraftDelay'].fillna(0).astype(int)

# bool 
df_89['Cancelled'] = df_89['Cancelled'].astype('bool')
df_89['Cancelled'].unique()
df_89['Diverted'] = df_89['Diverted'].astype('bool')

df_89.info()

df_89.isnull().sum()
df_89.head()

# 1990 
df_90 = pd.read_csv('1990.csv')
df_90.shape
df_90.info()
# how much missing data 
df_90.isnull().sum()/len(df_90) *100

df_90.isnull().sum()

# DepTime
df_90['DepTime'] = df_90['DepTime'].apply(lambda x: str(int(x)) if not pd.isnull(x) else x)
df_90['DepTime'] = df_90['DepTime'].map(Time, na_action= 'ignore')
df_90['DepTime'].isnull().sum()
df_90['DepTime'] = df_90['DepTime'].fillna(0)
df_90[df_90['DepTime']==0]

# clean CRSDepTime
df_90['CRSDepTime'] = df_90['CRSDepTime'].apply(lambda x: str(int(x)) if not pd.isnull(x) else x)
df_90['CRSDepTime'] = df_90['CRSDepTime'].map(Time, na_action = 'ignore')
df_90['CRSDepTime'].isnull().sum()

# ArrTime
df_90['ArrTime'] = df_90['ArrTime'].apply(lambda x: str(int(x)) if not pd.isnull(x) else x)
df_90['ArrTime'] = df_90['ArrTime'].map(Time, na_action= 'ignore')
df_90['ArrTime']
df_90['ArrTime'].isnull().sum() # check the null values
df_90['ArrTime'] = df_90['ArrTime'].fillna(0)
df_90[(df_90['ArrTime']==0) & (df_90['DepTime']=='00:00:00')]


# CRSArrTime
df_90['CRSArrTime'] = df_90['CRSArrTime'].apply(lambda x: str(int(x)) if not pd.isnull(x) else x)
df_90['CRSArrTime'] = df_90['CRSArrTime'].map(Time, na_action= 'ignore')
df_90['CRSArrTime']
df_90.isnull().sum()

# UniqueCarrier
df_90['UniqueCarrier'] = df_90['UniqueCarrier'].astype('category')

# ActualElapsedTime
df_90['ActualElapsedTime'].isnull().sum()
df_90['ActualElapsedTime'] = df_90['ActualElapsedTime'].fillna(0).astype(int)
df_90.isnull().sum()

# TailNum
df_90['TailNum'] = df_90['TailNum'].fillna(0).astype(int)
df_90['TailNum']

df_90['CRSElapsedTime']

df_90.isnull().sum()

# AirTime
df_90['AirTime'] = df_90['AirTime'].fillna(0).astype(int)

# ArrDelay
df_90['ArrDelay'] = df_90['ArrDelay'].fillna(0).astype(int)

# DepDelay
df_90['DepDelay'] = df_90['DepDelay'].fillna(0).astype(int)

# distance

## scraping data
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import chromedriver_autoinstaller
import time
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from geopy import distance

df_90_distance = df_90.loc[:,['Origin', 'Dest']]
df_90_distance.drop_duplicates(['Origin', 'Dest'],inplace=True)
df_90_distance
origin_names = []
dest_names = []
distances_be = []

chrome_ver = chromedriver_autoinstaller.get_chrome_version().split(',')[0]
print(chrome_ver)

try:
    crawler = webdriver.Chrome(f'./{chrome_ver}/chromedriver.exe')
except:
    chromedriver_autoinstaller.install(True)
    crawler = webdriver.Chrome(f'./{chrome_ver}/chromedriver.exe')

for i,j in zip(df_90_distance['Origin'],df_90_distance['Dest']):

    crawler.get('https://www.airportdistancecalculator.com/flight-{i}-to-{j}.html'.format(i=i, j=j))
    
    while True:
        bh = crawler.execute_script('return document.body.scrollHeight') # the top of a brower (before height)
        print(bh)
        crawler.implicitly_wait(10) # wait for 10 secs
        crawler.execute_script('window.scrollTo(0, document.body.scrollHeight)') # scrolling down
        crawler.implicitly_wait(10) # wait for 10 secs
        ah = crawler.execute_script('return document.body.scrollHeight') # after height
        if ah == bh:
            break
        bh = ah
    cities = crawler.find_elements(By.CSS_SELECTOR, '.yellowbox')
    for city in cities:
        try:
            origin_name = city.find_element(By.XPATH, '/html/body/div[2]/div/div[1]/div[1]/div[1]/div[1]/strong[2]').text
            origin_names.append(origin_name)
            dest_name = city.find_element(By.XPATH, '/html/body/div[2]/div/div[1]/div[1]/div[1]/div[1]/strong[3]').text
            dest_names.append(dest_name)
            distance_be = city.find_element(By.XPATH, '/html/body/div[2]/div/div[1]/div[1]/div[1]/div[1]/strong[1]').text
            distances_be.append(distance_be)
        except:
            print('Exception')
crawler.close()

df = pd.DataFrame({'origin':origin_names, 'destination':dest_names, 'distance':distances_be})
df.to_csv('1990_distance.csv',index=False)
df = pd.read_csv('1990_distance.csv')
df['origin'] = df['origin'].str.replace('distance from','').str.replace(',','')
df['distance'] =df['distance'].str.replace('kilometers','')
df['distance'] = df['distance'].astype(int)

# convert the unit
def km_to_mile(km):
    miles = km * 0.621371
    return miles

df['distance_mile'] = df['distance'].progress_apply(km_to_mile)
df['distance_mile'] = df['distance_mile'].round()
df['distance_mile'] = df['distance_mile'].astype(int)

# merge 
df_90_distance = df_90_distance.reset_index(drop=True)
df
merged_df_90 = pd.concat([df,df_90_distance],axis=1)


# for origin, dest in zip(df_90['Origin'], df_90['Dest']):
#     mask = (merged_df_90['Origin'] == origin) & (merged_df_90['Dest'] == dest)
#     if mask.any():
#         df_90.loc[(df_90['Origin'] == origin) & (df_90['Dest'] == dest), 'Distance'] = merged_df_90.loc[mask, 'distance_mile'].iloc[0]

merged_df_90
df_90
merged_90 = pd.merge(df_90,merged_df_90, how= 'outer', on= ['Origin','Dest'])
merged_90
merged_90 = merged_90.drop('Distance', axis=1)
merged_90 = merged_90.rename(columns={'origin':'Origin airport','destination':'Destination airport'})
merged_90 = merged_90.rename(columns={'distance':'Distance_km','distance_mile':'Distance'})
# check the null values and convert to 0
merged_90['CancellationCode'] = merged_90['CancellationCode'].fillna(0).astype(int)
merged_90['CarrierDelay'] = merged_90['CarrierDelay'].fillna(0).astype(int)
merged_90['WeatherDelay'] = merged_90['WeatherDelay'].fillna(0).astype(int)
merged_90['NASDelay'] = merged_90['NASDelay'].fillna(0).astype(int)
merged_90['SecurityDelay'] = merged_90['SecurityDelay'].fillna(0).astype(int)
merged_90['LateAircraftDelay'] = merged_90['LateAircraftDelay'].fillna(0).astype(int)
merged_90['TaxiIn'] = merged_90['TaxiIn'].fillna(0).astype(int)
merged_90['TaxiOut'] = merged_90['TaxiOut'].fillna(0).astype(int)
merged_90['Diverted'] = merged_90['Diverted'].astype(bool)
merged_90['Cancelled'] = merged_90['Cancelled'].astype('bool')
merged_90.isnull().sum()
merged_90.head()
merged_90.to_csv('df_90.csv',index=False)

# 1991

# 1990 
df_91 = pd.read_csv('1991.csv')
df_91.shape
df_91.info()
# how much missing data 
df_91.isnull().sum()/len(df_91) *100

df_91.isnull().sum()

# DepTime
df_91['DepTime'] = df_91['DepTime'].apply(lambda x: str(int(x)) if not pd.isnull(x) else x)
df_91['DepTime'] = df_91['DepTime'].map(Time, na_action= 'ignore')
df_91['DepTime'].isnull().sum()
df_91['DepTime'] = df_91['DepTime'].fillna(0)
df_91[df_91['DepTime']==0]

# clean CRSDepTime
df_91['CRSDepTime'] = df_91['CRSDepTime'].apply(lambda x: str(int(x)) if not pd.isnull(x) else x)
df_91['CRSDepTime'] = df_91['CRSDepTime'].map(Time, na_action = 'ignore')
df_91['CRSDepTime'].isnull().sum()

# ArrTime
df_91['ArrTime'] = df_91['ArrTime'].apply(lambda x: str(int(x)) if not pd.isnull(x) else x)
df_91['ArrTime'] = df_91['ArrTime'].map(Time, na_action= 'ignore')
df_91['ArrTime']
df_91['ArrTime'].isnull().sum() # check the null values
df_91['ArrTime'] = df_91['ArrTime'].fillna(0)
df_91[(df_91['ArrTime']==0) & (df_91['DepTime']=='00:00:00')]


# CRSArrTime
df_91['CRSArrTime'] = df_91['CRSArrTime'].apply(lambda x: str(int(x)) if not pd.isnull(x) else x)
df_91['CRSArrTime'] = df_91['CRSArrTime'].map(Time, na_action= 'ignore')
df_91['CRSArrTime']
df_91.isnull().sum()

# UniqueCarrier
df_91['UniqueCarrier'] = df_91['UniqueCarrier'].astype('category')

# ActualElapsedTime
df_91['ActualElapsedTime'].isnull().sum()
df_91['ActualElapsedTime'] = df_91['ActualElapsedTime'].fillna(0).astype(int)
df_91.isnull().sum()

# TailNum
df_91['TailNum'] = df_91['TailNum'].fillna(0).astype(int)
df_91['TailNum']

df_91['CRSElapsedTime']

df_91.isnull().sum()

# AirTime
df_91['AirTime'] = df_91['AirTime'].fillna(0).astype(int)

# ArrDelay
df_91['ArrDelay'] = df_91['ArrDelay'].fillna(0).astype(int)

# DepDelay
df_91['DepDelay'] = df_91['DepDelay'].fillna(0).astype(int)

# Distance
df_91_distance = df_91.loc[:,['Origin', 'Dest']]
df_91_distance.drop_duplicates(['Origin', 'Dest'],inplace=True)
df_91_distance.reset_index(drop=True)
df_91_distance[df_91_distance['Origin']==df_91_distance['Dest']]
origin_names_91 = []
dest_names_91 = []
distances_be_91 = []

chrome_ver = chromedriver_autoinstaller.get_chrome_version().split(',')[0]
print(chrome_ver)

try:
    crawler = webdriver.Chrome(f'./{chrome_ver}/chromedriver.exe')
except:
    chromedriver_autoinstaller.install(True)
    crawler = webdriver.Chrome(f'./{chrome_ver}/chromedriver.exe')

for i,j in zip(df_91_distance['Origin'],df_91_distance['Dest']):

    crawler.get('https://www.airportdistancecalculator.com/flight-{i}-to-{j}.html'.format(i=i, j=j))
    
    while True:
        bh = crawler.execute_script('return document.body.scrollHeight') # the top of a brower (before height)
        print(bh)
        crawler.implicitly_wait(20) # wait for 20 secs
        crawler.execute_script('window.scrollTo(0, document.body.scrollHeight)') # scrolling down
        crawler.implicitly_wait(20) # wait for 20 secs
        ah = crawler.execute_script('return document.body.scrollHeight') # after height
        if ah == bh:
            break
        bh = ah
    cities = crawler.find_elements(By.CSS_SELECTOR, '.yellowbox')
    for city in cities:
        try:
            origin_name = city.find_element(By.XPATH, '/html/body/div[2]/div/div[1]/div[1]/div[1]/div[1]/strong[2]').text
            origin_names_91.append(origin_name)
            dest_name = city.find_element(By.XPATH, '/html/body/div[2]/div/div[1]/div[1]/div[1]/div[1]/strong[3]').text
            dest_names_91.append(dest_name)
            distance_be = city.find_element(By.XPATH, '/html/body/div[2]/div/div[1]/div[1]/div[1]/div[1]/strong[1]').text
            distances_be_91.append(distance_be)
        except:
            origin_names_91.append('exception')
            dest_names_91.append('exception')
            distances_be_91.append('exception')
crawler.close()

dis_df_91 = pd.DataFrame({'origin':origin_names_91, 'destination':dest_names_91, 'distance':distances_be_91})
dis_df_91.to_csv('1991_distance.csv',index=False)
dis_df_91['origin'] = dis_df_91['origin'].str.replace('distance from','').str.replace(',','')
dis_df_91['distance'] =dis_df_91['distance'].str.replace('kilometers','')
dis_df_91['distance'] = dis_df_91['distance'].astype(int)
dis_df_91

# convert the unit
def km_to_mile(km):
    miles = km * 0.621371
    return miles

dis_df_91['distance_mile'] = dis_df_91['distance'].progress_apply(km_to_mile)
dis_df_91['distance_mile'] = dis_df_91['distance_mile'].round()
dis_df_91['distance_mile'] = dis_df_91['distance_mile'].astype(int)

# merge 
df_91_distance = df_91_distance.reset_index(drop=True)
dis_df_91
merged_df_91 = pd.concat([dis_df_91,df_91_distance],axis=1)
merged_df_91

merged_91 = pd.merge(df_91,merged_df_91, how= 'outer', on= ['Origin','Dest'])
merged_91
merged_91 = merged_91.drop('Distance', axis=1)
merged_91 = merged_91.rename(columns={'origin':'Origin airport','destination':'Destination airport'})
merged_91 = merged_91.rename(columns={'distance':'Distance_km','distance_mile':'Distance'})
# check the null values and convert to 0
merged_91['CancellationCode'] = merged_91['CancellationCode'].fillna(0).astype(int)
merged_91['CarrierDelay'] = merged_91['CarrierDelay'].fillna(0).astype(int)
merged_91['WeatherDelay'] = merged_91['WeatherDelay'].fillna(0).astype(int)
merged_91['NASDelay'] = merged_91['NASDelay'].fillna(0).astype(int)
merged_91['SecurityDelay'] = merged_91['SecurityDelay'].fillna(0).astype(int)
merged_91['LateAircraftDelay'] = merged_91['LateAircraftDelay'].fillna(0).astype(int)
merged_91['TaxiIn'] = merged_91['TaxiIn'].fillna(0).astype(int)
merged_91['TaxiOut'] = merged_91['TaxiOut'].fillna(0).astype(int)
merged_91['Diverted'] = merged_91['Diverted'].astype(bool)
merged_91['Cancelled'] = merged_91['Cancelled'].astype('bool')
merged_91.isnull().sum()

merged_91.head()
merged_91.to_csv('df_91.csv',index=False)

#1992
df_92 = pd.read_csv('1992.csv')
df_92.shape
df_92.info()
# how much missing data 
df_92.isnull().sum()/len(df_92) *100

df_92.isnull().sum()

# DepTime
df_92['DepTime'] = df_92['DepTime'].apply(lambda x: str(int(x)) if not pd.isnull(x) else x)
df_92['DepTime'] = df_92['DepTime'].map(Time, na_action= 'ignore')
df_92['DepTime'].isnull().sum()
df_92['DepTime'] = df_92['DepTime'].fillna(0)
df_92[df_92['DepTime']==0]

# clean CRSDepTime
df_92['CRSDepTime'] = df_92['CRSDepTime'].apply(lambda x: str(int(x)) if not pd.isnull(x) else x)
df_92['CRSDepTime'] = df_92['CRSDepTime'].map(Time, na_action = 'ignore')
df_92['CRSDepTime'].isnull().sum()

# ArrTime
df_92['ArrTime'] = df_92['ArrTime'].apply(lambda x: str(int(x)) if not pd.isnull(x) else x)
df_92['ArrTime'] = df_92['ArrTime'].map(Time, na_action= 'ignore')
df_92['ArrTime']
df_92['ArrTime'].isnull().sum() # check the null values
df_92['ArrTime'] = df_92['ArrTime'].fillna(0)
df_92[(df_92['ArrTime']==0) & (df_92['DepTime']=='00:00:00')]


# CRSArrTime
df_92['CRSArrTime'] = df_92['CRSArrTime'].apply(lambda x: str(int(x)) if not pd.isnull(x) else x)
df_92['CRSArrTime'] = df_92['CRSArrTime'].map(Time, na_action= 'ignore')
df_92['CRSArrTime']
df_92.isnull().sum()

# UniqueCarrier
df_92['UniqueCarrier'] = df_92['UniqueCarrier'].astype('category')

# ActualElapsedTime
df_92['ActualElapsedTime'].isnull().sum()
df_92['ActualElapsedTime'] = df_92['ActualElapsedTime'].fillna(0).astype(int)
df_92['ActualElapsedTime'].isnull().sum()

# TailNum
df_92['TailNum'] = df_92['TailNum'].fillna(0).astype(int)
df_92['TailNum'].isnull().sum()

df_92['CRSElapsedTime'].isnull().sum()

df_92.isnull().sum()

# AirTime
df_92['AirTime'] = df_92['AirTime'].fillna(0).astype(int)

# ArrDelay
df_92['ArrDelay'] = df_92['ArrDelay'].fillna(0).astype(int)

# DepDelay
df_92['DepDelay'] = df_92['DepDelay'].fillna(0).astype(int)

# Distance
df_92_distance = df_92.loc[:,['Origin', 'Dest']]
df_92_distance.drop_duplicates(['Origin', 'Dest'],inplace=True)
df_92_distance.reset_index(drop=True)
df_92_distance[df_92_distance['Origin']==df_92_distance['Dest']]
df_92_distance
origin_names_92 = []
dest_names_92 = []
distances_be_92 = []

chrome_ver = chromedriver_autoinstaller.get_chrome_version().split(',')[0]
print(chrome_ver)

try:
    crawler = webdriver.Chrome(f'./{chrome_ver}/chromedriver.exe')
except:
    chromedriver_autoinstaller.install(True)
    crawler = webdriver.Chrome(f'./{chrome_ver}/chromedriver.exe')

for i,j in zip(df_92_distance['Origin'],df_92_distance['Dest']):

    crawler.get('https://www.airportdistancecalculator.com/flight-{i}-to-{j}.html'.format(i=i, j=j))
    
    while True:
        bh = crawler.execute_script('return document.body.scrollHeight') # the top of a brower (before height)
        print(bh)
        crawler.implicitly_wait(20) # wait for 20 secs
        crawler.execute_script('window.scrollTo(0, document.body.scrollHeight)') # scrolling down
        crawler.implicitly_wait(20) # wait for 20 secs
        ah = crawler.execute_script('return document.body.scrollHeight') # after height
        if ah == bh:
            break
        bh = ah
    cities = crawler.find_elements(By.CSS_SELECTOR, '.yellowbox')
    for city in cities:
        try:
            origin_name = city.find_element(By.XPATH, '/html/body/div[2]/div/div[1]/div[1]/div[1]/div[1]/strong[2]').text
            origin_names_92.append(origin_name)
            dest_name = city.find_element(By.XPATH, '/html/body/div[2]/div/div[1]/div[1]/div[1]/div[1]/strong[3]').text
            dest_names_92.append(dest_name)
            distance_be = city.find_element(By.XPATH, '/html/body/div[2]/div/div[1]/div[1]/div[1]/div[1]/strong[1]').text
            distances_be_92.append(distance_be)
        except:
            origin_names_92.append('exception')
            dest_names_92.append('exception')
            distances_be_92.append('exception')
crawler.close()

dis_df_92 = pd.DataFrame({'origin':origin_names_92, 'destination':dest_names_92, 'distance':distances_be_92})
dis_df_92
dis_df_92.to_csv('1992_distance.csv',index=False)
dis_df_92['origin'] = dis_df_92['origin'].str.replace('distance from','').str.replace(',','')
dis_df_92['distance'] =dis_df_92['distance'].str.replace('kilometers','')
dis_df_92['distance'] = dis_df_92['distance'].astype(int)

# convert the unit
def km_to_mile(km):
    miles = km * 0.621371
    return miles

dis_df_92['distance_mile'] = dis_df_92['distance'].progress_apply(km_to_mile)
dis_df_92['distance_mile'] = dis_df_92['distance_mile'].round()
dis_df_92['distance_mile'] = dis_df_92['distance_mile'].astype(int)

# merge 
df_92_distance = df_92_distance.reset_index(drop=True)
dis_df_92
merged_df_92 = pd.concat([dis_df_92,df_92_distance],axis=1)
merged_df_92

merged_92 = pd.merge(df_92,merged_df_92, how= 'outer', on= ['Origin','Dest'])
merged_92
merged_92 = merged_92.drop('Distance', axis=1)
merged_92 = merged_92.rename(columns={'origin':'Origin airport','destination':'Destination airport'})
merged_92 = merged_92.rename(columns={'distance':'Distance_km','distance_mile':'Distance'})
# check the null values and convert to 0
merged_92['CancellationCode'] = merged_92['CancellationCode'].fillna(0).astype(int)
merged_92['CarrierDelay'] = merged_92['CarrierDelay'].fillna(0).astype(int)
merged_92['WeatherDelay'] = merged_92['WeatherDelay'].fillna(0).astype(int)
merged_92['NASDelay'] = merged_92['NASDelay'].fillna(0).astype(int)
merged_92['SecurityDelay'] = merged_92['SecurityDelay'].fillna(0).astype(int)
merged_92['LateAircraftDelay'] = merged_92['LateAircraftDelay'].fillna(0).astype(int)
merged_92['TaxiIn'] = merged_92['TaxiIn'].fillna(0).astype(int)
merged_92['TaxiOut'] = merged_92['TaxiOut'].fillna(0).astype(int)
merged_92['Diverted'] = merged_92['Diverted'].astype(bool)
merged_92['Cancelled'] = merged_92['Cancelled'].astype('bool')
merged_92.isnull().sum()

merged_92.head()
merged_92.isnull().sum()
merged_92.to_csv('df_92.csv',index=False)

#1993
df_93 = pd.read_csv('1993.csv')
df_93.shape
df_93.info()
# how much missing data 
df_93.isnull().sum()/len(df_93) *100

df_93.isnull().sum()

# DepTime
df_93['DepTime'] = df_93['DepTime'].apply(lambda x: str(int(x)) if not pd.isnull(x) else x)
df_93['DepTime'] = df_93['DepTime'].map(Time, na_action= 'ignore')
df_93['DepTime'].isnull().sum()
df_93['DepTime'] = df_93['DepTime'].fillna(0)
df_93[df_93['DepTime']==0]

# clean CRSDepTime
df_93['CRSDepTime'] = df_93['CRSDepTime'].apply(lambda x: str(int(x)) if not pd.isnull(x) else x)
df_93['CRSDepTime'] = df_93['CRSDepTime'].map(Time, na_action = 'ignore')
df_93['CRSDepTime'].isnull().sum()

# ArrTime
df_93['ArrTime'] = df_93['ArrTime'].apply(lambda x: str(int(x)) if not pd.isnull(x) else x)
df_93['ArrTime'] = df_93['ArrTime'].map(Time, na_action= 'ignore')
df_93['ArrTime']
df_93['ArrTime'].isnull().sum() # check the null values
df_93['ArrTime'] = df_93['ArrTime'].fillna(0)
df_93[(df_93['ArrTime']==0) & (df_93['DepTime']=='00:00:00')]


# CRSArrTime
df_93['CRSArrTime'] = df_93['CRSArrTime'].apply(lambda x: str(int(x)) if not pd.isnull(x) else x)
df_93['CRSArrTime'] = df_93['CRSArrTime'].map(Time, na_action= 'ignore')
df_93['CRSArrTime']
df_93.isnull().sum()

# UniqueCarrier
df_93['UniqueCarrier'] = df_93['UniqueCarrier'].astype('category')

# ActualElapsedTime
df_93['ActualElapsedTime'].isnull().sum()
df_93['ActualElapsedTime'] = df_93['ActualElapsedTime'].fillna(0).astype(int)
df_93['ActualElapsedTime'].isnull().sum()

# TailNum
df_93['TailNum'] = df_93['TailNum'].fillna(0).astype(int)
df_93['TailNum'].isnull().sum()

df_93['CRSElapsedTime'].isnull().sum()

df_93.isnull().sum()

# AirTime
df_93['AirTime'] = df_93['AirTime'].fillna(0).astype(int)

# ArrDelay
df_93['ArrDelay'] = df_93['ArrDelay'].fillna(0).astype(int)

# DepDelay
df_93['DepDelay'] = df_93['DepDelay'].fillna(0).astype(int)

# Distance
df_93_distance = df_93.loc[:,['Origin', 'Dest']]
df_93_distance.drop_duplicates(['Origin', 'Dest'],inplace=True)
df_93_distance.reset_index(drop=True)
df_93_distance[df_93_distance['Origin']==df_93_distance['Dest']]
df_93_distance
origin_names_93 = []
dest_names_93 = []
distances_be_93 = []

chrome_ver = chromedriver_autoinstaller.get_chrome_version().split(',')[0]
print(chrome_ver)

try:
    crawler = webdriver.Chrome(f'./{chrome_ver}/chromedriver.exe')
except:
    chromedriver_autoinstaller.install(True)
    crawler = webdriver.Chrome(f'./{chrome_ver}/chromedriver.exe')

for i,j in zip(df_93_distance['Origin'][1576:],df_93_distance['Dest'][1576:]):

    crawler.get('https://www.airportdistancecalculator.com/flight-{i}-to-{j}.html'.format(i=i, j=j))
    
    while True:
        bh = crawler.execute_script('return document.body.scrollHeight') # the top of a brower (before height)
        print(bh)
        crawler.implicitly_wait(20) # wait for 20 secs
        crawler.execute_script('window.scrollTo(0, document.body.scrollHeight)') # scrolling down
        crawler.implicitly_wait(20) # wait for 20 secs
        ah = crawler.execute_script('return document.body.scrollHeight') # after height
        if ah == bh:
            break
        bh = ah
    cities = crawler.find_elements(By.CSS_SELECTOR, '.yellowbox')
    for city in cities:
        try:
            origin_name = city.find_element(By.XPATH, '/html/body/div[2]/div/div[1]/div[1]/div[1]/div[1]/strong[2]').text
            origin_names_93.append(origin_name)
            dest_name = city.find_element(By.XPATH, '/html/body/div[2]/div/div[1]/div[1]/div[1]/div[1]/strong[3]').text
            dest_names_93.append(dest_name)
            distance_be = city.find_element(By.XPATH, '/html/body/div[2]/div/div[1]/div[1]/div[1]/div[1]/strong[1]').text
            distances_be_93.append(distance_be)
        except:
            origin_names_93.append('exception')
            dest_names_93.append('exception')
            distances_be_93.append('exception')
crawler.close()

dis_df_93 = pd.DataFrame({'origin':origin_names_93, 'destination':dest_names_93, 'distance':distances_be_93})
dis_df_93
dis_df_93.to_csv('1993_distance.csv',index=False)
dis_df_93['origin'] = dis_df_93['origin'].str.replace('distance from','').str.replace(',','')
dis_df_93['distance'] =dis_df_93['distance'].str.replace('kilometers','')
dis_df_93['distance'] = dis_df_93['distance'].astype(int)

# convert the unit
def km_to_mile(km):
    miles = km * 0.621371
    return miles

dis_df_93['distance_mile'] = dis_df_93['distance'].progress_apply(km_to_mile)
dis_df_93['distance_mile'] = dis_df_93['distance_mile'].round()
dis_df_93['distance_mile'] = dis_df_93['distance_mile'].astype(int)

# merge 
df_93_distance = df_93_distance.reset_index(drop=True)
dis_df_93
merged_df_93 = pd.concat([dis_df_93,df_93_distance],axis=1)
merged_df_93

merged_93 = pd.merge(df_93,merged_df_93, how= 'outer', on= ['Origin','Dest'])
merged_93
merged_93 = merged_93.drop('Distance', axis=1)
merged_93 = merged_93.rename(columns={'origin':'Origin airport','destination':'Destination airport'})
merged_93 = merged_93.rename(columns={'distance':'Distance_km','distance_mile':'Distance'})
# check the null values and convert to 0
merged_93['CancellationCode'] = merged_93['CancellationCode'].fillna(0).astype(int)
merged_93['CarrierDelay'] = merged_93['CarrierDelay'].fillna(0).astype(int)
merged_93['WeatherDelay'] = merged_93['WeatherDelay'].fillna(0).astype(int)
merged_93['NASDelay'] = merged_93['NASDelay'].fillna(0).astype(int)
merged_93['SecurityDelay'] = merged_93['SecurityDelay'].fillna(0).astype(int)
merged_93['LateAircraftDelay'] = merged_93['LateAircraftDelay'].fillna(0).astype(int)
merged_93['TaxiIn'] = merged_93['TaxiIn'].fillna(0).astype(int)
merged_93['TaxiOut'] = merged_93['TaxiOut'].fillna(0).astype(int)
merged_93['Diverted'] = merged_93['Diverted'].astype(bool)
merged_93['Cancelled'] = merged_93['Cancelled'].astype('bool')
merged_93.isnull().sum()

merged_93.head()
merged_93.isnull().sum()
merged_93.to_csv('df_93.csv',index=False)

# 1994

df_94 = pd.read_csv('1994.csv')
df_94.shape
df_94.info()
# how much missing data 
df_94.isnull().sum()/len(df_94) *100

df_94.isnull().sum()

# DepTime
df_94['DepTime'] = df_94['DepTime'].apply(lambda x: str(int(x)) if not pd.isnull(x) else x)
df_94['DepTime'] = df_94['DepTime'].map(Time, na_action= 'ignore')
df_94['DepTime'].isnull().sum()
df_94['DepTime'] = df_94['DepTime'].fillna(0)
df_94[df_94['DepTime']==0]

# clean CRSDepTime
df_94['CRSDepTime'] = df_94['CRSDepTime'].apply(lambda x: str(int(x)) if not pd.isnull(x) else x)
df_94['CRSDepTime'] = df_94['CRSDepTime'].map(Time, na_action = 'ignore')
df_94['CRSDepTime'].isnull().sum()

# ArrTime
df_94['ArrTime'] = df_94['ArrTime'].apply(lambda x: str(int(x)) if not pd.isnull(x) else x)
df_94['ArrTime'] = df_94['ArrTime'].map(Time, na_action= 'ignore')
df_94['ArrTime']
df_94['ArrTime'].isnull().sum() # check the null values
df_94['ArrTime'] = df_94['ArrTime'].fillna(0)
df_94[(df_94['ArrTime']==0) & (df_94['DepTime']=='00:00:00')]


# CRSArrTime
df_94['CRSArrTime'] = df_94['CRSArrTime'].apply(lambda x: str(int(x)) if not pd.isnull(x) else x)
df_94['CRSArrTime'] = df_94['CRSArrTime'].map(Time, na_action= 'ignore')
df_94['CRSArrTime']
df_94.isnull().sum()

# UniqueCarrier
df_94['UniqueCarrier'] = df_94['UniqueCarrier'].astype('category')

# ActualElapsedTime
df_94['ActualElapsedTime'].isnull().sum()
df_94['ActualElapsedTime'] = df_94['ActualElapsedTime'].fillna(0).astype(int)
df_94['ActualElapsedTime'].isnull().sum()

# TailNum
df_94['TailNum'] = df_94['TailNum'].fillna(0).astype(int)
df_94['TailNum'].isnull().sum()

df_94['CRSElapsedTime'].isnull().sum()

df_94.isnull().sum()

# AirTime
df_94['AirTime'] = df_94['AirTime'].fillna(0).astype(int)

# ArrDelay
df_94['ArrDelay'] = df_94['ArrDelay'].fillna(0).astype(int)
df_94['ArrDelay'].isnull().sum()
# DepDelay
df_94['DepDelay'] = df_94['DepDelay'].fillna(0).astype(int)
df_94['DepDelay'] .isnull().sum()
# Distance
df_94_distance = df_94.loc[:,['Origin', 'Dest']]
df_94_distance.drop_duplicates(['Origin', 'Dest'],inplace=True)
df_94_distance.reset_index(drop=True)
df_94_distance[df_94_distance['Origin']==df_94_distance['Dest']]
df_94_distance
origin_names_94 = []
dest_names_94 = []
distances_be_94 = []

chrome_ver = chromedriver_autoinstaller.get_chrome_version().split(',')[0]
print(chrome_ver)

try:
    crawler = webdriver.Chrome(f'./{chrome_ver}/chromedriver.exe')
except:
    chromedriver_autoinstaller.install(True)
    crawler = webdriver.Chrome(f'./{chrome_ver}/chromedriver.exe')

for i,j in zip(df_94_distance['Origin'][2621:],df_94_distance['Dest'][2621:]):

    crawler.get('https://www.airportdistancecalculator.com/flight-{i}-to-{j}.html'.format(i=i, j=j))
    
    while True:
        bh = crawler.execute_script('return document.body.scrollHeight') # the top of a brower (before height)
        print(bh)
        crawler.implicitly_wait(30) # wait for 20 secs
        crawler.execute_script('window.scrollTo(0, document.body.scrollHeight)') # scrolling down
        crawler.implicitly_wait(30) # wait for 20 secs
        ah = crawler.execute_script('return document.body.scrollHeight') # after height
        if ah == bh:
            break
        bh = ah
    cities = crawler.find_elements(By.CSS_SELECTOR, '.yellowbox')
    for city in cities:
        try:
            origin_name = city.find_element(By.XPATH, '/html/body/div[2]/div/div[1]/div[1]/div[1]/div[1]/strong[2]').text
            origin_names_94.append(origin_name)
            dest_name = city.find_element(By.XPATH, '/html/body/div[2]/div/div[1]/div[1]/div[1]/div[1]/strong[3]').text
            dest_names_94.append(dest_name)
            distance_be = city.find_element(By.XPATH, '/html/body/div[2]/div/div[1]/div[1]/div[1]/div[1]/strong[1]').text
            distances_be_94.append(distance_be)
        except:
            origin_names_94.append('exception')
            dest_names_94.append('exception')
            distances_be_94.append('exception')
crawler.close()

dis_df_94 = pd.DataFrame({'origin':origin_names_94, 'destination':dest_names_94, 'distance':distances_be_94})
dis_df_94
dis_df_94.to_csv('1994_distance.csv',index=False)
dis_df_94['origin'] = dis_df_94['origin'].str.replace('distance from','').str.replace(',','')
dis_df_94['distance'] =dis_df_94['distance'].str.replace('kilometers','')
dis_df_94['distance'] = dis_df_94['distance'].astype(int)

# convert the unit
def km_to_mile(km):
    miles = km * 0.621371
    return miles

dis_df_94['distance_mile'] = dis_df_94['distance'].progress_apply(km_to_mile)
dis_df_94['distance_mile'] = dis_df_94['distance_mile'].round()
dis_df_94['distance_mile'] = dis_df_94['distance_mile'].astype(int)

# merge 
df_94_distance = df_94_distance.reset_index(drop=True)
dis_df_94
merged_df_94 = pd.concat([dis_df_94,df_94_distance],axis=1)
merged_df_94

merged_94 = pd.merge(df_94,merged_df_94, how= 'outer', on= ['Origin','Dest'])
merged_94
merged_94 = merged_94.drop('Distance', axis=1)
merged_94 = merged_94.rename(columns={'origin':'Origin airport','destination':'Destination airport'})
merged_94 = merged_94.rename(columns={'distance':'Distance_km','distance_mile':'Distance'})
# check the null values and convert to 0
merged_94['CancellationCode'] = merged_94['CancellationCode'].fillna(0).astype(int)
merged_94['CarrierDelay'] = merged_94['CarrierDelay'].fillna(0).astype(int)
merged_94['WeatherDelay'] = merged_94['WeatherDelay'].fillna(0).astype(int)
merged_94['NASDelay'] = merged_94['NASDelay'].fillna(0).astype(int)
merged_94['SecurityDelay'] = merged_94['SecurityDelay'].fillna(0).astype(int)
merged_94['LateAircraftDelay'] = merged_94['LateAircraftDelay'].fillna(0).astype(int)
merged_94['TaxiIn'] = merged_94['TaxiIn'].fillna(0).astype(int)
merged_94['TaxiOut'] = merged_94['TaxiOut'].fillna(0).astype(int)
merged_94['Diverted'] = merged_94['Diverted'].astype(bool)
merged_94['Cancelled'] = merged_94['Cancelled'].astype('bool')
merged_94.isnull().sum()

merged_94.head()
merged_94.isnull().sum()
merged_94.to_csv('df_94.csv',index=False)

# 1995

df_95 = pd.read_csv('1995.csv')
df_95.shape
df_95.info()
# how much missing data 
df_95.isnull().sum()/len(df_95) *100

df_95.isnull().sum()

# DepTime
df_95['DepTime'] = df_95['DepTime'].apply(lambda x: str(int(x)) if not pd.isnull(x) else x)
df_95['DepTime'] = df_95['DepTime'].map(Time, na_action= 'ignore')
df_95['DepTime'].isnull().sum()
df_95['DepTime'] = df_95['DepTime'].fillna(0)
df_95[df_95['DepTime']==0]

# clean CRSDepTime
df_95['CRSDepTime'] = df_95['CRSDepTime'].apply(lambda x: str(int(x)) if not pd.isnull(x) else x)
df_95['CRSDepTime'] = df_95['CRSDepTime'].map(Time, na_action = 'ignore')
df_95['CRSDepTime'].isnull().sum()

# ArrTime
df_95['ArrTime'] = df_95['ArrTime'].apply(lambda x: str(int(x)) if not pd.isnull(x) else x)
df_95['ArrTime'] = df_95['ArrTime'].map(Time, na_action= 'ignore')
df_95['ArrTime']
df_95['ArrTime'].isnull().sum() # check the null values
df_95['ArrTime'] = df_95['ArrTime'].fillna(0)
df_95[(df_95['ArrTime']==0) & (df_95['DepTime']=='00:00:00')]


# CRSArrTime
df_95['CRSArrTime'] = df_95['CRSArrTime'].apply(lambda x: str(int(x)) if not pd.isnull(x) else x)
df_95['CRSArrTime'] = df_95['CRSArrTime'].map(Time, na_action= 'ignore')
df_95['CRSArrTime']
df_95.isnull().sum()

# UniqueCarrier
df_95['UniqueCarrier'] = df_95['UniqueCarrier'].astype('category')

# ActualElapsedTime
df_95['ActualElapsedTime'].isnull().sum()
df_95['ActualElapsedTime'] = df_95['ActualElapsedTime'].fillna(0).astype(int)
df_95['ActualElapsedTime'].isnull().sum()

# TailNum
df_95['TailNum'] = df_95['TailNum'].fillna(0)
df_95['TailNum'].isnull().sum()
df_95['TailNum']

df_95['CRSElapsedTime'].isnull().sum()
df_95['CRSElapsedTime'] = df_95['CRSElapsedTime'].fillna(0)


# AirTime
df_95['AirTime'] = df_95['AirTime'].fillna(0).astype(int)

# ArrDelay
df_95['ArrDelay'] = df_95['ArrDelay'].fillna(0).astype(int)
df_95['ArrDelay'].isnull().sum()
# DepDelay
df_95['DepDelay'] = df_95['DepDelay'].fillna(0).astype(int)
df_95['DepDelay'] .isnull().sum()
# Distance
df_95_distance = df_95.loc[:,['Origin', 'Dest']]
df_95_distance.drop_duplicates(['Origin', 'Dest'],inplace=True)
df_95_distance = df_95_distance.reset_index(drop=True)
df_95_distance[df_95_distance['Origin']==df_95_distance['Dest']]
df_95_distance
origin_names_95 = []
dest_names_95 = []
distances_be_95 = []

chrome_ver = chromedriver_autoinstaller.get_chrome_version().split(',')[0]
print(chrome_ver)

try:
    crawler = webdriver.Chrome(f'./{chrome_ver}/chromedriver.exe')
except:
    chromedriver_autoinstaller.install(True)
    crawler = webdriver.Chrome(f'./{chrome_ver}/chromedriver.exe')

for i,j in zip(df_95_distance['Origin'][3277:],df_95_distance['Dest'][3277:]):

    crawler.get('https://www.airportdistancecalculator.com/flight-{i}-to-{j}.html'.format(i=i, j=j))
    
    while True:
        bh = crawler.execute_script('return document.body.scrollHeight') # the top of a brower (before height)
        print(bh)
        crawler.implicitly_wait(30) # wait for 20 secs
        crawler.execute_script('window.scrollTo(0, document.body.scrollHeight)') # scrolling down
        crawler.implicitly_wait(30) # wait for 20 secs
        ah = crawler.execute_script('return document.body.scrollHeight') # after height
        if ah == bh:
            break
        bh = ah
    cities = crawler.find_elements(By.CSS_SELECTOR, '.yellowbox')
    for city in cities:
        try:
            origin_name = city.find_element(By.XPATH, '/html/body/div[2]/div/div[1]/div[1]/div[1]/div[1]/strong[2]').text
            origin_names_95.append(origin_name)
            dest_name = city.find_element(By.XPATH, '/html/body/div[2]/div/div[1]/div[1]/div[1]/div[1]/strong[3]').text
            dest_names_95.append(dest_name)
            distance_be = city.find_element(By.XPATH, '/html/body/div[2]/div/div[1]/div[1]/div[1]/div[1]/strong[1]').text
            distances_be_95.append(distance_be)
        except:
            origin_names_95.append('exception')
            dest_names_95.append('exception')
            distances_be_95.append('exception')
crawler.close()

dis_df_95 = pd.DataFrame({'origin':origin_names_95, 'destination':dest_names_95, 'distance':distances_be_95})
dis_df_95
dis_df_95.to_csv('1995_distance.csv',index=False)
dis_df_95['origin'] = dis_df_95['origin'].str.replace('distance from','').str.replace(',','')
dis_df_95['distance'] =dis_df_95['distance'].str.replace('kilometers','')
dis_df_95['distance'] = dis_df_95['distance'].astype(int)
dis_df_95[dis_df_95['distance']=='exception']
df_95_distance.loc[3348]
dis_df_95.loc[3348,'origin']= 'Monroe Regional Airport'
dis_df_95.loc[3348,'destination']= 'Birmingham Airport'
dis_df_95.loc[3348,'distance']= '507'
dis_df_95['distance'] = dis_df_95['distance'].astype(int)


# convert the unit
def km_to_mile(km):
    miles = km * 0.621371
    return miles

dis_df_95['distance_mile'] = dis_df_95['distance'].progress_apply(km_to_mile)
dis_df_95['distance_mile'] = dis_df_95['distance_mile'].round()
dis_df_95['distance_mile'] = dis_df_95['distance_mile'].astype(int)

# merge 
df_95_distance = df_95_distance.reset_index(drop=True)
dis_df_95
merged_df_95 = pd.concat([dis_df_95,df_95_distance],axis=1)
merged_df_95

merged_95 = pd.merge(df_95,merged_df_95, how= 'outer', on= ['Origin','Dest'])
merged_95
merged_95 = merged_95.drop('Distance', axis=1)
merged_95 = merged_95.rename(columns={'origin':'Origin airport','destination':'Destination airport'})
merged_95 = merged_95.rename(columns={'distance':'Distance_km','distance_mile':'Distance'})
# check the null values and convert to 0
merged_95['CancellationCode'] = merged_95['CancellationCode'].fillna(0).astype(int)
merged_95['CarrierDelay'] = merged_95['CarrierDelay'].fillna(0).astype(int)
merged_95['WeatherDelay'] = merged_95['WeatherDelay'].fillna(0).astype(int)
merged_95['NASDelay'] = merged_95['NASDelay'].fillna(0).astype(int)
merged_95['SecurityDelay'] = merged_95['SecurityDelay'].fillna(0).astype(int)
merged_95['LateAircraftDelay'] = merged_95['LateAircraftDelay'].fillna(0).astype(int)
merged_95['TaxiIn'] = merged_95['TaxiIn'].fillna(0).astype(int)
merged_95['TaxiOut'] = merged_95['TaxiOut'].fillna(0).astype(int)
merged_95['Diverted'] = merged_95['Diverted'].astype(bool)
merged_95['Cancelled'] = merged_95['Cancelled'].astype('bool')
merged_95.isnull().sum()

merged_95.head()
merged_95.isnull().sum()
merged_95.to_csv('df_95.csv',index=False)

## 1996


df_96 = pd.read_csv('1996.csv')
df_96.shape
df_96.info()
# how much missing data 
df_96.isnull().sum()/len(df_96) *100

df_95.isnull().sum()

# DepTime
df_96['DepTime'] = df_96['DepTime'].apply(lambda x: str(int(x)) if not pd.isnull(x) else x)
df_96['DepTime'] = df_96['DepTime'].map(Time, na_action= 'ignore')
df_96['DepTime'].isnull().sum()
df_96['DepTime'] = df_96['DepTime'].fillna(0)
df_96[df_96['DepTime']==0]

# clean CRSDepTime
df_96['CRSDepTime'] = df_96['CRSDepTime'].apply(lambda x: str(int(x)) if not pd.isnull(x) else x)
df_96['CRSDepTime'] = df_96['CRSDepTime'].map(Time, na_action = 'ignore')
df_96['CRSDepTime'].isnull().sum()

# ArrTime
df_96['ArrTime'] = df_96['ArrTime'].apply(lambda x: str(int(x)) if not pd.isnull(x) else x)
df_96['ArrTime'] = df_96['ArrTime'].map(Time, na_action= 'ignore')
df_96['ArrTime']
df_96['ArrTime'].isnull().sum() # check the null values
df_96['ArrTime'] = df_96['ArrTime'].fillna(0)
df_96[(df_96['ArrTime']==0) & (df_96['DepTime']=='00:00:00')]


# CRSArrTime
df_96['CRSArrTime'] = df_96['CRSArrTime'].apply(lambda x: str(int(x)) if not pd.isnull(x) else x)
df_96['CRSArrTime'] = df_96['CRSArrTime'].map(Time, na_action= 'ignore')
df_96['CRSArrTime']
df_96.isnull().sum()

# UniqueCarrier
df_96['UniqueCarrier'] = df_96['UniqueCarrier'].astype('category')

# ActualElapsedTime
df_96['ActualElapsedTime'].isnull().sum()
df_96['ActualElapsedTime'] = df_96['ActualElapsedTime'].fillna(0).astype(int)
df_96['ActualElapsedTime'].isnull().sum()

# TailNum
df_96['TailNum'] = df_96['TailNum'].fillna(0)
df_96['TailNum'].isnull().sum()
df_96['TailNum']

df_96['CRSElapsedTime'].isnull().sum()
df_96['CRSElapsedTime'] = df_96['CRSElapsedTime'].fillna(0)


# AirTime
df_96['AirTime'] = df_96['AirTime'].fillna(0).astype(int)

# ArrDelay
df_96['ArrDelay'] = df_96['ArrDelay'].fillna(0).astype(int)
df_96['ArrDelay'].isnull().sum()
# DepDelay
df_96['DepDelay'] = df_96['DepDelay'].fillna(0).astype(int)
df_96['DepDelay'] .isnull().sum()
# Distance
df_96_distance = df_96.loc[:,['Origin', 'Dest']]
df_96_distance.drop_duplicates(['Origin', 'Dest'],inplace=True)
df_96_distance = df_96_distance.reset_index(drop=True)
df_96_distance[df_96_distance['Origin']==df_96_distance['Dest']]
df_96_distance
origin_names_96 = []
dest_names_96 = []
distances_be_96 = []

chrome_ver = chromedriver_autoinstaller.get_chrome_version().split(',')[0]
print(chrome_ver)

try:
    crawler = webdriver.Chrome(f'./{chrome_ver}/chromedriver.exe')
except:
    chromedriver_autoinstaller.install(True)
    crawler = webdriver.Chrome(f'./{chrome_ver}/chromedriver.exe')

for i,j in zip(df_96_distance['Origin'][2204:],df_96_distance['Dest'][2204:]):

    crawler.get('https://www.airportdistancecalculator.com/flight-{i}-to-{j}.html'.format(i=i, j=j))
    
    while True:
        bh = crawler.execute_script('return document.body.scrollHeight') # the top of a brower (before height)
        print(bh)
        crawler.implicitly_wait(30) # wait for 20 secs
        crawler.execute_script('window.scrollTo(0, document.body.scrollHeight)') # scrolling down
        crawler.implicitly_wait(30) # wait for 20 secs
        ah = crawler.execute_script('return document.body.scrollHeight') # after height
        if ah == bh:
            break
        bh = ah
    cities = crawler.find_elements(By.CSS_SELECTOR, '.yellowbox')
    for city in cities:
        try:
            origin_name = city.find_element(By.XPATH, '/html/body/div[2]/div/div[1]/div[1]/div[1]/div[1]/strong[2]').text
            origin_names_96.append(origin_name)
            dest_name = city.find_element(By.XPATH, '/html/body/div[2]/div/div[1]/div[1]/div[1]/div[1]/strong[3]').text
            dest_names_96.append(dest_name)
            distance_be = city.find_element(By.XPATH, '/html/body/div[2]/div/div[1]/div[1]/div[1]/div[1]/strong[1]').text
            distances_be_96.append(distance_be)
        except:
            origin_names_96.append('exception')
            dest_names_96.append('exception')
            distances_be_96.append('exception')
crawler.close()

dis_df_96 = pd.DataFrame({'origin':origin_names_96, 'destination':dest_names_96, 'distance':distances_be_96})
dis_df_96[dis_df_96['distance']=='exception']
df_96_distance.loc[871]
# fill the exception rows 
dis_df_96.loc[871,'origin']= 'West Palm Beach International Airport'
dis_df_96.loc[871,'destination']= 'Orlando International Airport'
dis_df_96.loc[871,'distance']= '229'
dis_df_96.to_csv('1996_distance.csv',index=False)
dis_df_96['origin'] = dis_df_96['origin'].str.replace('distance from','').str.replace(',','')
dis_df_96['distance'] =dis_df_96['distance'].str.replace('kilometers','')
dis_df_96['distance'] = dis_df_96['distance'].astype(int)




# convert the unit
def km_to_mile(km):
    miles = km * 0.621371
    return miles

dis_df_96['distance_mile'] = dis_df_96['distance'].progress_apply(km_to_mile)
dis_df_96['distance_mile'] = dis_df_96['distance_mile'].round()
dis_df_96['distance_mile'] = dis_df_96['distance_mile'].astype(int)

# merge 
df_96_distance = df_96_distance.reset_index(drop=True)
dis_df_96
merged_df_96 = pd.concat([dis_df_96,df_96_distance],axis=1)
merged_df_96

merged_96 = pd.merge(df_96,merged_df_96, how= 'outer', on= ['Origin','Dest'])
merged_96
merged_96 = merged_96.drop('Distance', axis=1)
merged_96 = merged_96.rename(columns={'origin':'Origin airport','destination':'Destination airport'})
merged_96 = merged_96.rename(columns={'distance':'Distance_km','distance_mile':'Distance'})
# check the null values and convert to 0
merged_96['CancellationCode'] = merged_96['CancellationCode'].fillna(0).astype(int)
merged_96['CarrierDelay'] = merged_96['CarrierDelay'].fillna(0).astype(int)
merged_96['WeatherDelay'] = merged_96['WeatherDelay'].fillna(0).astype(int)
merged_96['NASDelay'] = merged_96['NASDelay'].fillna(0).astype(int)
merged_96['SecurityDelay'] = merged_96['SecurityDelay'].fillna(0).astype(int)
merged_96['LateAircraftDelay'] = merged_96['LateAircraftDelay'].fillna(0).astype(int)
merged_96['TaxiIn'] = merged_96['TaxiIn'].fillna(0).astype(int)
merged_96['TaxiOut'] = merged_96['TaxiOut'].fillna(0).astype(int)
merged_96['Diverted'] = merged_96['Diverted'].astype(bool)
merged_96['Cancelled'] = merged_96['Cancelled'].astype('bool')
merged_96['CRSElapsedTime'] = merged_96['CRSElapsedTime'].astype(int)
merged_96.isnull().sum()

merged_96.head()
merged_96.tail()
merged_96.isnull().sum()
merged_96.to_csv('df_96.csv',index=False)



# combine 
df_87 = pd.read_csv('df_87.csv') 
df_87.head()
df_87 = df_87.rename(columns={'Distance_mile':'Distance'})
df_87.info()
df_89 = pd.read_csv('df_89 copy.csv')
df_89 = df_89.rename(columns={'Distance_mile':'Distance'})
df_89.head()
df_89.info()
df_90 = pd.read_csv('df_90 copy.csv')
df_90.head()
df_90.info()
df_91 = pd.read_csv('df_91 copy.csv')
df_91.head()
df_91.info()
df_92 = pd.read_csv('df_92 copy.csv')
df_92.head()
df_93 = pd.read_csv('df_93 copy.csv')
df_93.head()
df_94 = pd.read_csv('df_94 copy.csv')
df_94.head()
df_95 = pd.read_csv('df_95 copy.csv')
df_95.head()
df_96 = pd.read_csv('df_96 copy.csv')
df_96.head()
master_df = pd.concat([df_87, df_89, df_90, df_91, df_92, df_93, df_94, df_95, df_96], axis=0)
master_df.info()
master_df.to_csv('combined_data.csv')