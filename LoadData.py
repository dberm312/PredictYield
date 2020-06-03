# -*- coding: utf-8 -*-
"""
Created on Sun May 31 00:03:51 2020

@author: Berma
"""
#%%
import ee
ee.Initialize()
import pandas as pd
import numpy as np
import datetime
from sklearn.cluster import KMeans
#%%
def inttostr(s,n=5):
    s = str(s)
    add = n-len(s)
    s = '0'*add+s
    return(s)

def loadlith(fips):
    geo  = ee.FeatureCollection("TIGER/2018/Counties")
    geo = geo.filter(ee.Filter.eq('STATEFP',fips[:2]))
    geo = geo.filter(ee.Filter.eq('COUNTYFP',fips[2:]))
    
    data= ee.Image('CSP/ERGo/1_0/US/lithology').select('b1')
    data = data.reduceRegion(
        reducer=ee.Reducer.autoHistogram(),
        geometry=geo,
        scale = 1000)
    data = pd.DataFrame(data.getInfo()['b1'])
    data =data[data[0]!=0]
    data[1]=data[1]/data[1].sum()
    data.columns = ['cat','percent']
    data = data.set_index('cat')
    return(data)

def loadclimate(fips,year,month):
    date = str(year)+'-'+str(month)
    geo  = ee.FeatureCollection("TIGER/2018/Counties")
    geo = geo.filter(ee.Filter.inList('STATEFP',list(map(lambda x:x[:2],fips))))
    geo = geo.filter(ee.Filter.inList('COUNTYFP',list(map(lambda x:x[2:],fips))))
#    geo = geo.filter(ee.Filter.eq('STATEFP',fips[:2]))
#    geo = geo.filter(ee.Filter.eq('COUNTYFP',fips[2:]))
    
    data = ee.ImageCollection('NASA/ORNL/DAYMET_V3')
    data = data.filter(ee.Filter.date(pd.to_datetime(date), pd.to_datetime(date)+pd.DateOffset(months=1)-datetime.timedelta(days=1)))
    data = data.select(['prcp','tmax','tmin'])
    data = data.mean()
    data = data.reduceRegion(
        reducer=ee.Reducer.mean(),
        geometry=geo,
        scale = 10**5)
    return(data)
#%%
corn = pd.read_csv('data/corn.csv')
corn['ANSI']=corn['ANSI'].apply(inttostr)

soy = pd.read_csv('data/soy.csv')
soy['ANSI']=soy['ANSI'].apply(inttostr)

wheat = pd.read_csv('data/wheat.csv')
wheat['ANSI']=wheat['ANSI'].apply(inttostr)
#%%
temp = pd.unique(corn['ANSI'].append(soy['ANSI']).append(wheat['ANSI']).values)
counties = ee.FeatureCollection("TIGER/2018/Counties")
counties = counties.filter(ee.Filter.inList('STATEFP',list(map(lambda x:x[:2],temp))))
counties = counties.filter(ee.Filter.inList('COUNTYFP',list(map(lambda x:x[2:],temp))))
counties = counties.getInfo()['features']
counties = pd.DataFrame(list(map(lambda x:x['properties'],counties)))
counties[['INTPTLON','INTPTLAT']]=counties[['INTPTLON','INTPTLAT']].applymap(float)
counties['fips']=counties['STATEFP']+counties['COUNTYFP']
#%%
soil = loadlith(temp[0])
soil.columns=[temp[0]]
for i,j in enumerate(temp[1:]):
    hold = loadlith(j)
    hold.columns=[j]
    soil = soil.join(hold)
    print(i/len(temp))
soil = soil.T.fillna(0)
#%%
hold = counties.set_index('fips')[['INTPTLAT','INTPTLON']].join(soil)#.dropna()
hold['Clust']=KMeans(100).fit(hold.values).labels_
hold.plot.scatter('INTPTLON','INTPTLAT',s=1,c='Clust')
soilclust = hold.reset_index()
soilclust.to_csv('data/soilclust.csv',index=None)
#%%
start = datetime.datetime.now()
lap = start
climate = {}
for clust in sorted(pd.unique(soilclust['Clust'])):
    climate1 = ee.Dictionary()
    for year in range(2000,2020):
        climate2 = ee.Dictionary()
        for month in range(1,13):
            fips = soilclust['fips'][soilclust['Clust']==clust].values
            fips= fips[:5]
            climate2=climate2.set(str(month),loadclimate(fips,year,month))
        climate1 = climate1.set(str(year),climate2)
    climate[clust] = climate1.getInfo()
    print(clust,datetime.datetime.now()-lap,datetime.datetime.now()-start)
    lap = datetime.datetime.now()
#%%
temp = {}
temp1= {}
for clust in climate.keys():
    temp[clust]=[]
    for year in climate[clust].keys():
        temp[clust].append(pd.DataFrame(climate[clust][year]).T)
        temp[clust][-1].index = list(map(lambda x: year+'-'+x,temp[clust][-1].index))
    temp1[clust]=temp[clust][0]
    for i in temp[0][1:]:
        temp1[clust]=temp1[clust].append(i)
    temp1[clust].index=pd.to_datetime(temp1[clust].index)
    temp1[clust]=temp1[clust].sort_index()
    
    temp1[clust]=temp1[clust].stack().reset_index()
    temp1[clust].columns=['Date','Measure',clust]
    temp1[clust]=temp1[clust].set_index(['Date','Measure',])
temp2 = list(temp1.values())[0]
for i in list(temp1.values())[1:]:
    temp2 = temp2.join(i)
temp2 = temp2.stack().reset_index()
temp2.columns=['Date','Measure','Clust','Val']
temp2 = temp2.set_index(['Date','Clust','Measure']).unstack()['Val']
temp2.reset_index().to_csv('data/climate.csv',index=None)
#%%
