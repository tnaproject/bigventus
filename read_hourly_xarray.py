import netCDF4 as nc
import numpy as np
import pandas as pd
import mysql.connector 
from datetime import datetime
import xarray as xr
from pymongo import MongoClient, UpdateMany
from pymongo.errors import BulkWriteError
#import calcWindPower
import math
from os import listdir
from os.path import isfile,exists, join
from datetime import datetime, date, timedelta

print("NC okuyucu Başlangıç tarihi:", datetime.now())
mydb = mysql.connector.connect(
  host="89.252.157.127",
  user="dbventus",
  password="z9nY3@f80YBk",
  database="musteriDB"
)
mycursor = mydb.cursor()

#server
myclient = MongoClient() 
     
# connecting with the portnumber and host 
myclient = MongoClient("mongodb://localhost:27017/")
mydb2 = myclient["bigventus"] #db


def wind_convert(u,v):
    conv = 45 / math.atan(1)
    wind_dir = (270 - math.atan2(v,u) * conv) % 360
    wind_speed = math.sqrt(math.pow(u,2) + math.pow(v,2))
    #wind_dir = 180 + (180 / math.pi) * math.atan2(v,u)
    return round(wind_speed,4),round(wind_dir,4)

t=""

if t==""  :

    mds_temp = xr.open_dataset("wrfpost_2022-11-30_00.nc")


    ft2    = mds_temp['T2'][:].data
    zaman =  [None] * ft2.shape[0]
    zamanArr=[]
    for t in range(1,ft2.shape[0]):
        zaman[t]=datetime.strptime(str(mds_temp.variables['Time'][:][t].data).replace("'",'').replace("b",'').replace('_',' '),'%Y-%m-%d %H:%M:%S')
        if zaman[t].minute!=0:
            zaman[t]=pd.to_datetime(str(zaman[t].year)+"-"+str(zaman[t].month)+"-"+str(zaman[t].day)+" "+str(zaman[t].hour)+":00")+timedelta(hours=1)
        else:
            zaman[t]=pd.to_datetime(str(zaman[t].year)+"-"+str(zaman[t].month)+"-"+str(zaman[t].day)+" "+str(zaman[t].hour)+":00")
            zamanArr.append(zaman[t])




    mds_temp["Time"] = ("Time", zaman)
    tmpTest=mds_temp.where(mds_temp["Time"]==pd.to_datetime("2022-11-30 02:00"), drop=True)
    ty=tmpTest[:,192,191]
    hourlyAvgm=mds_temp.where(mds_temp["Time"]==pd.to_datetime("2022-11-30 02:00"), drop=True)["T2"].data
    hourlyAvgz=mds_temp.where(mds_temp["Time"]==pd.to_datetime("2022-11-30 03:00"), drop=True)["RH2"].data
   
    print(hourlyAvgz["Time"])
    print("---")

    k=hourlyAvgm["T2"][:,0,0].data

    k=hourlyAvgz["T2"][:,192,195].data

    hourlyAvg= mds_temp.resample(Time='1H').mean()
    hourlySum= mds_temp.resample(Time='1H').sum()
    #daily_mean = mds_temp.resample(Time='D').mean()
    # hourlySum['T2'].values[0,0,0]
    fTime= np.array(hourlyAvg['Time'].values)
    fu10,fv10 = np.array(hourlyAvg['U10'].values), np.array(hourlyAvg['V10'].values)
    fu50,fv50 = np.array(hourlyAvg['U50'].values), np.array(hourlyAvg['V50'].values)
    fu100,fv100 = np.array(hourlyAvg['U100'].values), np.array(hourlyAvg['V100'].values)
    ft2, fpsfc = np.array(hourlyAvg['T2'].values),np.array(hourlyAvg['PSFC'].values)
    insert_time = date_1 + timedelta(days=1) + timedelta(hours=1)
    for i in range(len(gridList)):
        mycol = mydb2["site_" + str(siteList[i])] #collection
        requests = []
        for t in range(len(fTime[0:23])): #zaman
 
            u10, v10 =round(fu10[t,yGridList[i],xGridList[i]],4), round(fv10[t,yGridList[i],xGridList[i]],4)
            u50, v50 =round(fu50[t,yGridList[i],xGridList[i]],4), round(fv50[t,yGridList[i],xGridList[i]],4)
            u100, v100=round(fu100[t,yGridList[i],xGridList[i]],4), round(fv100[t,yGridList[i],xGridList[i]],4)
            t2, psfc=round((ft2[t,yGridList[i],xGridList[i]]- 273.15),4), round(fpsfc[t,yGridList[i],xGridList[i]],4)
            ws10,wd10 = wind_convert(u10,v10)
            ws50,wd50 = wind_convert(u50,v50)
            ws100,wd100 = wind_convert(u100,v100)
            modelId=2
            #f_ws10= calcWindPower.windPower(siteList[i],airDensity[i],ws10)
            # f_ws50= calcWindPower.windPower(siteList[i],airDensity[i],ws10,modelId)
            # f_ws100= calcWindPower.windPower(siteList[i],airDensity[i],ws10,modelId)
            # f_ws=calcWindPower.windPowerList(siteList[i],airDensity[i],[ws10,ws50,ws100])
            val = {"WS10_" + str(gridList[i]) + "_" + str(modelId) :float(ws10), "WD10_" + str(gridList[i]) + "_" + str(modelId) : float(wd10),
                    "WS50_" + str(gridList[i]) + "_" + str(modelId) : float(ws50), "WD50_" + str(gridList[i]) + "_" + str(modelId) :float(wd50), "WS100_" + str(gridList[i]) + "_" + str(modelId) : float(ws100), 
                     "WD100_" + str(gridList[i]) + "_" + str(modelId) : float(wd100),   "PSFC_" + str(gridList[i]) + "_" + str(modelId) : float(psfc), "T_" + str(gridList[i]) + "_" + str(modelId) : float(t2)}
                    # "f_WS10_" + str(gridList[i]) + "_" + str(modelId) : float(f_ws[0]),  "f_WS50_" + str(gridList[i]) + "_" + str(modelId) : float(f_ws[1]), "f_WS100_" + str(gridList[i]) + "_" + str(modelId) : float(f_ws[2])   }
            requests.append(UpdateMany({'timeStamp': str(fTime[t])}, {'$set': val}, upsert=True))    
            # mycol.insert_one(val)
        mycol.bulk_write(requests)       
    print(ncFile + " dosyası eklendi -- " + str(datetime.now()))
print("NC okundu Bitiş tarihi:", datetime.now())    

