import netCDF4 as nc
import numpy as np
import pandas as pd
import mysql.connector 
from datetime import datetime, date, timedelta
import math
from os import listdir
from os.path import isfile,exists, join
from pymongo import MongoClient, UpdateMany
from pymongo.errors import BulkWriteError
import calcWindPower

print("NC okuyucu Başlangıç tarihi:", datetime.now())
mydb = mysql.connector.connect(
  host="localhost",
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

#mypath="V:/NCFiles/WRF_ICON/"
mypath="/mnt/qNAPN2_vLM2_iMEFsys/NCFiles/WRF_ICON/"
#mypath="NC_Files/"
def next_file(file,date1):
    sonraki= "wrfpost_" + date1.strftime('%Y-%m-%d_%H') 
    if exists(mypath + sonraki +".nc"):
        return sonraki+".nc"
    elif exists(mypath + sonraki[0:19]+ "18.nc" ):     
        return sonraki[0:19]+ "18.nc"
    elif exists(mypath + sonraki[0:19]+ "12.nc" ):    
          return sonraki[0:19]+ "12.nc"
    elif exists(mypath + sonraki[0:19]+ "06.nc" ):    
          return sonraki[0:19]+ "06.nc"  
    else:
        return ""
        print(sonraki[0:19], " dosyası mevcut değil")

def next_date(date1):
    sonraki= "wrfpost_" + date1.strftime('%Y-%m-%d_%H') 
    if exists(mypath + sonraki +".nc"):
        return date1 
    else:
        return ""

#wrfpost_2022-12-16_00.nc
# öncelik sırası 00 > 18 > 06 > 12
#sadece 00 initleri çek
ncfiles = sorted([f for f in listdir(mypath) if isfile(join(mypath, f)) and f[19:21] == '00'])


#ICON için kalıp gridleri getir
# sql = "select DISTINCT xGrid, yGrid, modelGridListId,siteId from musteriDB.siteMetModelGridList  where meteoModelListId=2"
sql ="select  m.xGrid, m.yGrid, m.modelGridListId,m.siteId,s.airDensity from musteriDB.siteMetModelGridList m,musteriDB.siteList s  where m.meteoModelListId=2 and m.siteId= s.Id"
mycursor.execute(sql)
site = mycursor.fetchall()
xGridList = []
yGridList = []
gridList = []
siteList=[]
airDensity=[]

for row in site:
    xGridList.append(row[0])
    yGridList.append(row[1])
    gridList.append(row[2])
    siteList.append(row[3])
    airDensity.append(row[4])


for n in range(len(ncfiles)):
    ncFile = ""
    
    if n == 0:
        ncFile = ncfiles[0]
        date_1 = datetime.strptime(ncfiles[n][8:21].replace('_', ' '), '%Y-%m-%d %H')
    else:
        date_1=date_1 + timedelta(days=1)
        ncFile = next_file(ncfiles[n],date_1)

    if ncFile == "" or ncFile == None: 
        print(ncfiles[n] + " dosyası mevcut değil")
        continue

    insert_time = date_1 + timedelta(days=1) + timedelta(hours=1)
    initTime = ncFile[8:21]

    print(ncFile + " dosyası eklendi")
    f = nc.Dataset(mypath + ncFile)
    # print(list(f.variables))


    fu10 = f.variables['U10']
    fu50 = f.variables['U50']
    fu100 = f.variables['U100']
    fu200 = f.variables['U200']
    fv10 = f.variables['V10']
    fv50   = f.variables['V50']
    fv100  = f.variables['V100']
    fv200  = f.variables['V200']
    ft2    = f.variables['T2']
    frh2   = f.variables['RH2']
    fpsfc  = f.variables['PSFC']
    fghi   = f.variables['GHI']
    fdiff  = f.variables['DIFF']
    ftime  =  f.variables['Time']
    faccprec=f.variables['AccPrec']
    fsnow  = f.variables['SNOW']
    flat  = f.variables['XLAT']
    flon  = f.variables['XLONG']
    zaman =  [None] * ft2.shape[0]
    zamanUniq =  [None] * ft2.shape[0]
    for t in range(ft2.shape[0]):
        zaman[t]= "".join(map(str,f.variables['Time'][:][t])).replace("'",'').replace("b",'').replace('_','T')
        zamanUniq[t]=zaman[t][0:13]
        #'2022-11-15T00:00:00'
    x = np.array(zamanUniq)
    zamanHours= np.unique(x)

    
    for i in range(len(gridList)):
        mycol = mydb2["site_" + str(siteList[i])] #collection
        print(mycol)
        u10=0
        u50=0 
        u100=0
        u200=0
        v10=0 
        v50=0 
        v100=0 
        v200=0 
        tt2=0 
        rh2=0 
        psfc=0 
        ghi=0 
        diff=0 
        accprec=0 
        snow=0 
        say = 0 #325/6 = 54.1
        saat=0
        valList = []
        requests = []
        for t in range(len(zaman)):
           
           if datetime.strptime(zaman[t].replace('T',' ')[0:16], '%Y-%m-%d %H:%M')  <= insert_time :  
            
             if t>0: 
                if  zaman[t][14:16] == "00" or zaman[t][0:13] == zamanHours[saat]:
                    u10 = u10+ fu10[t,yGridList[i],xGridList[i]]
                    u50 = u50+ fu50[t,yGridList[i],xGridList[i]]
                    u100 = u100+ fu100[t,yGridList[i],xGridList[i]]
                    u200 = u200+ fu200[t,yGridList[i],xGridList[i]]
                    v10 = v10+ fv10[t,yGridList[i],xGridList[i]]
                    v50 = v50+ fv50[t,yGridList[i],xGridList[i]]
                    v100 = v100+ fv100[t,yGridList[i],xGridList[i]]
                    v200 = v200+ fv200[t,yGridList[i],xGridList[i]]
                    tt2 = tt2+ ft2[t,yGridList[i],xGridList[i]]
                    rh2 = rh2+ frh2[t,yGridList[i],xGridList[i]]
                    psfc = psfc+ fpsfc[t,yGridList[i],xGridList[i]]
                    ghi = ghi+ fghi[t,yGridList[i],xGridList[i]]
                    diff = diff+ fdiff[t,yGridList[i],xGridList[i]]
                    accprec = accprec+ faccprec[t,yGridList[i],xGridList[i]]
                    snow = snow+ fsnow[t,yGridList[i],xGridList[i]]
                    say=say+1
                else:
                    
                    u10=round(u10/say,4)
                    u50=round(u50/say,4)
                    u100=round(u100/say,4)
                    u200=round(u200/say,4)
                    v10=round(v10/say,4)
                    v50=round(v50/say,4)
                    v100=round(v100/say,4)
                    v200=round(v200/say,4)
                    tt2=round((tt2/say)- 273.15 ,4) # convert to celsius
                    rh2=round(rh2/say,4)
                    psfc=round(psfc/say,4)
                    ghi=round(ghi/say,4)
                    diff=round(diff/say,4)
                    accprec=round(accprec/say,4)
                    snow=round(snow/say,4)
                    ws10,wd10 = wind_convert(u10,v10)
                    ws50,wd50 = wind_convert(u50,v50)
                    ws100,wd100 = wind_convert(u100,v100)
                    ws200,wd200 = wind_convert(u200,v200)
                    saat=saat+1         
                    ncreaderRunListId=1
                    modelId=2
                    f_ws10= calcWindPower.windPower(siteList[i],airDensity[i],ws10,modelId)
                    f_ws50= calcWindPower.windPower(siteList[i],airDensity[i],ws10,modelId)
                    f_ws100= calcWindPower.windPower(siteList[i],airDensity[i],ws10,modelId)
                    lat =flat[t,yGridList[i],xGridList[i]]
                    lon =flon[t,yGridList[i],xGridList[i]]
                    #val = (zaman[t],initTime,xGridList[i], yGridList[i],ncreaderRunListId, float(u10),float(u50),float(u100),float(u200),float(v10),float(v50),float(v100),float(v200),float(tt2),float(rh2),float(psfc),float(diff),float(accprec),float(snow), ws10,wd10, ws50,wd50, ws100,wd100, ws200,wd200)

                    val = {"WS10_" + str(gridList[i]) + "_" + str(modelId) : ws10, "WD10_" + str(gridList[i]) + "_" + str(modelId) : wd10,
                           "WS50_" + str(gridList[i]) + "_" + str(modelId) : ws50, "WD50_" + str(gridList[i]) + "_" + str(modelId) : wd50, "WS100_" + str(gridList[i]) + "_" + str(modelId) : ws100, 
                           "WD100_" + str(gridList[i]) + "_" + str(modelId) : wd100, "WS200_" + str(gridList[i]) + "_" + str(modelId) : ws200, "WD200_" + str(gridList[i]) + "_" + str(modelId) : wd200,   
                           "PSFC_" + str(gridList[i]) + "_" + str(modelId) : psfc, "T_" + str(gridList[i]) + "_" + str(modelId) : tt2, "f_WS10_" + str(gridList[i]) + "_" + str(modelId) : f_ws10,
                           "f_WS50_" + str(gridList[i]) + "_" + str(modelId) : f_ws50, "f_WS100_" + str(gridList[i]) + "_" + str(modelId) : f_ws100   }
                   # mycol.insert_many(valList)          
                    #mycol.update_many({"timeStamp":zamanHours[saat]}, {'$set':val}, upsert=True)
                    requests.append(UpdateMany({'timeStamp': zamanHours[saat]}, {'$set': val}, upsert=True))


                    
                    #valList.append(val)
                    u10=0
                    u50=0 
                    u100=0
                    u200=0
                    v10=0 
                    v50=0 
                    v100=0 
                    v200=0 
                    tt2=0 
                    rh2=0 
                    psfc=0 
                    ghi=0 
                    diff=0 
                    accprec=0 
                    snow=0 
                    say=0 
        try:
            mycol.bulk_write(requests) 
            print(requests)
        except BulkWriteError as bwe:
            print(bwe.details)            
                   
    print(ncFile + " dosyası eklendi -- " + str(datetime.now()))
print("NC okundu Bitiş tarihi:", datetime.now())
