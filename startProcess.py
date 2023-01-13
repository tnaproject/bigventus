from ncReadToMysql import runNCWriter as runNC_1
import numpy as np
import netCDF4 as nc
import pandas as pd
import mysql.connector
import json
from multiprocessing import Process
from datetime import datetime
import sys
import time

if __name__=="__main__":
    
    baslangicZamani=datetime.now()

    print("Read NC File")

    # filePath="I:\\Belgeler\\Lazımlık\\Ozel\\modelWorks\\WRF_GFS\\wrfpost_2022-11-15_00.nc"

    filePath="/mnt/qNAPN2_vLM2_iMEFsys/NCFiles/WRF_GFS/wrfpost_2022-12-17_00.nc"
    f=nc.Dataset(filePath)

    fu10 = np.array(f.variables['U10'][:,:,:].data)
    fu10 = np.array(f.variables['U10'][:,:,:].data)
    fu50 = np.array(f.variables['U50'][:,:,:].data)
    fu100 = np.array(f.variables['U100'][:,:,:].data)
    fu200 = f.variables['U200'][:,:,:].data
    fv10 = np.array(f.variables['V10'][:,:,:].data)
    fv50   = np.array(f.variables['V50'][:,:,:].data)
    fv100  = np.array(f.variables['V100'][:,:,:].data)
    # fv200  = np.array(f.variables['V200'][:,:,:].data)
    ft2    = np.array(f.variables['T2'][:,:,:].data)
    frh2   = f.variables['RH2'][:,:,:].data
    fpsfc  = np.array(f.variables['PSFC'][:,:,:].data)
        # fghi   = f.variables['GHI'][:,:,:].data
        # fdiff  = f.variables['DIFF'][:,:,:].data
    ftime  =  np.array(f.variables['Time'][:,:].data)
        # faccprec=f.variables['AccPrec'][:,:,:].data
        # fsnow  = f.variables['SNOW'][:,:,:].data

    #ftime,fu10,fv10,fu50,fv50,fu100,fv100,ft2,fpsfc
    
    f.close()

    with open("config.json","r") as file:

        dbApiInfo=json.load(file)

    myDBConnect = mysql.connector.connect(host=dbApiInfo["dbInfo"]["dbAddress"], user = dbApiInfo["dbInfo"]["dbUsersName"], password=dbApiInfo["dbInfo"]["dbPassword"], database=dbApiInfo["dbInfo"]["database"])

    selectTxt="Select * From siteList where epiasEIC > 0"

    cursor=myDBConnect.cursor()
            
    cursor.execute(selectTxt)
    
    siteTable = cursor.fetchall()
   
    # select içinde modelno Göndermeyi unutma
    selectTxt="Select siteId,modelGridListId,meteoModelListId,xGrid,yGrid From siteGridList where meteoModelListId=1"

    cursor.execute(selectTxt)
 
    siteGridTable = cursor.fetchall()
    
    siteGridTableDF=pd.DataFrame(siteGridTable,columns=["siteId","modelGridListId","meteoModelListId","xGrid","yGrid"])

    startHour=abs(int(sys.argv[1]))

    endHour=abs(int(sys.argv[2]))

    # startHour=10

    # endHour=30
    if __name__=="__main__":


        with open("config.json","r") as file:

            dbApiInfo=json.load(file)

        runNC_1(ftime,fu10,fv10,fu50,fv50,fu100,fv100,ft2,fpsfc,1,siteGridTableDF,startHour,endHour)


      
        print("Process End")
    
        print((datetime.now()-baslangicZamani).total_seconds()/60)
