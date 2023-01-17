from ncReadToMysql import readwriteNCWithPoolxArray 
from ncReadToMysql import addSiteGridList as addGrid
import numpy as np
import netCDF4 as nc
import pandas as pd
import mysql.connector
import json
from datetime import datetime,timedelta,time
import sys
import time
import concurrent.futures
import math
import shutil
import os
import xarray as xr
def runMainProcess(filePath,ncsStartHour,ncEndHour,fileDirectory):
    
    if __name__!="__main__": 
        return

    print("The Name of Written File >"+filePath)
    
    baslangicZamani=datetime.now()

    print("Read NC File")


    # f=nc.Dataset(filePath)

    # ftime  =  np.array(f.variables['Time'][:,:].data)
    # fu10 = np.array(f.variables['U10'][:,:,:].data)
    # fu10 = np.array(f.variables['U10'][:,:,:].data)
    # fu50 = np.array(f.variables['U50'][:,:,:].data)
    # fu100 = np.array(f.variables['U100'][:,:,:].data)
    # # fu200 = f.variables['U200'][:,:,:].data
    # fv10 = np.array(f.variables['V10'][:,:,:].data)
    # fv50   = np.array(f.variables['V50'][:,:,:].data)
    # fv100  = np.array(f.variables['V100'][:,:,:].data)
    # fv200  = np.array(f.variables['V200'][:,:,:].data)
    # ft2    = np.array(f.variables['T2'][:,:,:].data)
    # frh2   = np.array(f.variables['RH2'][:,:,:].data)
    # fpsfc  = np.array(f.variables['PSFC'][:,:,:].data)
    # fghi   = np.array(f.variables['GHI'][:,:,:].data)
    # fdiff  = np.array(f.variables['DIFF'][:,:,:].data)
    # faccprec=np.array(f.variables['AccPrec'][:,:,:].data)
    # fsnow  = np.array(f.variables['SNOW'][:,:,:].data)
    # ftime,fu10,fv10,fu50,fv50,fu100,fv100,ft2,fpsfc,frh2,fghi,fdiff,faccprec,fsnow
    # f.close()

    #model Id Belirle
    modelNo=1

    if "ICON" in fileDirectory:

        modelNo=2




    #Gerekli Bilgileri Al
    with open("config.json","r") as file:

        dbApiInfo=json.load(file)

    myDBConnect = mysql.connector.connect(host=dbApiInfo["dbInfo"]["dbAddress"], user = dbApiInfo["dbInfo"]["dbUsersName"], password=dbApiInfo["dbInfo"]["dbPassword"], database=dbApiInfo["dbInfo"]["database"])
    

    # select içinde modelno Göndermeyi unutma
    selectTxt="Select siteId,modelGridListId,meteoModelListId,xGrid,yGrid From siteGridList where meteoModelListId="+str(modelNo)

    cursor=myDBConnect.cursor()

    cursor.execute(selectTxt)
 
    siteGridTable = cursor.fetchall()
    
    siteGridTableDF=pd.DataFrame(siteGridTable,columns=["siteId","modelGridListId","meteoModelListId","xGrid","yGrid"])


    #site listesi
    selectTxt="Select id,siteTypeId From siteList where activation=1"

    cursor.execute(selectTxt)
 
    siteTable = cursor.fetchall()
    
    siteTableDF=pd.DataFrame(siteTable,columns=["siteId","siteTypeId"])


    selectTxt="Select yGrid,xGrid From siteGridList where meteoModelListId="+str(modelNo)+" group by  yGrid,xGrid"

    cursor.execute(selectTxt)
 
    xyList = cursor.fetchall()
    
    

    myDBConnect.close()


   


    # xyKontrol=""

    # for rowCount in range(0,siteGridTableDF.shape[0]):

    #     if xyKontrol.__contains__(str(siteGridTableDF.iloc[rowCount]["yGrid"])+"-"+str(siteGridTableDF.iloc[rowCount]["xGrid"]))==False:
    #         if xyKontrol=="":
    #             xyKontrol=str(siteGridTableDF.iloc[rowCount]["yGrid"])+"-"+str(siteGridTableDF.iloc[rowCount]["xGrid"])
    #         else:
    #             xyKontrol+="|"+str(siteGridTableDF.iloc[rowCount]["yGrid"])+"-"+str(siteGridTableDF.iloc[rowCount]["xGrid"])

    #         xyList.append([str(siteGridTableDF.iloc[rowCount]["yGrid"]),str(siteGridTableDF.iloc[rowCount]["xGrid"])])

            
          
        
    hourDiff=ncEndHour-ncsStartHour
        
    hourPeriod=math.ceil(hourDiff/4)

    # processExecutor=concurrent.futures.ProcessPoolExecutor(max_workers=2)

    tmpHourStartHour=ncsStartHour


    print("Başlıyoruz.....")

    
    futuresList=[]

    init=0
    
    if "_06" in filePath:
        init=6
    elif "_12" in filePath:
        init=12
    elif "_18" in filePath:
        init=18
    
    modelName="WRF_GFS"

    if "_ICON" in fileDirectory:

        modelName="WRF_ICON"

    ncXRDataset = xr.open_dataset(filePath)


   

    zaman =  [None] * 325

    zamanArr=[]

    for t in range(1,325):
        try:
            zaman[t]=datetime.strptime(str(ncXRDataset.variables['Time'][:][t].data).replace("'",'').replace("b",'').replace('_',' '),'%Y-%m-%d %H:%M:%S')
            if zaman[t].minute!=0:
                zaman[t]=pd.to_datetime(str(zaman[t].year)+"-"+str(zaman[t].month)+"-"+str(zaman[t].day)+" "+str(zaman[t].hour)+":00")+timedelta(hours=1)
            else:
                zaman[t]=pd.to_datetime(str(zaman[t].year)+"-"+str(zaman[t].month)+"-"+str(zaman[t].day)+" "+str(zaman[t].hour)+":00")
                zamanArr.append(zaman[t])
        except:
            return "NC File Zaman Sayısı 325 Değil"


    ncXRDataset["Time"] = ("Time", zaman)

    
    readwriteNCWithPoolxArray(ncXRDataset,zamanArr,xyList,siteTableDF,siteGridTableDF,modelNo,tmpHourStartHour,(tmpHourStartHour+hourPeriod),init,modelName)

    # while tmpHourStartHour<ncEndHour:

    #     # runNC_1(ftime,fu10,fv10,fu50,fv50,fu100,fv100,ft2,fpsfc,frh2,fghi,fdiff,faccprec,fsnow,xyList,siteTableDF,siteGridTableDF,modelNo,tmpHourStartHour,(tmpHourStartHour+hourPeriod),init,modelName)
    #     futuresList.append(processExecutor.submit(readwriteNCWithPoolxArray,filePath,xyList,siteTableDF,siteGridTableDF,modelNo,tmpHourStartHour,(tmpHourStartHour+hourPeriod),init,modelName))

    #     tmpHourStartHour+=hourPeriod

      
    # threadBitti=0

    # for future in concurrent.futures.as_completed(futuresList):

    #     future.result()
    #     print(str(threadBitti)+" Bitti")
    #     threadBitti+=1

      
    print("Process End")
    
    print((datetime.now()-baslangicZamani).total_seconds()/60)

    myDBConnect = mysql.connector.connect(host=dbApiInfo["dbInfo"]["dbAddress"], user = dbApiInfo["dbInfo"]["dbUsersName"], password=dbApiInfo["dbInfo"]["dbPassword"], database=dbApiInfo["dbInfo"]["database"])

    print("Log Yazılıyor")

    insertTXT="Insert Into modelFileWriteLog(fileName,modelName,writeStartTime,writeEndTime) VALUES('"+filePath+"','"+modelName+"','"+str(baslangicZamani)+"','"+str(datetime.now())+"')"

    cursor=myDBConnect.cursor()
    
    cursor.execute(insertTXT)
    
    myDBConnect.commit()

    myDBConnect.close()

    print("Log Yazıldı")

    os.remove(filePath)




# filePath="I:\\Belgeler\\Lazımlık\\Ozel\\modelWorks\\WRF_GFS\\wrfpost_2022-11-15_00.nc"


def fileCopy(destFile,targetFile,init,fileDateTime,filePath,targetPath):

    
    return 0
    resultCopy=-1

    try:
        if os.path.exists(destFile)==True:

            shutil.copyfile(destFile, targetFile)

            resultCopy=0

        else:

            oldDayFileName=fileDateTime-timedelta(days=1)
        
            tmpFileName="wrfpost_"+oldDayFileName.strftime("%Y-%m-%d")+"_18.nc"
            
            destFile=filePath+tmpFileName

            if os.path.exists(destFile)==True:

                targetFile=targetPath+tmpFileName
            
                shutil.copyfile(destFile,targetFile)

                resultCopy=18

    except :

        resultCopy=-1  


    return resultCopy
    



if __name__=="__main__": 

    


     #Gerekli Bilgileri Al
    with open("config.json","r") as file:

        dbApiInfo=json.load(file)

    myDBConnect = mysql.connector.connect(host=dbApiInfo["dbInfo"]["dbAddress"], user = dbApiInfo["dbInfo"]["dbUsersName"], password=dbApiInfo["dbInfo"]["dbPassword"], database=dbApiInfo["dbInfo"]["database"])
    


    
    

    # select içinde modelno Göndermeyi unutma

    selectTxt="Select fileName,modelName From modelFileWriteLog"

    cursor=myDBConnect.cursor()

    cursor.execute(selectTxt)
 
    fileTable = cursor.fetchall()
    
    fileTableDF=pd.DataFrame(fileTable,columns=["fileName","modelName"])

    # 
    
    fileBaslangicZaman=pd.to_datetime("2019-01-01")

    

    
    # if sys.argv[1]=='-2':
    #     modelDirectoryId="WRF_ICON"
    # elif sys.argv[1]=='-1':
    #     modelDirectoryId="WRF_GFS"

 
    modelDirectoryId="WRF_GFS"
        
    os.system("mount -t nfs 192.168.20.28:/qNAPN2_vLM2_iMEFsys /mnt/qNAPN2_vLM2_iMEFsys")
    fileDirectory="D:\\bigventusNC\\"+modelDirectoryId
    # fileDirectory="/mnt/qNAPN2_vLM2_iMEFsys/NCFiles/"+modelDirectoryId
    
    while (fileBaslangicZaman-datetime.now()).total_seconds()<0:
       
        for i in range(0,5):

            try:
                fileName="wrfpost_2022-11-30_00.nc"
                # fileName="wrfpost_"+datetime.strftime(fileBaslangicZaman,"%Y-%m-%d")+"_00.nc"

                tmpFileList=fileTableDF[(fileTableDF["fileName"]==fileName)&(fileTableDF["modelName"]==modelDirectoryId)]
        
                if tmpFileList.shape[0]<=0:
                    initTime=fileCopy(fileDirectory+"\\"+fileName,fileName,0,fileBaslangicZaman,fileDirectory+"\\","")

                    # initTime=fileCopy(fileDirectory+"/"+fileName,fileName,0,fileBaslangicZaman,fileDirectory+"/","")

                    if initTime!=-1:
                        
                        lastHour=55

                        if (((datetime.now()-fileBaslangicZaman).total_seconds()/60)/60)>48:

                            lastHour=25

                        print(">>"+fileDirectory+"/"+fileName)

                        runMainProcess(fileName,6,lastHour,fileDirectory)
                        
                        exit

                        fileBaslangicZaman=fileBaslangicZaman+timedelta(days=1)

                        
                        break  

                else:
                    
                    print(fileName+" > Yazılmış")

                    fileBaslangicZaman=fileBaslangicZaman+timedelta(days=1)

            except Exception as e:

                print("HATA:"+str(e))

                time.sleep(60)

                if i<4:

                    continue 

                else:

                    raise
            
               


        

