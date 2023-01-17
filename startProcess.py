from ncReadToMysql import readwriteNCWithPoolxArray 
from ncReadToMysql import addSiteGridList as addGrid
import ncReadToMysql 
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

threadCount=0






# filePath="I:\\Belgeler\\Lazımlık\\Ozel\\modelWorks\\WRF_GFS\\wrfpost_2022-11-15_00.nc"


def fileCopy(destFile,targetFile,init,fileDateTime,filePath,targetPath):

    
  
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
def futureAnswer(future):

    global threadCount

    threadCount-=1

    print(str(future.result())+"/"+str(threadCount))  




if __name__=="__main__": 

    


     #Gerekli Bilgileri Al
    with open("config.json","r") as file:

        dbApiInfo=json.load(file)

    myDBConnect = mysql.connector.connect(host=dbApiInfo["dbInfo"]["dbAddress"], user = dbApiInfo["dbInfo"]["dbUsersName"], password=dbApiInfo["dbInfo"]["dbPassword"], database=dbApiInfo["dbInfo"]["database"])
    
    cursor=myDBConnect.cursor()

    selectTxt="Select fileName,modelName From modelFileWriteLog"

    cursor.execute(selectTxt)
 
    fileTable = cursor.fetchall()
    
    fileTableDF=pd.DataFrame(fileTable,columns=["fileName","modelName"])
    
    
    fileBaslangicZaman=pd.to_datetime("2022-11-15")

    modelDirectoryId="WRF_GFS"
    # if sys.argv[1]=='-2':
    #     modelDirectoryId="WRF_ICON"
    # elif sys.argv[1]=='-1':
    #     modelDirectoryId="WRF_GFS"

 
    modelDirectoryId="WRF_GFS"
        
    os.system("mount -t nfs 192.168.20.28:/qNAPN2_vLM2_iMEFsys /mnt/qNAPN2_vLM2_iMEFsys")
    fileDirectory="D:\\bigventusNC\\"+modelDirectoryId
    
    fileDirectory="I:\\Belgeler\\Lazımlık\\Ozel\\modelWorks\\"+modelDirectoryId
    # fileDirectory="/mnt/qNAPN2_vLM2_iMEFsys/NCFiles/"+modelDirectoryId
    
   

 
    kopyaExecutor=concurrent.futures.ThreadPoolExecutor(max_workers=1)

    while (fileBaslangicZaman-datetime.now()).total_seconds()<0:
       
        for i in range(0,5):

            try:
             

                fileName="wrfpost_"+datetime.strftime(fileBaslangicZaman,"%Y-%m-%d")+"_00.nc"

                tmpFileList=fileTableDF[(fileTableDF["fileName"]==fileName)&(fileTableDF["modelName"]==modelDirectoryId)]
        
                if tmpFileList.shape[0]<=0:

                    ftrList=[]

                    print(fileName+" > Kopyalanıyor")

                    initTime=-9

                    ftrList.append(kopyaExecutor.submit(fileCopy,fileDirectory+"\\"+fileName,fileName,0,fileBaslangicZaman,fileDirectory+"\\",""))
                                      

                  
                    for future in concurrent.futures.as_completed(ftrList):
                    
                        
                        initTime=future.result()

    #                   future.result()
    #                   print(str(threadBitti)+" Bitti")
    #                    threadBitti+=1

                  
                   
                    # initTime=fileCopy(fileDirectory+"/"+fileName,fileName,0,fileBaslangicZaman,fileDirectory+"/","")

                    if initTime!=-1:

                        print(fileName+" > Kopyalandı")

                        lastHour=55

                        if (((datetime.now()-fileBaslangicZaman).total_seconds()/60)/60)>48:

                            lastHour=25

                        ncXRDataset = xr.open_dataset(fileName)


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
                                hata=""

           

 
                        ncXRDataset["Time"] = ("Time", zaman)

                      #  ncReadToMysql.futureList.append(ncReadToMysql.processExecutorNC.submit(ncReadToMysql.runMainProcess,ncXRDataset,zamanArr,fileName,6,lastHour,fileDirectory))
                        
                        ncReadToMysql.runMainProcess(ncXRDataset,zamanArr,fileName,6,lastHour,fileDirectory)

                        

                        # ncReadToMysql.threadCount+=1
                        
                        # ncReadToMysql.futureList[len(ncReadToMysql.futureList)-1].add_done_callback(ncReadToMysql.futureAnswer)

                        # time.sleep(0.2)


                    fileBaslangicZaman=fileBaslangicZaman+timedelta(days=1)

                        
                       

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

    kopyaExecutor.shutdown() 

    concurrent.futures.wait(ncReadToMysql.futureList)

    


    t="bitti"

    ncReadToMysql.processExecutorNC.shutdown(wait=True,cancel_futures=False)
    
    print("İşlemler bitti")


        

