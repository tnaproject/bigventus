import numpy as np
import pandas as pd
import mysql.connector 
from datetime import datetime,timedelta,time
# import calcWindPower
# import calcSolarPower
import math
import json
from pymongo import MongoClient,UpdateMany
import concurrent.futures
import warnings
import xarray as xr
import os

threadCount=0



warnings.filterwarnings("ignore", category=DeprecationWarning)


def runMainProcess(xrDataSet,zamanArr,filePath,ncsStartHour,ncEndHour,fileDirectory):
  

    baslangicZamani=datetime.now()

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
   
    hourDiff=ncEndHour-ncsStartHour
        
    hourPeriod=math.ceil(hourDiff/4)

    tmpHourStartHour=ncsStartHour

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


    print("Read NC File>"+filePath)


    while tmpHourStartHour<ncEndHour:

        readwriteNCWithPoolxArray(xrDataSet,zamanArr,xyList,siteTableDF,siteGridTableDF,modelNo,tmpHourStartHour,(tmpHourStartHour+hourPeriod),init,modelName,dbApiInfo,filePath)

        tmpHourStartHour+=hourPeriod
        
    return  ""



   


def addColumnToTable(tableName,columnDescTxt,dbApiInfo):

    try:

        
        myDBConnect = mysql.connector.connect(host=dbApiInfo["dbInfo"]["dbAddress"], user = dbApiInfo["dbInfo"]["dbUsersName"], password=dbApiInfo["dbInfo"]["dbPassword"], database=dbApiInfo["dbInfo"]["database"])

        if getTableCount(tableName)>0:
            
            insertTXT="ALTER TABLE "+tableName+" ADD COLUMN "+columnDescTxt

            cursor=myDBConnect.cursor()

            cursor.execute(insertTXT)

            myDBConnect.commit()

        

            return True

    except:

        return False

def getColumnCountByTable(tableName,columnName,dbApiInfo):

    
    selectTXT="SELECT count(*) FROM information_schema.COLUMNS  WHERE  TABLE_NAME = '"+tableName+"' AND COLUMN_NAME = '"+columnName+"'"
    
    try:
   


        myDBConnect = mysql.connector.connect(host=dbApiInfo["dbInfo"]["dbAddress"], user = dbApiInfo["dbInfo"]["dbUsersName"], password=dbApiInfo["dbInfo"]["dbPassword"], database=dbApiInfo["dbInfo"]["database"])
        
        cursor=myDBConnect.cursor()
        
        cursor.execute(selectTXT)
    
        siteCountColumn = cursor.fetchall()

        myDBConnect.commit()
        
        myDBConnect.close()

        return siteCountColumn[0][0]
        
    except:

        print("Tablo Sayısı Kontrol Edilirken Hata Oluştu")   

def getTableCount(tableName,dbApiInfo):

  

    selectTXT="SELECT count(*)   FROM  information_schema.TABLES  WHERE  TABLE_NAME = '"+tableName+"'" 

    try:


        myDBConnect = mysql.connector.connect(host=dbApiInfo["dbInfo"]["dbAddress"], user = dbApiInfo["dbInfo"]["dbUsersName"], password=dbApiInfo["dbInfo"]["dbPassword"], database=dbApiInfo["dbInfo"]["database"])

        cursor=myDBConnect.cursor()
        
        cursor.execute(selectTXT)
    
        siteCountTable = cursor.fetchall()

        myDBConnect.close()

        return siteCountTable[0][0]
        

    except:

        print("Tablo Sayısı Kontrol Edilirken Hata Oluştu")


def wind_convert(u,v):

    wind_dir=np.zeros(len(u))

    wind_speed=np.zeros(len(u))

    for i in range(0,len(u)):
        conv = 45 / math.atan(1)
        wind_dir[i] = (270 - math.atan2(v[i],u[i]) * conv) % 360
        wind_speed[i] = math.sqrt(math.pow(u[i],2) + math.pow(v[i],2))
    #wind_dir = 180 + (180 / math.pi) * math.atan2(v,u)
    
    return wind_speed,wind_dir

def meanWindDirection(windDirections):
    
    V_east=np.zeros(len(windDirections))

    V_north=np.zeros(len(windDirections))

    for i in range(0,len(windDirections)):

        V_east[i] = np.mean( math.sin(windDirections[i] * math.pi/180))

        V_north[i] = np.mean(math.cos(windDirections[i] * math.pi/180))

    mean_WD = math.atan2(np.mean(V_east),np.mean(V_north)) * 180/math.pi

    mean_WD = (360 + mean_WD) % 360

    return mean_WD

def readNCAndWriteToMongo(fTimeArr,fu10,fv10,fu50,fv50,fu100,fv100,ft2,fpsfc,frh2,fghi,fdiff,faccprec,fsnow,xyList,siteTableDF,site,siteGridList,modelNo,startHour,init,modelName):
    
    siteDictList={}
    siteTimeList={}

    siteKontrolText=""

    startTime=pd.to_datetime(timeTotxt(fTimeArr[0]))+timedelta(hours=3)

    baslamaZamani=datetime.now()
    
    print("Start Hour:"+str(startHour)+">"+str(startTime)+" Loading")
                
    for xyGridNo in range(0,len(xyList)):
        
        yGrid=int(xyList[xyGridNo][0])

        xGrid=int(xyList[xyGridNo][1])

        tmpSiteGridList=siteGridList[(siteGridList["xGrid"]==xGrid) & (siteGridList["yGrid"]==yGrid)]
        

        if tmpSiteGridList.shape[0]>0:

                    
            ws10=[]
            ws50=[]
            ws100=[]
            wd10=[]
            wd50=[]
            wd100=[]
            t2=[]
            psfc=[]

            rh2=[]
            ghi=[]
            diff=[]
            accprec=[]
            snow=[]

        
        
            modelGridNo=tmpSiteGridList.iloc[0]["modelGridListId"]

        

            siteGridValueDict={}


            tableName="trainTable_"+str(tmpSiteGridList.iloc[0]["siteId"])
            
            modelGridNo=tmpSiteGridList.iloc[0]["modelGridListId"]

            siteInfoTable=siteTableDF[siteTableDF["siteId"]==tmpSiteGridList.iloc[0]["siteId"]]

            for siteSay in range(0,siteInfoTable.shape[0]):
            
        
                for timeNo in range(0,fTimeArr.shape[0]):
      
            
                    if pd.to_datetime(timeTotxt(fTimeArr[timeNo])).minute==0:

                        dataTime=pd.to_datetime(timeTotxt(fTimeArr[timeNo]))+timedelta(hours=3)

                  
                        if (siteInfoTable.iloc[siteSay]["siteTypeId"]==1):
       
                            u=[] 

                            v=[]

                            u.append(fu10[timeNo,yGrid,xGrid])
                                
                            v.append(fv10[timeNo,yGrid,xGrid])
                              
                            u.append(fu50[timeNo,yGrid,xGrid])
                                
                            v.append(fv50[timeNo,yGrid,xGrid])

                            u.append(fu100[timeNo,yGrid,xGrid])
                                
                            v.append(fv100[timeNo,yGrid,xGrid])

                            ws,wd=wind_convert(u,v)

                            ws10.append(ws[0])

                            ws50.append(ws[1])

                            ws100.append(ws[2])

                            wd10.append(wd[0])
            
                            wd50.append(wd[1])

                            wd100.append(wd[2])

                            t2.append(float(ft2[timeNo,yGrid,xGrid]))
                                
                            psfc.append(float(fpsfc[timeNo,yGrid,xGrid]))
       
                            tAvg=np.mean(t2)

                            tMax=np.max(t2)

                            psfcAvg=np.mean(psfc)

        
                            siteGridValueDict["WS10_"+str(modelGridNo)+"_"+str(modelNo)]=np.round(np.mean(ws10) ,2)
                            siteGridValueDict["WS50_"+str(modelGridNo)+"_"+str(modelNo)]=np.round(np.mean(ws50),2)
                            siteGridValueDict["WS100_"+str(modelGridNo)+"_"+str(modelNo)]=np.round(np.mean(ws100),2)

                            siteGridValueDict["WD10_"+str(modelGridNo)+"_"+str(modelNo)]=np.round(meanWindDirection(wd10),2)
                            siteGridValueDict["WD50_"+str(modelGridNo)+"_"+str(modelNo)]=np.round(meanWindDirection(wd50),2)
                            siteGridValueDict["WD100_"+str(modelGridNo)+"_"+str(modelNo)]=np.round(meanWindDirection(wd100),2)
        
                            siteGridValueDict["AVG_T2_"+str(modelGridNo)+"_"+str(modelNo)]=np.round(tAvg,2)
                            siteGridValueDict["MAX_T2"+str(modelGridNo)+"_"+str(modelNo)]=np.round(tMax,2)
                            siteGridValueDict["AVG_PSFC_"+str(modelGridNo)+"_"+str(modelNo)]=np.round(psfcAvg,2)

                            siteGridValueDict["dataTime"]=pd.to_datetime(dataTime)

                            siteGridValueDict["init"]=init
                            
                            siteGridValueDict["f_WS10_"+str(modelGridNo)+"_"+str(modelNo)]=-8888.0
                            siteGridValueDict["f_WS50_"+str(modelGridNo)+"_"+str(modelNo)]=-8888.0
                            siteGridValueDict["f_WS100_"+str(modelGridNo)+"_"+str(modelNo)]=-8888.00


                        else:

                            u=[] 

                            v=[]

                            u.append(fu10[timeNo,yGrid,xGrid])
                                
                            v.append(fv10[timeNo,yGrid,xGrid])
                              
                            ws,wd=wind_convert(u,v)

                            ws10.append(ws[0])

                            wd10.append(wd[0])

                            t2.append(float(ft2[timeNo,yGrid,xGrid]))

                            rh2.append(float(frh2[timeNo,yGrid,xGrid]))

                            ghi.append(float(fghi[timeNo,yGrid,xGrid]))

                            diff.append(float(fdiff[timeNo,yGrid,xGrid]))

                            accprec.append(float(faccprec[timeNo,yGrid,xGrid]))

                            snow.append(float(fsnow[timeNo,yGrid,xGrid]))

                            accprec.append(float(faccprec[timeNo,yGrid,xGrid]))

                            siteGridValueDict["SUM_SNOW_"+str(modelGridNo)+"_"+str(modelNo)]=np.round(np.sum(snow),2)

                            siteGridValueDict["SUM_ACCPRE_"+str(modelGridNo)+"_"+str(modelNo)]=np.round(np.sum(accprec),2)

                            siteGridValueDict["MAX_ACCPRE_"+str(modelGridNo)+"_"+str(modelNo)]=np.round(np.max(accprec),2)
 
                            siteGridValueDict["MAX_GHI_"+str(modelGridNo)+"_"+str(modelNo)]=np.round(np.max(ghi),2)

                            siteGridValueDict["AVG_GHI_"+str(modelGridNo)+"_"+str(modelNo)]=np.round(np.mean(ghi),2)

                            siteGridValueDict["WS10_"+str(modelGridNo)+"_"+str(modelNo)]=np.round(np.mean(ws10),2)

                            siteGridValueDict["WD10_"+str(modelGridNo)+"_"+str(modelNo)]=np.round(meanWindDirection(wd10),2)

                            siteGridValueDict["AVG_T2_"+str(modelGridNo)+"_"+str(modelNo)]=np.round(np.mean(t2),2)

                            siteGridValueDict["MAX_T2"+str(modelGridNo)+"_"+str(modelNo)]=np.round(np.max(t2),2)

                            siteGridValueDict["AVG_RH_"+str(modelGridNo)+"_"+str(modelNo)]=np.round(np.mean(rh2),2)

                            siteGridValueDict["dataTime"]=pd.to_datetime(dataTime)

                            siteGridValueDict["init"]=init

                            siteGridValueDict["f_GHI_"+str(modelGridNo)+"_"+str(modelNo)]=-8888.0

                        
                        tableName="trainTable_"+str(siteInfoTable.iloc[siteSay]["siteId"])


                        if siteKontrolText.__contains__(tableName+"|")==False:
                            if siteKontrolText=="":

                                siteKontrolText=tableName+"|"

                            else:

                                siteKontrolText+=tableName+"|"

                            siteDictList[tableName]=[]
                            siteTimeList[tableName]=[]

                        siteGridValueDict["f_WS10_"+str(modelGridNo)+"_"+str(modelNo)]=-8888.0
                        siteGridValueDict["f_WS50_"+str(modelGridNo)+"_"+str(modelNo)]=-8888.0
                        siteGridValueDict["f_WS100_"+str(modelGridNo)+"_"+str(modelNo)]=-8888.00

                        newvalues = { "$set": siteGridValueDict }

                        myquery = {"dataTime": dataTime}

                        siteDictList[tableName].append(UpdateMany(myquery,newvalues,upsert=True))

                        if modelName=="GFS":
                            siteTimeList[tableName]={"siteTable":tableName,"lastDataTime":dataTime,"GFS_INIT":init}
                        else:
                            siteTimeList[tableName]={"siteTable":tableName,"lastDataTime":dataTime,"ICON_INIT":init}

                    #site type sonu

                    else:

                        if (siteInfoTable.iloc[siteSay]["siteTypeId"]==1):
       
                            u=[] 

                            v=[]

                            u.append(fu10[timeNo,yGrid,xGrid])
                                
                            v.append(fv10[timeNo,yGrid,xGrid])
                              
                            u.append(fu50[timeNo,yGrid,xGrid])
                                
                            v.append(fv50[timeNo,yGrid,xGrid])

                            u.append(fu100[timeNo,yGrid,xGrid])
                                
                            v.append(fv100[timeNo,yGrid,xGrid])

                            ws,wd=wind_convert(u,v)

                            ws10.append(ws[0])

                            ws50.append(ws[1])

                            ws100.append(ws[2])

                            wd10.append(wd[0])
            
                            wd50.append(wd[1])

                            wd100.append(wd[2])

                            t2.append(float(ft2[timeNo,yGrid,xGrid]))
                                
                            psfc.append(float(fpsfc[timeNo,yGrid,xGrid]))
       


        
                        else:

                            u=[] 

                            v=[]

                            u.append(fu10[timeNo,yGrid,xGrid])
                                
                            v.append(fv10[timeNo,yGrid,xGrid])
                              
                            ws,wd=wind_convert(u,v)

                            ws10.append(ws[0])

                            wd10.append(wd[0])

                            t2.append(float(ft2[timeNo,yGrid,xGrid]))

                            rh2.append(float(frh2[timeNo,yGrid,xGrid]))

                            ghi.append(float(fghi[timeNo,yGrid,xGrid]))

                            diff.append(float(fdiff[timeNo,yGrid,xGrid]))

                            accprec.append(float(faccprec[timeNo,yGrid,xGrid]))

                            snow.append(float(fsnow[timeNo,yGrid,xGrid]))

                            accprec.append(float(faccprec[timeNo,yGrid,xGrid]))

    
    
    siteList=siteKontrolText.split("|")
    
    myclient = MongoClient("mongodb://localhost:27017/")
    
    mydbMongoDB = myclient["dbVentus"] #db

   

    

    for site in siteList:

        if site!="":

            kontrol=False

            for i in range(0,4):

                try:

                    if kontrol==False:
                      

                        mongoCol= mydbMongoDB[site]

                        mongoCol.bulk_write(siteDictList[site])

                        mongoCol.create_index("dataTime", unique = True)

                        siteTimeList[tableName]["updateTime"]=datetime.now()

                        mongoCol=mydbMongoDB["siteTimeList"]

                        newvalues = { "$set": siteTimeList[site] }

                        myquery = {"siteTable": site}

                        mongoCol.update_one(myquery,newvalues,upsert=True)


                        kontrol=True



                except:

                    if i<3:
                        continue
                    else:
                        raise
                    break

                if kontrol==True:

                    break


    
    myclient.close() 

    print("Start Hour:"+str(startHour)+"> "+str(startTime)+" Loaded >"+str(((datetime.now()-baslamaZamani).total_seconds()/60)))
    
    
    return "Done"

def readNCAndWriteToMongoWithXArray(xDataSet,timeValues,xyList,siteTableDF,site,siteGridList,modelNo,startHour,init,modelName,xyNo,dbApiInfo,filePath):
    try:
        print("Başlıyor>"+filePath+">"+str(timeValues[0])+"','"+str(timeValues[len(timeValues)-1])+"/"+str(xyNo))

        siteDictList={}

        siteTimeList={}

        siteKontrolText=""

        baslamaZamani=datetime.now()
        
        basladi=datetime.now()

        gridDictList={}

        columnsList=["WS10","WS50","WS100","WD10","WD50","WD100","T2","RH2","PSFC","GHI","DIFF","PREC","SNOW","SUM_SUNHOUR"]

        for column in columnsList:
        
            gridDictList[column]=[]

        for timeValue in timeValues:

            xData=xDataSet.where(xDataSet["Time"]==timeValue, drop=True)
            gridTimeDict={}


            for column in columnsList:
                gridTimeDict[column]={}

            for xyGridNo in range(0,len(xyList)):
            

                yGrid=int(xyList[xyGridNo][0])

                xGrid=int(xyList[xyGridNo][1])

                tmpSiteGridList=siteGridList[(siteGridList["xGrid"]==xGrid) & (siteGridList["yGrid"]==yGrid)]
                
                if tmpSiteGridList.shape[0]>0:



                    modelGridNo=tmpSiteGridList.iloc[0]["modelGridListId"]

                    siteGridValueDict={}

                    
                    modelGridNo=tmpSiteGridList.iloc[0]["modelGridListId"]
        
                    u=np.array(xData["U10"][:,yGrid,xGrid])
            
                    v=np.array(xData["V10"][:,yGrid,xGrid])

                    ws10,wd10=wind_convert(u,v)

                    u=np.array(xData["U50"][:,yGrid,xGrid])
            
                    v=np.array(xData["V50"][:,yGrid,xGrid])

                    ws50,wd50=wind_convert(u,v)
                                
                    u=np.array(xData["U100"][:,yGrid,xGrid])
            
                    v=np.array(xData["V100"][:,yGrid,xGrid])

                    ws100,wd100=wind_convert(u,v)

                    t2=np.array(xData["T2"][:,yGrid,xGrid])
                                    
                    psfc=np.array(xData["PSFC"][:,yGrid,xGrid])
                                                
                    rh2=np.array(xData["RH2"][:,yGrid,xGrid])

                    ghi=np.array(xData["GHI"][:,yGrid,xGrid])

                    diff=np.array(xData["DIFF"][:,yGrid,xGrid])

                    accprec=np.array(xData["AccPrec"][:,yGrid,xGrid])

                    snow=np.array(xData["SNOW"][:,yGrid,xGrid])

                    sunnyTime=0

                    for ghiSay in ghi:

                        if ghiSay>=360:

                            sunnyTime+=(10/60)


                    # siteGridValueDict["WS10_"+str(modelGridNo)+"_"+str(modelNo)]=float(np.round(np.mean(ws10) ,2))
                    # siteGridValueDict["WS50_"+str(modelGridNo)+"_"+str(modelNo)]=float(np.round(np.mean(ws50),2))
                    # siteGridValueDict["WS100_"+str(modelGridNo)+"_"+str(modelNo)]=float(np.round(np.mean(ws100),2))

                    gridTimeDict["WS10"]["WS10_"+str(modelGridNo)+"_"+str(modelNo)]=float(np.round(np.mean(ws10) ,2))
                    gridTimeDict["WS50"]["WS50_"+str(modelGridNo)+"_"+str(modelNo)]=float(np.round(np.mean(ws50),2))
                    gridTimeDict["WS100"]["WS100_"+str(modelGridNo)+"_"+str(modelNo)]=float(np.round(np.mean(ws100),2))

                    # siteGridValueDict["WD10_"+str(modelGridNo)+"_"+str(modelNo)]=float(np.round(meanWindDirection(wd10),2))
                    # siteGridValueDict["WD50_"+str(modelGridNo)+"_"+str(modelNo)]=float(np.round(meanWindDirection(wd50),2))
                    # siteGridValueDict["WD100_"+str(modelGridNo)+"_"+str(modelNo)]=float(np.round(meanWindDirection(wd100),2))

                    gridTimeDict["WD10"]["WD10_"+str(modelGridNo)+"_"+str(modelNo)]=float(np.round(meanWindDirection(wd10),2))
                    gridTimeDict["WD50"]["WD50_"+str(modelGridNo)+"_"+str(modelNo)]=float(np.round(meanWindDirection(wd50),2))
                    gridTimeDict["WD100"]["WD100_"+str(modelGridNo)+"_"+str(modelNo)]=float(np.round(meanWindDirection(wd100),2))





                    # siteGridValueDict["AVG_T2_"+str(modelGridNo)+"_"+str(modelNo)]=float(np.round(np.mean(t2),2))
                    # siteGridValueDict["MAX_T2"+str(modelGridNo)+"_"+str(modelNo)]=float(np.round(np.max(t2),2))

                    gridTimeDict["T2"]["AVG_T2_"+str(modelGridNo)+"_"+str(modelNo)]=float(np.round(np.mean(t2),2))
                    gridTimeDict["T2"]["MAX_T2"+str(modelGridNo)+"_"+str(modelNo)]=float(np.round(np.max(t2),2))

                    # siteGridValueDict["AVG_PSFC_"+str(modelGridNo)+"_"+str(modelNo)]=float(np.round(np.mean(psfc),2))
                    gridTimeDict["PSFC"]["AVG_PSFC_"+str(modelGridNo)+"_"+str(modelNo)]=float(np.round(np.mean(psfc),2))


                    
                    # siteGridValueDict["SUM_ACCPRE_"+str(modelGridNo)+"_"+str(modelNo)]=float(np.round(np.sum(accprec),2))

                    # siteGridValueDict["SUM_SNOW_"+str(modelGridNo)+"_"+str(modelNo)]=float(np.round(np.sum(snow),2))

                    # siteGridValueDict["MAX_ACCPRE_"+str(modelGridNo)+"_"+str(modelNo)]=float(np.round(np.max(accprec),2))


                    gridTimeDict["PREC"]["SUM_SNOW_"+str(modelGridNo)+"_"+str(modelNo)]=float(np.round(np.sum(snow),2))

                    gridTimeDict["PREC"]["SUM_ACCPRE_"+str(modelGridNo)+"_"+str(modelNo)]=float(np.round(np.sum(accprec),2))

                    gridTimeDict["PREC"]["MAX_ACCPRE_"+str(modelGridNo)+"_"+str(modelNo)]=float(np.round(np.max(accprec),2))


                    #columnsList=["WS10","WS50","WS100","T2","RH2","PSFC","GHI","DIFF","CLOUD","PREC"]

                    # siteGridValueDict["MAX_GHI_"+str(modelGridNo)+"_"+str(modelNo)]=float(np.round(np.max(ghi),2))

                    # siteGridValueDict["AVG_GHI_"+str(modelGridNo)+"_"+str(modelNo)]=float(np.round(np.mean(ghi),2))

                    # siteGridValueDict["AVG_DIFF_"+str(modelGridNo)+"_"+str(modelNo)]=float(np.round(np.mean(diff),2))

                    gridTimeDict["GHI"]["MAX_GHI_"+str(modelGridNo)+"_"+str(modelNo)]=float(np.round(np.max(ghi),2))
                    gridTimeDict["GHI"]["AVG_GHI_"+str(modelGridNo)+"_"+str(modelNo)]=float(np.round(np.mean(ghi),2))
                
                    gridTimeDict["DIFF"]["AVG_DIFF_"+str(modelGridNo)+"_"+str(modelNo)]=float(np.round(np.mean(diff),2))
                    gridTimeDict["DIFF"]["MAX_DIFF_"+str(modelGridNo)+"_"+str(modelNo)]=float(np.round(np.max(diff),2))

                    gridTimeDict["GHI"]["SUM_SUNHOUR_"+str(modelGridNo)+"_"+str(modelNo)]=float(np.round(sunnyTime,2))
                    gridTimeDict["DIFF"]["SUM_SUNHOUR_"+str(modelGridNo)+"_"+str(modelNo)]=float(np.round(sunnyTime,2))

                    # siteGridValueDict["SUM_SUNHOUR_"+str(modelGridNo)+"_"+str(modelNo)]=float(np.round(sunnyTime,2))


                    # siteGridValueDict["AVG_RH_"+str(modelGridNo)+"_"+str(modelNo)]=float(np.round(np.mean(rh2),2))
                    gridTimeDict["RH2"]["AVG_RH_"+str(modelGridNo)+"_"+str(modelNo)]=float(np.round(np.mean(rh2),2))

            
            for column in columnsList:
        
                gridTimeDict[column]["dataTime"]=timeValue
                gridTimeDict[column]["init"]=init
                gridTimeDict[column]["filePath"]=filePath
                newvalues = { "$set": gridTimeDict[column] }
                myquery = {"dataTime": timeValue}

                gridDictList[column].append(UpdateMany(myquery,newvalues,upsert=True))

    
            myclient = MongoClient("mongodb://89.252.157.127:27017/")
                            
            mydbMongoDB = myclient["dbVentus"] #db

            for column in columnsList:

                mongoCol= mydbMongoDB["ncDataModelParam_"+column]

                mongoCol.bulk_write(gridDictList[column])

                mongoCol.create_index("dataTime", unique = True)

            myclient.close()

            for column in columnsList:
        
                gridDictList[column]=[]
        
            
    
        
        myDBConnect = mysql.connector.connect(host=dbApiInfo["dbInfo"]["dbAddress"], user = dbApiInfo["dbInfo"]["dbUsersName"], password=dbApiInfo["dbInfo"]["dbPassword"], database=dbApiInfo["dbInfo"]["database"])

        print("Log Yazılıyor>"+filePath+">"+str(timeValues[0])+"','"+str(timeValues[len(timeValues)-1])+"/"+str(xyNo))


        insertTXT="Insert Into modelFileWriteLog(fileName,modelName,zamanBaslangic,zamanBitis,writeStartTime,writeEndTime,xyParcaNo) VALUES('"+filePath+"','"+modelName+"','"+str(timeValues[0])+"','"+str(timeValues[len(timeValues)-1])+"','"+str(baslamaZamani)+"','"+str(datetime.now())+"',"+str(xyNo)+")"
        
        cursor=myDBConnect.cursor()
        
        cursor.execute(insertTXT)
        
        myDBConnect.commit()

        myDBConnect.close()

        return "Done"
    except Exception as y:
        return str(y)
        


def futureAnswer(future):
    global threadCount
        
    threadCount-=1
 
    print("Kalan:"+str(threadCount))



def readwriteNCWithPool(ftime,fu10,fv10,fu50,fv50,fu100,fv100,ft2,fpsfc,frh2,fghi,fdiff,faccprec,fsnow,xyList,siteTableDF,siteGridListDF,modelNo,dataStartHourTime,dataEndHourTime,init,modelName):
       
    
    #ilk Veri Zamanı
    startTime=pd.to_datetime(timeTotxt(ftime[0]))+timedelta(hours=3)

    
    if ftime.shape[0]!=325:

        return

    futures=[]

    executor=concurrent.futures.ProcessPoolExecutor(max_workers=3)
    

    for timeNo in range(1,ftime.shape[0],6):

        dataTime=pd.to_datetime(timeTotxt(ftime[timeNo]))+timedelta(hours=3)

        hourDiff=(dataTime-startTime).total_seconds()/60

        hourDiff=hourDiff/60
        
        if (hourDiff>=dataStartHourTime and hourDiff<dataEndHourTime):

            # readNCAndWriteToMongo(ftime[timeNo:timeNo+6],fu10[timeNo:timeNo+6],fv10[timeNo:timeNo+6],fu50[timeNo:timeNo+6],fv50[timeNo:timeNo+6],fu100[timeNo:timeNo+6],fv100[timeNo:timeNo+6],ft2[timeNo:timeNo+6],fpsfc[timeNo:timeNo+6],frh2[timeNo:timeNo+6],fghi[timeNo:timeNo+6],fdiff[timeNo:timeNo+6],faccprec[timeNo:timeNo+6],fsnow[timeNo:timeNo+6],xyList,siteTableDF,1,siteGridListDF,modelNo,dataStartHourTime,init,modelName,)
            futures.append(executor.submit(readNCAndWriteToMongo,ftime[timeNo:timeNo+6],fu10[timeNo:timeNo+6],fv10[timeNo:timeNo+6],fu50[timeNo:timeNo+6],fv50[timeNo:timeNo+6],fu100[timeNo:timeNo+6],fv100[timeNo:timeNo+6],ft2[timeNo:timeNo+6],fpsfc[timeNo:timeNo+6],frh2[timeNo:timeNo+6],fghi[timeNo:timeNo+6],fdiff[timeNo:timeNo+6],faccprec[timeNo:timeNo+6],fsnow[timeNo:timeNo+6],xyList,siteTableDF,1,siteGridListDF,modelNo,dataStartHourTime,init,modelName,))


    
    for future in concurrent.futures.as_completed(futures):

        future.result()

        print(str(future.result())+" Bitti")

    
    executor.shutdown()


    

    return "bitti"
    


def readwriteNCWithPoolxArray(xDataSet,zamanArr,xyList,siteTableDF,siteGridListDF,modelNo,dataStartHourTime,dataEndHourTime,init,modelName,dbaInfo,fileName):
    

    processExecutorNC=concurrent.futures.ProcessPoolExecutor(max_workers=7)

    futureList=[]

    #kontrol için ilk veri zamanını alıyoruz
    
    startTime=zamanArr[0]-timedelta(hours=1)

    
    futures=[]

    maxWorker=3

    executor=concurrent.futures.ProcessPoolExecutor(max_workers=maxWorker)
    
    zamanSelected=[]


    #yazılacak saatleri seçiyoruz
    for timeNo in range(len(zamanArr)):

        hourDiff=(zamanArr[timeNo]-startTime).total_seconds()/60

        hourDiff=hourDiff/60
       
        if (hourDiff>=dataStartHourTime and hourDiff<dataEndHourTime):
       
            zamanSelected.append(zamanArr[timeNo])

   
    
    tmpZamanSay=0
    threadCount=0
    
    for zamanSay in range(0,len(zamanSelected)-1,3):

        tmpZamanSay=zamanSay+3

        sonZamanNo=zamanSay+3

        if zamanSay+3>len(zamanSelected):
            sonZamanNo=len(zamanSelected)
       
       

        tmpCopyR=xDataSet.copy()

        futureList.append(processExecutorNC.submit(readNCAndWriteToMongoWithXArray,tmpCopyR,zamanSelected[zamanSay:sonZamanNo],xyList[0:3000],siteTableDF,1,siteGridListDF,modelNo,dataStartHourTime,init,modelName,1,dbaInfo,fileName))
        threadCount+=1
      

        tmpCopyT=xDataSet.copy()

        futureList.append(processExecutorNC.submit(readNCAndWriteToMongoWithXArray,tmpCopyT,zamanSelected[zamanSay:sonZamanNo],xyList[3000:6000],siteTableDF,1,siteGridListDF,modelNo,dataStartHourTime,init,modelName,2,dbaInfo,fileName))
        threadCount+=1

        
        tmpCopyK=xDataSet.copy()

        futureList.append(processExecutorNC.submit(readNCAndWriteToMongoWithXArray,tmpCopyK,zamanSelected[zamanSay:sonZamanNo],xyList[6000:],siteTableDF,1,siteGridListDF,modelNo,dataStartHourTime,init,modelName,3,dbaInfo,fileName))
        threadCount+=1
     


    
    #kalan saat var ise onu başlatıyoruz

    if tmpZamanSay<len(zamanSelected)-1:

        
        tmpCopyV=xDataSet.copy()
        
        futureList.append(executor.submit(readNCAndWriteToMongoWithXArray,tmpCopyV,zamanSelected[zamanSay:sonZamanNo],xyList[0:3000],siteTableDF,1,siteGridListDF,modelNo,dataStartHourTime,init,modelName,1,dbaInfo,fileName))
       
        threadCount+=1


        tmpCopyO=xDataSet.copy()

        futureList.append(executor.submit(readNCAndWriteToMongoWithXArray,tmpCopyO,zamanSelected[zamanSay:sonZamanNo],xyList[3000:6000],siteTableDF,1,siteGridListDF,modelNo,dataStartHourTime,init,modelName,2,dbaInfo,fileName))
       

        
        futureList.append(executor.submit(readNCAndWriteToMongoWithXArray,tmpCopyV,zamanSelected[zamanSay:sonZamanNo],xyList[6000:],siteTableDF,1,siteGridListDF,modelNo,dataStartHourTime,init,modelName,3,dbaInfo,fileName))
       
        threadCount+=1


    print("Calisan:"+str(threadCount))


    for future in concurrent.futures.as_completed(futureList):
        
        print("Kalan:"+str(threadCount))

        futureList=[]


    return "bitti"

    
    

  








def addSiteGridList(site,modelNo,yakinEkle,uzakekle,dbApiInfo,gridTable):



    siteLat=site[4]

    siteLng=site[5]
    


    # myDBConnect = mysql.connector.connect(host=dbApiInfo["dbInfo"]["dbAddress"], user = dbApiInfo["dbInfo"]["dbUsersName"], password=dbApiInfo["dbInfo"]["dbPassword"], database="metmodelDB")

    # selectTxt="Select * From metModelGridList where meteoModelListId="+str(modelNo) 

    # cursor=myDBConnect.cursor()
            
    # cursor.execute(selectTxt)
    
    # gridTable = cursor.fetchall()

    gridTableDF=pd.DataFrame(gridTable,columns=["id","modelId","xGridNo","yGridNo","xLon","yLat"])
    
    if len(gridTable)<=0:
        return -9999
    
    tmpDistance=calcDistance(siteLat,siteLng,gridTable[0][5],gridTable[0][4])

    selectedGridX=gridTable[0][2]

    selectedGridY=gridTable[0][3]

    selectedGridYLat=gridTable[0][5]
    
    selectedGridXLon=gridTable[0][4]

    ustLatGridNo=-9999
    altLatGridNo=-9999

    ustLonGridNo=-9999
    altLonGridNo=-9999


    for gridRowNo in gridTable:
        
        gridDistance=calcDistance(siteLat,siteLng,gridRowNo[5],gridRowNo[4])
        if gridDistance<tmpDistance:
            selectedGridX=gridRowNo[2]
            selectedGridY=gridRowNo[3]
            selectedGridYLat=gridRowNo[5]
            selectedGridXLon=gridRowNo[4]
            tmpDistance=gridDistance

    selectedGrids=[]
   
    if siteLat>selectedGridYLat:

        ustLatGridNo=selectedGridY+uzakekle
        altLatGridNo=selectedGridY-yakinEkle

    else:

        ustLatGridNo=selectedGridY+yakinEkle
        altLatGridNo=selectedGridY-uzakekle

    
    if siteLng<selectedGridXLon:

        ustLonGridNo=selectedGridX+uzakekle
        altLonGridNo=selectedGridX-yakinEkle

    else:

        ustLonGridNo=selectedGridX+yakinEkle
        altLonGridNo=selectedGridX-uzakekle
    
    myDBConnect = mysql.connector.connect(host=dbApiInfo["dbInfo"]["dbAddress"], user = dbApiInfo["dbInfo"]["dbUsersName"], password=dbApiInfo["dbInfo"]["dbPassword"], database="musteriDB")

    insertTXT="Insert Into siteGridList (siteId,modelGridListId,meteoModelListId,xGrid,yGrid) VALUES(%s,%s,%s,%s,%s)"

    for yGridNo in range(altLatGridNo,ustLatGridNo+1):

        for xGridNo in range(altLonGridNo,ustLonGridNo+1):

            gridTableTmpDF=gridTableDF[(gridTableDF["xGridNo"]==xGridNo) & (gridTableDF["yGridNo"]==yGridNo)]

            try:
                if gridTableTmpDF.shape[0]>0:

                    selectedGrids.append((site[0],gridTableTmpDF.iloc[0]["id"],modelNo,xGridNo,yGridNo))

            except:

                t=""

    cursor=myDBConnect.cursor()

    cursor.executemany(insertTXT,selectedGrids)

    myDBConnect.commit()

    myDBConnect.close()

    if gridTableTmpDF.shape[0]>0:

        return str(gridTableTmpDF.iloc[0]["id"])+"-"+str(modelNo)+" Bitti"     
        
    else:

        return "Bitti"

def calcDistance(siteLat,siteLon,yLat,xLon):
    try:

        yDistance=abs(siteLat-yLat)

        xDistance=abs(siteLon-xLon)

        distance=math.sqrt(math.pow(yDistance,2)+math.pow(xDistance,2))

        return distance

    except:

        return 10000



def timeTotxt(timeTXT):
    timeTotxt=str(timeTXT).replace("b'","")
    timeTotxt=timeTotxt.replace("' ","")
    timeTotxt=timeTotxt.replace("'\n","")
    timeTotxt=timeTotxt.replace("_"," ")
    timeTotxt=timeTotxt.replace("[","")
    timeTotxt=timeTotxt.replace("]","")
    timeTotxt=timeTotxt.replace("'","")
    timeTotxt=timeTotxt.replace(": ",":")
    return timeTotxt


                
def runNCWriter(ftime,fu10,fv10,fu50,fv50,fu100,fv100,ft2,fpsfc,frh2,fghi,fdiff,faccprec,fsnow,modelNo,xyList,siteTableDF,siteIdList,siteGridTableDF,startHour,endHour):


                        
    readwriteNCWithPool(ftime,fu10,fv10,fu50,fv50,fu100,fv100,ft2,fpsfc,frh2,fghi,fdiff,faccprec,fsnow,xyList,siteTableDF,siteIdList,siteGridTableDF,modelNo,startHour,endHour)

        




#runNCWriter(sys.argv[1],sys.argv[2])

# runNCWriter('1',"/mnt/qNAPN2_vLM2_iMEFsys/NCFiles/WRF_GFS/wrfpost_2022-12-17_06.nc")

# runNCWriter('1',"C:\\Users\\user\\Downloads\Gfs\\wrfpost_2022-11-30_00.nc")




