import numpy as np
import pandas as pd
import mysql.connector 
from datetime import datetime,timedelta
# import calcWindPower
# import calcSolarPower
import math
import json
from pymongo import MongoClient,UpdateMany
import concurrent.futures
import warnings
import xarray as xr


warnings.filterwarnings("ignore", category=DeprecationWarning)





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

def readNCAndWriteToMongoWithXArray(xDataSet,timeValues,xyList,siteTableDF,site,siteGridList,modelNo,startHour,init,modelName,xyNo):

    
    siteDictList={}

    siteTimeList={}

    siteKontrolText=""

    baslamaZamani=datetime.now()
    
    print("Start Hour:"+str(startHour)+">"+str(timeValues[0])+" "+str(xyNo)+" Loading")

    basladi=datetime.now()

    siteDictList=[]

    for timeValue in timeValues:
        xData=xDataSet.where(xDataSet["Time"]==timeValue, drop=True)

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

        
                siteGridValueDict["WS10_"+str(modelGridNo)+"_"+str(modelNo)]=float(np.round(np.mean(ws10) ,2))
                siteGridValueDict["WS50_"+str(modelGridNo)+"_"+str(modelNo)]=float(np.round(np.mean(ws50),2))
                siteGridValueDict["WS100_"+str(modelGridNo)+"_"+str(modelNo)]=float(np.round(np.mean(ws100),2))

                siteGridValueDict["WD10_"+str(modelGridNo)+"_"+str(modelNo)]=float(np.round(meanWindDirection(wd10),2))
                siteGridValueDict["WD50_"+str(modelGridNo)+"_"+str(modelNo)]=float(np.round(meanWindDirection(wd50),2))
                siteGridValueDict["WD100_"+str(modelGridNo)+"_"+str(modelNo)]=float(np.round(meanWindDirection(wd100),2))
        
                siteGridValueDict["AVG_T2_"+str(modelGridNo)+"_"+str(modelNo)]=float(np.round(np.mean(t2),2))
                siteGridValueDict["MAX_T2"+str(modelGridNo)+"_"+str(modelNo)]=float(np.round(np.max(t2),2))
                siteGridValueDict["AVG_PSFC_"+str(modelGridNo)+"_"+str(modelNo)]=float(np.round(np.mean(psfc),2))

                siteGridValueDict["SUM_SNOW_"+str(modelGridNo)+"_"+str(modelNo)]=float(np.round(np.sum(snow),2))

                siteGridValueDict["SUM_ACCPRE_"+str(modelGridNo)+"_"+str(modelNo)]=float(np.round(np.sum(accprec),2))

                siteGridValueDict["MAX_ACCPRE_"+str(modelGridNo)+"_"+str(modelNo)]=float(np.round(np.max(accprec),2))
 
                siteGridValueDict["MAX_GHI_"+str(modelGridNo)+"_"+str(modelNo)]=float(np.round(np.max(ghi),2))

                siteGridValueDict["AVG_GHI_"+str(modelGridNo)+"_"+str(modelNo)]=float(np.round(np.mean(ghi),2))

                siteGridValueDict["SUM_SUNHOUR_"+str(modelGridNo)+"_"+str(modelNo)]=float(np.round(sunnyTime,2))

                siteGridValueDict["AVG_DIFF_"+str(modelGridNo)+"_"+str(modelNo)]=float(np.round(np.mean(diff),2))

                siteGridValueDict["WS10_"+str(modelGridNo)+"_"+str(modelNo)]=float(np.round(np.mean(ws10),2))

                siteGridValueDict["WD10_"+str(modelGridNo)+"_"+str(modelNo)]=float(np.round(meanWindDirection(wd10),2))

                siteGridValueDict["AVG_T2_"+str(modelGridNo)+"_"+str(modelNo)]=float(np.round(np.mean(t2),2))

                siteGridValueDict["MAX_T2"+str(modelGridNo)+"_"+str(modelNo)]=float(np.round(np.max(t2),2))

                siteGridValueDict["AVG_RH_"+str(modelGridNo)+"_"+str(modelNo)]=float(np.round(np.mean(rh2),2))

                siteGridValueDict["dataTime"]=timeValue

                siteGridValueDict["init"]=init

                siteGridValueDict["f_GHI_"+str(modelGridNo)+"_"+str(modelNo)]=-8888.0

            
            
        siteGridValueDict["dataTime"]=timeValue

        siteGridValueDict["init"]=init
           
        newvalues = { "$set": siteGridValueDict }

        myquery = {"dataTime": timeValue}


        siteDictList.append(UpdateMany(myquery,newvalues,upsert=True))

 
    myclient = MongoClient("mongodb://89.252.157.127:27017/")
    
    mydbMongoDB = myclient["dbVentus"] #db

    mongoCol= mydbMongoDB["ncDataModel_"+str(modelNo)]

    mongoCol.bulk_write(siteDictList)

    mongoCol.create_index("dataTime", unique = True)

    myclient.close() 
   
    print("Start Hour:"+str(startHour)+"> "+str(timeValues[0])+" HourLoaded >"+str(((datetime.now()-baslamaZamani).total_seconds()/60)))
    
    
    return "Done"



def futureAnswer(future):
    
    cvp=future.result()


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
    


def readwriteNCWithPoolxArray(xDataSet,zamanArr,xyList,siteTableDF,siteGridListDF,modelNo,dataStartHourTime,dataEndHourTime,init,modelName):
    


    #ilk Veri Zamanı
    
   
    startTime=zamanArr[0]-timedelta(hours=1)

    





    futures=[]

    maxWorker=1

    executor=concurrent.futures.ProcessPoolExecutor(max_workers=maxWorker)
    
    zamanSelected=[]
    zamanCopyArr=zamanArr.copy()
    zamanSecilen=0


    for timeNo in range(len(zamanArr)):

        hourDiff=(zamanArr[timeNo]-startTime).total_seconds()/60

        hourDiff=hourDiff/60
       
        if (hourDiff>=dataStartHourTime and hourDiff<dataEndHourTime):
       
            zamanSelected.append(zamanArr[timeNo])


            

    
    
    
    tmpZamanSay=0

    for zamanSay in range(0,len(zamanSelected)-1,3):
        tmpZamanSay=zamanSay+3
        tmpCopy=xDataSet.copy()
        futures.append(executor.submit(readNCAndWriteToMongoWithXArray,tmpCopy,zamanSelected[zamanSay:zamanSay+3],xyList[0:10000],siteTableDF,1,siteGridListDF,modelNo,dataStartHourTime,init,modelName,1,))
        tmpCopyR=xDataSet.copy()
        futures.append(executor.submit(readNCAndWriteToMongoWithXArray,tmpCopyR,zamanSelected[zamanSay:zamanSay+3],xyList[10000:],siteTableDF,1,siteGridListDF,modelNo,dataStartHourTime,init,modelName,2,))


    
    
    if tmpZamanSay<len(zamanSelected)-1:

        tmpZamanSayy=tmpZamanSay
        tmpCopyT=xDataSet.copy()
        
        futures.append(executor.submit(readNCAndWriteToMongoWithXArray,tmpCopyT,zamanSelected[tmpZamanSay:],xyList[0:10000],siteTableDF,1,siteGridListDF,modelNo,dataStartHourTime,init,modelName,1,))

        tmpCopyV=xDataSet.copy()
        futures.append(executor.submit(readNCAndWriteToMongoWithXArray,xDataSet,zamanSelected[tmpZamanSay:],xyList[10000:],siteTableDF,1,siteGridListDF,modelNo,dataStartHourTime,init,modelName,2,))


 
   
    for future in concurrent.futures.as_completed(futures):

         future.result()

         print(str(future.result())+" Bitti")

       
   


    
    executor.shutdown()


    

    return "bitti"

    
    

  








def addSiteGridList(site,modelNo,yakinEkle,uzakekle,dbApiInfo):


    siteLat=site[4]

    siteLng=site[5]
    


    myDBConnect = mysql.connector.connect(host=dbApiInfo["dbInfo"]["dbAddress"], user = dbApiInfo["dbInfo"]["dbUsersName"], password=dbApiInfo["dbInfo"]["dbPassword"], database="metmodelDB")

    selectTxt="Select * From metModelGridList where meteoModelListId="+str(modelNo) 

    cursor=myDBConnect.cursor()
            
    cursor.execute(selectTxt)
    
    gridTable = cursor.fetchall()

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
           
            selectedGrids.append((site[0],gridTableTmpDF.iloc[0]["id"],modelNo,xGridNo,yGridNo))
    
    cursor=myDBConnect.cursor()

    cursor.executemany(insertTXT,selectedGrids)

    myDBConnect.commit()

    myDBConnect.close()

    return selectedGrids        

def calcDistance(siteLat,siteLon,yLat,xLon):

    yDistance=abs(siteLat-yLat)

    xDistance=abs(siteLon-xLon)

    distance=math.sqrt(math.pow(yDistance,2)+math.pow(xDistance,2))

    return distance



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




