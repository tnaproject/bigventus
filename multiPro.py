import pandas as pd
from datetime import datetime,timedelta
import numpy as np
import math
from pymongo import MongoClient,UpdateMany,UpdateOne

def wind_convert(u,v):
    wind_dir=np.zeros(len(u))
    wind_speed=np.zeros(len(u))
    for i in range(0,len(u)):

        conv = 45 / math.atan(1)
        wind_dir[i] = (270 - math.atan2(v[i],u[i]) * conv) % 360
        wind_speed[i] = math.sqrt(math.pow(u[i],2) + math.pow(v[i],2))
    #wind_dir = 180 + (180 / math.pi) * math.atan2(v,u)
    
    return wind_speed,wind_dir

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

def meanWindDirection(windDirections):
    
    V_east=np.zeros(len(windDirections))
    V_north=np.zeros(len(windDirections))

    for i in range(0,len(windDirections)):


        V_east[i] = np.mean( math.sin(windDirections[i] * math.pi/180))
        V_north[i] = np.mean(math.cos(windDirections[i] * math.pi/180))

    mean_WD = math.atan2(np.mean(V_east),np.mean(V_north)) * 180/math.pi
    mean_WD = (360 + mean_WD) % 360

    return mean_WD

def readandwriteToMysqlNC(fTimeArr,fu10,fv10,fu50,fv50,fu100,fv100,ft2,fpsfc,xyGridList,siteGridList,modelNo):
         
    # with open("config.json","r") as file:
    #     dbApiInfo=json.load(file)

    # myDBConnect = mysql.connector.connect(host=dbApiInfo["dbInfo"]["dbAddress"], user = dbApiInfo["dbInfo"]["dbUsersName"], password=dbApiInfo["dbInfo"]["dbPassword"], database=dbApiInfo["dbInfo"]["database"])
    

   
    startTime=pd.to_datetime(timeTotxt(fTimeArr[0]))+timedelta(hours=3)

    # for i in range(0,480):
    #     addColumnToTable("tmpNCFile","xGridNo"+str(i)+" double DEFAULT NULL")
       

    myclient = MongoClient() 
     
    # connecting with the portnumber and host 
    myclient = MongoClient("mongodb://89.252.157.127:27017/")
    mydbMongoDB = myclient["dbVentusDB"] #db
    

    # myDBConnect.commit()
    xList=""
    yList=""

    dosyaAdi=str(startTime.year)+str(startTime.month)+str(startTime.day)+str(startTime.hour)+str(startTime.minute)


    f=open("d"+dosyaAdi+".txt", "a") 


    siteDictList={}

    siteKontrolText=""

    for xyGridNo in range(0,len(xyGridList)):

        yGrid=int(xyGridList[xyGridNo][0])
        xGrid=int(xyGridList[xyGridNo][1])
        ws10=[]
        ws50=[]
        ws100=[]
        wd10=[]
        wd50=[]
        wd100=[]
        t2=[]
        psfc=[]

        tmpSiteGridList=siteGridList[(siteGridList["xGrid"]==xGrid) & (siteGridList["yGrid"]==yGrid)]
        
        modelGridNo=tmpSiteGridList.iloc[0]["modelGridListId"]
       

        siteGridValueDict={}

        for timeNo in range(0,fTimeArr.shape[0]):

            dataTime=pd.to_datetime(timeTotxt(fTimeArr[timeNo]))+timedelta(hours=3)
         
            dataValueList=[]

            kolonSayi=0
   
            u=[]     
            v=[]
            kolonSayi+=1
                     
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
            
        wsAvg10=np.mean(ws10)                    
        wsAvg50=np.mean(ws50)
        wsAvg100=np.mean(ws100)

        wdAvg10=meanWindDirection(wd10)
        wdAvg50=meanWindDirection(wd50)
        wdAvg100=meanWindDirection(wd100)

        tAvg=np.mean(t2)
        tMax=np.max(t2)
        psfcAvg=np.mean(psfc)
        
        siteGridValueDict["WS10_"+str(modelGridNo)+"_"+str(modelNo)]=np.round(wsAvg10,2)
        siteGridValueDict["WS50_"+str(modelGridNo)+"_"+str(modelNo)]=np.round(wsAvg50,2)
        siteGridValueDict["WS100_"+str(modelGridNo)+"_"+str(modelNo)]=np.round(wsAvg100,2)

        siteGridValueDict["WD10_"+str(modelGridNo)+"_"+str(modelNo)]=np.round(wdAvg10,2)
        siteGridValueDict["WD50_"+str(modelGridNo)+"_"+str(modelNo)]=np.round(wdAvg50,2)
        siteGridValueDict["WD100_"+str(modelGridNo)+"_"+str(modelNo)]=np.round(wdAvg100,2)
        
        siteGridValueDict["AVG_T2_"+str(modelGridNo)+"_"+str(modelNo)]=np.round(tAvg,2)
        siteGridValueDict["MAX_T2"+str(modelGridNo)+"_"+str(modelNo)]=np.round(tMax,2)
        siteGridValueDict["AVG_PSFC_"+str(modelGridNo)+"_"+str(modelNo)]=np.round(psfcAvg,2)
        siteGridValueDict["dataTime"]=pd.to_datetime(dataTime)

        
        

        for i in range(0,tmpSiteGridList.shape[0]):

            tableName="trainTable_"+str(tmpSiteGridList.iloc[i]["siteId"])

            if siteKontrolText.__contains__(tableName+"|")==False:
                if siteKontrolText=="":

                    siteKontrolText=tableName+"|"

                else:

                    siteKontrolText+=tableName+"|"

                siteDictList[tableName]=[]


            newvalues = { "$set": siteGridValueDict }

            myquery = {"dataTime": dataTime}

            siteDictList[tableName].append(UpdateMany(myquery,newvalues,upsert=True))
            
            f.write(str(siteGridValueDict)+"\n")


    siteList=siteKontrolText.split("|")

    for site in siteList:

        if site!="":

            mongoCol= mydbMongoDB[site]
        
            mongoCol.bulk_write(siteDictList[site])
            
        

            

        
    f.close()
    
    