import netCDF4 as nc
import numpy as np
import pandas as pd
import mysql.connector 
from datetime import datetime,timedelta
import calcWindPower
import calcSolarPower
import math
import json
import sys
from pymongo import MongoClient,UpdateMany
import threading
import time
import multiprocessing



yazmaBekle=False



def addColumnToTable(tableName,columnDescTxt):

    try:
        with open("config.json","r") as file:
            dbApiInfo=json.load(file)
        
        myDBConnect = mysql.connector.connect(host=dbApiInfo["dbInfo"]["dbAddress"], user = dbApiInfo["dbInfo"]["dbUsersName"], password=dbApiInfo["dbInfo"]["dbPassword"], database=dbApiInfo["dbInfo"]["database"])

        if getTableCount(tableName)>0:
            
            insertTXT="ALTER TABLE "+tableName+" ADD COLUMN "+columnDescTxt

            cursor=myDBConnect.cursor()

            cursor.execute(insertTXT)

            myDBConnect.commit()

        

            return True

    except:

        return False

def getColumnCountByTable(tableName,columnName):

    
    selectTXT="SELECT count(*) FROM information_schema.COLUMNS  WHERE  TABLE_NAME = '"+tableName+"' AND COLUMN_NAME = '"+columnName+"'"
    
    try:
   
        with open("config.json","r") as file:
            dbApiInfo=json.load(file)

        myDBConnect = mysql.connector.connect(host=dbApiInfo["dbInfo"]["dbAddress"], user = dbApiInfo["dbInfo"]["dbUsersName"], password=dbApiInfo["dbInfo"]["dbPassword"], database=dbApiInfo["dbInfo"]["database"])
        
        cursor=myDBConnect.cursor()
        
        cursor.execute(selectTXT)
    
        siteCountColumn = cursor.fetchall()

        myDBConnect.commit()
        
        myDBConnect.close()

        return siteCountColumn[0][0]
        
    except:

        print("Tablo Sayısı Kontrol Edilirken Hata Oluştu")   

def getTableCount(tableName):

  

    selectTXT="SELECT count(*)   FROM  information_schema.TABLES  WHERE  TABLE_NAME = '"+tableName+"'" 

    try:

        with open("config.json","r") as file:
            dbApiInfo=json.load(file)

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





    

            


def readandwriteSiteToMysqlNC(fTimeArr,fu10,fv10,fu50,fv50,fu100,fv100,ft2,fpsfc,xyGridList,siteGridList,siteId,modelNo):
    
    # with open("config.json","r") as file:
    #     dbApiInfo=json.load(file)

    # myDBConnect = mysql.connector.connect(host=dbApiInfo["dbInfo"]["dbAddress"], user = dbApiInfo["dbInfo"]["dbUsersName"], password=dbApiInfo["dbInfo"]["dbPassword"], database=dbApiInfo["dbInfo"]["database"])
    

   
    startTime=pd.to_datetime(timeTotxt(fTimeArr[0]))+timedelta(hours=3)

    # for i in range(0,480):
    #     addColumnToTable("tmpNCFile","xGridNo"+str(i)+" double DEFAULT NULL")
       

    myclient = MongoClient() 
     

    

    # myDBConnect.commit()
    xList=""
    yList=""

    dosyaAdi=str(startTime.year)+str(startTime.month)+str(startTime.day)+str(startTime.hour)+str(startTime.minute)


    # f=open("d"+dosyaAdi+".txt", "a") 


    siteDictList=[]

    siteKontrolText=""
   
    siteMainGridList=siteGridList[siteGridList["siteId"]==siteId]

    siteGridValueDict={}
    for timeNo in range(0,fTimeArr.shape[0]):

        
        if pd.to_datetime(timeTotxt(fTimeArr[timeNo])).minute==0:

            dataTime=pd.to_datetime(timeTotxt(fTimeArr[timeNo]))+timedelta(hours=3)
        
        
        ws10=[]
        ws50=[]
        ws100=[]
        wd10=[]
        wd50=[]
        wd100=[]
        t2=[]
        psfc=[]

        for xyGridNo in range(0,siteMainGridList.shape[0]):


            yGrid=int(siteMainGridList.iloc[xyGridNo]["yGrid"])

            xGrid=int(siteMainGridList.iloc[xyGridNo]["yGrid"])

            modelGridNo=siteMainGridList.iloc[xyGridNo]["modelGridListId"]

            if pd.to_datetime(timeTotxt(fTimeArr[timeNo])).minute!=0:

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

                dataTime=pd.to_datetime(timeTotxt(fTimeArr[timeNo]))+timedelta(hours=3)
                
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

                ws10=[]
                ws50=[]
                ws100=[]
                wd10=[]
                wd50=[]
                wd100=[]
                t2=[]
                psfc=[]
        

       
                
           

                newvalues = { "$set": siteGridValueDict }

                myquery = {"dataTime": dataTime}

                siteDictList.append(UpdateMany(myquery,newvalues,upsert=True))
            
            

            # f.write(str(siteGridValueDict)+"\n")
    
   
    global yazmaBekle

    while yazmaBekle==True:

        bh=1
    
    yazmaBekle=True
    try:
        myclient = MongoClient("mongodb://89.252.157.127:27017/")
    
        mydbMongoDB = myclient["dbVentusDB_Local"] #db
   
        tableName="trainTable_"+str(siteId)

        mongoCol= mydbMongoDB[tableName]

        mongoCol.create_index("dataTime", unique = True)

        mongoCol.bulk_write(siteDictList)
        f=open("Site_"+str(siteId)+"_"+str(dataTime.to_julian_date())+".txt","a")
        f.write(str(dataTime))
        f.close()

        myclient.close()
    except:
        ps=""

    yazmaBekle=False
    




    

def readandwriteToMysqlNC(fTimeArr,fu10,fv10,fu50,fv50,fu100,fv100,ft2,fpsfc,xyGridList,siteGridList,modelNo):
    
    # with open("config.json","r") as file:
    #     dbApiInfo=json.load(file)

    # myDBConnect = mysql.connector.connect(host=dbApiInfo["dbInfo"]["dbAddress"], user = dbApiInfo["dbInfo"]["dbUsersName"], password=dbApiInfo["dbInfo"]["dbPassword"], database=dbApiInfo["dbInfo"]["database"])
    

   
    startTime=pd.to_datetime(timeTotxt(fTimeArr[0]))+timedelta(hours=3)

    # for i in range(0,480):
    #     addColumnToTable("tmpNCFile","xGridNo"+str(i)+" double DEFAULT NULL")
       

    myclient = MongoClient() 
     

    

    # myDBConnect.commit()
    xList=""
    yList=""

    dosyaAdi=str(startTime.year)+str(startTime.month)+str(startTime.day)+str(startTime.hour)+str(startTime.minute)


    # f=open("d"+dosyaAdi+".txt", "a") 


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
            if pd.to_datetime(timeTotxt(fTimeArr[timeNo])).minute==0:
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
            
            

            # f.write(str(siteGridValueDict)+"\n")
    
   
    siteList=siteKontrolText.split("|")
    
    myclient = MongoClient("mongodb://localhost:27017/")
    
    mydbMongoDB = myclient["dbVentusDB"] #db
   

    for site in siteList:

        if site!="":

            mongoCol= mydbMongoDB[site]

            mongoCol.create_index("dataTime", unique = True)

            mongoCol.bulk_write(siteDictList[site])

    myclient.close()
            

    # connecting with the portnumber and host 
    # myclient = MongoClient("mongodb://localhost:27017/")

        

            

        
    # f.close()



def readwriteNCNew(filePath,siteList,siteGridListDF,modelNo):
     
        



    baslangicZmn=datetime.now()
    print("dataOkuyor")
    print(datetime.now())
    f=nc.Dataset(filePath)


    fu10 = f.variables['U10'][:,:,:].data
    fu10 = f.variables['U10'][:,:,:].data
    fu50 = f.variables['U50'][:,:,:].data
    fu100 = f.variables['U100'][:,:,:].data
    fu200 = f.variables['U200'][:,:,:].data
    fv10 = f.variables['V10'][:,:,:].data
    fv50   = f.variables['V50'][:,:,:].data
    fv100  = f.variables['V100'][:,:,:].data
    fv200  = f.variables['V200'][:,:,:].data
    ft2    = f.variables['T2'][:,:,:].data
    frh2   = f.variables['RH2'][:,:,:].data
    fpsfc  = f.variables['PSFC'][:,:,:].data
    fghi   = f.variables['GHI'][:,:,:].data
    fdiff  = f.variables['DIFF'][:,:,:].data
    ftime  =  f.variables['Time'][:,:].data
    faccprec=f.variables['AccPrec'][:,:,:].data
    fsnow  = f.variables['SNOW'][:,:,:].data
  
    print(datetime.now())
    print("data okundu")

    with open("config.json","r") as file:
        dbApiInfo=json.load(file)

    myDBConnect = mysql.connector.connect(host=dbApiInfo["dbInfo"]["dbAddress"], user = dbApiInfo["dbInfo"]["dbUsersName"], password=dbApiInfo["dbInfo"]["dbPassword"], database=dbApiInfo["dbInfo"]["database"])
    

    #server
    myclient = MongoClient() 
     
    # connecting with the portnumber and host 


    


    #ilk Veri Zamanı
    startTime=pd.to_datetime(timeTotxt(ftime[0]))+timedelta(hours=3)

    # for i in range(280,480):
    #     addColumnToTable("tmpNCFile","xGridNo"+str(i)+" double DEFAULT NULL")
    
    libModelGrid={"gridNo":0,"dataTime":pd.to_datetime("1900-01-01 00:00"),"Values":{"TS":""}}

    libModelGridDF=pd.DataFrame(libModelGrid)

    
    columnNameValueList=""

    sTxt=""

    insertTXT="Delete From tmpNCFileWS"

    cursor=myDBConnect.cursor()

    cursor.execute(insertTXT)

    myDBConnect.commit()
    xList=""
    yList=""


    xyList=[]
    xyKontrol=""
    siteId=[]
    siteIdKontrol=""

    for rowCount in range(0,siteGridListDF.shape[0]):
        if xyKontrol.__contains__(str(siteGridListDF.iloc[rowCount]["yGrid"])+"-"+str(siteGridListDF.iloc[rowCount]["xGrid"]))==False:
            if xyKontrol=="":
                xyKontrol=str(siteGridListDF.iloc[rowCount]["yGrid"])+"-"+str(siteGridListDF.iloc[rowCount]["xGrid"])
            else:
                xyKontrol+="|"+str(siteGridListDF.iloc[rowCount]["yGrid"])+"-"+str(siteGridListDF.iloc[rowCount]["xGrid"])

            xyList.append([str(siteGridListDF.iloc[rowCount]["yGrid"]),str(siteGridListDF.iloc[rowCount]["xGrid"])])

        if siteIdKontrol.__contains__(str(siteGridListDF.iloc[rowCount]["siteId"])+"|")==False:
            if siteIdKontrol=="":
                siteIdKontrol=str(str(siteGridListDF.iloc[rowCount]["siteId"])+"|")
                
            else:
                siteIdKontrol=str(str(siteGridListDF.iloc[rowCount]["siteId"])+"|")
            
            siteId.append(siteGridListDF.iloc[rowCount]["siteId"])

    if ftime.shape[0]!=325:
        return

    calistirProcess=0

    p=[]

    for timeNo in range(1,ftime.shape[0],6):
        for site in siteId:
            p.append(timeNo)

    pSay=0
    print(datetime.now())
    print("+++++++++++++++")

    
    for timeNo in range(1,ftime.shape[0],6):

        dataTime=pd.to_datetime(timeTotxt(ftime[timeNo]))+timedelta(hours=3)
         


        dataValueList=[]

        siteGridValueDict={}  
        

        threadCalisiyor=True


        if __name__ == '__main__':
            
            for site in siteId:
                p[pSay]=threading.Thread(target=readandwriteSiteToMysqlNC,args=(ftime[timeNo:timeNo+6],fu10[timeNo:timeNo+6],fv10[timeNo:timeNo+6],fu50[timeNo:timeNo+6],fv50[timeNo:timeNo+6],fu100[timeNo:timeNo+6],fv100[timeNo:timeNo+6],ft2[timeNo:timeNo+6],fpsfc[timeNo:timeNo+6],xyList,siteGridListDF,site,1,))

                p[pSay].start()
          
                

                pSay+=1
            
        
        

        #readandwriteToMysqlNC(ftime[timeNo:timeNo+6],fu10[timeNo:timeNo+6],fv10[timeNo:timeNo+6],fu50[timeNo:timeNo+6],fv50[timeNo:timeNo+6],fu100[timeNo:timeNo+6],fv100[timeNo:timeNo+6],ft2[timeNo:timeNo+6],fpsfc[timeNo:timeNo+6],xyList,siteGridListDF,1)


                    

                              


   
                
                    # ws10,wd10=wind_convert(u10[0],u10[1])
                    # ws50,wd50=wind_convert(u10[0],u10[1])
                    # ws100,wd100=wind_convert(u10[0],u10[1])  

      

                # insertTXT="Insert Into tmpNCFileWS VALUES("+sTxt+")"

                # cursor=myDBConnect.cursor()

                # cursor.executemany(insertTXT,dataValueList)




    for timeNo in range(0,len(p)):
        print("processs "+str(timeNo)+" bitti")
        p[timeNo].join()
        print("processs "+str(timeNo)+" bitti")
    
    


  
    print("+++++++++++++++")
    print(datetime.now())
    
    return

    for site in siteList:
        

        siteGridListRowDF=siteGridListDF[siteGridListDF["siteId"]==site[0]]

        siteGridListRowDF=siteGridListRowDF[siteGridListRowDF["meteoModelListId"]==int(modelNo)]
                                   
        tableName="trainTable_"+str(site[0])
        
        mydbMongoCol = mydbMongoDB[tableName]


        tblCount=getTableCount(tableName)

        siteValueDictArr=[]
        vWS10={}
        vWS50={}
        vWS100={}
        vWD10={}
        vWD50={}
        vWD100={}
        vT2={}
        vRH={}
        vPrec={}
        vSnow={}
        vPSFC={}
        vGHI={}
        vDIFF={}

        for siteGridNo in range(0,siteGridListRowDF.shape[0]):
            modelGridNo=str(siteGridListRowDF.iloc[siteGridNo]["modelGridListId"])
            vWS10[modelGridNo]=[]
            vWS50[modelGridNo]=[]
            vWS100[modelGridNo]=[]
            vWD10[modelGridNo]=[]
            vWD50[modelGridNo]=[]
            vWD100[modelGridNo]=[]
            vT2[modelGridNo]=[]
            vRH[modelGridNo]=[]
            vPrec[modelGridNo]=[]
            vSnow[modelGridNo]=[]
            vPSFC[modelGridNo]=[]
            vGHI[modelGridNo]=[]
            vDIFF[modelGridNo]=[]
        
        columnNameValueList=""


        for timeNo in range(0,ftime.shape[0]-1):

            dataTime=pd.to_datetime(timeTotxt(ftime[timeNo]))+timedelta(hours=3)
            dataTimeHour=pd.to_datetime(datetime.strftime(dataTime,"%y-%m-%d")+" "+str(dataTime.hour)+":00")+timedelta(hours=1)


            siteGridValueDict={}  
            
            if dataTime>(startTime+timedelta(hours=9)) :
              

                for siteGridNo in range(0,siteGridListRowDF.shape[0]):
                    
                    modelGridNo=str(siteGridListRowDF.iloc[siteGridNo]["modelGridListId"])

                    tmpGridRow=libModelGridDF.loc[(libModelGridDF["gridNo"]== modelGridNo) &(libModelGridDF["dataTime"]==dataTimeHour)]
                    
                    isFindGrid=False

                    if tmpGridRow.shape[0]>0 :


                        isFindGrid=True
                        
                        tmpGridDict=dict(tmpGridRow.iloc[0]["Values"])
                        if columnNameValueList.__contains__(modelGridNo+"/"+str(dataTimeHour))==False:
                            if site[2]==1:
                                columnNameValueList+="|"+modelGridNo+"/"+str(dataTimeHour)

                                columnName="WS10_"+str(modelGridNo)+"_"+str(modelNo)
                                siteGridValueDict[columnName]=tmpGridDict[columnName]

                                columnName="WD10_"+str(modelGridNo)+"_"+str(modelNo)
                                siteGridValueDict[columnName]=tmpGridDict[columnName]

                                columnName="WS50_"+str(modelGridNo)+"_"+str(modelNo)
                                siteGridValueDict[columnName]=tmpGridDict[columnName]

                                columnName="WD50_"+str(modelGridNo)+"_"+str(modelNo)
                                siteGridValueDict[columnName]=tmpGridDict[columnName]

                                columnName="WS100_"+str(modelGridNo)+"_"+str(modelNo)
                                siteGridValueDict[columnName]=tmpGridDict[columnName]

                                columnName="WD100_"+str(modelGridNo)+"_"+str(modelNo)
                                siteGridValueDict[columnName]=tmpGridDict[columnName]

                                columnName="AVGT2_"+str(modelGridNo)+"_"+str(modelNo)
                                siteGridValueDict[columnName]=tmpGridDict[columnName]

                                columnName="MAXT2_"+str(modelGridNo)+"_"+str(modelNo)
                                siteGridValueDict[columnName]=tmpGridDict[columnName]

                                columnName="AVGPSFC_"+str(modelGridNo)+"_"+str(modelNo)
                                siteGridValueDict[columnName]=tmpGridDict[columnName]


                    xGridNo=siteGridListRowDF.iloc[siteGridNo]["xGrid"]
                    yGridNo=siteGridListRowDF.iloc[siteGridNo]["yGrid"]
                    
                    if dataTime.minute==0 and isFindGrid==False:
                                                                   

                        if site[2]==1:

                            tmpGridDict={}

                            wS,wD=wind_convert(fu10[timeNo,yGridNo,xGridNo],fv10[timeNo,yGridNo,xGridNo])

                            #10 m Rüzgar Hız 
                        
                            vWS10[modelGridNo].append(wS)
                                              
                            columnName="WS10_"+str(modelGridNo)+"_"+str(modelNo)

                            siteGridValueDict[columnName]=np.round(float(np.mean(vWS10[modelGridNo])),2)
                            tmpGridDict[columnName]=np.round(float(np.mean(vWS10[modelGridNo])),2)

                            #10 m Rüzgar Yönü
                            vWD10[modelGridNo].append(wD)
                        
                            columnName="WD10_"+str(modelGridNo)+"_"+str(modelNo)

                            siteGridValueDict[columnName]=np.round(float(meanWindDirection(vWD10[modelGridNo])),2)

                            tmpGridDict[columnName]=np.round(float(meanWindDirection(vWD10[modelGridNo])),2)

                            wS,wD=wind_convert(fu50[timeNo,yGridNo,xGridNo],fv50[timeNo,yGridNo,xGridNo])

                            #50 m Rüzgar Hızı
                            vWS50[modelGridNo].append(wS)
                        
                            columnName="WS50_"+str(modelGridNo)+"_"+str(modelNo)

                            siteGridValueDict[columnName]=np.round(float(np.mean(vWS50[modelGridNo])),2)
                            tmpGridDict[columnName]=np.round(float(np.mean(vWS50[modelGridNo])),2)

                            #50 m Rüzgar Yönü
                            vWD50[modelGridNo].append(wD)
                        
                            columnName="WD50_"+str(modelGridNo)+"_"+str(modelNo)
                            

                            siteGridValueDict[columnName]=np.round(float(meanWindDirection(vWD50[modelGridNo])),2)
                            tmpGridDict[columnName]=np.round(float(meanWindDirection(vWD50[modelGridNo])),2)

                            wS,wD=wind_convert(fu100[timeNo,yGridNo,xGridNo],fv100[timeNo,yGridNo,xGridNo])

                            #100 m Rüzgar Hızı
                            vWS100[modelGridNo].append(wS)
                        
                            columnName="WS100_"+str(modelGridNo)+"_"+str(modelNo)

                            siteGridValueDict[columnName]=np.round(float(np.mean(vWS100[modelGridNo])),2)
                            tmpGridDict[columnName]=np.round(float(np.mean(vWS100[modelGridNo])),2)

                            #50 m Rüzgar Yönü
                            vWD100[modelGridNo].append(wD)
                        
                            columnName="WD100_"+str(modelGridNo)+"_"+str(modelNo)

                            siteGridValueDict[columnName]=np.round(float(meanWindDirection(vWD100[modelGridNo])),2)
                            tmpGridDict[columnName]=np.round(float(meanWindDirection(vWD100[modelGridNo])),2)

                            #sıcaklık
                            vT2[modelGridNo].append(ft2[timeNo,yGridNo,xGridNo])

                            columnName="AVGT2_"+str(modelGridNo)+"_"+str(modelNo)

                            siteGridValueDict[columnName]=np.round(float(np.mean(vT2[modelGridNo])),2)
                            tmpGridDict[columnName]=np.round(float(np.mean(vT2[modelGridNo])),2)

                            columnName="MAXT2_"+str(modelGridNo)+"_"+str(modelNo)

                            siteGridValueDict[columnName]=np.round(float(np.max(vT2[modelGridNo])),2)
                            tmpGridDict[columnName]=np.round(float(np.max(vT2[modelGridNo])),2)

                            #basınç
                            vPSFC[modelGridNo].append(ft2[timeNo,yGridNo,xGridNo])

                            columnName="AVGPSFC_"+str(modelGridNo)+"_"+str(modelNo)

                            siteGridValueDict[columnName]=np.round(float(np.mean(vPSFC[modelGridNo])),2)

                            tmpGridDict[columnName]=np.round(float(np.mean(vPSFC[modelGridNo])),2)
                           
                            libModelGridDF.loc[len(libModelGridDF.index)]=[modelGridNo,dataTime,tmpGridDict]

                            # libModelGridDF=libModelGridDF.append({"gridNo":modelGridNo,"dataTime":dataTime,"Values":tmpGridDict},ignore_index=True)

                            


                        #dakika sıfır rüzgar Sonu:
                    else:

                        if site[2]==1 and isFindGrid==False:
                        
                            #10 m rüzgar
                            wS,wD=wind_convert(fu10[timeNo,yGridNo,xGridNo],fv10[timeNo,yGridNo,xGridNo])
                            vWS10[modelGridNo].append(wS)
                            vWD10[modelGridNo].append(wD)

                            #50 m rüzgar
                            wS,wD=wind_convert(fu50[timeNo,yGridNo,xGridNo],fv50[timeNo,yGridNo,xGridNo])
                            vWS50[modelGridNo].append(wS)
                            vWD50[modelGridNo].append(wD)

                            #100 m rüzgar
                            wS,wD=wind_convert(fu100[timeNo,yGridNo,xGridNo],fv100[timeNo,yGridNo,xGridNo])
                            vWS100[modelGridNo].append(wS)
                            vWD100[modelGridNo].append(wD)

                            #Sıcaklık
                            vT2[modelGridNo].append(ft2[timeNo,yGridNo,xGridNo])

                            #Basınç
                            vPSFC[modelGridNo].append(fpsfc[timeNo,yGridNo,xGridNo])
                        #dakika sıfır olmayan rüzgar Sonu:
                    

                #Grid Listesi Sonu
                if dataTime>(startTime+timedelta(hours=9)) :

                    if dataTime.minute==0:
                        #Sonraki Saat için sıfırla
                        for siteGridNo in range(0,siteGridListRowDF.shape[0]):
                            modelGridNo=str(siteGridListRowDF.iloc[siteGridNo]["modelGridListId"])
                            vWS10[modelGridNo]=[]
                            vWS50[modelGridNo]=[]
                            vWS100[modelGridNo]=[]
                            vWD10[modelGridNo]=[]
                            vWD50[modelGridNo]=[]
                            vWD100[modelGridNo]=[]
                            vT2[modelGridNo]=[]
                            vRH[modelGridNo]=[]
                            vPrec[modelGridNo]=[]
                            vSnow[modelGridNo]=[]
                            vPSFC[modelGridNo]=[]
                            vGHI[modelGridNo]=[]
                            vDIFF[modelGridNo]=[]
                        
                        siteGridValueDict["dataTime"]=pd.to_datetime(dataTime)

                        myquery = {"timeStamp": dataTime}

                        newvalues = { "$set": siteGridValueDict }

                        siteValueDictArr.append(UpdateMany(myquery,newvalues,upsert=True))
                        

                       
                        


        #Time Sonu

        # mydbMongoCol.bulk_write(siteValueDictArr)


        # updateTXT="Update "+tableName+" Set "+columnNameValueList+" where timestamp=%(dataTime)s"

        # cursor=myDBConnect.cursor()

        # cursor.executemany(updateTXT,siteValueDictArr)

        # myDBConnect.commit()

        print(site[1]+"/"+str(datetime.now()))

        
    #Site Listesi Sonu


def updateTrainTable(columnValueList,timestamp,tableName,dbConnect):

       
    updateTxt="Update "+tableName+" Set "+columnValueList+" where  timestamp='"+str(timestamp)+"'"
    

    
    


def countTimeStamp(tableName,timestamp):
    
    with open("config.json","r") as file:
        dbApiInfo=json.load(file)
    
    myDBConnect = mysql.connector.connect(host=dbApiInfo["dbInfo"]["dbAddress"], user = dbApiInfo["dbInfo"]["dbUsersName"], password=dbApiInfo["dbInfo"]["dbPassword"], database=dbApiInfo["dbInfo"]["database"])

    selectTxt="Select * From "+tableName+" where timestamp='"+str(timestamp)+"'"
    
    cursor=myDBConnect.cursor()
            
    cursor.execute(selectTxt)
    
    timeTable = cursor.fetchall()

    myDBConnect.commit()

    if len(timeTable)==0:
        
        insertTxt="Insert Into "+tableName+" (timestamp) VALUES('"+str(timestamp)+"')"
    
        cursor=myDBConnect.cursor()
            
        cursor.execute(insertTxt)
    
        myDBConnect.commit()

       
    return len(timeTable)


def addSiteGridList(site,modelNo,yakinEkle,uzakekle):


    siteLat=site[4]

    siteLng=site[5]
    
    with open("config.json","r") as file:

        dbApiInfo=json.load(file)

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

    
def addSiteTrainDataTable(tableName):
    try:
        with open("config.json","r") as file:
            dbApiInfo=json.load(file)


        myDBConnect = mysql.connector.connect(host=dbApiInfo["dbInfo"]["dbAddress"], user = dbApiInfo["dbInfo"]["dbUsersName"], password=dbApiInfo["dbInfo"]["dbPassword"], database=dbApiInfo["dbInfo"]["database"])

        cursor=myDBConnect.cursor()

        cursor.callproc("createSiteTrainDataTable",[tableName,])
        
        myDBConnect.close()

        return True

    except:

        return False




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



def runNCWriter(modelNo,filePath):


    with open("config.json","r") as file:

        dbApiInfo=json.load(file)

    myDBConnect = mysql.connector.connect(host=dbApiInfo["dbInfo"]["dbAddress"], user = dbApiInfo["dbInfo"]["dbUsersName"], password=dbApiInfo["dbInfo"]["dbPassword"], database=dbApiInfo["dbInfo"]["database"])

    selectTxt="Select * From siteList where epiasEIC > 0"

    cursor=myDBConnect.cursor()
            
    cursor.execute(selectTxt)
    
    siteTable = cursor.fetchall()
   

    selectTxt="Select siteId,modelGridListId,meteoModelListId,xGrid,yGrid From siteGridList where meteoModelListId="+str(modelNo)

    cursor.execute(selectTxt)
 
    siteGridTable = cursor.fetchall()
    
    siteGridTableDF=pd.DataFrame(siteGridTable,columns=["siteId","modelGridListId","meteoModelListId","xGrid","yGrid"])

   
    selectTxt="SELECT distinct(modelGridListId) FROM musteriDB.siteGridList where meteoModelListId="+str(modelNo)

    cursor=myDBConnect.cursor()
    
    cursor.execute(selectTxt)
    
    modelGridList = cursor.fetchall()

    modelGridListDF=pd.DataFrame(modelGridList,columns=["modelGridListId"])

    readwriteNCNew(filePath,siteTable,siteGridTableDF,modelNo)


#runNCWriter(sys.argv[1],sys.argv[2])

# runNCWriter('1',"/mnt/qNAPN2_vLM2_iMEFsys/NCFiles/WRF_GFS/wrfpost_2022-12-17_06.nc")

runNCWriter('1',"C:\\Users\\user\\Downloads\Gfs\\wrfpost_2022-11-30_00.nc")

# runNCWriter('1',"I:\\Belgeler\\Lazımlık\\Ozel\\modelWorks\\WRF_GFS\\wrfpost_2022-11-15_00.nc")



