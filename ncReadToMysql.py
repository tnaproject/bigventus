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





def readwriteNCNew(filePath,siteList,siteGridListDF,modelNo):

    baslangicZmn=datetime.now()
    print("dataOkuyor")

    f=nc.Dataset(filePath)
    fur10=f.variables['U10'][0][:][:]
    fu10 = f.variables['U10'][:].data   
    fu10 = f.variables['U10'][:].data 
    fu50 = f.variables['U50'][:].data 
    fu100 = f.variables['U100'][:].data 
    fu200 = f.variables['U200'][:].data 
    fv10 = f.variables['V10'][:].data 
    fv50   = f.variables['V50'][:].data 
    fv100  = f.variables['V100'][:].data 
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
    
 




    with open("config.json","r") as file:
        dbApiInfo=json.load(file)

    myDBConnect = mysql.connector.connect(host=dbApiInfo["dbInfo"]["dbAddress"], user = dbApiInfo["dbInfo"]["dbUsersName"], password=dbApiInfo["dbInfo"]["dbPassword"], database=dbApiInfo["dbInfo"]["database"])
    

    #server
    myclient = MongoClient() 
     
    # connecting with the portnumber and host 
    myclient = MongoClient("mongodb://localhost:27017/")
    mydbMongoDB = myclient["dbVentus"] #db

    


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
    for xSay in range(0,siteGridListDF.shape[0]):

        if xList.__contains__(str(siteGridListDF.iloc[siteGridNo]["xGrid"])):
            if xList=="":
                xList=str(siteGridListDF.iloc[siteGridNo]["xGrid"])
            else:
                xList+="|"+str(siteGridListDF.iloc[siteGridNo]["xGrid"])


        if yList.__contains__(str(siteGridListDF.iloc[siteGridNo]["yGrid"])):
            if xList=="":
                yList=str(siteGridListDF.iloc[siteGridNo]["yGrid"])
            else:
                yList+="|"+str(siteGridListDF.iloc[siteGridNo]["yGrid"])


    for i in range(0,483):
        if sTxt=="":
            sTxt="%s"
        else:
            sTxt+=",%s"

    for timeNo in range(0,ftime.shape[0]-1):

            dataTime=pd.to_datetime(timeTotxt(ftime[timeNo]))+timedelta(hours=3)
         


            dataValueList=[]

            siteGridValueDict={}  
            kolonSayi=0
            
            if dataTime>(startTime+timedelta(hours=9)) :
                xGridArr=xList.split("|")
                yGridArr=xList.split("|")

                for xGridNo in range(0,len(xGridArr)):
                    
                    u10=np.array([])
                    u50=np.array([])
                    u100=np.array([])
                    v10=np.array([])
                    v50=np.array([])
                    v100=np.array([]) 

                    for yGridNo in range(0,len(yGridArr)):
                        

                         if columnNameValueList.__contains__(str(yGridNo)+"/"+str(dataTime))==False:
                                kolonSayi+=1
                                
                                columnNameValueList+=str(yGridArr[yGridNo])

                                uValue=np.array(np.ma.getdata(fu10[timeNo][yGridNo][:]))
                                vValue=np.array(np.ma.getdata(fv10[timeNo][yGridNo][:]))

                                ws,wd=wind_convert(uValue,vValue)

                                ws=np.append("ws10",ws)

                                ws=np.append(int(yGridNo),ws)

                                ws=np.append(dataTime,ws)

                                tubWS=tuple(ws)

                                dataValueList.append(tubWS)

                                uValue=np.array(np.ma.getdata(fu50[timeNo][yGridNo][:]))
                                vValue=np.array(np.ma.getdata(fv50[timeNo][yGridNo][:]))

                                ws,wd=wind_convert(uValue,vValue)

                                ws=np.append("ws50",ws)
                            
                                ws=np.append(int(yGridNo),ws)

                                ws=np.append(dataTime,ws)

                                tubWS=tuple(ws)

                                dataValueList.append(tubWS)


                                uValue=np.array(np.ma.getdata(fu100[timeNo][yGridNo][:]))
                                vValue=np.array(np.ma.getdata(fv100[timeNo][yGridNo][:]))

                                ws,wd=wind_convert(uValue,vValue)

                                ws=np.append("ws50",ws)
                            
                                ws=np.append(int(yGridNo),ws)

                            ws=np.append(dataTime,ws)

                            tubWS=tuple(ws)

                            dataValueList.append(tubWS)

                            # ws,wd=wind_convert(fu10[timeNo,yGridNo,xGridNo],fv10[timeNo,yGridNo,xGridNo])
                                
                            # columnName="WS10_"+str(modelGridNo)+"_"+str(modelNo)

                            # dataValueList.append((modelGridNo,dataTime,columnName,ws))
                               
                            # columnName="WD10_"+str(modelGridNo)+"_"+str(modelNo)

                            # dataValueList.append((modelGridNo,dataTime,columnName,wd))

                            # ws,wd=wind_convert(fu50[timeNo,yGridNo,xGridNo],fv50[timeNo,yGridNo,xGridNo])
                                
                            # columnName="WS50_"+str(modelGridNo)+"_"+str(modelNo)

                            # dataValueList.append((modelGridNo,dataTime,columnName,ws))
                               
                            # columnName="WD50_"+str(modelGridNo)+"_"+str(modelNo)

                            # dataValueList.append((modelGridNo,dataTime,columnName,wd))

                            # ws,wd=wind_convert(fu100[timeNo,yGridNo,xGridNo],fv100[timeNo,yGridNo,xGridNo])
                                
                            # columnName="WS100_"+str(modelGridNo)+"_"+str(modelNo)

                            # dataValueList.append((modelGridNo,dataTime,columnName,ws))
                               
                            # columnName="WD100_"+str(modelGridNo)+"_"+str(modelNo)

                            # dataValueList.append((modelGridNo,dataTime,columnName,wd))

                            # columnName="T2_"+str(modelGridNo)+"_"+str(modelNo)

                            # dataValueList.append((modelGridNo,dataTime,columnName,float(ft2[timeNo,yGridNo,xGridNo])))

                            # columnName="PSFC_"+str(modelGridNo)+"_"+str(modelNo)

                            #dataValueList.append((modelGridNo,dataTime,columnName,float(fpsfc[timeNo,yGridNo,xGridNo])))
                    else:
                            oldu=""

                            oldyy=""
                
                print(datetime.now())
                # insertTXT="Insert Into tmpNCFileWS VALUES("+sTxt+")"

                # cursor=myDBConnect.cursor()

                # cursor.executemany(insertTXT,dataValueList)

                # myDBConnect.commit()


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

runNCWriter('1',"D:\\bigventusNC\\Gfs\\wrfpost_2022-11-30_00.nc")

# runNCWriter('1',"I:\\Belgeler\\Lazımlık\\Ozel\\modelWorks\\WRF_GFS\\wrfpost_2022-11-15_00.nc")



