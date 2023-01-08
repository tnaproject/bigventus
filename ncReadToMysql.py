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
    conv = 45 / math.atan(1)
    wind_dir = (270 - math.atan2(v,u) * conv) % 360
    wind_speed = math.sqrt(math.pow(u,2) + math.pow(v,2))
    #wind_dir = 180 + (180 / math.pi) * math.atan2(v,u)
    return wind_speed,wind_dir

def readwriteNC(filePath,siteList,siteGridListDF,modelGridListDF,modelNo):

    baslangicZmn=datetime.now()

    f=nc.Dataset(filePath)

    fu10 = f.variables['U10']
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
    
    with open("config.json","r") as file:
        dbApiInfo=json.load(file)

    myDBConnect = mysql.connector.connect(host=dbApiInfo["dbInfo"]["dbAddress"], user = dbApiInfo["dbInfo"]["dbUsersName"], password=dbApiInfo["dbInfo"]["dbPassword"], database=dbApiInfo["dbInfo"]["database"])
    
    siteTimeList=[]
    
    for gridRow in range(0,modelGridListDF.size):
        
        gecensure=((datetime.now()-baslangicZmn).total_seconds()/60)

        print(str(gridRow+1)+"/"+str(gecensure))

        #testDF=pd.DataFrame(siteList,columns=[""])

        startTime=pd.to_datetime(timeTotxt(ftime[0]))+timedelta(hours=3)
     
        
        for site in siteList:

            siteGridListRowDF=siteGridListDF[siteGridListDF["siteId"]==site[0]]

            siteGridListRowDF=siteGridListRowDF[siteGridListRowDF["meteoModelListId"]==int(modelNo)]

            siteGridListRowDF=siteGridListRowDF[siteGridListRowDF["modelGridListId"]==modelGridListDF.loc[gridRow]["modelGridListId"]]
                       
            tableName="trainTable_"+str(site[0])
                      
            vWS10=[]
            vWS50=[]
            vWS100=[]
            vT2=[]
            vRH=[]
            vPrec=[]
            vSnow=[]
            vPSFC=[]
            vGHI=[]
            vDIFF=[]
            vCloundIndex=[]

            if siteGridListRowDF.shape[0]>0:



                print(str(datetime.now())+" Grid" +"/"+str(site[1])+"/"+str(siteGridListDF.loc[gridRow]["modelGridListId"]))
                
                xGridNo=siteGridListRowDF.iloc[0]["xGrid"]

                yGridNo=siteGridListRowDF.iloc[0]["yGrid"]

                columnBool=False

                tblCount=getTableCount("trainTable_"+str(site[0]))

                if tblCount==0:
                    addSiteTrainDataTable("trainTable_"+str(site[0]))

                valueArr=[]
                columnValueList=""

                lastTime=ftime[ftime.shape[0]-1]

                lastDate=startTime+timedelta(hours=96)
                
                # if (pd.to_datetime(timeTotxt(ftime[ftime.shape[0]-1]))-datetime.now()).total_seconds()<0:
                #     lastDate=startTime+timedelta(hours=34)

                dataTime=pd.to_datetime(timeTotxt(ftime[ftime.shape[0]-1]))

                for timeNo in range(0,ftime.shape[0]-1):
                    valueList=[]
                    dataTime=pd.to_datetime(timeTotxt(ftime[timeNo]))+timedelta(hours=3)
            
                    if dataTime>(startTime+timedelta(hours=9)) :
                        
                        if pd.to_datetime(dataTime).minute==0:
                                                        
                            if (tableName in siteTimeList)==False:
                                timestampCount=countTimeStamp(tableName,dataTime)
                                siteTimeList.append(tableName)

                            if len(vWS10)>0:
                            
                                if site[2]==1:
                                    
                                    powerWSList=[]
                                    wS,wD=wind_convert(fu10[timeNo,yGridNo,xGridNo],fv10[timeNo,yGridNo,xGridNo])
                                    vWS10.append(wS)
                                    vWD10=wD

                                    columName="WS10_"+str(siteGridListDF.loc[gridRow]["modelGridListId"])+"_"+str(modelNo)

                                    if columnBool==False:
                                        if getColumnCountByTable(tableName,columName)==0:
                                            addColumnToTable(tableName,columName+" double DEFAULT NULL")
                                
                                        if columnValueList=="":
                                            columnValueList=columName+"=%s"
                                        else:
                                            columnValueList+=","+columName+"=%s"

                                    
                                     
                                    columName="f_WS10_"+str(siteGridListDF.loc[gridRow]["modelGridListId"])+"_"+str(modelNo)

                                    ws10=np.round(np.mean(vWS10),3)
                                    powerWSList.append(ws10)


                                    if columnBool==False:
                                        if getColumnCountByTable(tableName,columName)==0:
                                            addColumnToTable(tableName,columName+" double DEFAULT NULL")

                                


                                ##GÜÇ KONTROLÜ EKLE
                                ##3 haneye yuvarla
                                    wS,wD=wind_convert(fu50[timeNo,yGridNo,xGridNo],fv50[timeNo,yGridNo,xGridNo])
                                    vWS50.append(wS)
                                    vWD50=wD

                                    columName="WS50_"+str(siteGridListDF.loc[gridRow]["modelGridListId"])+"_"+str(modelNo)

                                    if columnBool==False:
                                        if getColumnCountByTable(tableName,columName)==0:
                                            addColumnToTable(tableName,columName+" double DEFAULT NULL")

                                
                                        if columnValueList=="":
                                            columnValueList=columName+"=%s"
                                        else:
                                            columnValueList+=","+columName+"=%s"

                                    

                                    columName="f_WS50_"+str(siteGridListDF.loc[gridRow]["modelGridListId"])+"_"+str(modelNo)
                                                                      
                                    

                                    ws50=np.round(np.mean(vWS50),3)
                                    powerWSList.append(ws50)


                                    if columnBool==False:
                                        if getColumnCountByTable(tableName,columName)==0:
                                            addColumnToTable(tableName,columName+" double DEFAULT NULL")

                                

                                    wS,wD=wind_convert(fu100[timeNo,yGridNo,xGridNo],fv100[timeNo,yGridNo,xGridNo])
                                    vWS100.append(wS)
                                    vWD100=wD

                                    columName="WS100_"+str(siteGridListDF.loc[gridRow]["modelGridListId"])+"_"+str(modelNo)

                                    if columnBool==False:
                                        if getColumnCountByTable(tableName,columName)==0:
                                            addColumnToTable(tableName,columName+" double DEFAULT NULL")
    
                                        if columnValueList=="":
                                            columnValueList=columName+"=%s"
                                        else:
                                            columnValueList+=","+columName+"=%s"                     
                            
                                    

                                    columName="f_WS100_"+str(siteGridListDF.loc[gridRow]["modelGridListId"])+"_"+str(modelNo)

                                    

                                    ws100=np.round(np.mean(vWS100),3)
                                    powerWSList.append(ws100)

                                    if columnBool==False:
                                        if getColumnCountByTable(tableName,columName)==0:
                                            addColumnToTable(tableName,columName+" double DEFAULT NULL")

                               

                                    columName="T2_"+str(siteGridListDF.loc[gridRow]["modelGridListId"])+"_"+str(modelNo)

                                    if columnBool==False:
                                        if getColumnCountByTable(tableName,columName)==0:
                                            addColumnToTable(tableName,columName+" double DEFAULT NULL")

                                        if columnValueList=="":
                                            columnValueList=columName+"=%s"
                                        else:
                                            columnValueList+=","+columName+"=%s"

                                    vT2.append(ft2[timeNo,yGridNo,xGridNo])



                                    t2=float(np.round(np.mean(vT2),3))

                                    columName="PSFC_"+str(siteGridListDF.loc[gridRow]["modelGridListId"])+"_"+str(modelNo)

                                    vPSFC.append(fpsfc[timeNo,yGridNo,xGridNo])
                                    if columnBool==False:
                                        if getColumnCountByTable(tableName,columName)==0:
                                            addColumnToTable(tableName,columName+" double DEFAULT NULL")

                                    
                                
                                        if columnValueList=="":
                                            columnValueList=columName+"=%s"
                                        else:
                                            columnValueList+=","+columName+"=%s"

                                    psfc=float(np.round(np.mean(vPSFC),3))

                                    # powerList=calcWindPower.windPowerLİst(site,16,powerWSList)

                                    # for pwr in range(0,len(powerList)):
                                    #     if powerList[pwr]>site[6]:
                                    #         powerList[pwr]=site[6]

                                        

                                    # if columnBool==False:
                                    #     columnValueList+=",f_WS10_"+str(siteGridListDF.loc[gridRow]["modelGridListId"])+"_"+str(modelNo)+"=%s"
                                    #     columnValueList+=",f_WS50_"+str(siteGridListDF.loc[gridRow]["modelGridListId"])+"_"+str(modelNo)+"=%s"
                                    #     columnValueList+=",f_WS100_"+str(siteGridListDF.loc[gridRow]["modelGridListId"])+"_"+str(modelNo)+"=%s"
                                    
                                    # valueArr.append((ws10,ws50,ws100,t2,psfc,powerList[0],powerList[1],powerList[2],pd.to_datetime(dataTime)))
                                    valueArr.append((ws10,ws50,ws100,t2,psfc,pd.to_datetime(dataTime)))

                                columnBool=True

                        else:

                            if site[2]==1:

                                wS,wD=wind_convert(fu10[timeNo,yGridNo,xGridNo],fv10[timeNo,yGridNo,xGridNo])
                                vWS10.append(wS)
                                    
                                wS,wD=wind_convert(fu50[timeNo,yGridNo,xGridNo],fv50[timeNo,yGridNo,xGridNo])
                                vWS50.append(wS)
                                    
                                wS,wD=wind_convert(fu100[timeNo,yGridNo,xGridNo],fv100[timeNo,yGridNo,xGridNo])
                                vWS100.append(wS)

                                vT2.append(ft2[timeNo,yGridNo,xGridNo])

                                vPSFC.append(fpsfc[timeNo,yGridNo,xGridNo])           

                #updateTrainTable(columnValueList,dataTime,tableName,myDBConnect)               
                updateTXT="Update "+tableName+" Set "+columnValueList+" where timestamp=%s"
                
                cursor=myDBConnect.cursor()
                
                cursor.executemany(updateTXT,valueArr)
    
                myDBConnect.commit()
                
                print(dataTime)
                print("plantCount"+str(len(siteTimeList)))

                
    myDBConnect.close()


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

    readwriteNC(filePath,siteTable,siteGridTableDF,modelGridListDF,modelNo)


#runNCWriter(sys.argv[1],sys.argv[2])

runNCWriter('1',"/mnt/qNAPN2_vLM2_iMEFsys/NCFiles/WRF_GFS/wrfpost_2022-12-17_06.nc")

# runNCWriter('1',"D:\\bigventusNC\\Gfs\\wrfpost_2022-11-30_00.nc")



