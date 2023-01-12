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
import concurrent.futures
import warnings

warnings.filterwarnings("ignore")




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


def readNCAndWriteToMongo(fTimeArr,fu10,fv10,fu50,fv50,fu100,fv100,ft2,fpsfc,xyGridList,siteGridList,modelNo):
   
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

           
            pw10=calcWindPower.windPowerList(tmpSiteGridList.iloc[i]["siteId"],1.225,ws10)
            
            pw50=calcWindPower.windPowerList(tmpSiteGridList.iloc[i]["siteId"],1.225,ws50)

            pw100=calcWindPower.windPowerList(tmpSiteGridList.iloc[i]["siteId"],1.225,ws100)

            siteGridValueDict["f_WS10_"+str(modelGridNo)+"_"+str(modelNo)]=np.round(pw10[0],2)
            siteGridValueDict["f_WS50_"+str(modelGridNo)+"_"+str(modelNo)]=np.round(pw50[1],2)
            siteGridValueDict["f_WS100_"+str(modelGridNo)+"_"+str(modelNo)]=np.round(pw100[2],2)


            newvalues = { "$set": siteGridValueDict }

            myquery = {"dataTime": dataTime}

            siteDictList[tableName].append(UpdateMany(myquery,newvalues,upsert=True))
            
   
    siteList=siteKontrolText.split("|")
    
    myclient = MongoClient("mongodb://89.252.157.127:27017/")
    
    mydbMongoDB = myclient["dbVentusDB"] #db
   

    for site in siteList:

        if site!="":
            
            mongoCol= mydbMongoDB[site]

            mongoCol.create_index("dataTime", unique = True)

            mongoCol.bulk_write(siteDictList[site])

    myclient.close()
            
    return str(dataTime)+" bitti "+str(datetime.now())



        

            

        
    # f.close()



def readwriteNCWithPool(ftime,fu10,fv10,fu50,fv50,fu100,fv100,ft2,fpsfc,siteGridListDF,modelNo,dataStartHourTime,dataEndHourTime):
      
   
    #ilk Veri Zamanı
    startTime=pd.to_datetime(timeTotxt(ftime[0]))+timedelta(hours=3)
  
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

    
    for timeNo in range(1,ftime.shape[0],6):

        dataTime=pd.to_datetime(timeTotxt(ftime[timeNo]))+timedelta(hours=3)

        hourDiff=(dataTime-startTime).total_seconds()/60

        hourDiff=hourDiff/60
        
        if (hourDiff>=dataStartHourTime and hourDiff<dataEndHourTime):
       
           # readandwriteToMysqlNC(ftime[timeNo:timeNo+6],fu10[timeNo:timeNo+6],fv10[timeNo:timeNo+6],fu50[timeNo:timeNo+6],fv50[timeNo:timeNo+6],fu100[timeNo:timeNo+6],fv100[timeNo:timeNo+6],ft2[timeNo:timeNo+6],fpsfc[timeNo:timeNo+6],xyList,siteGridListDF,modelNo)
       
                  
            if __name__ == '__main__':
            
            
                    with concurrent.futures.ThreadPoolExecutor(max_workers = 10) as executor:
                
                        threadAnswer={executor.submit(readNCAndWriteToMongo,ftime[timeNo:timeNo+6],fu10[timeNo:timeNo+6],fv10[timeNo:timeNo+6],fu50[timeNo:timeNo+6],fv50[timeNo:timeNo+6],fu100[timeNo:timeNo+6],fv100[timeNo:timeNo+6],ft2[timeNo:timeNo+6],fpsfc[timeNo:timeNo+6],xyList,siteGridListDF,modelNo)}

               
                        for future in concurrent.futures.as_completed(threadAnswer):

                            try:
                                cevap=future.result()

                                print("İlk Saat "+str(dataStartHourTime)+"/"+cevap+"/"+str(datetime.now()))

                            except Exception as exc:

                                

                                print("Hata"+"/"+exc+"/"+str(datetime.now()))


        else:

            print("İlk Saat "+str(dataStartHourTime)+"Geçti>"+str(dataTime)+"/"+str(datetime.now()))
            

    
    
 
  
def threadRunnner(filePath,siteList,siteGridListDF,modelNo,startHour,endHour):

    print("Dosya Okunuyor")

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

    f.close()

    print(datetime.now())

    print("Yazmaya Başlıyor")

    readwriteNCWithPool(ftime,fu10,fv10,fu50,fv50,fu100,fv100,ft2,fpsfc,siteGridListDF,modelNo,startHour,endHour)

    print(datetime.now())

    print("Yazma Bitti")




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

    # startHour=abs(int(sys.argv[1]))

    # endHour=abs(int(sys.argv[2]))

    startHour=10

    endHour=20

    threadRunnner(filePath,siteTable,siteGridTableDF,modelNo,startHour,endHour)



#runNCWriter(sys.argv[1],sys.argv[2])

# runNCWriter('1',"/mnt/qNAPN2_vLM2_iMEFsys/NCFiles/WRF_GFS/wrfpost_2022-12-17_06.nc")

# runNCWriter('1',"C:\\Users\\user\\Downloads\Gfs\\wrfpost_2022-11-30_00.nc")

runNCWriter('1',"I:\\Belgeler\\Lazımlık\\Ozel\\modelWorks\\WRF_GFS\\wrfpost_2022-11-15_00.nc")



