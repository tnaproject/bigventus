import netCDF4 as nc
import numpy as np
import pandas as pd
import mysql.connector 
from datetime import datetime
import math
from epiasFuncs import getTableCount,getColumnCountByTable,addColumnToTable
import json

def wind_convert(u,v):
    conv = 45 / math.atan(1)
    wind_dir = (270 - math.atan2(v,u) * conv) % 360
    wind_speed = math.sqrt(math.pow(u,2) + math.pow(v,2))
    #wind_dir = 180 + (180 / math.pi) * math.atan2(v,u)
    return wind_speed,wind_dir

def readwriteNCGFS(filePath,siteList,siteGridListDF):


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


    for site in siteList:
        #testDF=pd.DataFrame(siteList,columns=[""])

        siteGridListRowDF=siteGridListDF[siteGridListDF["siteId"]==site[0]]

        siteGridListRowDF=siteGridListRowDF[siteGridListRowDF["meteoModelListId"]==1]
        
        print(siteGridListRowDF)

        tblCount=getTableCount("trainTable_"+str(site[0]))

        if tblCount==0:
            addSiteTrainDataTable("trainTable_"+str(site[0]))

        if siteGridListDF.shape[0]==0:
            addSiteGridList(site,1,1,2)




        
        for gridRow in siteGridListRowDF.index:
            xGrid=siteGridListRowDF["xGrid"][gridRow]
            yGrid=siteGridListRowDF["yGrid"][gridRow]
            print(ft2[0,yGrid,xGrid])



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


    if siteLat<selectedGridYLat:
        ustLatGridNo=selectedGridY+yakinEkle
        altLatGridNo=selectedGridY-uzakekle
    else:
        ustLatGridNo=selectedGridY+uzakekle
        altLatGridNo=selectedGridY-yakinEkle
    if siteLng<selectedGridXLon:
        ustLonGridNo=selectedGridX+yakinEkle
        altLonGridNo=selectedGridX-uzakekle



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


with open("config.json","r") as file:


    dbApiInfo=json.load(file)

myDBConnect = mysql.connector.connect(host=dbApiInfo["dbInfo"]["dbAddress"], user = dbApiInfo["dbInfo"]["dbUsersName"], password=dbApiInfo["dbInfo"]["dbPassword"], database=dbApiInfo["dbInfo"]["database"])

selectTxt="Select * From siteList where epiasEIC > 0"

cursor=myDBConnect.cursor()
            
cursor.execute(selectTxt)
    
siteTable = cursor.fetchall()


selectTxt="Select * From siteGridList"


cursor.execute(selectTxt)


 
siteGridTable = cursor.fetchall()

myDBConnect.close()

siteGridTableDF=pd.DataFrame(siteGridTable,columns=["id","siteId","modelGridList","meteoModelListId","useInTrain","calcParamList","calcType","xGrid","yGrid"])

readwriteNCGFS("I:\\Belgeler\\Lazımlık\\Ozel\\modelWorks\\WRF_GFS\\wrfpost_2022-11-15_00.nc",siteTable,siteGridTableDF)

