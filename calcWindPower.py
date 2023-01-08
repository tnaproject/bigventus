from datetime import datetime
import mysql.connector
import math
import numpy
import json

def calcWindPower(windSpeed,airDensity,powerCurveTable,powerCurveId):
        
    with open("config.json","r") as file:

        dbApiInfo=json.load(file)


    myDBConnect = mysql.connector.connect(host=dbApiInfo["dbInfo"]["dbAddress"], user = dbApiInfo["dbInfo"]["dbUsersName"], password=dbApiInfo["dbInfo"]["dbPassword"], database="bigventusDB")

    tmpAirDensityDiff=-9999
    selectedAirDensity=-9999
    for powerCurveRow in powerCurveTable:
        

        rowAirDensityDiff=abs(powerCurveRow[1]-airDensity)

        if tmpAirDensityDiff==-9999:

            tmpAirDensityDiff=rowAirDensityDiff
            selectedAirDensity=powerCurveRow[1]

        else:

            if rowAirDensityDiff<tmpAirDensityDiff:
                tmpAirDensityDiff=rowAirDensityDiff
                selectedAirDensity=powerCurveRow[1]



    selectTxt="Select * From powerCurve where powerCurveId="+str(powerCurveId)+" and airdensity="+str(selectedAirDensity)+" order by windspeed"
    
    cursor=myDBConnect.cursor()
            
    cursor.execute(selectTxt)
    
    powerCurveAirDensityTable = cursor.fetchall()
   

    maxWindSpeed=powerCurveAirDensityTable[len(powerCurveAirDensityTable)-1][2]

    myDBConnect.close()

    # en yüsek rüzgar üzerine üretim 0
    if windSpeed>maxWindSpeed:

        return 0

    if windSpeed==maxWindSpeed:
        return powerCurveAirDensityTable[len(powerCurveAirDensityTable)-1][3]



    for rowCurve in range(0,len(powerCurveAirDensityTable)):
        
        if powerCurveAirDensityTable[rowCurve][2]>windSpeed:
            
            altRowNumber=rowCurve-1
            ustRowNumber=rowCurve

            windSpeedMainDiff=powerCurveAirDensityTable[ustRowNumber][2]-powerCurveAirDensityTable[altRowNumber][2]

            powerMainDiff=powerCurveAirDensityTable[ustRowNumber][3]-powerCurveAirDensityTable[altRowNumber][3]

            windSpeedDiff=windSpeed-powerCurveAirDensityTable[altRowNumber][2]

            powerDiff=(windSpeedDiff*powerMainDiff)/windSpeedMainDiff

            powerCalc=powerCurveAirDensityTable[altRowNumber][3]+powerDiff

            return powerCalc



def windPower(site,airDensityColumnNumber,windSpeed):

    with open("config.json","r") as file:

        dbApiInfo=json.load(file)


    myDBConnect = mysql.connector.connect(host=dbApiInfo["dbInfo"]["dbAddress"], user = dbApiInfo["dbInfo"]["dbUsersName"], password=dbApiInfo["dbInfo"]["dbPassword"], database="musteriDB")

    selectTxt="Select * From sitewtgList where siteId="+str(site[0])


    cursor=myDBConnect.cursor()
            
    cursor.execute(selectTxt)
    
    wtgTable = cursor.fetchall()

    myDBConnect.close()

    if len(wtgTable)<=0:
        return -9999

    myDBConnect = mysql.connector.connect(host=dbApiInfo["dbInfo"]["dbAddress"], user = dbApiInfo["dbInfo"]["dbUsersName"], password=dbApiInfo["dbInfo"]["dbPassword"], database="bigventusDB")

    selectTxt="Select * From wtgLibrary where id="+str(wtgTable[0][1])

    cursor=myDBConnect.cursor()
            
    cursor.execute(selectTxt)
    
    wtgLibTable = cursor.fetchall()

    myDBConnect.close()

    if len(wtgLibTable)<=0:

        return -9999


    cPower=calcWindPower(windSpeed,site[airDensityColumnNumber],wtgLibTable[0][3])
    
    cPower=cPower*wtgTable[0][4]
    

    return cPower


def windPowerLİst(site,airDensityColumnNumber,windSpeedList):

    with open("config.json","r") as file:

        dbApiInfo=json.load(file)


    myDBConnect = mysql.connector.connect(host=dbApiInfo["dbInfo"]["dbAddress"], user = dbApiInfo["dbInfo"]["dbUsersName"], password=dbApiInfo["dbInfo"]["dbPassword"], database="musteriDB")

    selectTxt="Select * From sitewtgList where siteId="+str(site[0])


    cursor=myDBConnect.cursor()
            
    cursor.execute(selectTxt)
    
    wtgTable = cursor.fetchall()

    myDBConnect.close()

    if len(wtgTable)<=0:
        return -9999

    myDBConnect = mysql.connector.connect(host=dbApiInfo["dbInfo"]["dbAddress"], user = dbApiInfo["dbInfo"]["dbUsersName"], password=dbApiInfo["dbInfo"]["dbPassword"], database="bigventusDB")

    selectTxt="Select * From wtgLibrary where id="+str(wtgTable[0][1])

    cursor=myDBConnect.cursor()
            
    cursor.execute(selectTxt)
    
    wtgLibTable = cursor.fetchall()
  

    if len(wtgLibTable)<=0:

        return -9999

    
    
    selectTxt="Select * From powerCurve where powerCurveId="+str(wtgLibTable[0][3])

    cursor=myDBConnect.cursor()
            
    cursor.execute(selectTxt)
    
    powerCurveTable = cursor.fetchall()


    cpowerList=[]
    
    for wsValue in windSpeedList:

        cPower=calcWindPower(wsValue,site[airDensityColumnNumber],powerCurveTable,str(wtgLibTable[0][3]))
    
        cpowerList.append(cPower*wtgTable[0][4])

    return cpowerList




