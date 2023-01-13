from datetime import datetime
import mysql.connector
import math
import numpy
import json
import pandas as pd
import warnings

warnings.filterwarnings("ignore", category=DeprecationWarning)



print(__name__)

powerListDF=pd.DataFrame.from_dict([{"windSpeed":-9999,"siteId":-9999,"power":-9999}])

print("Yükleniyor....")

with open("config.json","r") as file:

        dbApiInfo=json.load(file)


myDBConnect = mysql.connector.connect(host=dbApiInfo["dbInfo"]["dbAddress"], user = dbApiInfo["dbInfo"]["dbUsersName"], password=dbApiInfo["dbInfo"]["dbPassword"], database="bigventusDB")


#PowerCurveTable
selectTxt="Select * From powerCurve order by windspeed"
    
cursor=myDBConnect.cursor()
            
cursor.execute(selectTxt)
    
powerCurveAirDensityTable = cursor.fetchall()

powerCurveAirDensityTableDF=pd.DataFrame(powerCurveAirDensityTable)

#wtgLibrary
selectTxt="Select * From wtgLibrary"
            
cursor.execute(selectTxt)
            
wtgLibTable = cursor.fetchall()

wtgLibTableDF=pd.DataFrame(wtgLibTable)

#powerCurveTable
selectTxt="Select * From powerCurve"

cursor=myDBConnect.cursor()
            
cursor.execute(selectTxt)
        
powerCurveTable = cursor.fetchall()

powerCurveTableDF=pd.DataFrame(powerCurveTable)

myDBConnect.close()

#siteWTGList
myDBConnect = mysql.connector.connect(host=dbApiInfo["dbInfo"]["dbAddress"], user = dbApiInfo["dbInfo"]["dbUsersName"], password=dbApiInfo["dbInfo"]["dbPassword"], database="musteriDB")

selectTxt="Select * From sitewtgList"

cursor=myDBConnect.cursor()
            
cursor.execute(selectTxt)
    
wtgTable = cursor.fetchall()

wtgTableDF=pd.DataFrame(wtgTable)

myDBConnect.close()





def calcWindPower(windSpeed,airDensity,powerCurveTable,powerCurveId):
        
   
        tmpAirDensityDiff=-9999
        selectedAirDensity=-9999


        for powerCurveRow in range(0,powerCurveTable.shape[0]):
        

            rowAirDensityDiff=abs(powerCurveTable.iloc[powerCurveRow][1]-airDensity)

            if tmpAirDensityDiff==-9999:

                tmpAirDensityDiff=rowAirDensityDiff
                selectedAirDensity=powerCurveTable.iloc[powerCurveRow][1]

            else:

                if rowAirDensityDiff<tmpAirDensityDiff:
                    tmpAirDensityDiff=rowAirDensityDiff
                    selectedAirDensity=powerCurveTable.iloc[powerCurveRow][1]



        global powerCurveAirDensityTableDF

        powerCurveAirDensityTable = powerCurveAirDensityTableDF[(powerCurveAirDensityTableDF[4]==int(powerCurveId)) & (powerCurveAirDensityTableDF[1]==selectedAirDensity)]
   
        if powerCurveAirDensityTable.shape[0]>0:

            maxWindSpeed=powerCurveAirDensityTable.iloc[powerCurveAirDensityTable.shape[0]-1][2]


    # en yüsek rüzgar üzerine üretim 0
            if windSpeed>maxWindSpeed:

                return 0

            if windSpeed==maxWindSpeed:

                return powerCurveAirDensityTable.iloc[powerCurveAirDensityTable.shape[0]-1][3]



            for rowCurve in range(0,powerCurveAirDensityTable.shape[0]-1):
        
                if powerCurveAirDensityTable.iloc[rowCurve][2]>windSpeed:
            
                    altRowNumber=rowCurve-1
                    ustRowNumber=rowCurve

                    windSpeedMainDiff=powerCurveAirDensityTable.iloc[ustRowNumber][2]-powerCurveAirDensityTable.iloc[altRowNumber][2]

                    powerMainDiff=powerCurveAirDensityTable.iloc[ustRowNumber][3]-powerCurveAirDensityTable.iloc[altRowNumber][3]

                    windSpeedDiff=windSpeed-powerCurveAirDensityTable.iloc[altRowNumber][2]

                    powerDiff=(windSpeedDiff*powerMainDiff)/windSpeedMainDiff

                    powerCalc=powerCurveAirDensityTable.iloc[altRowNumber][3]+powerDiff

                    return powerCalc

            return powerCurveAirDensityTable.iloc[powerCurveAirDensityTable.shape[0]-1][3]

                
  


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
  

    if len(wtgLibTable)<=0:

        return -9999

    selectTxt="Select * From powerCurve where powerCurveId="+str(wtgLibTable[0][3])

    cursor=myDBConnect.cursor()
            
    cursor.execute(selectTxt)
    
    myDBConnect.commit()

    powerCurveTable = cursor.fetchall()

    cPower=0

    for wtg in wtgLibTable:

        cTmpPower=calcWindPower(windSpeed,site[airDensityColumnNumber],powerCurveTable,str(wtg[3]))
       
        cPower+=cTmpPower*wtg[4]
    

    return cPower


def windPowerList(siteId,airDensity,windSpeedList):

    global powerListDF

    pwrList=[]

    for windSpeedCount in range(0,len(windSpeedList)):

        tmpPowerListDF=powerListDF[(powerListDF["windSpeed"]==windSpeedList[windSpeedCount])&(powerListDF["siteId"]==siteId)]
        
        if tmpPowerListDF.shape[0]>0:

            pwrList.append(tmpPowerListDF.iloc[0]["power"])

            bitti=""
            

    if len(pwrList)==len(windSpeedList):

        return  pwrList


    
    global wtgTableDF


    wtgTable=wtgTableDF[wtgTableDF[5]==siteId]



    if wtgTable.shape[0]<=0:
        return -9999

    
    global wtgLibTableDF

    wtgLibTable=wtgLibTableDF[wtgLibTableDF[0]==wtgTable.iloc[0][1]]

    if wtgLibTable.shape[0]<=0:

        return -9999

    

    global powerCurveTableDF

    wtgListPowerCurveID=wtgLibTable.iloc[0][3]

    
    powerCurveTable=powerCurveTableDF[powerCurveTableDF[4]==wtgListPowerCurveID]

    cpowerList=[]
    

    for wsValue in windSpeedList:

        
        cPower=0

        for wtg in range(0,wtgLibTable.shape[0]):
            

                

                cTmpPower=calcWindPower(wsValue,airDensity,powerCurveTable,str(wtgLibTable.iloc[wtg][3]))
                             
                cPower+=cTmpPower*wtgTable.iloc[0][4]
        
                cpowerList.append(cPower)

                powerListDF=powerListDF.append([{"windSpeed":wsValue,"siteId":siteId,"power":cPower}])




    return cpowerList




