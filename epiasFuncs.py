import mysql.connector
import json
import requests
import pandas as pd
from datetime import datetime
from datetime import timedelta




def updateEpiasProductions():
   
    with open("config.json","r") as file:

        dbApiInfo=json.load(file)

    myDBConnect = mysql.connector.connect(host=dbApiInfo["dbInfo"]["dbAddress"], user = dbApiInfo["dbInfo"]["dbUsersName"], password=dbApiInfo["dbInfo"]["dbPassword"], database=dbApiInfo["dbInfo"]["database"])

    selectTxt="Select id,siteName,epiasEIC,epiasCompanyId,epiasOrgEIC,realProductionDateTime From siteList where epiasEIC > 0"

    cursor=myDBConnect.cursor(buffered=True)
            
    cursor.execute(selectTxt)
    
    siteTable = cursor.fetchall()
 
    for site in siteTable:
        
        if getTableCount("siteDataList_"+str(site[0]))==0:
            
            addSiteDataTable("siteDataList_"+str(site[0]))

        startDate="2018-09-01 00:00"
        

        if site[5] is not None:

            if (datetime.now()-site[5]).days<10:

                startDate=str(datetime.now()-timedelta(days=10))

            else:

                startDate=str(site[5]+timedelta(days=1))


              
        productionDF=getEpiasProduction(site[2],startDate,startDate)
        
        updateTXT="Update siteDataList_"+str(site[0]) +" Set realProduction=%s where timeStamp=%s"
       

        dataList=[]

        lastDateTime=""

        for row in range(0,productionDF.shape[0]):

            dateTMP=pd.to_datetime(productionDF["date"][row])

            dateTMP=str(dateTMP).replace("+03:00","")
            
            lastDateTime=dateTMP

            dataList.append((str(productionDF["total"][row]*1000),dateTMP))
            

        cursor.executemany(updateTXT,dataList)

        myDBConnect.commit()

        rowCount=cursor.rowcount

        if rowCount<=0:

            lastDateTime=""

            dataList=[]

            for row in range(0,productionDF.shape[0]):

                dateTMP=pd.to_datetime(productionDF["date"][row])

                dateTMP=str(dateTMP).replace("+03:00","")
            
                lastDateTime=dateTMP

                dataList.append((dateTMP,str(productionDF["total"][row]*1000)))


            insertTXT="Insert Into siteDataList_"+str(site[0]) +" (timeStamp,realProduction) VALUES(%s,%s)"

            cursor.executemany(insertTXT,dataList)

            myDBConnect.commit()

            rowCount=cursor.rowcount


            
        

        if lastDateTime=="":
            
            lastDateTime=str(pd.to_datetime(startDate))




        updateTXT="Update siteList set realProductionDateTime='"+str(lastDateTime)+"' where id='"+str(site[0])+"'"

        cursor.execute(updateTXT)

        myDBConnect.commit()



     
    myDBConnect.close()


def getEpiasProduction(epiasEIC,starDate,endDate):

    with open("config.json","r") as file:
        dbApiInfo=json.load(file)
    
        

    siteEpiasId=getEpiasProductionId(epiasEIC,starDate)

    apiURL=dbApiInfo["seffafApi"]["apiUrl"]+"production/real-time-generation_with_powerplant"
   
    siteIdList=siteEpiasId.split(",")

    powerDFList=[]

    for siteId in siteIdList:

        paramList={"powerPlantId":siteId,"startDate":starDate,"endDate":endDate}

        response=requests.get(apiURL,params=paramList,verify=False,timeout=30)

        powerList=response.json()

        responseDF=pd.DataFrame(powerList["body"]["hourlyGenerations"])
        
        powerDFList.append(responseDF)

        print(powerDFList)
    
    tmpPowerDF=powerDFList[0]
    
    if len(siteIdList)>1:

        print(tmpPowerDF)

        for powerDF in range(1,len(powerDFList)):

            tmpPowerDF=tmpPowerDF+powerDFList[powerDF]

    
    print(tmpPowerDF)
  
    return tmpPowerDF




    # if len(siteEpiasId)==1:    

    #     paramList={"powerPlantId":str(siteEpiasId[0]),"startDate":starDate,"endDate":endDate}

    #     response=requests.get(apiURL,params=paramList,verify=False,timeout=30)

    #     productionList=response.json()

    #     if productionList["resultDescription"]=="success":

    #         for proValue in productionList["body"]["hourlyGenerations"]:

    #             valueDate=proValue["date"]

    #             valuePro=proValue["total"]

    # else:







def addSiteDataTable(tableName):
    try:
        with open("config.json","r") as file:
            dbApiInfo=json.load(file)


        myDBConnect = mysql.connector.connect(host=dbApiInfo["dbInfo"]["dbAddress"], user = dbApiInfo["dbInfo"]["dbUsersName"], password=dbApiInfo["dbInfo"]["dbPassword"], database=dbApiInfo["dbInfo"]["database"])

        cursor=myDBConnect.cursor()

        cursor.callproc("createSiteDataTable",[tableName,])
    
        return True

    except:

        return False


def addColumnToTable(tableName,columnDescTxt):

    try:
        with open("config.json","r") as file:
            dbApiInfo=json.load(file)
        
        myDBConnect = mysql.connector.connect(host=dbApiInfo["dbInfo"]["dbAddress"], user = dbApiInfo["dbInfo"]["dbUsersName"], password=dbApiInfo["dbInfo"]["dbPassword"], database=dbApiInfo["dbInfo"]["database"])

        if getTableCount(tableName)>0:
            
            insertTXT="ALTER TABLE "+tableName+" "+columnDescTxt

            cursor=myDBConnect.cursor()

            cursor.execute(insertTXT)

            myDBConnect.commit()

            myDBConnect.close()

            return True

    except:

        return False



def getEpiasProductionId(epiasEIC,productionDate):
    
    with open("config.json","r") as file:
        dbApiInfo=json.load(file)

    apiUrl=dbApiInfo["seffafApi"]["apiUrl"]+"production/real-time-generation-power-plant-list?"

    paramList={"period":productionDate}

    response = requests.get(apiUrl,params=paramList,verify=False,timeout=30)

    planList=response.json()

    plantSelect=""

    
    if planList["resultDescription"]=="success":

        for plant in planList["body"]["powerPlantList"]:
            
            if plant["eic"]==epiasEIC:

                if plantSelect=="":

                    plantSelect=str(plant["id"])

                else:

                    plantSelect+=","+str(plant["id"])
    

    return plantSelect



                


    

def getColumnCountByTable(tableName,columnName):

    selectTXT="SELECT count(*) FROM information_schema.COLUMNS  WHERE  TABLE_NAME = '"+tableName+"' AND COLUMN_NAME = '"+columnName+"'"
    
    try:
   
        with open("config.json","r") as file:
            dbApiInfo=json.load(file)

        myDBConnect = mysql.connector.connect(host=dbApiInfo["dbInfo"]["dbAddress"], user = dbApiInfo["dbInfo"]["dbUsersName"], password=dbApiInfo["dbInfo"]["dbPassword"], database=dbApiInfo["dbInfo"]["database"])
        
        cursor=myDBConnect.cursor()
        
        cursor.execute(selectTXT)
    
        siteCountColumn = cursor.fetchall()

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




updateEpiasProductions()
