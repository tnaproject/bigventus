import mysql.connector
import json
import requests
import pandas as pd
from datetime import datetime
from datetime import timedelta

#request sertifika hatası almamak için
requests.packages.urllib3.disable_warnings()



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


        if startDate<datetime.now():

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

            else:

                print("HATA")
                #hataları hesapla

                # for row in range(0,productionDF.shape[0]):

                #     dateTMP=pd.to_datetime(productionDF["date"][row])

                #     dateTMP=str(dateTMP).replace("+03:00","")
            
                #     lastDateTime=dateTMP

                #     dataList.append((dateTMP,str(productionDF["total"][row]*1000)))


            if lastDateTime=="":
            
                lastDateTime=str(pd.to_datetime(startDate))


            print(str(site[0])+"/"+site[1]+"/"+lastDateTime+"/"+str(rowCount))

            updateTXT="Update siteList set realProductionDateTime='"+str(lastDateTime)+"' where id='"+str(site[0])+"'"

            cursor.execute(updateTXT)

            myDBConnect.commit()
    
        myDBConnect.close()


#Epias Emre Amade Kapasite Verisi
def getEpiasAIC(organizationEIC,epiasOrgEIC,starDate,endDate,isTurkey=False):
    try:

        with open("config.json","r") as file:

            dbApiInfo=json.load(file)
            

        apiURL=dbApiInfo["seffafApi"]["apiUrl"]+"production/aic"

        if isTurkey==False:
 
            paramList={"organizationEIC":organizationEIC,"uevcbEIC":epiasOrgEIC,"startDate":starDate,"endDate":endDate}

        else:
  
            paramList={"startDate":starDate,"endDate":endDate}


        response=requests.get(apiURL,params=paramList,verify=False,timeout=30)

        powerList=response.json()

        tmpAICDF=pd.DataFrame(powerList["body"]["aicList"])

        return tmpAICDF

    except:

        return "hata"


#Epias KUDUP Verisi
def getEpiasKUDUP(organizationEIC,epiasOrgEIC,starDate,endDate):

    try:

        orgId=str(int(organizationEIC[5:-1]))

        plantId=str(int(epiasOrgEIC[5:-1]))

        with open("config.json","r") as file:
            dbApiInfo=json.load(file)
               

        apiURL=dbApiInfo["seffafApi"]["apiUrl"]+"production/sbfgp"
             
        paramList={"organizationId":orgId,"uevcbId":plantId,"startDate":starDate,"endDate":endDate}

        response=requests.get(apiURL,params=paramList,verify=False,timeout=30)

        kgupList=response.json()

        responseDF=pd.DataFrame(kgupList["body"]["dppList"])
    
        return responseDF

    except:

        return "hata"



#Epias KGUP Verisi
def getEpiasKGUP(organizationEIC,epiasOrgEIC,starDate,endDate):

    try:

        with open("config.json","r") as file:
            dbApiInfo=json.load(file)
               

        apiURL=dbApiInfo["seffafApi"]["apiUrl"]+"production/dpp"
             
        paramList={"organizationEIC":organizationEIC,"uevcbEIC":epiasOrgEIC,"startDate":starDate,"endDate":endDate}

        response=requests.get(apiURL,params=paramList,verify=False,timeout=30)

        kgupList=response.json()

        responseDF=pd.DataFrame(kgupList["body"]["dppList"])
    
        return responseDF

    except:

        return "hata"

#Epias Üretim Verisi
def getEpiasProduction(epiasEIC,starDate,endDate,isTurkey=False):
    try:

        with open("config.json","r") as file:

            dbApiInfo=json.load(file)
            

        if isTurkey==False:

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

            
            tmpPowerDF=powerDFList[0]
    
            if len(siteIdList)>1:

                print(tmpPowerDF)

                if tmpPowerDF.shape[0]>0:

                    for powerDF in range(1,len(powerDFList)):

                        tmpPowerDF["total"]=tmpPowerDF["total"]+powerDFList[powerDF]["total"]


        else:

            apiURL=dbApiInfo["seffafApi"]["apiUrl"]+"production/real-time-generation"

            paramList={"startDate":starDate,"endDate":endDate}

            response=requests.get(apiURL,params=paramList,verify=False,timeout=30)

            powerList=response.json()

            tmpPowerDF=pd.DataFrame(powerList["body"]["hourlyGenerations"])

        return tmpPowerDF

    except:

        return "hata"



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




# updateEpiasProductions()
