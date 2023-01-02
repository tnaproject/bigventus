import mysql.connector
import json
import requests

def getProduction():

    with open("config.json","r") as file:
        dbInfo=json.load(file)

    mydbConnect = mysql.connector.connect(host=dbInfo["dbInfo"]["dbAddress"], user = dbInfo["dbInfo"]["dbUsersName"], password=dbInfo["dbInfo"]["dbPassword"], database=dbInfo["dbInfo"]["database"])

    selectTxt="Select id,siteName,epiasEIC,epiasCompanyId,epiasOrgEIC From siteList where epiasEIC > 0"

    cursor=mydbConnect.cursor()
        
    cursor.execute(selectTxt)
    
    siteTable = cursor.fetchall()
 
    for site in siteTable:
        

        if getTableCount("siteDataList_"+str(site[0]))>0:
            
            if getColumnCountByTable("siteDataList_"+str(site[0]),'kgupActErr')==0:

                insertTXT="ALTER TABLE siteDataList_"+str(site[0])+" ADD COLUMN kgupActErr double DEFAULT NULL"

                cursor.execute(insertTXT)

                mydbConnect.commit()

                print("siteDataList_"+str(site[0])+" KGUP")


            if getColumnCountByTable("siteDataList_"+str(site[0]),'kudupActErr')==0:

                insertTXT="ALTER TABLE siteDataList_"+str(site[0])+" ADD COLUMN kudupActErr double DEFAULT NULL"

                cursor.execute(insertTXT)

                mydbConnect.commit()

                print("siteDataList_"+str(site[0])+" KUDUP")




        


    mydbConnect.close()

        

def getEpiasProductionId(epiasEIC,productionDate):
    
    
    with open("config.json","r") as file:
        apiInfo=json.load(file)

    apiUrl=apiInfo["seffafApi"]["apiUrl"]+"production/real-time-generation-power-plant-list?"

    paramList={"period":productionDate}

    response = requests.get(apiUrl,params=paramList,verify=True,timeout=30)

    planList=response.json()

    plantSelect=-9999

    if planList["resultDescription"]=="success":

        for plant in planList["body"]["powerPlantList"]:


            if plant["eic"]==epiasEIC:

                plantSelect=plant["id"]

                return plantSelect


    

def getColumnCountByTable(tableName,columnName):

    selectTXT="SELECT count(*) FROM information_schema.COLUMNS  WHERE  TABLE_NAME = '"+tableName+"' AND COLUMN_NAME = '"+columnName+"'"
    
    try:
   
        with open("config.json","r") as file:
            dbInfo=json.load(file)

        mydbConnect = mysql.connector.connect(host=dbInfo["dbInfo"]["dbAddress"], user = dbInfo["dbInfo"]["dbUsersName"], password=dbInfo["dbInfo"]["dbPassword"], database=dbInfo["dbInfo"]["database"])
        
        cursor=mydbConnect.cursor()
        
        cursor.execute(selectTXT)
    
        siteCountColumn = cursor.fetchall()

        mydbConnect.close()

        return siteCountColumn[0][0]
        

    except:

        print("Tablo Sayısı Kontrol Edilirken Hata Oluştu")   


def getTableCount(tableName):

    selectTXT="SELECT count(*)   FROM  information_schema.TABLES  WHERE  TABLE_NAME = '"+tableName+"'" 

    try:
   
        with open("config.json","r") as file:
            dbInfo=json.load(file)

        mydbConnect = mysql.connector.connect(host=dbInfo["dbInfo"]["dbAddress"], user = dbInfo["dbInfo"]["dbUsersName"], password=dbInfo["dbInfo"]["dbPassword"], database=dbInfo["dbInfo"]["database"])
        
        cursor=mydbConnect.cursor()
        
        cursor.execute(selectTXT)
    
        siteCountTable = cursor.fetchall()

        mydbConnect.close()

        return siteCountTable[0][0]
        

    except:

        print("Tablo Sayısı Kontrol Edilirken Hata Oluştu")




getProduction()
