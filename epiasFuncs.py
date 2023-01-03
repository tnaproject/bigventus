import mysql.connector
import json
import requests

dbApiInfo=""

with open("config.json","r") as file:
    dbApiInfo=json.load(file)


def createConnection():
    
    mydbConnect=mysql.connector.connect(host=dbApiInfo["dbInfo"]["dbAddress"], user = dbApiInfo["dbInfo"]["dbUsersName"], password=dbApiInfo["dbInfo"]["dbPassword"], database=dbApiInfo["dbInfo"]["database"])

    return mydbConnect


def getProduction():


    mydbCnnct = createConnection()

    selectTxt="Select id,siteName,epiasEIC,epiasCompanyId,epiasOrgEIC From siteList where epiasEIC > 0"

    cursor=mydbCnnct.cursor()
        
    cursor.execute(selectTxt)
    
    siteTable = cursor.fetchall()
 
    for site in siteTable:
        

        if getTableCount("siteDataList_"+str(site[0]))>0:
            
            if getColumnCountByTable("siteDataList_"+str(site[0]),'kgupActErr')==0:

                insertTXT="ALTER TABLE siteDataList_"+str(site[0])+" ADD COLUMN kgupActErr double DEFAULT NULL"

                cursor.execute(insertTXT)

                mydbCnnct.commit()

                print("siteDataList_"+str(site[0])+" KGUP")


            if getColumnCountByTable("siteDataList_"+str(site[0]),'kudupActErr')==0:

                insertTXT="ALTER TABLE siteDataList_"+str(site[0])+" ADD COLUMN kudupActErr double DEFAULT NULL"

                cursor.execute(insertTXT)

                mydbCnnct.commit()

                print("siteDataList_"+str(site[0])+" KUDUP")

     
    mydbCnnct.close()

        
def addColumnToTable(tableName,columnDescTxt):

    try:
        
        myDBConnect = createConnection()

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
    

    apiUrl=dbApiInfo["seffafApi"]["apiUrl"]+"production/real-time-generation-power-plant-list?"

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
   


        myDBConnect = createConnection()
        
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
   

        mydbConnect = mysql.connector.connect(host=dbInfo["dbInfo"]["dbAddress"], user = dbInfo["dbInfo"]["dbUsersName"], password=dbInfo["dbInfo"]["dbPassword"], database=dbInfo["dbInfo"]["database"])
        
        cursor=mydbConnect.cursor()
        
        cursor.execute(selectTXT)
    
        siteCountTable = cursor.fetchall()

        mydbConnect.close()

        return siteCountTable[0][0]
        

    except:

        print("Tablo Sayısı Kontrol Edilirken Hata Oluştu")





