import mysql.connector
import json
import requests

dbApiInfo=""

with open("config.json","r") as file:
    dbApiInfo=json.load(file)


def createConnection():
    
    mydbConnect=mysql.connector.connect(host=dbApiInfo["dbInfo"]["dbAddress"], user = dbApiInfo["dbInfo"]["dbUsersName"], password=dbApiInfo["dbInfo"]["dbPassword"], database=dbApiInfo["dbInfo"]["database"])

    return mydbConnect


def updateEpiasProductions():

    myDBConnect = createConnection()

    selectTxt="Select id,siteName,epiasEIC,epiasCompanyId,epiasOrgEIC,kgupDateTime,kudupDateTime,aicDateTime From siteList where epiasEIC > 0"

    cursor=myDBConnect.cursor()
        
    cursor.execute(selectTxt)
    
    siteTable = cursor.fetchall()
 
    for site in siteTable:
        
        if getTableCount("siteDataList_"+str(site[0]))>0:
            
            addSiteDataTable("siteDataList_"+str(site[0]))
        
        listId=getEpiasProduction(siteTable[2],"2022-01-01","2022-01-01")

        print(list)



     
    myDBConnect.close()


def getEpiasProduction(epiasEIC,starDate,endDate):
    
    apiURL=dbApiInfo["seffafApi"]+"production/real-time-generation_with_powerplant"

    siteEpiasId=getEpiasProductionId(epiasEIC,starDate)

    return siteEpiasId




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

        myDBConnect = createConnection()

        cursor=myDBConnect.cursor()

        cursor.callproc("createSiteDataTable",[tableName,])
    
        return True

    except:

        return False


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

    plantSelect=[]

    if planList["resultDescription"]=="success":

        for plant in planList["body"]["powerPlantList"]:


            if plant["eic"]==epiasEIC:
                
                plantSelect.append(plant["id"])


                


    

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
   
        myDBConnect = createConnection()

        cursor=myDBConnect.cursor()
        
        cursor.execute(selectTXT)
    
        siteCountTable = cursor.fetchall()

        myDBConnect.close()

        return siteCountTable[0][0]
        

    except:

        print("Tablo Sayısı Kontrol Edilirken Hata Oluştu")




updateEpiasProductions()
