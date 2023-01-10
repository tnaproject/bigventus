from pymongo import MongoClient
import mysql.connector
from dateutil import parser
import json
import requests
import pandas as pd
from datetime import datetime
from datetime import timedelta
import sys
#request sertifika hatası almamak için
requests.packages.urllib3.disable_warnings()


client=MongoClient("mongodb://89.252.157.127:27017/")

print(client)

def waitTime(waitSecond):
    waitTime=datetime.now()+timedelta(seconds=waitSecond)

    while (waitTime-datetime.now()).total_seconds<0:
        bekle=True

    return

def executeMany(connect,cursorTmp,queryText,dataList):
    rowCount=-1

    for i in range(0,4):

        try:

            cursorTmp.execute(queryText,dataList)

            connect.commit()

            rowCount=cursorTmp.rowcount

        except:

            waitTime(5)
            
            if i<3:
                continue
            else:
                raise
            break
    
    return rowCount,cursorTmp


def executeOne(connect,cursorTmp,queryText):

    rowCount=-1

    for i in range(0,4):

        try:

            cursorTmp.execute(queryText)

            connect.commit()

            rowCount=cursorTmp.rowcount

        except:

            waitTime(5)

            if i<3:
                continue
            else:
                raise
            break
    
    return rowCount

def getOrganizationInfo(orgId):

    try:

        with open("config.json","r") as file:

            dbApiInfo=json.load(file)


        selectTXT="Select * From epiasCompanyList where id="+str(orgId)

        myDBConnect = mysql.connector.connect(host=dbApiInfo["dbInfo"]["dbAddress"], user = dbApiInfo["dbInfo"]["dbUsersName"], password=dbApiInfo["dbInfo"]["dbPassword"], database=dbApiInfo["dbInfo"]["database"])
        
        cursor=myDBConnect.cursor()
        
        cursor.execute(selectTXT)

        orgList=cursor.fetchall()

        myDBConnect.commit()

        myDBConnect.close()

        for org in orgList:

            return org

    except:

        return "-9999"

def getColumnCountByTable(tableName,columnName):

    selectTXT="SELECT count(*) FROM information_schema.COLUMNS  WHERE  TABLE_NAME = '"+tableName+"' AND COLUMN_NAME = '"+columnName+"'"

    tries=4

    for i in range(0,tries):
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
       
            if i < tries-1: # i is zero indexed

                continue
            else:
                raise
    
        break

       

def addSiteDataTable(tableName):
    try:
        with open("config.json","r") as file:
            dbApiInfo=json.load(file)


        myDBConnect = mysql.connector.connect(host=dbApiInfo["dbInfo"]["dbAddress"], user = dbApiInfo["dbInfo"]["dbUsersName"], password=dbApiInfo["dbInfo"]["dbPassword"], database=dbApiInfo["dbInfo"]["database"])

        cursor=myDBConnect.cursor()

        cursor.callproc("createSiteDataTable",[tableName,])
        
        myDBConnect.close()

        return True

    except:

        return False

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

def updateSitesEpiasAIC():

    with open("config.json","r") as file:

        dbApiInfo=json.load(file)

    myDBConnect = mysql.connector.connect(host=dbApiInfo["dbInfo"]["dbAddress"], user = dbApiInfo["dbInfo"]["dbUsersName"], password=dbApiInfo["dbInfo"]["dbPassword"], database=dbApiInfo["dbInfo"]["database"])

    selectTxt="Select id,siteName,epiasEIC,epiasCompanyId,epiasOrgEIC,realProductionDateTime,kgupDateTime,epiasCompanyId,kudupDateTime,aicDateTime From siteList where epiasEIC > 0"

    cursor=myDBConnect.cursor()
            
    cursor.execute(selectTxt)
    
    siteTable = cursor.fetchall()

      #AIC Update
    for site in siteTable:
        
        tmpDate=datetime.now()-timedelta(seconds=1)

        if getTableCount("siteDataList_"+str(site[0]))==0:
            
            addSiteDataTable("siteDataList_"+str(site[0]))

        while (tmpDate-(datetime.now()+timedelta(seconds=10))).total_seconds()<0:

            responseDate=updateSiteAIC(site)

            if responseDate!="-9999":

                tmpDate=pd.to_datetime(responseDate)

            else:

                tmpDate=datetime.now()+timedelta(seconds=2)


def updateSitesEpiasKGUP():

    with open("config.json","r") as file:

        dbApiInfo=json.load(file)

    myDBConnect = mysql.connector.connect(host=dbApiInfo["dbInfo"]["dbAddress"], user = dbApiInfo["dbInfo"]["dbUsersName"], password=dbApiInfo["dbInfo"]["dbPassword"], database=dbApiInfo["dbInfo"]["database"])

    selectTxt="Select id,siteName,epiasEIC,epiasCompanyId,epiasOrgEIC,realProductionDateTime,kgupDateTime,epiasCompanyId,kudupDateTime,aicDateTime From siteList where epiasEIC > 0"

    cursor=myDBConnect.cursor()
            
    cursor.execute(selectTxt)
    
    siteTable = cursor.fetchall()

    #KGUP Update
    for site in siteTable:
        
        tmpDate=datetime.now()-timedelta(hours=1)

        while (tmpDate-(datetime.now()+timedelta(days=1))).total_seconds()<0:

            responseDate=updateSiteKGUP(site)

            if responseDate!="-9999":

                tmpDate=pd.to_datetime(responseDate)

            else:

                tmpDate=datetime.now()+timedelta(days=2)




def updateSitesEpiasKUDUP():

    with open("config.json","r") as file:

        dbApiInfo=json.load(file)

    myDBConnect = mysql.connector.connect(host=dbApiInfo["dbInfo"]["dbAddress"], user = dbApiInfo["dbInfo"]["dbUsersName"], password=dbApiInfo["dbInfo"]["dbPassword"], database=dbApiInfo["dbInfo"]["database"])

    selectTxt="Select id,siteName,epiasEIC,epiasCompanyId,epiasOrgEIC,realProductionDateTime,kgupDateTime,epiasCompanyId,kudupDateTime,aicDateTime From siteList where epiasEIC > 0"

    cursor=myDBConnect.cursor()
            
    cursor.execute(selectTxt)
    
    siteTable = cursor.fetchall()

    #KUDUP Update
    for site in siteTable:
        
        tmpDate=datetime.now()-timedelta(hours=1)

        while (tmpDate-(datetime.now()+timedelta(days=1))).total_seconds()<0:

            responseDate=updateSiteKUDUP(site)

            if responseDate!="-9999":

                tmpDate=pd.to_datetime(responseDate)

            else:

                tmpDate=datetime.now()+timedelta(days=2)


def updateSitesEpiasProduction():

    with open("config.json","r") as file:

        dbApiInfo=json.load(file)

    myDBConnect = mysql.connector.connect(host=dbApiInfo["dbInfo"]["dbAddress"], user = dbApiInfo["dbInfo"]["dbUsersName"], password=dbApiInfo["dbInfo"]["dbPassword"], database=dbApiInfo["dbInfo"]["database"])

    selectTxt="Select id,siteName,epiasEIC,epiasCompanyId,epiasOrgEIC,realProductionDateTime,kgupDateTime,epiasCompanyId,kudupDateTime,aicDateTime From siteList where epiasEIC > 0"

    cursor=myDBConnect.cursor()
            
    cursor.execute(selectTxt)
    
    siteTable = cursor.fetchall()

    #Production Update
    for site in siteTable:
        
        tmpDate=datetime.now()-timedelta(hours=1)

        while ((tmpDate-datetime.now())).total_seconds()<0:

            responseDate=updateSiteProduction(site)

            if responseDate!="-9999":

                tmpDate=pd.to_datetime(responseDate)

            else:

                tmpDate=datetime.now()+timedelta(hours=2)


def updateSiteEPIASData():
   
    with open("config.json","r") as file:

        dbApiInfo=json.load(file)

    myDBConnect = mysql.connector.connect(host=dbApiInfo["dbInfo"]["dbAddress"], user = dbApiInfo["dbInfo"]["dbUsersName"], password=dbApiInfo["dbInfo"]["dbPassword"], database=dbApiInfo["dbInfo"]["database"])

    selectTxt="Select id,siteName,epiasEIC,epiasCompanyId,epiasOrgEIC,realProductionDateTime,kgupDateTime,epiasCompanyId,kudupDateTime,aicDateTime From siteList where epiasEIC > 0"

    cursor=myDBConnect.cursor()
            
    cursor.execute(selectTxt)
    
    siteTable = cursor.fetchall()

      #AIC Update
    for site in siteTable:
        
        tmpDate=datetime.now()-timedelta(seconds=1)

        while (tmpDate-(datetime.now()+timedelta(seconds=10))).total_seconds()<0:

            responseDate=updateSiteAIC(site)

            if responseDate!="-9999":

                tmpDate=pd.to_datetime(responseDate)

            else:

                tmpDate=datetime.now()+timedelta(seconds=2)


    #  #KUDUP Update
    for site in siteTable:
        
        tmpDate=datetime.now()-timedelta(hours=1)

        while (tmpDate-(datetime.now()+timedelta(days=1))).total_seconds()<0:

            responseDate=updateSiteKUDUP(site)

            if responseDate!="-9999":

                tmpDate=pd.to_datetime(responseDate)

            else:

                tmpDate=datetime.now()+timedelta(days=2)


    #KGUP Update
    for site in siteTable:
        
        tmpDate=datetime.now()-timedelta(hours=1)

        while (tmpDate-(datetime.now()+timedelta(days=1))).total_seconds()<0:

            responseDate=updateSiteKGUP(site)

            if responseDate!="-9999":

                tmpDate=pd.to_datetime(responseDate)

            else:

                tmpDate=datetime.now()+timedelta(days=2)



    #Production Update
    for site in siteTable:
        
        tmpDate=datetime.now()-timedelta(hours=1)

        while ((tmpDate-datetime.now())).total_seconds()<0:

            responseDate=updateSiteProduction(site)

            if responseDate!="-9999":

                tmpDate=pd.to_datetime(responseDate)

            else:

                tmpDate=datetime.now()+timedelta(hours=2)



def startProcess():
    print(sys.argv[1])
    processId=sys.argv[1]
    if processId=='0':
        updateSitesEpiasAIC()

    if processId=='1':
        updateSitesEpiasKGUP()
    
    if processId=='2':
        updateSitesEpiasKUDUP()

    if processId=='3':
        updateSitesEpiasProduction()


def updateSiteAIC(site):

   

        with open("config.json","r") as file:

            dbApiInfo=json.load(file)
        
        myDBConnect = mysql.connector.connect(host=dbApiInfo["dbInfo"]["dbAddress"], user = dbApiInfo["dbInfo"]["dbUsersName"], password=dbApiInfo["dbInfo"]["dbPassword"], database=dbApiInfo["dbInfo"]["database"])
        
        selectTxt="Select id,siteName,epiasEIC,epiasCompanyId,epiasOrgEIC,realProductionDateTime,kgupDateTime,epiasCompanyId,kudupDateTime,aicDateTime From siteList where  id="+str(site[0])

        cursor=myDBConnect.cursor()
            
        cursor.execute(selectTxt)
    
        siteTable = cursor.fetchall()

        cursor=myDBConnect.cursor()

        startDate="2018-09-01 00:00"
        
        if siteTable[0][9] is not None:

            startDate=str(siteTable[0][9]+timedelta(days=1))


        if (pd.to_datetime(startDate)-(datetime.now()+timedelta(days=1))).total_seconds()<0:
            
            orgEpias=getOrganizationInfo(site[7])

            endDate=str(pd.to_datetime(startDate)+timedelta(days=10))

            aicDF=getEpiasAIC(orgEpias[3],site[4],startDate,endDate)
        
            updateTXT="Update siteDataList_"+str(site[0]) +" Set aic=%s where timeStamp=%s"
       
            dataList=[]

            lastDateTime=""

            for row in range(0,aicDF.shape[0]):
                deger=str(aicDF["toplam"][row])
                if aicDF["toplam"][row]!=None and str(aicDF["toplam"][row])!='nan' :
                

                    dateTMP=pd.to_datetime(aicDF["tarih"][row])

                    dateTMP=str(dateTMP).replace("+03:00","")
            
                    lastDateTime=dateTMP

                    updateTMPTxt="Update siteDataList_"+str(site[0]) +" Set aic = NULL where timeStamp='"+str(dateTMP)+"'"

                    cursor.execute(updateTMPTxt)

                    myDBConnect.commit()

                    dataList.append((str(aicDF["toplam"][row]*1000),dateTMP))
                        

            rowCount=executeMany(myDBConnect,cursor,updateTXT,dataList)

            # cursor.executemany(updateTXT,dataList)

            # myDBConnect.commit()

            # rowCount=cursor.rowcount

            if rowCount<=0:

                lastDateTime=""

                dataList=[]

                for row in range(0,aicDF.shape[0]):
                    deger=aicDF["toplam"][row]
                    if aicDF["toplam"][row]!=None and str(aicDF["toplam"][row])!='nan' :
                  

                        dateTMP=pd.to_datetime(aicDF["tarih"][row])

                        dateTMP=str(dateTMP).replace("+03:00","")
            
                        lastDateTime=dateTMP

                        dataList.append((dateTMP,str(aicDF["toplam"][row]*1000)))


                insertTXT="Insert Into siteDataList_"+str(site[0]) +" (timeStamp,aic) VALUES(%s,%s)"

                rowCount=executeMany(myDBConnect,cursor,insertTXT,dataList)

                # cursor.executemany(insertTXT,dataList)

                # myDBConnect.commit()

                # rowCount=cursor.rowcount

               


            if lastDateTime=="":
            
                lastDateTime=str(pd.to_datetime(startDate))


            print("AIC>"+str(site[0])+"/"+site[1]+"/"+lastDateTime+"/"+str(rowCount)+"/"+str(datetime.now()))

            
            updateTXT="Update siteList set aicDateTime='"+str(lastDateTime)+"' where id='"+str(site[0])+"'"

            cursor.execute(updateTXT)

            myDBConnect.commit()

            myDBConnect.close()

            return lastDateTime

        else:
            
            return startDate



def dropSiteTable(tableName):

    queryTXT="Drop table "


def updateSiteKUDUP(site):

    try:

        with open("config.json","r") as file:

            dbApiInfo=json.load(file)
        
        
        myDBConnect = mysql.connector.connect(host=dbApiInfo["dbInfo"]["dbAddress"], user = dbApiInfo["dbInfo"]["dbUsersName"], password=dbApiInfo["dbInfo"]["dbPassword"], database=dbApiInfo["dbInfo"]["database"])
        
        selectTxt="Select id,siteName,epiasEIC,epiasCompanyId,epiasOrgEIC,realProductionDateTime,kgupDateTime,epiasCompanyId,kudupDateTime,aicDateTime From siteList where  id="+str(site[0])

        cursor=myDBConnect.cursor()
            
        cursor.execute(selectTxt)
    
        siteTable = cursor.fetchall()

        cursor=myDBConnect.cursor()

        if getTableCount("siteDataList_"+str(site[0]))==0:
            
            addSiteDataTable("siteDataList_"+str(site[0]))

        startDate="2018-09-01 00:00"
        
        if siteTable[0][8] is not None:

            startDate=str(siteTable[0][8]+timedelta(days=1))


        if (pd.to_datetime(startDate)-(datetime.now()+timedelta(days=1))).total_seconds()<0:
            
            orgEpias=getOrganizationInfo(site[7])
            
            endDate=str(pd.to_datetime(startDate)+timedelta(days=10))

            kudupDF=getEpiasKUDUP(orgEpias[3],site[4],startDate,endDate)

        
            updateTXT="Update siteDataList_"+str(site[0]) +" Set KUDUP=%s where timeStamp=%s"
       
            dataList=[]

            lastDateTime=""

            for row in range(0,kudupDF.shape[0]):

                if kudupDF["toplam"][row]!=None and str(kudupDF["toplam"][row])!='nan':
                    
                    dateTMP=pd.to_datetime(kudupDF["tarih"][row])

                    dateTMP=str(dateTMP).replace("+03:00","")
            
                    lastDateTime=dateTMP

                    updateTMPTxt="Update siteDataList_"+str(site[0]) +" Set KUDUP = NULL where timeStamp='"+str(dateTMP)+"'"

                    cursor.execute(updateTMPTxt)

                    myDBConnect.commit()

                    dataList.append((str(kudupDF["toplam"][row]*1000),dateTMP))
                        

            rowCount=executeMany(myDBConnect,cursor,updateTXT,dataList)

            # cursor.executemany(updateTXT,dataList)

            # myDBConnect.commit()

            # rowCount=cursor.rowcount

            if rowCount<=0:

                lastDateTime=""

                dataList=[]

                for row in range(0,kudupDF.shape[0]):
                    if kudupDF["toplam"][row]!=None and str(kudupDF["toplam"][row])!='nan':

                        dateTMP=pd.to_datetime(kudupDF["tarih"][row])

                        dateTMP=str(dateTMP).replace("+03:00","")
            
                        lastDateTime=dateTMP

                        dataList.append((dateTMP,str(kudupDF["toplam"][row]*1000)))


                insertTXT="Insert Into siteDataList_"+str(site[0]) +" (timeStamp,KUDUP) VALUES(%s,%s)"

                rowCount=executeMany(myDBConnect,cursor,insertTXT,dataList)

                # cursor.executemany(insertTXT,dataList)

                # myDBConnect.commit()

                # rowCount=cursor.rowcount

            else:

                

                for row in range(0,kudupDF.shape[0]):

                    dateTMP=pd.to_datetime(kudupDF["tarih"][row])

                    updateTXT="Update siteDataList_"+str(site[0])  +" Set kudupErr=KUDUP-realProduction where timeStamp='"+str(dateTMP)+"'"
                                
                    cursor.execute(updateTXT)

                    myDBConnect.commit()

                    updateTXT="Update siteDataList_"+str(site[0])  +" Set kudupActErr=KUDUP-actualProduction where timeStamp='"+str(dateTMP)+"'"
                                
                    cursor.execute(updateTXT)

                    myDBConnect.commit()

                


            if lastDateTime=="":
            
                lastDateTime=str(pd.to_datetime(startDate))


            print("KUDUP>"+str(site[0])+"/"+site[1]+"/"+lastDateTime+"/"+str(rowCount)+"/"+str(datetime.now()))
            
            updateTXT="Update siteList set kudupDateTime='"+str(lastDateTime)+"' where id='"+str(site[0])+"'"

            cursor.execute(updateTXT)

            myDBConnect.commit()
            
            myDBConnect.close()
            
            return lastDateTime

        else:
            
            return startDate

    except:

        return "-9999"



#update Site KGUP       
def updateSiteKGUP(site):

    

        with open("config.json","r") as file:

            dbApiInfo=json.load(file)
        
        myDBConnect = mysql.connector.connect(host=dbApiInfo["dbInfo"]["dbAddress"], user = dbApiInfo["dbInfo"]["dbUsersName"], password=dbApiInfo["dbInfo"]["dbPassword"], database=dbApiInfo["dbInfo"]["database"])
        
        selectTxt="Select id,siteName,epiasEIC,epiasCompanyId,epiasOrgEIC,realProductionDateTime,kgupDateTime,epiasCompanyId,kudupDateTime,aicDateTime From siteList where  id="+str(site[0])

        cursor=myDBConnect.cursor()
            
        cursor.execute(selectTxt)
    
        siteTable = cursor.fetchall()


        cursor=myDBConnect.cursor()

        if getTableCount("siteDataList_"+str(site[0]))==0:
            
            addSiteDataTable("siteDataList_"+str(site[0]))

        startDate="2018-09-01 00:00"
        
        if siteTable[0][6] is not None:

            startDate=str(siteTable[0][6]+timedelta(days=1))


        if (pd.to_datetime(startDate)-(datetime.now()+timedelta(days=1))).total_seconds()<0:
            
            orgEpias=getOrganizationInfo(site[7])

            endDate=str(pd.to_datetime(startDate)+timedelta(days=10))

            kgupDF=getEpiasKGUP(orgEpias[3],site[4],startDate,endDate)


            
            updateTXT="Update siteDataList_"+str(site[0]) +" Set KGUP=%s where timeStamp=%s"
       
            dataList=[]

            lastDateTime=""

            for row in range(0,kgupDF.shape[0]):
                try:
                    
                    if kgupDF["toplam"][row]!=None and str(kgupDF["toplam"][row])!='nan':

                        dateTMP=pd.to_datetime(kgupDF["tarih"][row])
                        dateTMP=parser.parse(kgupDF["tarih"][row])
                        dateTMP=str(dateTMP).replace("+03:00","")
            
                        lastDateTime=dateTMP
                
                        updateTMPTxt="Update siteDataList_"+str(site[0]) +" Set KGUP = NULL where timeStamp='"+str(dateTMP)+"'"

                        cursor.execute(updateTMPTxt)

                        myDBConnect.commit()
                        
                        dataList.append((str(kgupDF["toplam"][row]*1000),dateTMP))

                except:

                    dataList.append(("-9999",dateTMP))
            
          



            rowCount=executeMany(myDBConnect,cursor,updateTXT,dataList)

                    # cursor.executemany(updateTXT,dataList)

                    # myDBConnect.commit()

                    # rowCount=cursor.rowcount



            if rowCount<=0:
                
                lastDateTime=""

                dataList=[]

                for row in range(0,kgupDF.shape[0]):

                    if kgupDF["toplam"][row]!=None and str(kgupDF["toplam"][row])!='nan':
                        
                        dateTMP=pd.to_datetime(kgupDF["tarih"][row])

                        dateTMP=str(dateTMP).replace("+03:00","")
            
                        lastDateTime=dateTMP

                        dataList.append((dateTMP,str(kgupDF["toplam"][row]*1000)))


             
                insertTXT="Insert Into siteDataList_"+str(site[0]) +" (timeStamp,KGUP) VALUES(%s,%s)"

                rowCount=executeMany(myDBConnect,cursor,insertTXT,dataList)

                        # cursor.executemany(insertTXT,dataList)

                        # myDBConnect.commit()

                        # rowCount=cursor.rowcount
                    


            else:

                

                for row in range(0,kgupDF.shape[0]):

                    dateTMP=pd.to_datetime(kgupDF["tarih"][row])

                    updateTXT="Update siteDataList_"+str(site[0])  +" Set kgupErr=KGUP-realProduction where timeStamp='"+str(dateTMP)+"'"
                                
                    cursor.execute(updateTXT)

                    myDBConnect.commit()



                


            if lastDateTime=="":
            
                lastDateTime=str(pd.to_datetime(startDate))


            print("KGUP>"+str(site[0])+"/"+site[1]+"/"+lastDateTime+"/"+str(rowCount)+"/"+str(datetime.now()))
            
            updateTXT="Update siteList set kgupDateTime='"+str(lastDateTime)+"' where id='"+str(site[0])+"'"

            cursor.execute(updateTXT)

            myDBConnect.commit()

            myDBConnect.close()

            return lastDateTime

        else:
            
            return startDate








def updateSiteProduction(site):

    try:

        with open("config.json","r") as file:

            dbApiInfo=json.load(file)
        
        myDBConnect = mysql.connector.connect(host=dbApiInfo["dbInfo"]["dbAddress"], user = dbApiInfo["dbInfo"]["dbUsersName"], password=dbApiInfo["dbInfo"]["dbPassword"], database=dbApiInfo["dbInfo"]["database"])
        
        selectTxt="Select id,siteName,epiasEIC,epiasCompanyId,epiasOrgEIC,realProductionDateTime,kgupDateTime,epiasCompanyId,kudupDateTime,aicDateTime From siteList where  id="+str(site[0])

        cursor=myDBConnect.cursor()
            
        cursor.execute(selectTxt)
    
        siteTable = cursor.fetchall()



        cursor=myDBConnect.cursor()

        if getTableCount("siteDataList_"+str(site[0]))==0:
            
            addSiteDataTable("siteDataList_"+str(site[0]))

        startDate="2018-09-01 00:00"
        
        if siteTable[0][5] is not None:

            if (datetime.now()-siteTable[0][5]).days<10:

                startDate=str(datetime.now()-timedelta(days=10))

            else:

                startDate=str(siteTable[0][5]+timedelta(days=1))


        if (pd.to_datetime(startDate)-datetime.now()).total_seconds()<0:
            
            
            productionDF=getEpiasProduction(site[2],startDate,startDate)
        
            updateTXT="Update siteDataList_"+str(site[0]) +" Set realProduction=%s where timeStamp=%s"
       
            dataList=[]

            lastDateTime=""

            for row in range(0,productionDF.shape[0]):
                if productionDF["total"][row]!=None and str(productionDF["total"][row])!='nan':

                    dateTMP=pd.to_datetime(productionDF["date"][row])

                    dateTMP=str(dateTMP).replace("+03:00","")
            
                    lastDateTime=dateTMP

                    updateTMPTxt="Update siteDataList_"+str(site[0]) +" Set realProduction = NULL where timeStamp='"+str(dateTMP)+"'"

                    cursor.execute(updateTMPTxt)

                    myDBConnect.commit()

                    dataList.append((str(productionDF["total"][row]*1000),dateTMP))
                        
            
            tries=4

            rowCount=1

            rowCount=executeMany(myDBConnect,cursor,updateTXT,dataList)

                    # cursor.executemany(updateTXT,dataList)

                    # myDBConnect.commit()

                    # rowCount=cursor.rowcount


            if rowCount<=0:

                lastDateTime=""

                dataList=[]

                for row in range(0,productionDF.shape[0]):
                    
                    if productionDF["total"][row]!=None and str(productionDF["total"][row])!='nan':
                        
                        dateTMP=pd.to_datetime(productionDF["date"][row])

                        dateTMP=str(dateTMP).replace("+03:00","")
            
                        lastDateTime=dateTMP

                        dataList.append((dateTMP,str(productionDF["total"][row]*1000)))


                insertTXT="Insert Into siteDataList_"+str(site[0]) +" (timeStamp,realProduction) VALUES(%s,%s)"
                
                rowCount=executeMany(myDBConnect,cursor,insertTXT,dataList)

                # cursor.executemany(insertTXT,dataList)

                # myDBConnect.commit()

                # rowCount=cursor.rowcount

            else:

                

                for row in range(0,productionDF.shape[0]):

                    dateTMP=pd.to_datetime(productionDF["date"][row])

                    updateTXT="Update siteDataList_"+str(site[0])  +" Set kgupErr=KGUP-realProduction where timeStamp='"+str(dateTMP)+"'"
                                
                    cursor.execute(updateTXT)

                    myDBConnect.commit()

                    updateTXT="Update siteDataList_"+str(site[0])  +" Set kudupErr=KUDUP-realProduction where timeStamp='"+str(dateTMP)+"'"
                                
                    cursor.execute(updateTXT)

                    myDBConnect.commit()
                


            if lastDateTime=="":
            
                lastDateTime=str(pd.to_datetime(startDate))


            print(str(site[0])+"/"+site[1]+"/"+lastDateTime+"/"+str(rowCount)+"/"+str(datetime.now()))

            updateTXT="Update siteList set realProductionDateTime='"+str(lastDateTime)+"' where id='"+str(site[0])+"'"

            cursor.execute(updateTXT)

            myDBConnect.commit()
            
            myDBConnect.close()

            return lastDateTime

        else:
            
            return startDate

    except:

        return "-9999"

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

#Epias KGUP Verisi
def getEpiasKGUPTurkey(site,starDate,endDate):
   
    try:

         for i in range(0,4):
            with open("config.json","r") as file:
                dbApiInfo=json.load(file)
               

            apiURL=dbApiInfo["seffafApi"]["apiUrl"]+"production/dpp"
             
            paramList={"startDate":starDate,"endDate":endDate}

            response=requests.get(apiURL,params=paramList,verify=False,timeout=30)

            kgupList=response.json()

            responseDF=pd.DataFrame(kgupList["body"]["dppList"])
    
            return responseDF
    except:

        waitTime =datetime.now()+timedelta(seconds=5)

        while (waitTime-datetime.now()).total_seconds()<0:
            bekle=True

            if i<3:
                continue
            else:
                raise
            break   

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

        
    for i in range(0,4):
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

            waitTime =datetime.now()+timedelta(seconds=5)

            while (waitTime-datetime.now()).total_seconds()<0:
                bekle=True

            if i<3:
                continue
            else:
                raise
            break

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








updateSitesEpiasKGUP()





def startProcess(processId):
    if processId==-1:
        updateSitesEpiasAIC()
    
    if processId==-2:
        updateSitesEpiasKGUP()

    if processId==-3:
        updateSitesEpiasKUDUP()

    if processId==-4:
        updateSitesEpiasProduction()


#startProcess(sys.argv[1])



