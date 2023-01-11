import pandas as pd
from datetime import datetime,timedelta
import numpy as np
import math
import threading

from pymongo import MongoClient,UpdateMany,UpdateOne

def wind_convert(u,v):
    wind_dir=np.zeros(len(u))
    wind_speed=np.zeros(len(u))
    for i in range(0,len(u)):

        conv = 45 / math.atan(1)
        wind_dir[i] = (270 - math.atan2(v[i],u[i]) * conv) % 360
        wind_speed[i] = math.sqrt(math.pow(u[i],2) + math.pow(v[i],2))
    #wind_dir = 180 + (180 / math.pi) * math.atan2(v,u)
    
    return wind_speed,wind_dir

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

def meanWindDirection(windDirections):
    
    V_east=np.zeros(len(windDirections))
    V_north=np.zeros(len(windDirections))

    for i in range(0,len(windDirections)):


        V_east[i] = np.mean( math.sin(windDirections[i] * math.pi/180))
        V_north[i] = np.mean(math.cos(windDirections[i] * math.pi/180))

    mean_WD = math.atan2(np.mean(V_east),np.mean(V_north)) * 180/math.pi
    mean_WD = (360 + mean_WD) % 360

    return mean_WD


    
    