from datetime import datetime
import math
import numpy


def calcPower(Dates,GHI,t,lat,lon,Beta,EWLim,countPV,lossFactor,eref,alpha,PVArea):
    
    
    SL=0
    
    DS=0
    
    for tValue in t:
        tValue=tValue-272.15

   
    tip=type(t)

    t=t.mean()-272.15

    DNI=EstimateDNI(Dates,GHI,Beta,EWLim,lat,lon,SL,DS)

    DNI=DNI.mean()

    e=EstimateEff(DNI,t,eref,0.12,alpha)/100

    TempPOWER=(DNI*e*PVArea*countPV*lossFactor)/1000


    power=TempPOWER

    if (power<0):
        power=0

    return DNI,power


def EstimateDNI(Dates,GHI,Beta,BetaLim,L,LL,SL,DS):
    
    DNI=numpy.zeros(GHI.shape,dtype=float)

    for i in range(0,len(Dates)-1):
         
        if GHI[i]>1:
            
            yearofDate=datetime.strptime(Dates[i],"%Y-%m-%d %H:%M")
            
            yearofDateTxt=datetime.strftime(yearofDate,"%Y")
            
            beginDate=datetime.strptime(yearofDateTxt+"-01-01","%Y-01-01")

            
            begin=beginDate.toordinal()
            
            N=math.floor((yearofDate-beginDate).days)+1
            
            B=(N-81)*360/365 * math.pi/180;
            
            ET=9.87*math.sin(2*B)-7.53*math.cos(B)-1.5*math.sin(B)
            
            MIN=yearofDate.hour*60+yearofDate.minute
            
            AST = MIN+ET-4*(SL-LL)-DS;
            
            h = (AST/60-12)*15
            
            Sigma = 23.45*math.sin(360/365*(284+N)*math.pi/180)
            
            if Beta==-2:
          
                Beta=L-Sigma;
            
                        
            if BetaLim!=-1:
            
                if 90-abs(h)< BetaLim:
                    
                    hm=abs(h)-BetaLim
                    
                else:
                    
                    hm=0
               
            
            else:
            
                hm=h
            
            
            #CosTheta = math.sin((L-Beta)*(math.pi/100))*math.sin(Sigma*(math.pi/100))+math.cos((L-Beta)*(math.pi/100))*math.cos(Sigma)*math.cos(hm*(math.pi/100))
            
            #CosOmega = math.sin(L*(math.pi/100))*math.sin(Sigma*(math.pi/100))+math.cos(L*(math.pi/100))*math.cos(Sigma*(math.pi/100))*math.cos(h*(math.pi/100))
                        
            CosTheta = math.sin(L-Beta)*math.sin(Sigma)+math.cos(L-Beta)*math.cos(Sigma)*math.cos(hm)
            
            CosOmega = math.sin(L)*math.sin(Sigma)+math.cos(L)*math.cos(Sigma)*math.cos(h)
            

            RB = CosTheta/CosOmega
            
            if RB<0:
                RB=0
            
            DNI[i]=GHI[i]*RB
        
        else:
            
            DNI[i]=0

    return DNI

def  HourlyMean(Data):

    HourlyMean=[]
    
    for i in range(0,(len(Data)),6):
        print(HourlyMean)
        HourlyMean.append(Data[i:i+6].mean())
        
    return HourlyMean



def  HourlySum(Data):

    HourlySum=[]
    
    for i in range(0,(len(Data)),6):
        print(HourlySum)
        HourlySum.append(Data[i:i+6].sum())
        
    return HourlySum



def EstimateEff(DNI,T,eref,ins,pcoeff):

    Gref=1000;
    Tref=25;
    Gnoct=800;
    Tanoct=20;
    Tcnoct=45;
    e=0
    
    
            
    if DNI>1:
            
        Tc=T+(Tcnoct-Tanoct)*DNI/Gnoct
            
        e=(eref*(1+pcoeff*(Tc-Tref)+ins*math.log10(DNI/Gref)))

            
    else:
            
        e=0

    return e




GHI=numpy.array([129.58856,154.87785,192.66818,217.51332,254.01997,277.66394])

t=numpy.array([278.0102,278.567,279.04688,279.55347,280.07114,280.56323])

Dates=numpy.array(['2022-01-15 08:10','2022-01-15 08:20','2022-01-15 08:30','2022-01-15 08:40','2022-01-15 08:50','2022-01-15 09:00'])

Beta=25

EWLimit=-1

countPV=4038.5

lossFactor=0.913

eref=15.98

alpha=0.0042

PVArea=1.626

DNI,power=calcPower(Dates,GHI,t,38.68267,27.57684,Beta,EWLimit,countPV,lossFactor,eref,alpha,PVArea)

print(DNI)

print(power)
