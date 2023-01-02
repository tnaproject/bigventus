import netCDF4 as nc
import numpy as np
import pandas as pd
import mysql.connector 
from datetime import datetime

ncUrl="J:\\SampleModelData\\wrfpost_2022-11-15_00.nc"

mydb = mysql.connector.connect(
  host="89.252.157.127",
  user="dbventus",
  password="z9nY3@f80YBk",
  database="sys"
)

mycursor = mydb.cursor()

f = nc.Dataset(ncUrl)

print(list(f.variables))


# for name, variable in f.variables.items():            
#     for attrname in variable.ncattrs():
#         print("{} -- {}".format(attrname, getattr(variable, attrname)))
            # or :
            # print("{} -- {}".format(attrname, variable.getncattr(attrname))) 
    


fu10 = f.variables['U10'][:].data
fu50 = f.variables['U50'][:].data
fu100 = f.variables['U100'][:].data
fu200 = f.variables['U200'][:].data
fv10 = f.variables['V10'][:].data
fv50   = f.variables['V50'][:].data
fv100  = f.variables['V100'][:].data
fv200  = f.variables['V200'][:].data
ft2    = f.variables['T2'][:].data
frh2   = f.variables['RH2'][:].data
fpsfc  = f.variables['PSFC'][:].data
fghi   = f.variables['GHI'][:].data
fdiff  = f.variables['DIFF'][:].data
ftime  =  f.variables['Time'][:].data
faccprec=f.variables['AccPrec'][:].data
fsnow  = f.variables['SNOW'][:].data
flat  = f.variables['XLAT'][:].data
flon  = f.variables['XLONG'][:].data
zaman =  [None] * ft2.shape[0]
for t in range(ft2.shape[0]):
  zaman[t]= "".join(map(str,f.variables['Time'][:][t])).replace("'",'').replace("b",'').replace('_','T')

for y in range(ft2.shape[1]):
  for x in range(ft2.shape[2]):
    u10=0
    u50=0 
    u100=0
    u200=0
    v10=0 
    v50=0 
    v100=0 
    v200=0 
    tt2=0 
    rh2=0 
    psfc=0 
    ghi=0 
    diff=0 
    accprec=0 
    snow=0 
    say = 0 #325/6 = 54.1

    for t in range(ft2.shape[0]-1):
        
        if str(zaman[t])[14:16] != "00" or t == 0:
              u10 = u10+ fu10[t,y,x]
              u50 = u50+ fu50[t,y,x]
              u100 = u100+ fu100[t,y,x]
              u200 = u200+ fu200[t,y,x]
              v10 = v10+ fv10[t,y,x]
              v50 = v50+ fv50[t,y,x]
              v100 = v100+ fv100[t,y,x]
              v200 = v200+ fv200[t,y,x]
              tt2 = tt2+ ft2[t,y,x]
              rh2 = rh2+ frh2[t,y,x]
              psfc = psfc+ fpsfc[t,y,x]
              ghi = ghi+ fghi[t,y,x]
              diff = diff+ fdiff[t,y,x]
              accprec = accprec+ faccprec[t,y,x]
              snow = snow+ fsnow[t,y,x]
              say=say + 1
        else:
              u10=u10/6
              u50=u50/6
              u100=u100/6
              u200=u200/6 
              v10=v10/6
              v50=v50/6
              v100=v100/6
              v200=v200/6
              tt2=tt2/6
              rh2=rh2/6
              psfc=psfc/6
              ghi=ghi/6
              diff=diff/6
              accprec=accprec/6
              snow=snow/6

              lat =flat[t,y,x]
              lon =flon[t,y,x]
              sql = "INSERT INTO sys.gridMetParList1 (timestamp,XGRID,YGRID,XLAT,XLON, U10,U50,U100,U200,V10,V50,V100,V200,T2,RH2,PSFC,GHI,DIFF,ACCPREC,SNOW) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
              val = (zaman[t],x,y,float(lat),float(lon), float(u10),float(u50),float(u100),float(u200),float(v10),float(v50),float(v100),float(v200),float(tt2),float(rh2),float(psfc),float(ghi),float(diff),float(accprec),float(snow))
              #val = (zaman[t-6],x,y,float(lat),float(lon), float(1.1),float(1.1),float(1.1),float(1.1),float(1.1),float(1.1),float(1.1),float(1.1),float(1.1),float(1.1),float(1.1),float(ghi),float(diff),float(accprec),float(snow))
              mycursor = mydb.cursor()
              mycursor.execute(sql, val)
              mydb.commit()
              
              u10=0
              u50=0 
              u100=0
              u200=0
              v10=0 
              v50=0 
              v100=0 
              v200=0 
              tt2=0 
              rh2=0 
              psfc=0 
              ghi=0 
              diff=0 
              accprec=0 
              snow=0 
              say = 0

print("NC okundu Bitiş tarihi:" + datetime.now())
