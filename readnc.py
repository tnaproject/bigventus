import library.libReadNETCDF  as toolReadNC
import netCDF4
import numpy as np
import pandas as pd

ncUrl="ncFiles/wrfpost_2022-11-15_00.nc"

f = netCDF4.Dataset(ncUrl)

print(list(f.variables))

def wind_convert(u,v):
    conv = 45 / math.atan(1)
    wind_dir = (270 - math.atan2(v,u) * conv) % 360
    wind_speed = math.sqrt(math.pow(u,2) + math.pow(v,2))
    #wind_dir = 180 + (180 / math.pi) * math.atan2(v,u)
    return wind_speed,wind_dir


for name, variable in f.variables.items():            
    for attrname in variable.ncattrs():
        print("{} -- {}".format(attrname, getattr(variable, attrname)))
            # or :
            #print("{} -- {}".format(attrname, variable.getncattr(attrname))) 
    
 
#ws = sqrt(u2+v2)
#http://colaweb.gmu.edu/dev/clim301/lectures/wind/wind-uv#:~:text=A%20positive%20u%20wind%20is,wind%20is%20from%20the%20north.

    
x,y = f.variables['XLAT'], f.variables['XLONG']
    
V10 = f.variables['V10']

nm=V10[1,1,4]

print(V10[1,1,4])

print(V10[1,1,5])

print(V10[1,1,6])

print(V10[1,1,7])

print(V10[2,1,4])

print(V10[2,1,5])

print(V10[2,1,6])
    
print(V10[2,1,7])

coordinates=[[0,0,0,0,0]]

print("NC Okunuyor")
    
