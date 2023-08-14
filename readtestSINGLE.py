import os
import datapoint as dp
import time
import tqdm
import numpy as np

start = time.time()

datapath = "F:\\DataforAndy\\ASOS\\SEEDdata\\"

if not os.path.exists(datapath):
     print("Datapath does not exist!")
     exit(-1)

filenames = os.listdir(datapath)

datapoints = {}

for i in tqdm.tqdm(filenames):
    datapoints[i] = []

    with open(datapath+i, "r") as f:
        lines = f.readlines()
    
    for line in lines:
            try:
                parts = line.split()
                
                if(len(parts) < 2 or len(parts[0]) != 9 or len(parts[1]) != 19):
                    continue
                callsign = line[10:13]
                year = line[13:17]
                month = line[17:19]
                day = line[19:21]
                loctime = line[21:25]
                utctime = line[25:29]

                winddir = line[71:74]
                windspeed = line[76:79]

                gustdir = line[81:84]
                gustspeed = line[86:89]

                dp.Datapoint(callsign, year, month, day, loctime, utctime, winddir, windspeed, gustdir, gustspeed)
            except:
                print("Failed: {line}!")
                continue

elapsed = time.time() - start
print(f"{elapsed} s")