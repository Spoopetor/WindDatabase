import os
import datapoint as dp
import datetime as dt
import time
import tqdm
import numpy as np
from multiprocessing.pool import ThreadPool
from psycopg2.pool import ThreadedConnectionPool as tcp

MAXTHREADS = 16

params = {
    'database': 'postgres',
    'user': 'postgres',
    'password': 'drowssaP',
    'host': 'localhost',
    'port': 5432
}

start = time.time()

datapath = ""

while not os.path.exists(datapath):
    

    datapath = input("Input datapath: ")

    if not os.path.exists(datapath):
        print("Datapath does not exist!")


filenames = os.listdir(datapath)

datapoints = {}
fails = []
try:
    connpool = tcp(MAXTHREADS, MAXTHREADS, **params)
    print(f"CONNECTED TO DATABASE {params.get('database')}@{params.get('host')}:{params.get('port')}")
except:
    print("Unable to connect to database! QUITTING!")
    exit(1)

def processFile(i):

    with open(datapath+i, "r") as f:
        lines = f.readlines()
    
    conn = connpool.getconn()
    curs = conn.cursor()
    qArgs = []
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

                locdatetime = dt.datetime(int(year), int(month), int(day), int(loctime[0:2]), int(loctime[2:4]))
                utctimestamp = dt.time(int(utctime[0:2]), int(utctime[2:4]))

                winddir = int(line[71:74])
                windspeed = int(line[76:79])

                gustdir = int(line[81:84])
                gustspeed = int(line[86:89])
                
                qArgs.append((callsign, locdatetime.strftime("%Y-%m-%d %H:%M:%S"), utctimestamp.strftime("%H:%M:%S"), winddir, windspeed, gustdir, gustspeed))
            except:
                fails.append(line)
                continue
    query = "INSERT INTO WindgustFull (callsign, loctime, utctime, winddir, windspeed, gustdir, gustspeed) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    curs.executemany(query, qArgs)
    conn.commit()
    connpool.putconn(conn)
        
with ThreadPool(MAXTHREADS) as pool:
    list(tqdm.tqdm(pool.imap(processFile, filenames), total=len(filenames)))
        
elapsed = time.time() - start
print(f"{elapsed} s")

print(f"{len(fails)} Fails!")
input("Press Enter to see Failed Lines: ")
for f in fails:
    print(f"\t{f}")
