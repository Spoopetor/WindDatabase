import numpy as np
from multiprocessing.pool import ThreadPool
from psycopg2.pool import ThreadedConnectionPool as tcp
import datetime as dt
import tqdm

MAXTHREADS = 16

BASEPOLLRATE = 60 #seconds between each sample
MAXPOLLRATE = 1800 #seconds between maximum supersamples

def tupToVal(t):
    return t[0]



def subsample(cs):
    
    conn = connpool.getconn()
    curs = conn.cursor()

    curs.execute(f"SELECT * FROM windgust WHERE callsign = \'{cs}\' ORDER BY loctime LIMIT 1")
    earliest = curs.fetchone()[2]

    curs.execute(f"SELECT * FROM windgust WHERE callsign = \'{cs}\' ORDER BY loctime DESC LIMIT 1")
    latest = curs.fetchone()[2]

    samples = []
    gettable = "windgust"
    samplerates = []
    i = BASEPOLLRATE
    while i <= MAXPOLLRATE:
        i*=2
        samplerates.append(i)
        if i > 2*BASEPOLLRATE:
            gettable = f"wg{int(i//2)}"
        
        for j in range(int(dt.datetime.timestamp(earliest)), int(dt.datetime.timestamp(latest) - i), i):
            curs.execute(f"SELECT * FROM {gettable} WHERE callsign = \'{cs}\' AND loctime between \'{dt.datetime.fromtimestamp(j)}\' AND \'{dt.datetime.fromtimestamp(j+i-1)}\'")
            samples = curs.fetchall()
            if(len(samples) < (i//(BASEPOLLRATE))):
                
                continue
            try:
                if gettable == "windgust":
                    avgwind = np.average(list(w[5] for w in samples))
                    maxgust = np.max(list(g[7] for g in samples))
                else:
                    avgwind = np.average(list(w[4] for w in samples))
                    maxgust = np.max(list(g[5] for g in samples))
                curs.execute(f"INSERT INTO wg{int(i)} (callsign, loctime, utctime, windspeed, gustspeed) VALUES (\'{cs}\', \'{dt.datetime.fromtimestamp(j)}\', \'{samples[0][3].strftime('%H:%M:%S')}\', {avgwind}, {maxgust})")
                conn.commit()
            except:
                continue 
        
    connpool.putconn(conn)

def getCreateTableQuery(timespan):
    return f"create table if not exists wg{timespan} (datid serial8 primary key, callsign char(3), loctime timestamp, utctime time, windspeed double precision, gustspeed double precision)"

params = {
    'database': 'postgres',
    'user': 'postgres',
    'password': 'drowssaP',
    'host': 'localhost',
    'port': 5432
}

try:
    connpool = tcp(MAXTHREADS, MAXTHREADS, **params)
    print(f"CONNECTED TO DATABASE {params.get('database')}@{params.get('host')}:{params.get('port')}")
except:
    print("Unable to connect to database! QUITTING!")
    exit(1)

conn = connpool.getconn()
curs = conn.cursor()

curs.execute("SELECT DISTINCT callsign FROM windgust")
callsigns = list(map(tupToVal,curs.fetchall()))

#print(callsigns)

i = BASEPOLLRATE
while i <= MAXPOLLRATE:
    i*=2
    curs.execute(getCreateTableQuery(i))
    conn.commit()
    

connpool.putconn(conn)

with ThreadPool(MAXTHREADS) as pool:
    list(tqdm.tqdm(pool.imap(subsample, callsigns), total=len(callsigns)))



'''for c in callsigns:
    subsample(c)'''