from psycopg2.pool import ThreadedConnectionPool as tcp
MAXTHREADS = 8

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

curs.execute("create table if not exists windgust (datid serial8 primary key, callsign char(3), loctime timestamp, utctime time, winddir smallint, windspeed double precision, gustdir smallint, gustspeed double precision)")
conn.commit()
connpool.putconn(conn)
print("Success!")