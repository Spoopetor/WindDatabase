import datetime
import numpy as np

class Datapoint:

    def __init__(self, callsign, year, month, day, localtime, utctime, winddir, windspeed, gustdir, gustspeed):
        winddir = int(winddir) if (str(winddir).isnumeric() and winddir != np.NaN) else np.NaN
        windspeed = int(windspeed) if (str(windspeed).isnumeric() and windspeed != np.NaN) else np.NaN
        gustdir = int(gustdir) if (str(gustdir).isnumeric() and gustdir != np.NaN) else np.NaN
        gustspeed = int(gustspeed) if (str(gustspeed).isnumeric() and gustspeed != np.NaN) else np.NaN
        self.callsign = callsign
        self.locdatetime = datetime.datetime(int(year), int(month), int(day), int(localtime[0:2]), int(localtime[2:4]))
        self.utctime = datetime.time(int(utctime[0:2]), int(utctime[2:4]))
        self.winddir = winddir
        self.windspeed = windspeed
        self.gustdir = gustdir
        self.gustspeed = gustspeed
