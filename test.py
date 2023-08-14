import time
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import animation
import animatplot as amp

datadir = "S:\CRREL\ASOSSEEDtestdata\\"
filedir = "64050K0J4201801.dat"
kfac = 1.9438444924406

start = time.perf_counter()

with open(datadir+filedir) as f:
    lines = f.readlines()

plotdata = []

for i in lines:
    parts = i.split()

    callsign = parts[1][0:3]
    year = parts[1][3:7]
    month = parts[1][7:9]
    day = parts[1][9:11]
    loctime = parts[1][11:15]
    utctime = parts[1][15:19]
    if parts[4] == "M" or parts[5] == "M":
        plotdata.append((0, 0, 0, f"{month}/{day}/{year}", loctime))
        continue
    direction = int(parts[4])
    avgvel = int(parts[5]) * kfac
    plotdata.append(((avgvel * np.cos(np.deg2rad(direction + 90))), (avgvel * np.sin(np.deg2rad(direction + 90))), avgvel, f"{month}/{day}/{year}", loctime))

    print(f"{callsign}|| {month}/{day}/{year} - local ({loctime[0:2]}:{loctime[2:4]}) : UTC ({utctime[0:2]}:{utctime[2:4]}) || {(avgvel):06.3f} m/s at {direction: >3}°")

print(f"{time.perf_counter() - start} s")


fig, ax = plt.subplots()

t = np.linspace(0,len(plotdata),len(plotdata))

def animate(i):
    ax.set_title(f"{plotdata[i][3]} : LOCAL {plotdata[i][4][0:2]}:{plotdata[i][4][2:4]}")
    ax.set_aspect('equal')
    ax.set(xlim=(-30, 30), ylim=(-30,30))
    
    qax = ax.quiver(.000001,.000001,plotdata[i][0], plotdata[i][1], scale_units='xy', scale=1.)

animate(0)
timeline = amp.Timeline(t, units='m', fps=30)
block = amp.blocks.Nuke(animate, length=len(timeline), ax=ax)
anim = amp.Animation([block], timeline)

anim.controls()
#anim.save_gif('0J4')
plt.show()