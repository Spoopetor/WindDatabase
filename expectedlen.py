import os
import tqdm

datapath = "F:\\DataforAndy\\ASOS\\SEEDdata\\"

expectedlen = {}

filenames = os.listdir(datapath)
print("Getting Expected Data Length...")
for i in tqdm.tqdm(filenames):
    with open(datapath+i, "r") as f:
        lines = f.readlines()
    
    for line in lines:
        parts = line.split()
        if len(parts) < 2:
            continue
        callsign = parts[1][0:3]
        if callsign not in expectedlen.keys():
            expectedlen[callsign] = []
        expectedlen[callsign].append(len(parts))

for c in expectedlen.keys():
    exlens = expectedlen[c]
    expectedlen[c] = round(sum(exlens) / len(exlens))

    print(f"{c} : {expectedlen[c]}")
