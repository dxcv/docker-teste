
import os
from backtesting import MarketData, evaluateMult, evaluateTick

strategy_files = []
# r=root, d=directories, f = files
for r, d, f in os.walk("./strategies"):
    for file in f:
        if file[-3:] == ".py":
            strategy_files.append(file[:-3])

sm_relations = {}
sp_relations = {}

with open("relations.txt", "r") as file:
    line = file.readline()
    while line:
        relat = line.strip('\n').split(':')
        sm_relations[relat[0]] = relat[1]
        line = file.readline()

print(sm_relations)


strategy_list = []

for f in strategy_files:
    #print("sou um " + f)
    # print(type(f))
    if(f in sm_relations):
        # exec(f"from strategies.{f} import {f}")
        print(sm_relations[f])
        # exec(f"strategy_list.append({f}(sm_relations['{f}']))")

instrument_list = []

for s in strategy_list:
    print(s.name)
    instrument_list.append(['ABEV3', 'PETR3'])

print("instrumentos:", instrument_list)

response = evaluateMult(strategy_list, instrument_list, 10000)

for summary in response:
    print("Summary")
    print(summary)
