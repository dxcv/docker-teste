from marketdata import MarketData
from tradingsystem import TradingSystem
from strategy import Strategy


def evaluate(strategy, type, files):
    strategy.clear()
    data = MarketData()

    ts = TradingSystem()

    for instrument, file in files.items():
        ts.createBook(instrument)
        ts.subscribe(instrument, strategy)
        if type == MarketData.TICK:
            data.loadBBGTick(file, instrument)
        elif type == MarketData.HIST:
            data.loadYAHOOHist(file, instrument)
        elif type == MarketData.INTR:
            data.loadBBGIntr(file, instrument)

    data.run(ts)

    ts.submit(strategy.id, strategy.close())
    return strategy.summary()


def evaluateMult(strategies, instruments_list, var):
    #print('types: ', types)
    #print('files: ', files)
    #print('strategies: ', strategies)

    if(len(strategies) != len(instruments_list)):
        return("ERROR! - evaluateMult arguments require the same number of items")

    for s in strategies:
        s.clear()

    data = MarketData()
    ts = TradingSystem()

    ts.createRisk(var)

    for i in range(0, len(strategies)):
        for instrument in instruments_list[i]:
            ts.createBook(instrument)
            ts.subscribe(instrument, strategies[i])
            '''
      if(types[i] == MarketData.TICK):
        data.loadBBGTick(file, instrument)
      if(types[i] == MarketData.HIST):
        data.loadYAHOOHist(file, instrument)
      if(types[i] == MarketData.INTR):
        data.loadBBGIntr(file, instrument)
      '''

    #data.loadBBGTick('2018-03-07.csv', "PETR4")
    data.loadFTP()

    data.run(ts)

    summary = []

    for strategy in strategies:
        print("strategy")
        ts.submit(strategy.id, strategy.close())
        summary.append(strategy.summary(name=strategy.name))

    return summary


def evaluateTick(strategy, files):
    return evaluate(strategy, MarketData.TICK, files)


def evaluateHist(strategy, files):
    return evaluate(strategy, MarketData.HIST, files)


def evaluateIntr(strategy, files):
    return evaluate(strategy, MarketData.INTR, files)
