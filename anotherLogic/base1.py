from backtestTools.histData import getFnoBacktestData, connectToMongo
from backtestTools.algoLogic import optOverNightAlgoLogic
from backtestTools.expiry import getExpiryData
from backtestTools.util import calculate_mtm
from datetime import datetime, time
import numpy as np
import talib


class algoLogic(optOverNightAlgoLogic):

    global conn
    conn = connectToMongo()

    def fetchAndCacheFnoHistData(self, symbol, timestamp, maxCacheSize=100, conn=conn):

        if len(self.symbolDataCache) > maxCacheSize:
            symbolToDelete = []

            for sym in self.symbolDataCache.keys():
                idx = next(i for i, char in enumerate(sym) if char.isdigit())
                optionExpiry = (datetime.strptime(sym[idx:idx + 7], "%d%b%y").timestamp() + 55800)

                if self.timeData > optionExpiry:
                    symbolToDelete.append(sym)

            if symbolToDelete:
                for sym in symbolToDelete:
                    del self.symbolDataCache[sym]

        if symbol in self.symbolDataCache.keys():
            return self.symbolDataCache[symbol].loc[timestamp]

        else:
            idx = next(i for i, char in enumerate(symbol) if char.isdigit())
            optionExpiry = (datetime.strptime(symbol[idx:idx + 7], "%d%b%y").timestamp() + 55800)
            self.symbolDataCache[symbol] = getFnoBacktestData(symbol, timestamp, optionExpiry, "1Min", conn)

            return self.symbolDataCache[symbol].loc[timestamp]


    def run(self, startDate, endDate, baseSym, indexSym):

        startEpoch = startDate.timestamp()
        endEpoch = endDate.timestamp()

        try:
            df = getFnoBacktestData(indexSym, startEpoch, endEpoch, "1Min")
        except Exception as e:
            self.strategyLogger.info(f"Data not found for spot {baseSym}")
            raise Exception(e)

        df['rsi'] = talib.RSI(df['c'], timeperiod=14)
        df['callSell'] = np.where((df['rsi'] < 50) & (df['c'] < df['o']) & ((df['rsi'] > 30)), "callSell", "")
        df['putSell'] = np.where((df['rsi'] > 50) & (df['c'] > df['o']) & ((df['rsi'] < 70)), "putSell", "")

        df['callExit'] = np.where((df['rsi'] > 70) & (df['rsi'].shift(1) > 70) & (df['c'] > df['o']), "callExit", "")
        df['putExit'] = np.where((df['rsi'] < 30) & (df['rsi'].shift(1) < 30) & (df['c'] < df['o']), "putExit", "")

        df.dropna(inplace=True)
        df.to_csv(f"{self.fileDir['backtestResultsCandleData']}{indexSym}_1Min.csv")

        lastIndexTimeData = [0, 0]
        Currentexpiry = getExpiryData(startEpoch, baseSym)['NextExpiry']
        lotSize = int(getExpiryData(self.timeData, baseSym)["LotSize"])
        expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
        expiryEpoch = expiryDatetime.timestamp()

        for timeData in df.index:

            self.timeData = float(timeData)
            self.humanTime = datetime.fromtimestamp(timeData)
            print(self.humanTime)

            if (self.humanTime.time() < time(9, 16)) | (self.humanTime.time() > time(15, 30)):
                continue

            lastIndexTimeData.pop(0)
            lastIndexTimeData.append(timeData - 60)

            if (self.humanTime.time() < time(9, 16)) | (self.humanTime.time() > time(15, 25)):
                continue

            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():
                    try:
                        data = self.fetchAndCacheFnoHistData(row["Symbol"], lastIndexTimeData[1])
                        self.openPnl.at[index, "CurrentPrice"] = data["c"]
                        self.strategyLogger.info(f"EntryPrice:{row['EntryPrice']}, Symbol: {row['Symbol']}, CurrentPrice:{row['CurrentPrice']}")

                        if row['CurrentPrice'] < (row['EntryPrice'] * 0.1):
                            self.openPnl.at[index, "Stoploss"] = row['EntryPrice'] * 0.2
                            self.strategyLogger.info(f"SL1 EntryPrice:{row['EntryPrice']} CurrentPrice:{row['CurrentPrice']} NewSL:{row['EntryPrice'] * 0.2}")

                        elif row['CurrentPrice'] < (row['EntryPrice'] * 0.2):
                            self.openPnl.at[index, "Stoploss"] = row['EntryPrice'] * 0.3
                            self.strategyLogger.info(f"SL1 EntryPrice:{row['EntryPrice']} CurrentPrice:{row['CurrentPrice']} NewSL:{row['EntryPrice'] * 0.3}")

                        elif row['CurrentPrice'] < (row['EntryPrice'] * 0.3):
                            self.openPnl.at[index, "Stoploss"] = row['EntryPrice'] * 0.4
                            self.strategyLogger.info(f"SL1 EntryPrice:{row['EntryPrice']} CurrentPrice:{row['CurrentPrice']} NewSL:{row['EntryPrice'] * 0.4}")

                        elif row['CurrentPrice'] < (row['EntryPrice'] * 0.5):
                            self.openPnl.at[index, "Stoploss"] = row['EntryPrice']
                            self.strategyLogger.info(f"SL3 EntryPrice:{row['EntryPrice']} CurrentPrice:{row['CurrentPrice']} NewSL:{row['EntryPrice']}")

                    except Exception as e:
                        self.strategyLogger.info(e)
                self.strategyLogger.info(f"{self.humanTime} | current strangle: {self.openPnl['CurrentPrice'].sum()} | entry strangle: {self.openPnl['EntryPrice'].sum()}")
            self.pnlCalculator()

            if self.humanTime.date() >= expiryDatetime.date():
                Currentexpiry = getExpiryData(self.timeData + 86400, baseSym)['NextExpiry']
                expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
                expiryEpoch = expiryDatetime.timestamp()

            if not self.openPnl.empty and lastIndexTimeData[1] in df.index:

                for index, row in self.openPnl.iterrows():

                    symbol = row["Symbol"]
                    symSide = symbol[-2:]

                    try:
                        # if self.humanTime.time() >= time(15, 15):
                        #     exitType = f"timeUp"
                        #     self.exitOrder(index, exitType)
                            
                        if self.timeData >= row["Expiry"]:
                            exitType = f"ExpiryHit"
                            self.exitOrder(index, exitType)
                        
                        elif row['PositionStatus'] == -1:

                            if (row['EntryPrice'] * 2) < row['CurrentPrice']:
                                exitType = f"Stoploss"
                                self.exitOrder(index, exitType)

                            elif row['Stoploss'] < row['CurrentPrice']:
                                exitType = f"TSL"
                                self.exitOrder(index, exitType)

                            elif (row['EntryPrice'] * 0.1) > row['CurrentPrice']:
                                exitType = f"Target"
                                self.exitOrder(index, exitType)

                            if symSide == "CE" and df.at[lastIndexTimeData[1], "callExit"] == "callExit" and row['EntryPrice'] < row['CurrentPrice']:
                                exitType = f"callRsiExit"
                                self.exitOrder(index, exitType)

                            elif symSide == "PE" and df.at[lastIndexTimeData[1], "putExit"] == "putExit" and row['EntryPrice'] < row['CurrentPrice']:
                                exitType = f"putRsiExit"
                                self.exitOrder(index, exitType)

                    except Exception as e:
                        self.strategyLogger.info(f"{self.humanTime} exit block Exception occurred: {e}")

            if not self.openPnl.empty and lastIndexTimeData[1] in df.index:
                sym = self.openPnl['Symbol'].dropna().astype(str).str[-2:]
                callTradeCounter = (sym == 'CE').sum()
                putTradeCounter  = (sym == 'PE').sum()
                for index, row in self.openPnl.iterrows():
                    
                    symbol = row["Symbol"]
                    symSide = symbol[-2:]
                    try:

                        if row['PositionStatus'] == 1:

                            if symSide == "CE" and callTradeCounter == 1:
                                exitType = f"CE_spread_exit"
                                self.exitOrder(index, exitType)

                            elif symSide == "PE" and putTradeCounter == 1:
                                exitType = f"PE_spread_exit"
                                self.exitOrder(index, exitType)
                    except Exception as e:
                        self.strategyLogger.info(f"{self.humanTime} exit block Exception occurred: {e}")

            sym = self.openPnl['Symbol'].dropna().astype(str).str[-2:]
            callTradeCounter = (sym == 'CE').sum()
            putTradeCounter  = (sym == 'PE').sum()

            if lastIndexTimeData[1] in df.index and self.humanTime.time() < time(15, 15) and self.humanTime.time() >= time(9, 16):

                if df.at[lastIndexTimeData[1], "putSell"] == "putSell" and putTradeCounter == 0:

                    try:
                        underlying_price = df.at[lastIndexTimeData[1], "c"]
                        putSym = self.getPutSym(self.timeData, baseSym, underlying_price, Currentexpiry, 2)
                        putSymHedge = self.getPutSym(self.timeData, baseSym, underlying_price, Currentexpiry, 4)

                        dataPut = self.fetchAndCacheFnoHistData(putSym, lastIndexTimeData[1])
                        dataPutHedge = self.fetchAndCacheFnoHistData(putSymHedge, lastIndexTimeData[1])

                        self.entryOrder(dataPut["c"], putSym, lotSize, "SELL", {"Expiry": expiryEpoch})
                        self.entryOrder(dataPutHedge["c"], putSymHedge, lotSize, "BUY", {"Expiry": expiryEpoch})

                    except Exception as e:
                        self.strategyLogger.info(f"{self.humanTime} Exception occurred in PUT branch: {e}")

                elif df.at[lastIndexTimeData[1], "callSell"] == "callSell" and callTradeCounter == 0:

                    try:
                        underlying_price = df.at[lastIndexTimeData[1], "c"]
                        callSym = self.getCallSym(self.timeData, baseSym, underlying_price, Currentexpiry, 2)
                        callSymHedge = self.getCallSym(self.timeData, baseSym, underlying_price, Currentexpiry, 4)

                        dataCall = self.fetchAndCacheFnoHistData(callSym, lastIndexTimeData[1])
                        dataCallHedge = self.fetchAndCacheFnoHistData(callSymHedge, lastIndexTimeData[1])

                        self.entryOrder(dataCall["c"], callSym, lotSize, "SELL", {"Expiry": expiryEpoch})
                        self.entryOrder(dataCallHedge["c"], callSymHedge, lotSize, "BUY", {"Expiry": expiryEpoch})

                    except Exception as e:
                        self.strategyLogger.info(f"{self.humanTime} Exception occurred in CALL branch: {e}")

        self.pnlCalculator()
        self.combinePnlCsv()

        return self.closedPnl, self.fileDir["backtestResultsStrategyUid"]


if __name__ == "__main__":
    startTime = datetime.now()

    devName = "AM"
    strategyName = "base_1"
    version = "v1"

    startDate = datetime(2024, 1, 1, 9, 15)
    endDate = datetime(2026, 1, 31, 15, 30)

    algo = algoLogic(devName, strategyName, version)

    baseSym = "NIFTY"
    indexName = "NIFTY 50"

    closedPnl, fileDir = algo.run(startDate, endDate, baseSym, indexName)

    print("Calculating Daily Pnl")
    dr = calculate_mtm(closedPnl, fileDir, timeFrame="1Min", mtm=False, equityMarket=False)

    endTime = datetime.now()
    print(f"Done. Ended in {endTime - startTime}")