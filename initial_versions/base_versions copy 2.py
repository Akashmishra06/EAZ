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
            df = getFnoBacktestData(indexSym, startEpoch-(86400*500), endEpoch, "1Min")
        except Exception as e:
            self.strategyLogger.info(f"Data not found for spot {baseSym}")
            raise Exception(e)

        df['ema10'] = talib.EMA(df['c'], timeperiod=10)
        df['prev_ema'] = df['ema10'].shift(1)
        prev_c = df['c'].shift(1)

        curr_50 = (df['c'] // 50) * 50
        df['curr_50'] = (df['c'] // 50) * 50
        prev_50 = (prev_c // 50) * 50

        df['upBreakSpotFiftyMultiple'] = np.where(curr_50 > prev_50, "upBreakSpotFiftyMultiple", "")
        df['downBreakSpotFiftyMultiple'] = np.where(curr_50 < prev_50, "downBreakSpotFiftyMultiple", "")

        df['callEntry'] = np.where((df['downBreakSpotFiftyMultiple'] == "downBreakSpotFiftyMultiple"), "callEntry", "")
        df['putEntry'] = np.where((df['upBreakSpotFiftyMultiple'] == "upBreakSpotFiftyMultiple"), "putEntry", "")
        df['callExit'] = np.where((df['ema10'] > df['ema10'].shift(1)), "callExit", "")
        df['putExit'] = np.where((df['ema10'] < df['ema10'].shift(1)), "putExit", "")

        df.dropna(inplace=True)
        df = df[df.index >= startEpoch]
        df.to_csv(f"{self.fileDir['backtestResultsCandleData']}{indexSym}_1Min.csv")

        lastIndexTimeData = [0, 0]
        Currentexpiry = getExpiryData(startEpoch, baseSym)['CurrentExpiry']
        lotSize = int(getExpiryData(self.timeData, baseSym)["LotSize"])
        expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
        expiryEpoch = expiryDatetime.timestamp()

        callEntry1 = False
        callEntry1_c = None
        putEntry1 = False
        putEntry1_c = None

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
                    except Exception as e:
                        self.strategyLogger.info(e)
                self.strategyLogger.info(f"{self.humanTime} | current strangle: {self.openPnl['CurrentPrice'].sum()} | entry strangle: {self.openPnl['EntryPrice'].sum()}")
            self.pnlCalculator()

            if self.humanTime.date() > expiryDatetime.date():
                Currentexpiry = getExpiryData(self.timeData + 86400, baseSym)['CurrentExpiry']
                expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
                expiryEpoch = expiryDatetime.timestamp()

            if self.humanTime.date() != expiryDatetime.date():
                continue

            if not self.openPnl.empty and lastIndexTimeData[1] in df.index:
                for index, row in self.openPnl.iterrows():

                    symbol = row["Symbol"]
                    symSide = symbol[-2:]

                    try:
                        if self.humanTime.time() >= time(15, 15):
                            exitType = f"timeUp"
                            self.exitOrder(index, exitType)

                        elif (row['EntryPrice'] * 1.5) < row['CurrentPrice']:
                            exitType = f"Stoploss"
                            self.exitOrder(index, exitType)

                        elif (row['EntryPrice'] * 0.2) > row['CurrentPrice']:
                            exitType = f"Target"
                            self.exitOrder(index, exitType)

                        elif symSide == "CE" and df.at[lastIndexTimeData[1], "callExit"] == "callExit" and row['strike'] < df.at[lastIndexTimeData[1], "c"] and df.at[lastIndexTimeData[1], "ema10"] > row['strike']:
                            exitType = f"callRsiExit"
                            self.exitOrder(index, exitType)

                        elif symSide == "PE" and df.at[lastIndexTimeData[1], "putExit"] == "putExit" and row['strike'] > df.at[lastIndexTimeData[1], "c"] and df.at[lastIndexTimeData[1], "ema10"] < row['strike']:
                            exitType = f"putRsiExit"
                            self.exitOrder(index, exitType)

                    except Exception as e:
                        self.strategyLogger.info(f"{self.humanTime} exit block Exception occurred: {e}")

            putTradeCounter = self.openPnl['Symbol'].str[-2:].value_counts().get('PE', 0)
            callTradeCounter = self.openPnl['Symbol'].str[-2:].value_counts().get('CE', 0)


            if not self.openPnl.empty:

                # identify option type from symbol
                is_CE = self.openPnl['Symbol'].str.endswith('CE')
                is_PE = self.openPnl['Symbol'].str.endswith('PE')

                # separate strike lists
                strike_list_CE = self.openPnl.loc[is_CE, 'strike'].dropna().tolist()
                strike_list_PE = self.openPnl.loc[is_PE, 'strike'].dropna().tolist()

            if lastIndexTimeData[1] in df.index and self.humanTime.time() < time(15, 15) and self.humanTime.time() >= time(9, 16):

                if df.at[lastIndexTimeData[1], "putEntry"] == "putEntry":
                    putEntry1 = True
                    putEntry1_c = df.at[lastIndexTimeData[1], "curr_50"]
                    
                if df.at[lastIndexTimeData[1], "callEntry"] == "callEntry":
                    callEntry1_c = df.at[lastIndexTimeData[1], "curr_50"]
                    callEntry1 = True
                
                if putEntry1 and putEntry1_c > df.at[lastIndexTimeData[1], "ema10"]:
                    putEntry1 = False
                    putEntry1_c = None

                if callEntry1 and callEntry1_c < df.at[lastIndexTimeData[1], "ema10"]:
                    callEntry1 = False
                    callEntry1_c = None

            if lastIndexTimeData[1] in df.index and self.humanTime.time() < time(15, 15) and self.humanTime.time() >= time(9, 16):

                if putEntry1 and df.at[lastIndexTimeData[1], "ema10"] > df.at[lastIndexTimeData[1], "prev_ema"]:

                    try:
                        underlying_price = df.at[lastIndexTimeData[1], "c"]
                        put_sym_atm = self.getPutSym(self.timeData, baseSym, underlying_price, Currentexpiry)
                        data_put_atm = self.fetchAndCacheFnoHistData(put_sym_atm, lastIndexTimeData[1])
                        strike = int(put_sym_atm[-7:-2])
                        if not self.openPnl.empty:
                            if strike in strike_list_PE:
                                continue
                        self.entryOrder(data_put_atm["c"], put_sym_atm, lotSize, "SELL", {"Expiry": expiryEpoch, "strike": strike})
                        putEntry1 = False
                        putEntry1_c = None
                    except Exception as e:
                        self.strategyLogger.info(f"{self.humanTime} Exception occurred in PUT branch: {e}")

                elif callEntry1 and df.at[lastIndexTimeData[1], "prev_ema"] > df.at[lastIndexTimeData[1], "ema10"]:

                    try:
                        underlying_price = df.at[lastIndexTimeData[1], "c"]
                        call_sym_atm = self.getCallSym(self.timeData, baseSym, underlying_price, Currentexpiry)
                        data_call_atm = self.fetchAndCacheFnoHistData(call_sym_atm, lastIndexTimeData[1])
                        strike = int(call_sym_atm[-7:-2])
                        if not self.openPnl.empty:
                            if strike in strike_list_CE:
                                continue
                        self.entryOrder(data_call_atm["c"], call_sym_atm, lotSize, "SELL", {"Expiry": expiryEpoch, "strike": strike})
                        callEntry1 = False
                        callEntry1_c = None
                    except Exception as e:
                        self.strategyLogger.info(f"{self.humanTime} Exception occurred in CALL branch: {e}")

        self.pnlCalculator()
        self.combinePnlCsv()

        return self.closedPnl, self.fileDir["backtestResultsStrategyUid"]


if __name__ == "__main__":
    startTime = datetime.now()

    devName = "AM"
    strategyName = "base_versions"
    version = "v1_EAZ"

    startDate = datetime(2026, 1, 1, 9, 15)
    endDate = datetime(2026, 1, 31, 15, 30)

    algo = algoLogic(devName, strategyName, version)

    baseSym = "NIFTY"
    indexName = "NIFTY 50"

    closedPnl, fileDir = algo.run(startDate, endDate, baseSym, indexName)

    print("Calculating Daily Pnl")
    # dr = calculate_mtm(closedPnl, fileDir, timeFrame="1Min", mtm=False, equityMarket=False)

    endTime = datetime.now()
    print(f"Done. Ended in {endTime - startTime}")