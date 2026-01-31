from datetime import datetime, timedelta, date
from connectToMongo import connectToMongo
import pandas as pd
import numpy as np
import talib
import time
import os
import logging

client = connectToMongo()

BASE_DIR = "portfolio_iPad_logs"
OPEN_CSV = f"{BASE_DIR}/positions/open/open_positions.csv"
CLOSE_CSV = f"{BASE_DIR}/positions/close/close_positions.csv"

HOLIDAY_DATES = [
    date(2026, 1, 15),
    date(2026, 1, 26),
    date(2026, 3, 3),
    date(2026, 3, 26),
    date(2026, 3, 31),
    date(2026, 4, 3),
    date(2026, 4, 14),
    date(2026, 5, 1),
    date(2026, 5, 28),
]

def setUpLogger():
    os.makedirs(f"{BASE_DIR}/logs", exist_ok=True)
    today = datetime.now().strftime("%Y-%m-%d")
    log_dir = f"{BASE_DIR}/logs/{today}"
    os.makedirs(log_dir, exist_ok=True)

    log_file = f"{log_dir}/algo_{datetime.now().strftime('%H-%M-%S')}.log"

    logger = logging.getLogger("PortfolioAlgo")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    if not logger.handlers:
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(filename)s:%(lineno)d | %(message)s"
        )
        fh = logging.FileHandler(log_file)
        ch = logging.StreamHandler()
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        logger.addHandler(fh)
        logger.addHandler(ch)

    return logger

logger = setUpLogger()

def fetch_alpha_cumulative_pnl(clientID, startDate, endDate):
    col = client["AlphaCumulativePnlDB"]["AlphaCumulative_Pnl"]

    query = {
        "clientID": clientID,
        "timestamp": {
            "$gte": int(startDate.timestamp()),
            "$lte": int(endDate.timestamp())
        }
    }

    data = list(col.find(query, {"_id": 0}))
    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)
    df["datetime"] = pd.to_datetime(df["date"].astype(str) + " " + df["time"].astype(str))
    df.sort_values("timestamp", inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df

def load_csv(path, columns):
    if os.path.exists(path):
        return pd.read_csv(path, parse_dates=True)
    return pd.DataFrame(columns=columns)

def save_csv(df, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)

def currentValues(clientID, startDate, endDate):
    df_main = fetch_alpha_cumulative_pnl(clientID, startDate, endDate)
    if df_main.empty:
        logger.error("No initial data")
        return

    open_cols = [
        "EntryTime", "Symbol", "EntryPrice",
        "CurrentPrice", "Quantity",
        "PositionStatus", "Pnl"
    ]

    close_cols = [
        "Key", "ExitTime", "Symbol",
        "EntryPrice", "ExitPrice",
        "Quantity", "PositionStatus",
        "Pnl", "ExitType"
    ]

    open_df = load_csv(OPEN_CSV, open_cols)
    close_df = load_csv(CLOSE_CSV, close_cols)

    while True:
        time.sleep(50)

        last_ts = int(df_main["timestamp"].max())

        col = client["AlphaCumulativePnlDB"]["AlphaCumulative_Pnl"]
        new_data = list(
            col.find(
                {
                    "clientID": clientID,
                    "timestamp": {"$gt": last_ts}
                },
                {"_id": 0}
            ).sort("timestamp", 1)
        )

        if not new_data:
            logger.info("No new data yet")
            continue

        df_new = pd.DataFrame(new_data)
        df_new["datetime"] = pd.to_datetime(
            df_new["date"].astype(str) + " " + df_new["time"].astype(str)
        )

        df_main = pd.concat([df_main, df_new]).drop_duplicates("timestamp")
        df_main.sort_values("timestamp", inplace=True)
        df_main.reset_index(drop=True, inplace=True)

        # ===== Indicators =====
        df_main["rsi"] = talib.RSI(df_main["accumulated_pnl"], 2)
        df_main["ema10"] = talib.EMA(df_main["accumulated_pnl"], 2)
        df_main["ema100"] = talib.EMA(df_main["accumulated_pnl"], 10)

        df_main["prev_ema10"] = df_main["ema10"].shift(1)
        df_main["prev_ema100"] = df_main["ema100"].shift(1)

        # ❗ only drop rows where indicators are not ready
        df_main = df_main[df_main["ema100"].notna()]

        df_main["entry_signal"] = np.where(
            (df_main["ema10"] > df_main["prev_ema10"]) &
            (df_main["ema100"] > df_main["prev_ema100"]) &
            (df_main["ema10"] > df_main["ema100"]),
            "entry_signal",
            ""
        )

        df_main["exit_signal"] = np.where(
            (df_main["ema10"] < df_main["ema100"]) &
            (df_main["rsi"] < 30),
            "exit_signal",
            ""
        )

        latest = df_main.iloc[-1]

        entry_signal = (
            latest["ema10"] > latest["prev_ema10"] and
            latest["ema100"] > latest["prev_ema100"] and
            latest["ema10"] > latest["ema100"]
        )

        # ===== ENTRY =====
        if entry_signal and open_df.empty:
            open_df = pd.DataFrame([{
                "EntryTime": latest["datetime"],
                "Symbol": clientID,
                "EntryPrice": latest["accumulated_pnl"],
                "CurrentPrice": latest["accumulated_pnl"],
                "Quantity": 1,
                "PositionStatus": 1,
                "Pnl": 0.0
            }])
            logger.info("ENTRY TAKEN")

        if not open_df.empty:
            open_df["CurrentPrice"] = latest["accumulated_pnl"]
            open_df["Pnl"] = (
                open_df["CurrentPrice"] - open_df["EntryPrice"]
            ) * open_df["Quantity"]

            row = open_df.iloc[0]
            pnl = open_df["Pnl"].iloc[0]
            exit_type = None

            if pnl <= -100000:
                exit_type = "STOPLOSS"

            elif (latest["datetime"] + timedelta(days=1)).date() in HOLIDAY_DATES:
                exit_type = "HOLIDAY_EXIT"

            elif latest["exit_signal"] == "exit_signal":
                exit_type = "EMA_RSI_EXIT"

            if exit_type:
                close_df = pd.concat([close_df, pd.DataFrame([{
                    "Key": row["EntryTime"],
                    "ExitTime": latest["datetime"],
                    "Symbol": row["Symbol"],
                    "EntryPrice": row["EntryPrice"],
                    "ExitPrice": latest["accumulated_pnl"],
                    "Quantity": row["Quantity"],
                    "PositionStatus": 1,
                    "Pnl": pnl,
                    "ExitType": exit_type
                }])], ignore_index=True)

                open_df = open_df.iloc[0:0]
                logger.info(f"EXIT TAKEN | {exit_type} | PnL: {pnl}")

        save_csv(open_df, OPEN_CSV)
        save_csv(close_df, CLOSE_CSV)
        df_main = df_main.sort_values(by="datetime", ascending=False)
        df_main.to_csv(f"AlphaCumulative_{clientID}_LIVE.csv", index=False)

if __name__ == "__main__":
    clientID = "U4560001"
    end = datetime.now()
    start = end - timedelta(days=30)
    currentValues(clientID, start, end)
