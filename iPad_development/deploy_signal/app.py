import streamlit as st
import pandas as pd
import os
from streamlit_autorefresh import st_autorefresh
from datetime import datetime, timedelta

st.set_page_config(page_title="Portfolio iPad", layout="wide")
st.title("Portfolio iPad")

# ================= CSV PATHS =================
CLIENT_LIVE_CSV = "/root/development/EAZ/iPad_development/fetchClientAlphaData/AlphaCumulative_U4560001_LIVE.csv"
OPEN_POSITIONS_CSV = "/root/development/EAZ/iPad_development/fetchClientAlphaData/portfolio_iPad_logs/positions/open/open_positions.csv"
CLOSE_POSITIONS_CSV = "/root/development/EAZ/iPad_development/fetchClientAlphaData/portfolio_iPad_logs/positions/close/close_positions.csv"

# ================= AUTO REFRESH =================
st_autorefresh(interval=5 * 1000, key="auto_refresh")

# ================= CSV LOADER =================
def load_csv(path):
    if os.path.exists(path):
        return pd.read_csv(path)
    else:
        return pd.DataFrame()

# ================= LAST OPEN POSITION =================
df_open_last = load_csv(OPEN_POSITIONS_CSV)
if not df_open_last.empty:
    last_row = df_open_last.iloc[[-1]]  # keep as DataFrame
    st.subheader("📌 Last Open Position")
    st.dataframe(last_row, use_container_width=True)

# ================= SIDEBAR =================
st.sidebar.header("📂 Dashboard Menu")
page = st.sidebar.radio(
    "Select Section",
    ["Open Trades", "Closed Trades", "Client Data"]
)

# ================= POPUP ALERT FUNCTION =================
def show_popup(message, duration=5):
    """Show a temporary alert message for `duration` seconds"""
    # Create a persistent placeholder in session_state
    if 'popup_placeholder' not in st.session_state:
        st.session_state['popup_placeholder'] = st.empty()
    st.session_state['popup_placeholder'].success(message)
    # Set popup end time
    st.session_state['popup_end_time'] = datetime.now() + timedelta(seconds=duration)

# ================= POPUP TIMER CHECK =================
if 'popup_end_time' in st.session_state:
    if datetime.now() > st.session_state['popup_end_time']:
        if 'popup_placeholder' in st.session_state:
            st.session_state['popup_placeholder'].empty()
        st.session_state.pop('popup_end_time', None)
        st.session_state.pop('popup_placeholder', None)

# ================= OPEN TRADES =================
if page == "Open Trades":
    st.subheader("🟢 Open Positions")
    df_open = df_open_last.copy()  # reload full open CSV
    if df_open.empty:
        st.info("No open positions")
    else:
        st.dataframe(df_open, use_container_width=True)
        col1, col2, col3 = st.columns(3)
        col1.metric("Open Trades", len(df_open))
        col2.metric("Total Qty", df_open["Quantity"].sum())
        col3.metric("Total PnL", round(df_open["Pnl"].sum(), 2))

        # ================= NEW OPEN POSITION POPUP =================
        if 'last_open_count' not in st.session_state:
            st.session_state['last_open_count'] = len(df_open)
        if len(df_open) > st.session_state['last_open_count']:
            show_popup("🟢 New Open Position Entered!", duration=5)
        st.session_state['last_open_count'] = len(df_open)

# ================= CLOSED TRADES =================
elif page == "Closed Trades":
    st.subheader("🔴 Closed Positions")
    df_close = load_csv(CLOSE_POSITIONS_CSV)
    if df_close.empty:
        st.info("No closed trades")
    else:
        st.dataframe(df_close, use_container_width=True)
        col1, col2, col3 = st.columns(3)
        col1.metric("Closed Trades", len(df_close))
        col2.metric("Total Qty", df_close["Quantity"].sum())
        col3.metric("Total PnL", round(df_close["Pnl"].sum(), 2))

        # ================= NEW CLOSED POSITION POPUP =================
        if 'last_close_count' not in st.session_state:
            st.session_state['last_close_count'] = len(df_close)
        if len(df_close) > st.session_state['last_close_count']:
            show_popup("🔴 New Closed Position!", duration=5)
        st.session_state['last_close_count'] = len(df_close)

# ================= CLIENT DATA =================
elif page == "Client Data":
    st.subheader("📈 Client LIVE Cumulative PnL")
    df_client = load_csv(CLIENT_LIVE_CSV)
    if df_client.empty:
        st.info("No client data available")
    else:
        st.dataframe(df_client, use_container_width=True)
        col1, col2, col3 = st.columns(3)
        col1.metric("Rows", len(df_client))
        col2.metric("Latest PnL", round(df_client["accumulated_pnl"].iloc[-1], 2))
        col3.metric("Last Update", df_client["datetime"].iloc[-1])
