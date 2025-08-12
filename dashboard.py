import streamlit as st
import pandas as pd
import sqlite3
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from datetime import datetime, timedelta, timezone
from sklearn.linear_model import LinearRegression


try:
    from streamlit_autorefresh import st_autorefresh
    AUTORELOAD_AVAILABLE = True
except Exception:
    AUTORELOAD_AVAILABLE = False

#  Page Configuration 
st.set_page_config(
    page_title=" Real-Time Stock Sentiment vs Stock Price",
    page_icon="📈",
    layout="wide"
)

# Database Connection
DB_FILE = "sentiment_data.db"

@st.cache_data(ttl=60)
def get_data_from_db():
    conn = sqlite3.connect(DB_FILE)
    sentiment_query = "SELECT * FROM sentiment"
    df_sentiment = pd.read_sql_query(sentiment_query, conn)
    if not df_sentiment.empty:
        df_sentiment['timestamp_utc'] = pd.to_datetime(df_sentiment['timestamp_utc'], unit='s').dt.tz_localize('UTC')

    price_query = "SELECT * FROM stock_prices"
    df_prices = pd.read_sql_query(price_query, conn)
    if not df_prices.empty:
        df_prices['timestamp'] = pd.to_datetime(df_prices['timestamp'], unit='s').dt.tz_localize('UTC')

    conn.close()
    return df_sentiment, df_prices

# Load Data 
with st.spinner("Loading data..."):
    df_sentiment, df_prices = get_data_from_db()

if df_sentiment.empty:
    st.error("No sentiment data found.")
    st.stop()


if "all_symbols" not in st.session_state:
    st.session_state.all_symbols = sorted(df_sentiment['stock_symbol'].unique())

all_symbols = st.session_state.all_symbols

# Time references
now_utc = datetime.now(timezone.utc)
one_hour_ago = now_utc - timedelta(hours=1)
two_hours_ago = now_utc - timedelta(hours=2)
start_of_today = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)

# Header & Snapshot
st.title("Real-Time Stock Sentiment vs Stock Price")
st.caption("Monitor market sentiment and price action in real time. Updates every 60 seconds.")
st.markdown("---")

# KPIs 
st.header("💹 Live Sentiment & Volume Snapshot")

cols_per_row = 3
symbols = list(all_symbols)
remainder = len(symbols) % cols_per_row
if remainder != 0:
    symbols_padded = symbols + ["__EMPTY__"] * (cols_per_row - remainder)
else:
    symbols_padded = symbols

for i in range(0, len(symbols_padded), cols_per_row):
    row_symbols = symbols_padded[i:i+cols_per_row]
    row_cols = st.columns(cols_per_row)
    for col, symbol in zip(row_cols, row_symbols):
        if symbol == "__EMPTY__":
            col.empty()
            continue

        df_symbol = df_sentiment[df_sentiment['stock_symbol'] == symbol]

        with col:
            # clickable to Yahoo finance
            col.markdown(f"### [{symbol}](https://finance.yahoo.com/quote/{symbol})")

            # Sentiment 
            current_series = df_symbol[df_symbol['timestamp_utc'] >= one_hour_ago]['sentiment_score']
            previous_series = df_symbol[
                (df_symbol['timestamp_utc'] >= two_hours_ago) &
                (df_symbol['timestamp_utc'] < one_hour_ago)
            ]['sentiment_score']

            current_avg = current_series.mean() if not current_series.empty else np.nan
            previous_avg = previous_series.mean() if not previous_series.empty else np.nan

            # safe gain computation
            if pd.notna(current_avg) and pd.notna(previous_avg):
                gain = current_avg - previous_avg
            else:
                gain = 0.0

            # delta color: normal => green if positive, inverse => red if positive
            if gain > 0:
                delta_color = "normal"
            elif gain < 0:
                delta_color = "inverse"
            else:
                delta_color = "off"

            # display safely formatted value
            value_str = f"{current_avg:.2f}" if pd.notna(current_avg) else "N/A"
            delta_str = f"{gain:+.2f}"

            col.metric(label="Sentiment (1hr avg)", value=value_str, delta=delta_str, delta_color=delta_color)

            # Volume Spike Logic 
            comments_today = len(df_symbol[df_symbol['timestamp_utc'] >= start_of_today])
            comments_last_hour = len(df_symbol[df_symbol['timestamp_utc'] >= one_hour_ago])
            hours_elapsed = max(now_utc.hour + 1, 1)  
            avg_hourly_volume_today = comments_today / hours_elapsed

            min_required_volume = 10 
            if comments_today >= min_required_volume and avg_hourly_volume_today > 0:
                spike_ratio = comments_last_hour / avg_hourly_volume_today
                if spike_ratio >= 2:
                    col.error(f"🚨 High Volume!  🗣️ {comments_last_hour} comments (1hr)")
                elif spike_ratio >= 1.5:
                    col.warning(f"⚠️ Volume Spike — {comments_last_hour} vs {avg_hourly_volume_today:.0f} avg")
                else:
                    col.success(f"✅ Normal Volume — 🗣️ {comments_last_hour} comments")
            else:
                col.info("ℹ️ Not enough volume data today")

st.markdown("---")

# Chart Section 
st.header("📈 Sentiment Forecast & Price Trend")

selected_stock = st.selectbox("📍 Select a stock for detailed view", all_symbols, index=0)

df_sentiment_filtered = df_sentiment[df_sentiment['stock_symbol'] == selected_stock].copy()
df_prices_filtered = df_prices[df_prices['symbol'] == selected_stock].copy()

if not df_sentiment_filtered.empty and df_sentiment_filtered['timestamp_utc'].dtype == object:
    try:
        df_sentiment_filtered['timestamp_utc'] = pd.to_datetime(df_sentiment_filtered['timestamp_utc']).dt.tz_localize('UTC')
    except Exception:
        pass

if not df_prices_filtered.empty and df_prices_filtered['timestamp'].dtype == object:
    try:
        df_prices_filtered['timestamp'] = pd.to_datetime(df_prices_filtered['timestamp']).dt.tz_localize('UTC')
    except Exception:
        pass

# smooth sentiment series 
df_smoothed_sentiment = pd.Series(dtype=float)
if not df_sentiment_filtered.empty:
    df_sentiment_filtered = df_sentiment_filtered.set_index('timestamp_utc').sort_index()
    try:
        df_smoothed_sentiment = df_sentiment_filtered['sentiment_score'].resample('1min').mean().rolling(window=5, min_periods=1).mean().dropna()
    except Exception:
        df_smoothed_sentiment = pd.Series(dtype=float)

# Forecast (linear) 
forecast_line = pd.Series(dtype='float64')
if len(df_smoothed_sentiment) > 5:
    X = np.arange(len(df_smoothed_sentiment)).reshape(-1, 1)
    y = df_smoothed_sentiment.values
    try:
        model = LinearRegression().fit(X, y)
        future_steps = 30
        future_X = np.arange(len(df_smoothed_sentiment), len(df_smoothed_sentiment) + future_steps).reshape(-1, 1)
        predictions = model.predict(future_X)
        future_index = pd.date_range(df_smoothed_sentiment.index[-1] + timedelta(minutes=1), periods=future_steps, freq='1min')
        forecast_line = pd.Series(predictions, index=future_index)
    except Exception:
        forecast_line = pd.Series(dtype='float64')

# Plotly Chart 
fig = make_subplots(specs=[[{"secondary_y": True}]])

if not df_smoothed_sentiment.empty:
    fig.add_trace(go.Scatter(
        x=df_smoothed_sentiment.index, y=df_smoothed_sentiment,
        name="Smoothed Sentiment", line=dict(color='royalblue')
    ), secondary_y=False)

if not forecast_line.empty:
    fig.add_trace(go.Scatter(
        x=forecast_line.index, y=forecast_line,
        name="Forecast (30 min)", line=dict(color='royalblue', dash='dash')
    ), secondary_y=False)

if not df_prices_filtered.empty:
    df_prices_filtered = df_prices_filtered.sort_values('timestamp')
    fig.add_trace(go.Scatter(
        x=df_prices_filtered['timestamp'], y=df_prices_filtered['price'],
        name="Stock Price", line=dict(color='firebrick')
    ), secondary_y=True)

fig.update_layout(
    title=f"🧐 Sentiment vs. 💰 Price for {selected_stock}",
    legend_title="Metric",
    margin=dict(t=50, l=30, r=30, b=30),
    height=600,
    hovermode="x unified"
)
fig.update_xaxes(title_text="Time")
fig.update_yaxes(title_text="Sentiment Score", secondary_y=False)
fig.update_yaxes(title_text="Price ($)", secondary_y=True)

st.plotly_chart(fig, use_container_width=True)

# Correlation Calculation
st.subheader("📊 Correlation between Sentiment and Price")

merged = pd.DataFrame()
if not df_smoothed_sentiment.empty and not df_prices_filtered.empty:
    left = df_smoothed_sentiment.reset_index().rename(columns={'timestamp_utc': 'timestamp_utc', 0: 'sentiment_score'}) \
        if 'sentiment_score' not in df_smoothed_sentiment.reset_index().columns else df_smoothed_sentiment.reset_index()
    left = left.rename(columns={left.columns[0]: 'timestamp_utc'})  
    left = left.sort_values('timestamp_utc')

    right = df_prices_filtered[['timestamp', 'price']].rename(columns={'timestamp': 'timestamp_utc'}).sort_values('timestamp_utc')

    try:
        merged = pd.merge_asof(left, right, on='timestamp_utc').dropna()
    except Exception:
        merged = pd.DataFrame()

if not merged.empty and 'sentiment_score' in merged.columns and 'price' in merged.columns:
    if merged['sentiment_score'].nunique() > 1 and merged['price'].nunique() > 1:
        correlation = merged['sentiment_score'].corr(merged['price'])
        if pd.notna(correlation):
            st.metric(label=f"Correlation (Sentiment vs Price): {selected_stock}", value=f"{correlation:.2f}")
        else:
            st.warning("Correlation computed to NaN (not enough variation).")
    else:
        st.warning("Not enough variance in data to calculate correlation.")
else:
    st.warning("Not enough aligned data to calculate correlation.")

# Latest 10 Comments 
st.header(f"Latest Comments for {selected_stock}")
if not df_sentiment_filtered.empty:
    latest_comments = df_sentiment_filtered.sort_index(ascending=False).reset_index().head(10)
    if 'timestamp_utc' not in latest_comments.columns and latest_comments.index.name:
        latest_comments = latest_comments.reset_index().rename(columns={latest_comments.index.name: 'timestamp_utc'})
    st.dataframe(latest_comments, use_container_width=True)
else:
    st.info("No sentiment comments for this symbol yet.")

# Sidebar alerts 
st.sidebar.header("🚨 Alerts")
for symbol in all_symbols:
    df_symbol = df_sentiment[df_sentiment['stock_symbol'] == symbol]
    comments_last_hour = len(df_symbol[df_symbol['timestamp_utc'] >= one_hour_ago])
    comments_today = len(df_symbol[df_symbol['timestamp_utc'] >= start_of_today])
    hours_elapsed = max(now_utc.hour + 1, 1)
    avg_hourly_volume_today = comments_today / hours_elapsed if hours_elapsed > 0 else 0
    if comments_today >= 10 and avg_hourly_volume_today > 0 and comments_last_hour >= avg_hourly_volume_today * 2:
        if st.sidebar.button(f"{symbol}: HIGH VOL"):
            st.session_state['selected_stock'] = symbol
            st.experimental_rerun()
        else:
            st.sidebar.error(f"{symbol}: High Volume!")
    elif comments_today >= 10 and avg_hourly_volume_today > 0 and comments_last_hour >= avg_hourly_volume_today * 1.5:
        st.sidebar.warning(f"{symbol}: Volume Spike ({comments_last_hour})")

# Auto Refresh 
if AUTORELOAD_AVAILABLE:
    st_autorefresh(interval=60000, key="autorefresh")

