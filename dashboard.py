
import os
import sqlite3
from datetime import datetime, timedelta, timezone

import pandas as pd
import streamlit as st
import plotly.graph_objects as go

# --- Paths / constants (ABSOLUTE, shared with Spark) ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_FILE = os.path.join(BASE_DIR, "sentiment_data.db")
TABLE_NAME = "stock_news_sentiment"

# --- Page Configuration ---
st.set_page_config(
    page_title="Stock News Sentiment Dashboard (Improved)",
    page_icon="📊",
    layout="wide",
)

@st.cache_data(ttl=30)
def get_data_from_db():
    expected_cols = [
        "rowid",
        "stock_symbol",
        "headline",
        "sentiment",
        "source",
        "published_at",
        "summary",
        "url",
        "ingestion_timestamp",
    ]
    try:
        conn = sqlite3.connect(DB_FILE)
        df = pd.read_sql_query(f"SELECT rowid, * FROM {TABLE_NAME}", conn)
        conn.close()
    except Exception as e:
        st.warning(f"Note: {e}")
        return pd.DataFrame(columns=expected_cols)

    # Parse timestamps
    if "published_at" in df.columns:
        df["published_at"] = pd.to_datetime(df["published_at"], utc=True, errors="coerce")
    if "ingestion_timestamp" in df.columns:
        df["ingestion_timestamp"] = pd.to_datetime(df["ingestion_timestamp"], utc=True, errors="coerce")

    # Prefer published_at; fallback to ingestion_timestamp
    df["timestamp"] = df.get("published_at").fillna(df.get("ingestion_timestamp"))

    # Ensure tz-aware UTC
    if "timestamp" in df.columns and not df["timestamp"].empty:
        try:
            if df["timestamp"].dt.tz is None:
                df["timestamp"] = df["timestamp"].dt.tz_localize("UTC")
            else:
                df["timestamp"] = df["timestamp"].dt.tz_convert("UTC")
        except Exception:
            pass

    return df

df = get_data_from_db()

st.title("📊 Stock News Sentiment Dashboard")
st.markdown("A modern, interactive dashboard for real-time stock news sentiment analysis.")

# Stop early if there is no data
if df.empty or "timestamp" not in df.columns:
    st.info("No data yet. Start the Spark consumer and producer to populate the database.")
    st.stop()

# --- Global KPIs ---
st.header("Global KPIs")
now = datetime.now(timezone.utc)
twenty_four_hours_ago = now - timedelta(hours=24)
seven_days_ago = now - timedelta(days=7)

df_24h = df[df["timestamp"] >= twenty_four_hours_ago]
df_7d = df[df["timestamp"] >= seven_days_ago]

st.metric("Total Headlines (24h)", int(len(df_24h)))

# Sentiment distribution (24h)
if not df_24h.empty and "sentiment" in df_24h.columns:
    sentiment_counts = df_24h["sentiment"].value_counts()
    if not sentiment_counts.empty:
        fig_pie = go.Figure(
            data=[go.Pie(labels=sentiment_counts.index, values=sentiment_counts.values, hole=0.4)]
        )
        fig_pie.update_layout(title="Sentiment Distribution (24h)")
        st.plotly_chart(fig_pie, use_container_width=True)
    else:
        st.caption("No sentiment data in the last 24h.")
else:
    st.caption("No data in the last 24h.")

# Most mentioned stock (24h)
if not df_24h.empty and "stock_symbol" in df_24h.columns:
    most_mentioned = df_24h["stock_symbol"].value_counts().idxmax()
else:
    most_mentioned = "N/A"
st.metric("Most Mentioned Stock (24h)", most_mentioned)

# Headline volume trend (7d)
if not df_7d.empty and {"timestamp", "stock_symbol", "headline"}.issubset(df_7d.columns):
    volume_trend = (
        df_7d.set_index("timestamp")
        .groupby("stock_symbol")["headline"]
        .resample("1D")
        .count()
        .unstack(0)
        .fillna(0)
    )
    if not volume_trend.empty:
        fig_vol = go.Figure()
        for symbol in volume_trend.columns:
            fig_vol.add_trace(go.Bar(x=volume_trend.index, y=volume_trend[symbol], name=symbol))
        fig_vol.update_layout(
            title="Headline Volume Trend (Last 7 Days)",
            barmode="group",
            xaxis_title="Date",
            yaxis_title="Headline Count",
        )
        st.plotly_chart(fig_vol, use_container_width=True)

st.markdown("---")

# --- Stock-Specific KPIs ---
st.header("Stock-Specific KPIs")
all_symbols = sorted(df["stock_symbol"].dropna().unique().tolist()) if "stock_symbol" in df.columns else []
if not all_symbols:
    st.info("No symbols available yet.")
else:
    cols = st.columns(len(all_symbols))
    for i, symbol in enumerate(all_symbols):
        with cols[i]:
            df_symbol = df[df["stock_symbol"] == symbol]
            df_recent = df_symbol[df_symbol["timestamp"] >= twenty_four_hours_ago]
            total_headlines = int(len(df_recent))
            st.subheader(symbol)
            st.metric("Total Headlines (24h)", total_headlines)
            if total_headlines > 0 and "sentiment" in df_recent.columns:
                positive = int((df_recent["sentiment"] == "positive").sum())
                neutral = int((df_recent["sentiment"] == "neutral").sum())
                negative = int((df_recent["sentiment"] == "negative").sum())
                denom = max(total_headlines, 1)
                st.metric("Positive (%)", f"{(positive/denom)*100:.1f}%")
                st.metric("Neutral (%)", f"{(neutral/denom)*100:.1f}%")
                st.metric("Negative (%)", f"{(negative/denom)*100:.1f}%")

                latest_row = df_recent.sort_values(by="timestamp", ascending=False).head(1)
                if not latest_row.empty:
                    latest_sentiment = latest_row.iloc[0]["sentiment"]
                    latest_headline = latest_row.iloc[0]["headline"]
                    st.write(f"**Latest Headline Sentiment:** {latest_sentiment}")
                    st.caption(f"{latest_headline}")
            else:
                st.metric("Positive (%)", "N/A")
                st.metric("Neutral (%)", "N/A")
                st.metric("Negative (%)", "N/A")
                st.write("No recent headlines.")

st.markdown("---")

# --- Interactive Section ---
st.header("Interactive Stock Analysis")

if all_symbols:
    selected_stock = st.selectbox("Select a stock for detailed analysis", all_symbols)
    time_window = st.selectbox("Select time window", ["Last hour", "Last 24 hours", "Last 7 days"])

    if time_window == "Last hour":
        window_start = now - timedelta(hours=1)
    elif time_window == "Last 24 hours":
        window_start = now - timedelta(hours=24)
    else:
        window_start = now - timedelta(days=7)

    df_filtered = df[(df["stock_symbol"] == selected_stock) & (df["timestamp"] >= window_start)]

    if df_filtered.empty:
        st.info("No headlines available in the selected window.")
    else:
        # Sentiment Over Time
        st.subheader(f"Sentiment Over Time for {selected_stock}")
        try:
            hourly_index = pd.date_range(
                start=window_start.replace(minute=0, second=0, microsecond=0),
                end=now,
                freq="H",
                tz=now.tzinfo,
            )
            df_sent_time = (
                df_filtered.set_index("timestamp")
                .groupby([pd.Grouper(freq="H"), "sentiment"])
                .size()
                .unstack(fill_value=0)
                .reindex(hourly_index, fill_value=0)
            )
            fig_sent = go.Figure()
            for s in ["positive", "neutral", "negative"]:
                if s in df_sent_time.columns:
                    fig_sent.add_trace(
                        go.Scatter(x=df_sent_time.index, y=df_sent_time[s], mode="lines+markers", name=s.title())
                    )
            fig_sent.update_layout(xaxis_title="Time", yaxis_title="Number of Headlines")
            st.plotly_chart(fig_sent, use_container_width=True)
        except Exception as e:
            st.warning(f"Could not plot sentiment over time: {e}")

        # Headline Volume Over Time
        st.subheader(f"Headline Volume Over Time for {selected_stock}")
        try:
            df_vol_time = df_filtered.set_index("timestamp")["headline"].resample("1H").count()
            fig_vol_time = go.Figure()
            fig_vol_time.add_trace(go.Bar(x=df_vol_time.index, y=df_vol_time.values, name="Headline Volume"))
            fig_vol_time.update_layout(xaxis_title="Time", yaxis_title="Headline Count")
            st.plotly_chart(fig_vol_time, use_container_width=True)
        except Exception as e:
            st.warning(f"Could not plot volume over time: {e}")

        # Latest Headlines Table
        st.subheader(f"Latest Headlines for {selected_stock}")
        cols_to_show = [c for c in ["timestamp", "headline", "sentiment", "source", "url", "summary"] if c in df_filtered.columns]
        st.dataframe(
            df_filtered[cols_to_show].sort_values(by="timestamp", ascending=False).head(20),
            use_container_width=True,
        )
else:
    st.info("No stocks available yet. Start the producer/consumer to populate data.")

st.markdown("---")

# --- Alerts & Insights ---
st.header("Alerts & Insights")
if all_symbols and selected_stock:
    df_sel = df[(df["stock_symbol"] == selected_stock)]
    df_filtered = df_sel[df_sel["timestamp"] >= (now - timedelta(days=7))]

    hourly_counts = df_filtered.set_index("timestamp")["headline"].resample("1H").count() if not df_filtered.empty else pd.Series(dtype=float)
    avg_hourly = float(hourly_counts.mean()) if not hourly_counts.empty else 0.0
    last_hour_count = int(hourly_counts.iloc[-1]) if not hourly_counts.empty else 0
    if avg_hourly > 0 and last_hour_count > 2 * avg_hourly:
        st.warning(f"Volume spike detected for {selected_stock}: {last_hour_count} headlines in last hour (avg: {avg_hourly:.1f})")

    if not df_filtered.empty:
        last_hour = df_filtered[df_filtered["timestamp"] >= now - timedelta(hours=1)]
        prev_hour = df_filtered[(df_filtered["timestamp"] < now - timedelta(hours=1)) & (df_filtered["timestamp"] >= now - timedelta(hours=2))]
        if not last_hour.empty and not prev_hour.empty:
            last_pos = int((last_hour["sentiment"] == "positive").sum())
            last_neg = int((last_hour["sentiment"] == "negative").sum())
            prev_pos = int((prev_hour["sentiment"] == "positive").sum())
            prev_neg = int((prev_hour["sentiment"] == "negative").sum())
            if (prev_pos + prev_neg) > 0:
                last_ratio = last_pos / max((last_pos + last_neg), 1)
                prev_ratio = prev_pos / (prev_pos + prev_neg)
                if abs(last_ratio - prev_ratio) > 0.5:
                    st.warning(f"Sentiment shift detected for {selected_stock}: Positive ratio changed from {prev_ratio:.2f} to {last_ratio:.2f}")

# --- Top sources (7d for selected stock or all) ---
st.subheader("Top News Sources")
scope_df = df[(df["timestamp"] >= seven_days_ago)]
if all_symbols and selected_stock:
    scope_df = scope_df[scope_df["stock_symbol"] == selected_stock]
if not scope_df.empty and "source" in scope_df.columns:
    source_counts = scope_df["source"].value_counts().head(5)
    st.table(source_counts)
