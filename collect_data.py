import os
import time
import json
import socket
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import finnhub

# --- Configuration ---
load_dotenv()
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
if not FINNHUB_API_KEY:
    raise RuntimeError("FINNHUB_API_KEY is not set in your environment (.env).")

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "stock_news")
KAFKA_SERVER = os.getenv("KAFKA_SERVER", "localhost:9092")
STOCKS_TO_TRACK = os.getenv("STOCKS_TO_TRACK", "AAPL,GOOGL,TSLA,MSFT,AMZN").split(",")

POLL_SLEEP_BETWEEN_STOCKS_SEC = 2
SLEEP_BETWEEN_CYCLES_SEC = 300  # 5 minutes

def create_kafka_producer():
    """Creates and returns a Kafka producer (JSON serializer)."""
    print(f"Connecting to Kafka at {KAFKA_SERVER}…")
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_SERVER,
            client_id=f"news-producer-{socket.gethostname()}",
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            linger_ms=100,
            acks="all",
            retries=5,
        )
        print("Successfully connected to Kafka.")
        return producer
    except NoBrokersAvailable as e:
        raise RuntimeError(f"Could not connect to Kafka at {KAFKA_SERVER}: {e}")

def fetch_news_and_send_finnhub(producer: KafkaProducer, finnhub_client: finnhub.Client, stock_symbol: str):
    print(f"Fetching news for {stock_symbol} from Finnhub…")
    now_utc = datetime.now(timezone.utc)
    yesterday_utc = now_utc - timedelta(hours=24)

    try:
        items = finnhub_client.company_news(
            stock_symbol,
            _from=yesterday_utc.strftime("%Y-%m-%d"),
            to=now_utc.strftime("%Y-%m-%d"),
        ) or []
    except Exception as e:
        print(f"[{stock_symbol}] Finnhub error: {e}")
        return

    sent = 0
    for article in items:
        headline = article.get("headline")
        ts_unix = article.get("datetime")  # seconds since epoch
        source = article.get("source")
        summary = article.get("summary")
        url = article.get("url")

        if not headline or not ts_unix:
            continue

        # Normalize to ISO-8601 UTC
        published_at_iso = datetime.fromtimestamp(int(ts_unix), tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        record = {
            "stock_symbol": stock_symbol,
            "headline": headline,
            "source": source,
            "published_at": published_at_iso,
            "summary": summary,
            "url": url,
        }

        try:
            producer.send(KAFKA_TOPIC, record)
            sent += 1
        except Exception as e:
            print(f"[{stock_symbol}] Failed to send record to Kafka: {e}")

    if sent:
        producer.flush()
        print(f"[{stock_symbol}] Sent {sent} article(s) to Kafka.")
    else:
        print(f"[{stock_symbol}] No new articles to send.")

def main():
    producer = create_kafka_producer()
    finnhub_client = finnhub.Client(api_key=FINNHUB_API_KEY)

    while True:
        for sym in STOCKS_TO_TRACK:
            fetch_news_and_send_finnhub(producer, finnhub_client, sym.strip().upper())
            time.sleep(POLL_SLEEP_BETWEEN_STOCKS_SEC)
        print("\n--- Completed a cycle. Waiting for 5 minutes. ---\n")
        time.sleep(SLEEP_BETWEEN_CYCLES_SEC)

if __name__ == "__main__":
    main()
