import finnhub
import sqlite3
import time
import schedule
import os 
from dotenv import load_dotenv

load_dotenv()

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
DB_FILE = "sentiment_data.db"
STOCKS_TO_TRACK = ['AAPL', 'GOOGL', 'TSLA', 'MSFT', 'AMZN']

def setup_database():
    conn = sqlite3.connect(DB_FILE)
    conn.execute('''
        CREATE TABLE IF NOT EXISTS stock_prices (
            symbol TEXT,
            price REAL,
            timestamp INTEGER
        )
    ''')
    conn.close()
    print("Database and 'stock_prices' table set up.")

def fetch_and_save_prices():

    print("Fetching stock prices")
    finnhub_client = finnhub.Client(api_key=FINNHUB_API_KEY)
    conn = sqlite3.connect(DB_FILE)
    
    for symbol in STOCKS_TO_TRACK:
        try:
           
            quote = finnhub_client.quote(symbol)
            price = quote.get('c', 0) 
            timestamp = int(time.time()) # Current Unix timestamp
            
            # Insert data into the table
            if price > 0:
                conn.execute(
                    "INSERT INTO stock_prices (symbol, price, timestamp) VALUES (?, ?, ?)",
                    (symbol, price, timestamp)
                )
                print(f"  > Saved {symbol}: ${price:.2f}")
            else:
                print(f"  > Could not fetch a valid price for {symbol}")

        except Exception as e:
            print(f"Error fetching price for {symbol}: {e}")
            
    conn.commit()
    conn.close()
    print("Finished price fetch cycle.\n")

if __name__ == '__main__':
    setup_database()
    print("Starting price fetcher to run every minute")
    
   
    fetch_and_save_prices()

    # run every minute
    schedule.every(1).minutes.do(fetch_and_save_prices)
    
    while True:
        schedule.run_pending()
        time.sleep(1)