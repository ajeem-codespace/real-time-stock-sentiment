import praw
import time
import json
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")
USER_AGENT = os.getenv("REDDIT_USER_AGENT")


KAFKA_TOPIC = 'reddit_comments'
KAFKA_SERVER = 'localhost:9092' 


STOCKS_TO_TRACK = {
    'AAPL': ['apple', 'aapl', '$aapl'],
    'GOOGL': ['google', 'googl', '$googl', 'alphabet'],
    'TSLA': ['tesla', 'tsla', '$tsla'],
    'MSFT': ['microsoft', 'msft', '$msft'],
    'AMZN': ['amazon', 'amzn', '$amzn']
}

SUBREDDITS_TO_STREAM = [
    'wallstreetbets', 'stocks', 'investing',
    'StockMarket', 'options', 'RobinHood', 'pennystocks'
] 

def find_all_mentioned_stocks(text):
    mentioned_stocks = set()
    text_lower = text.lower()
    for symbol, keywords in STOCKS_TO_TRACK.items():
        for keyword in keywords:
            if keyword in text_lower:
                mentioned_stocks.add(symbol)
    return mentioned_stocks

def create_kafka_producer():
    print("Connecting to Kafka")
    retries = 5
    for i in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_SERVER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("Successfully connected to Kafka.")
            return producer
        except Exception as e:
            print(f"Attempt {i+1}/{retries}: Error connecting to Kafka: {e}")
            if i < retries - 1:
                print("Retrying")
                time.sleep(5)
    
    print("Could not connect to Kafka.")
    return None

def stream_reddit_comments(producer):
    print("Connecting to Reddit to stream comments")
    try:
        reddit = praw.Reddit(
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
            user_agent=USER_AGENT,
            check_for_async=False
        )
        
        subreddit_string = "+".join(SUBREDDITS_TO_STREAM)
        subreddit = reddit.subreddit(subreddit_string)
        print(f"Successfully connected to Reddit, streaming from r/{subreddit_string}.")
        
        for comment in subreddit.stream.comments(skip_existing=True):
            mentioned_stocks = find_all_mentioned_stocks(comment.body)
            if mentioned_stocks and comment.body and comment.body != '[deleted]' and comment.body != '[removed]':
                for stock_symbol in mentioned_stocks:
                    data_record = {
                        'stock_symbol': stock_symbol,
                        'timestamp_utc': comment.created_utc,
                        'comment_body': comment.body
                    }
                    producer.send(KAFKA_TOPIC, data_record)
                    print(f"Sent to Kafka -> {stock_symbol}: {comment.body[:50]}...")
            
            time.sleep(1)

    except Exception as e:
        print(f"An error occurred during streaming: {e}")

if __name__ == '__main__':
    kafka_producer = create_kafka_producer()
    if kafka_producer:
        stream_reddit_comments(kafka_producer)
