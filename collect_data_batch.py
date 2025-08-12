import praw
import csv
import os
from dotenv import load_dotenv

load_dotenv()

CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")
USER_AGENT = os.getenv("REDDIT_USER_AGENT")



STOCKS_TO_TRACK = {
    'AAPL': ['apple', 'aapl', '$aapl'],
    'GOOGL': ['google', 'googl', '$googl', 'alphabet'],
    'TSLA': ['tesla', 'tsla', '$tsla'],
    'MSFT': ['microsoft', 'msft', '$msft'],
    'AMZN': ['amazon', 'amzn', '$amzn']
}
SUBREDDITS = ['stocks', 'wallstreetbets', 'investing']
POST_LIMIT = 50
COMMENT_LIMIT = 100 
OUTPUT_FILENAME = 'reddit_comments.csv'

def find_all_mentioned_stocks(text):
    mentioned_stocks = set()
    text_lower = text.lower()
    for symbol, keywords in STOCKS_TO_TRACK.items():
        for keyword in keywords:
            if keyword in text_lower:
                mentioned_stocks.add(symbol)
    return mentioned_stocks

def collect_reddit_data():
    print("Connecting to Reddit")
    try:
        reddit = praw.Reddit(
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
            user_agent=USER_AGENT
        )
        print("Connection successful.")
    except Exception as e:
        print(f"Error: {e}")
        return

    with open(OUTPUT_FILENAME, 'w', newline='', encoding='utf-8') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(['stock_symbol', 'timestamp_utc', 'comment_body'])
        print(f"Created output file: {OUTPUT_FILENAME}")

        comment_count = 0
        for subreddit_name in SUBREDDITS:
            print(f"\nFetching from r/{subreddit_name}...")
            subreddit = reddit.subreddit(subreddit_name)
            
            for post in subreddit.hot(limit=POST_LIMIT):
                post.comments.replace_more(limit=0)
                for comment in post.comments.list()[:COMMENT_LIMIT]:
                    mentioned_stocks = find_all_mentioned_stocks(comment.body)
                    
                    if mentioned_stocks and comment.body and comment.body != '[deleted]' and comment.body != '[removed]':
                        for stock_symbol in mentioned_stocks:
                            cleaned_body = comment.body.replace('\n', ' ').replace('\r', ' ')
                            csv_writer.writerow([stock_symbol, comment.created_utc, cleaned_body])
                            comment_count += 1
                        if len(mentioned_stocks) > 1:
                            print(f"Found multiple stocks in a comment: {mentioned_stocks}")

    print(f"\nFinished collection. Total rows saved: {comment_count}")

if __name__ == '__main__':
    collect_reddit_data()