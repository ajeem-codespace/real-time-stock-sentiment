#for testing the db

import sqlite3
conn = sqlite3.connect('sentiment_data.db')
cursor = conn.cursor()
cursor.execute('SELECT * FROM stock_news_sentiment LIMIT 5')
print(cursor.fetchall())
conn.close()