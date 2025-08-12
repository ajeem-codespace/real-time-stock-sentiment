import sqlite3
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf, lower, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, FloatType
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

def get_sentiment_score(comment_text):
    if not comment_text:
        return 0.0
    analyzer = SentimentIntensityAnalyzer()
    score = analyzer.polarity_scores(comment_text)['compound']
    return float(score)

# Function to handle writing each micro-batch to SQLite 
def process_and_write_to_db(df, epoch_id):
    if df.isEmpty():
        return
    
    print(f"--- Processing Batch ID: {epoch_id} ---")
    
    # Convert the Spark DataFrame to a Pandas DataFrame
    pandas_df = df.toPandas()
    
    # Connect to the SQLite database 
    conn = sqlite3.connect("sentiment_data.db")
  
    conn.execute('''
        CREATE TABLE IF NOT EXISTS sentiment (
            stock_symbol TEXT,
            timestamp_utc REAL,
            comment_body TEXT,
            sentiment_score REAL
        )
    ''')
    

    pandas_df.to_sql("sentiment", conn, if_exists="append", index=False)
    
    print(f"Successfully wrote {len(pandas_df)} rows to the database.")
    

    conn.close()


def run_streaming_job():
    spark = SparkSession.builder \
        .appName("RedditStreamToDB") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("stock_symbol", StringType(), True),
        StructField("timestamp_utc", DoubleType(), True),
        StructField("comment_body", StringType(), True)
    ])

    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "reddit_comments") \
        .load()

    df_json = df_kafka.selectExpr("CAST(value AS STRING)")
    df_structured = df_json.withColumn("data", from_json(col("value"), schema)).select("data.*")

    
    df_cleaned = df_structured.dropna(subset=['comment_body']).withColumn(
        'cleaned_comment',
        lower(col('comment_body'))
    ).withColumn(
        'cleaned_comment',
        regexp_replace(col('cleaned_comment'), r'[^a-zA-Z\s]', '')
    )

    sentiment_udf = udf(get_sentiment_score, FloatType())
    df_sentiment = df_cleaned.withColumn(
        'sentiment_score',
        sentiment_udf(col('cleaned_comment'))
    ).select("stock_symbol", "timestamp_utc", "comment_body", "sentiment_score")

    query = df_sentiment.writeStream \
        .foreachBatch(process_and_write_to_db) \
        .start()

    print("Streaming job started. Waiting for data from Kafka")
    query.awaitTermination()

if __name__ == '__main__':
    run_streaming_job()