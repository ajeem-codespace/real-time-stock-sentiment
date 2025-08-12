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

def process_stream():
    spark = SparkSession.builder \
        .appName("RedditStreamProcessing") \
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

    df_structured = df_json.withColumn("data", from_json(col("value"), schema)) \
                           .select("data.*")

    df_cleaned = df_structured.withColumn(
        'cleaned_comment',
        lower(col('comment_body'))
    ).withColumn(
        'cleaned_comment',
        regexp_replace(col('cleaned_comment'), r'http\S+', '')
    ).withColumn(
        'cleaned_comment',
        regexp_replace(col('cleaned_comment'), r'[^a-zA-Z\s]', '')
    )

    sentiment_udf = udf(get_sentiment_score, FloatType())
    df_sentiment = df_cleaned.withColumn(
        'sentiment_score',
        sentiment_udf(col('cleaned_comment'))
    )

    query = df_sentiment.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    query.awaitTermination()

if __name__ == '__main__':
    process_stream()