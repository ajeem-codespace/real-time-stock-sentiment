from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, lower, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, FloatType
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

#get sentiment 
def get_sentiment_score(comment_text):
    if not comment_text:
        return 0.0
    
    analyzer = SentimentIntensityAnalyzer()
    score = analyzer.polarity_scores(comment_text)['compound']
    return float(score)
#processing data
def process_data():
    print("Starting Spark session")
    spark = SparkSession.builder \
        .appName("RedditSentimentAnalysis") \
        .getOrCreate()
    print("Spark session started.")

    schema = StructType([
        StructField("stock_symbol", StringType(), True),
        StructField("timestamp_utc", DoubleType(), True),
        StructField("comment_body", StringType(), True)
    ])

    print("Loading data from reddit_comments.csv")
    df = spark.read.csv(
        'reddit_comments.csv',
        header=True,
        schema=schema,
        quote='"',
        escape='"',
        multiLine=True
    )
    
    df = df.dropna(subset=['comment_body'])
    print("Data loaded and nulls dropped.")

    print("Cleaning comment text...")
    df_cleaned = df.withColumn(
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
    
    print("Analyzing sentiment")
    df_sentiment = df_cleaned.withColumn(
        'sentiment_score',
        sentiment_udf(col('cleaned_comment'))
    )
    
    
    print("Processing complete. Showing final results:")
    df_sentiment.select(
        'stock_symbol',
        'sentiment_score',
        'comment_body'
    ).show(truncate=False)

    spark.stop()

if __name__ == '__main__':
    process_data()