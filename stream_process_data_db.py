import os
import sqlite3
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.ml import PipelineModel
from pyspark.ml.feature import IndexToString

# --- Paths / Constants ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_FILE = os.path.join(BASE_DIR, "sentiment_data.db")
TABLE_NAME = "stock_news_sentiment"
CHECKPOINT_DIR = os.path.join(BASE_DIR, "_chkpt_stock_news")

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "stock_news")
KAFKA_SERVER = os.getenv("KAFKA_SERVER", "localhost:9092")
MODEL_PATH = os.path.join(BASE_DIR, os.getenv("MODEL_PATH", "sentiment_model"))


def init_db():
    """Ensure the SQLite DB/table exists before Spark writes."""
    os.makedirs(BASE_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            stock_symbol TEXT,
            headline TEXT,
            sentiment TEXT,
            source TEXT,
            published_at TEXT,
            summary TEXT,
            url TEXT,
            ingestion_timestamp TEXT
        )
    """)
    conn.commit()
    conn.close()


def create_spark_session():
    """Create and return a Spark session with Kafka support only."""
    spark = (
        SparkSession.builder
        .appName("NewsSentimentProcessor")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1"
        )
        .getOrCreate()
    )

    # Reduce log noise (set Spark & Kafka to WARN)
    spark.sparkContext.setLogLevel("WARN")
    return spark


def write_to_sqlite(df, epoch_id: int):
    """Writes a micro-batch DataFrame to SQLite using pandas (no JDBC)."""
    if df.rdd.isEmpty():
        print(f"--- Batch {epoch_id}: empty, skipping ---")
        return

    pdf: pd.DataFrame = df.toPandas()

    conn = sqlite3.connect(DB_FILE)
    pdf.to_sql(TABLE_NAME, conn, if_exists="append", index=False)
    conn.close()

    print(f"--- Batch {epoch_id} written to SQLite ({len(pdf)} rows) ---")


def process_stream(spark, model):
    """Read from Kafka -> parse JSON -> predict -> write to SQLite (pandas)."""
    df_kafka = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    schema = StructType([
        StructField("stock_symbol", StringType(), True),
        StructField("headline", StringType(), True),
        StructField("source", StringType(), True),
        StructField("published_at", StringType(), True),
        StructField("summary", StringType(), True),
        StructField("url", StringType(), True),
    ])

    df_parsed = (
        df_kafka
        .select(from_json(col("value").cast("string"), schema).alias("data"))
        .select("data.*")
        .withColumnRenamed("headline", "text")
    )

    predictions = model.transform(df_parsed)

    label_converter = IndexToString(
        inputCol="prediction",
        outputCol="predicted_sentiment",
        labels=model.stages[0].labels
    )
    df_final = label_converter.transform(predictions)

    df_to_write = df_final.select(
        col("stock_symbol"),
        col("text").alias("headline"),
        col("predicted_sentiment").alias("sentiment"),
        col("source"),
        col("published_at"),
        col("summary"),
        col("url"),
        current_timestamp().cast("string").alias("ingestion_timestamp"),  # 👈 FIX
    )

    query = (
        df_to_write.writeStream
        .foreachBatch(write_to_sqlite)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .start()
    )

    query.awaitTermination()


def main():
    init_db()
    spark = create_spark_session()

    print("Loading the trained sentiment model…")
    sentiment_model = PipelineModel.load(MODEL_PATH)
    print("Model loaded successfully.")

    process_stream(spark, sentiment_model)


if __name__ == "__main__":
    main()
