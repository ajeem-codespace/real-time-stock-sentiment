from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
from pyspark.ml import Pipeline
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF, StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

def clean_data(df):
    """Selects and cleans the input DataFrame."""
    # The dataset has two columns: 'sentiment' and 'text'. Let's rename them.
    df = df.withColumnRenamed(df.columns[0], "sentiment") \
           .withColumnRenamed(df.columns[1], "text")
           
    # Remove any rows with null values
    df = df.na.drop()
    
    # Ensure sentiment column has only 'positive', 'negative', 'neutral'
    valid_sentiments = ['positive', 'negative', 'neutral']
    df = df.filter(col("sentiment").isin(valid_sentiments))
    
    return df

def train_sentiment_model():
    """Trains and saves a sentiment analysis model."""
    spark = SparkSession.builder.appName("SentimentModelTraining").getOrCreate()
    
    # 1. Load and Clean Data
    print("Loading and cleaning data...")
    data_path = "data/all-data.csv"
    df = spark.read.csv(data_path, header=False, inferSchema=True, sep=',', encoding='ISO-8859-1')
    df = clean_data(df)

    # 2. Define the ML Pipeline Stages
    print("Defining ML pipeline stages...")
    # Stage 1: Convert sentiment labels (strings) to numerical indices
    label_indexer = StringIndexer(inputCol="sentiment", outputCol="label")

    # Stage 2: Tokenize the text (split into words)
    tokenizer = Tokenizer(inputCol="text", outputCol="words")

    # Stage 3: Remove stop words
    stopwords_remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")

    # Stage 4: Convert words to numerical features using HashingTF
    hashing_tf = HashingTF(inputCol="filtered_words", outputCol="raw_features")

    # Stage 5: Apply IDF to weigh the features
    idf = IDF(inputCol="raw_features", outputCol="features")

    # Stage 6: Define the classifier (Logistic Regression)
    lr = LogisticRegression(featuresCol="features", labelCol="label")

    # 3. Assemble the Pipeline
    pipeline = Pipeline(stages=[label_indexer, tokenizer, stopwords_remover, hashing_tf, idf, lr])

    # 4. Split Data and Train the Model
    print("Splitting data and training the model...")
    (training_data, test_data) = df.randomSplit([0.8, 0.2], seed=42)
    
    model = pipeline.fit(training_data)
    
    # 5. Evaluate the Model
    print("Evaluating the model...")
    predictions = model.transform(test_data)
    evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator.evaluate(predictions)
    print(f"Test Set Accuracy = {accuracy * 100:.2f}%")

    # 6. Save the Model
    print("Saving the model to 'sentiment_model'...")
    model_path = "sentiment_model"
    model.write().overwrite().save(model_path)
    print("Model saved successfully.")

    spark.stop()

if __name__ == "__main__":
    train_sentiment_model()