import json
import os

from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, LongType
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load Kafka configuration
with open('config/kafka_config.json', 'r') as f:
    kafka_config = json.load(f)

# Initialize Spark session with Kafka support
spark = SparkSession.builder \
    .appName("MovieRecommender") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.default.parallelism", "200") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

# Define schema for ratings data
ratings_schema = StructType([
    StructField("userId", IntegerType(), True),
    StructField("movieId", IntegerType(), True),
    StructField("rating", FloatType(), True),
    StructField("timestamp", LongType(), True)
])

def load_data(file_path):
    """Load and preprocess the ratings data."""
    logger.info(f"Loading data from {file_path}")
    df = spark.read.csv(file_path, header=True, schema=ratings_schema)
    return df

def train_model(ratings_df):
    """Train ALS model on the ratings data."""
    logger.info("Training ALS model...")
    
    # Split data into training and test sets
    train, test = ratings_df.randomSplit([0.8, 0.2], seed=42)
    
    # Build ALS model
    als = ALS(
        userCol='userId',
        itemCol='movieId',
        ratingCol='rating',
        coldStartStrategy='drop',
        nonnegative=True,
        rank=10,
        maxIter=10,
        regParam=0.1
    )
    
    # Train model
    model = als.fit(train)
    
    # Evaluate model
    predictions = model.transform(test)
    return model, predictions

def send_to_kafka(recommendations_df):
    """Send recommendations to Kafka topic using Spark's Kafka integration."""
    logger.info("Sending recommendations to Kafka...")
    
    # Convert recommendations to JSON format
    recommendations_json = recommendations_df.selectExpr(
        "CAST(userId AS STRING) as key",
        "to_json(struct(userId, recommendations)) as value"
    )
    
    # Write to Kafka
    recommendations_json.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers']) \
        .option("topic", kafka_config['output_topic']) \
        .save()
    
    logger.info("Finished sending recommendations to Kafka")

def main():
    # Load data
    ratings_df = load_data('data/ratings.csv')
    
    # Train model
    model, predictions = train_model(ratings_df)
    
    # Generate recommendations
    recommendations_df = model.recommendForAllUsers(10)
    
    # Send to Kafka
    send_to_kafka(recommendations_df)
    
    # Save model (optional)
    model.save("models/als_model")
    
    spark.stop()

if __name__ == "__main__":
    main() 