import json
import os
import shutil
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, LongType
from pyspark.sql.functions import col, expr, collect_list, struct, explode
import logging
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import functions as F

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load Kafka configuration
config_path = os.path.join(os.path.dirname(__file__), 'config/kafka_config.json')
with open(config_path, 'r') as f:
    kafka_config = json.load(f)

# Initialize Spark session with Kafka support
spark = SparkSession.builder \
    .appName("MovieRecommender") \
    .config("spark.driver.host", "localhost") \
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
        rank=16,
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
    
    try:
        # Convert recommendations to JSON format
        recommendations_json = recommendations_df.selectExpr(
            "CAST(userId AS STRING) as key",
            "to_json(struct(userId, recommendations)) as value"
        )
        recommendations_json.show(truncate=False)
        logger.info(f"Recommendations JSON: {recommendations_json.show()}")
        
        # Write to Kafka
        recommendations_json.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers']) \
            .option("topic", kafka_config['output_topic']) \
            .save()
        
        logger.info("Finished sending recommendations to Kafka")
    except Exception as e:
        logger.error(f"Failed to send to Kafka: {e}", exc_info=True)

def main():
    # Load data
    ratings_df = load_data('/data/ratings.csv')
    
    # Train model
    model, predictions = train_model(ratings_df)
    evaluate_model(model, predictions, ratings_df)

    # Generate recommendations
#     recommendations_df = model.recommendForAllUsers(10)

#     logger.info(f"Recommendations: {recommendations_df.show()}")
#
#     recommendations_df.printSchema()
#     recommendations_df.show(truncate=False)
#     recommendations_df.count()
    
    # Send to Kafka
#     send_to_kafka(recommendations_df)
    
    # Save model (optional)
    model.write().overwrite().save("/models/als_model")

    spark.stop()


def evaluate_model(model, predictions, ratings_df, top_k=10):
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")
    rmse = evaluator.evaluate(predictions)

    logger.info(f"✅ RMSE = {rmse:.4f}")

    # Precision@K, NDCG@K
    userRecs = model.recommendForAllUsers(top_k)
    relevant = ratings_df.filter("rating >= 4.0").groupBy("userId").agg(collect_list("movieId").alias("true_items"))
    predicted = userRecs.select("userId", explode("recommendations").alias("rec")) \
        .select("userId", col("rec.movieId").alias("movieId"))
    predicted_grouped = predicted.groupBy("userId").agg(collect_list("movieId").alias("predicted_items"))
    joined = predicted_grouped.join(relevant, on="userId")

    import math
    def precision_at_k(pred, actual, k=10):
        pred_k = pred[:k]
        return len(set(pred_k) & set(actual)) / float(k)

    def ndcg_at_k(pred, actual, k=10):
        pred_k = pred[:k]
        dcg = sum(1.0 / math.log2(i + 2) for i, p in enumerate(pred_k) if p in actual)
        idcg = sum(1.0 / math.log2(i + 2) for i in range(min(len(actual), k)))
        return dcg / idcg if idcg > 0 else 0.0

    precision_list = []
    ndcg_list = []

    for row in joined.collect():
        precision_list.append(precision_at_k(row['predicted_items'], row['true_items'], k=top_k))
        ndcg_list.append(ndcg_at_k(row['predicted_items'], row['true_items'], k=top_k))

    avg_precision = sum(precision_list) / len(precision_list)
    avg_ndcg = sum(ndcg_list) / len(ndcg_list)

    logger.info(f"🎯 Precision@{top_k} = {avg_precision:.4f}")
    logger.info(f"📈 NDCG@{top_k} = {avg_ndcg:.4f}")


if __name__ == "__main__":
    main() 