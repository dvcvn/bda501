import json
import os
import shutil

from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, LongType
import logging

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

def main():
    # Load data
    test_df = spark.createDataFrame([("test-key", '{"userId":1,"recommendations":[{"movieId":123,"rating":4.5}]}')], ["key", "value"])

    test_df = test_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    test_df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers']) \
        .option("topic", kafka_config['output_topic']) \
        .save()
 
    spark.stop()

if __name__ == "__main__":
    main() 
