import json
import logging
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, to_json, struct
from pyspark.sql.types import StructType, StructField, IntegerType, ArrayType, FloatType, StringType
from pyspark.ml.recommendation import ALSModel
import time

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load Kafka configuration
with open('config/kafka_config.json', 'r') as f:
    kafka_config = json.load(f)

# Initialize Spark session with Kafka support
spark = SparkSession.builder \
    .appName("MovieRecommenderConsumer") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

# Define schemas
user_schema = StructType([
    StructField("userId", IntegerType(), True)
])

recommendation_schema = StructType([
    StructField("movieId", IntegerType(), True),
    StructField("rating", FloatType(), True)
])

recommendations_schema = StructType([
    StructField("userId", IntegerType(), True),
    StructField("recommendations", ArrayType(recommendation_schema), True)
])

def load_model(model_path):
    """Load the trained ALS model."""
    try:
        return ALSModel.load(model_path)
    except Exception as e:
        logger.error(f"Error loading model: {str(e)}")
        raise

def process_user_recommendations(model, user_id):
    """Generate recommendations for a user using the trained model."""
    try:
        # Create a DataFrame with the user ID
        user_df = spark.createDataFrame([(user_id,)], ["userId"])
        
        # Generate recommendations
        recommendations = model.recommendForUserSubset(user_df, 10)
        
        # Extract recommendations
        if recommendations.count() > 0:
            return recommendations.collect()[0].recommendations
        return None
    except Exception as e:
        logger.error(f"Error generating recommendations for user {user_id}: {str(e)}")
        return None

def send_recommendations_to_kafka(recommendations_df):
    """Send recommendations to the output Kafka topic."""
    try:
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
        
        logger.info("Successfully sent recommendations to Kafka")
    except Exception as e:
        logger.error(f"Error sending recommendations to Kafka: {str(e)}")

def process_kafka_messages(model):
    """Process user IDs from Kafka and generate recommendations."""
    logger.info("Starting to process Kafka messages...")
    
    while True:
        try:
            # Read from Kafka
            df = spark.read \
                .format("kafka") \
                .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers']) \
                .option("subscribe", kafka_config['input_topic']) \
                .option("startingOffsets", "latest") \
                .option("failOnDataLoss", "false") \
                .load()
            
            # Parse JSON and extract user IDs
            parsed_df = df.select(
                from_json(col("value").cast("string"), user_schema).alias("data")
            ).select("data.*")
            
            # Process each user ID
            user_ids = parsed_df.collect()
            
            if user_ids:
                logger.info(f"Processing {len(user_ids)} user IDs...")
                
                # Generate recommendations for all users
                recommendations_list = []
                for row in user_ids:
                    user_id = row.userId
                    recommendations = process_user_recommendations(model, user_id)
                    if recommendations:
                        recommendations_list.append((user_id, recommendations))
                
                if recommendations_list:
                    # Create DataFrame with recommendations
                    recommendations_df = spark.createDataFrame(
                        recommendations_list,
                        ["userId", "recommendations"]
                    )
                    
                    # Send to Kafka
                    send_recommendations_to_kafka(recommendations_df)
            
            logger.info("Waiting for new messages...")
            time.sleep(5)  # Wait before checking for new messages
            
        except Exception as e:
            logger.error(f"Error processing Kafka messages: {str(e)}")
            time.sleep(5)  # Wait before retrying
            continue

def main():
    try:
        # Load the trained model
        model = load_model("models/als_model")
        logger.info("Model loaded successfully")
        
        # Start processing Kafka messages
        process_kafka_messages(model)
        
    except KeyboardInterrupt:
        logger.info("Stopping consumer...")
    except Exception as e:
        logger.error(f"Error: {str(e)}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main() 