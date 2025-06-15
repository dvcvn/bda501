# Movie Recommendation System with Apache Spark and Kafka

This project implements a movie recommendation system using Apache Spark's Alternating Least Squares (ALS) algorithm and Apache Kafka for real-time recommendation delivery. The system processes MovieLens data to generate personalized movie recommendations for users.

## Features

- Load and process MovieLens dataset
- Train collaborative filtering model using Spark's ALS implementation
- Generate top-10 movie recommendations for all users
- Real-time recommendation delivery using Kafka
- Query recommendations for specific users
- Scalable and distributed processing using Apache Spark

## Prerequisites

- Python 3.8+
- Apache Spark 3.5.0
- Apache Kafka
- Java 8 or later

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd movie_recommender
```

2. Create a virtual environment (optional but recommended):
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Set up environment variables:
```bash
export SPARK_HOME=/path/to/spark
export PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH
```

## Project Structure

```
movie_recommender/
├── train_recommender.py         # Spark ALS training + Kafka producer
├── consume_recommendations.py   # Kafka consumer for user recommendations
├── config/
│   └── kafka_config.json       # Kafka configuration
├── data/
│   └── ratings.csv             # MovieLens ratings data
└── requirements.txt            # Python dependencies
```

## Configuration

The Kafka configuration is stored in `config/kafka_config.json`:
```json
{
    "bootstrap_servers": "localhost:9092",
    "topic": "movie-recommendations",
    "group_id": "movie-recommender-group",
    "auto_offset_reset": "earliest"
}
```

## Running with Spark

### Using spark-submit

1. Start Apache Kafka:
```bash
# Start Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

# Start Kafka server
bin/kafka-server-start.sh config/server.properties
```

2. Place your MovieLens ratings.csv file in the `data/` directory.

3. Train the model and generate recommendations using spark-submit:
```bash
# For local mode
spark-submit \
    --master local[*] \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    --driver-memory 4g \
    --executor-memory 4g \
    train_recommender.py

# For cluster mode (YARN)
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    --driver-memory 4g \
    --executor-memory 4g \
    --num-executors 2 \
    --executor-cores 2 \
    train_recommender.py
```

4. Run the consumer to get recommendations:
```bash
# For local mode
spark-submit \
    --master local[*] \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    --driver-memory 4g \
    --executor-memory 4g \
    consume_recommendations.py

# For cluster mode (YARN)
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    --driver-memory 4g \
    --executor-memory 4g \
    --num-executors 2 \
    --executor-cores 2 \
    consume_recommendations.py
```

### Spark Configuration Options

Common spark-submit options:
- `--master`: Specify the cluster manager (local[*], yarn, mesos, etc.)
- `--deploy-mode`: Choose between client and cluster mode
- `--driver-memory`: Memory for driver process
- `--executor-memory`: Memory per executor
- `--num-executors`: Number of executors to launch
- `--executor-cores`: Number of cores per executor
- `--packages`: External packages to include

### Running in Different Environments

1. Local Mode (Development):
```bash
spark-submit --master local[*] train_recommender.py
```

2. YARN Cluster (Production):
```bash
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --num-executors 4 \
    --executor-cores 2 \
    train_recommender.py
```

3. Standalone Cluster:
```bash
spark-submit \
    --master spark://master:7077 \
    --deploy-mode cluster \
    train_recommender.py
```

## System Architecture

### Training Pipeline
1. Load MovieLens ratings data
2. Preprocess and split data into training/test sets
3. Train ALS model with the following parameters:
   - rank=10 (latent factors)
   - maxIter=10 (iterations)
   - regParam=0.1 (regularization)
4. Generate top-10 recommendations for all users
5. Send recommendations to Kafka topic

### Recommendation Query
1. Read recommendations from Kafka topic
2. Filter recommendations for requested user
3. Display top-10 movie recommendations with predicted ratings

## Model Details

The system uses Spark's ALS implementation with the following configuration:
- User-based collaborative filtering
- Cold start strategy: drop
- Non-negative constraints enabled
- 80/20 train/test split
- 10 latent factors
- 10 training iterations

## Performance Considerations

- Spark configurations are optimized for 4GB memory
- Parallelism is set to 200 partitions
- Kafka integration uses Spark's native connector
- Recommendations are cached in Kafka for quick retrieval

## Error Handling

The system includes comprehensive error handling for:
- Data loading and preprocessing
- Model training and prediction
- Kafka message production and consumption
- JSON parsing and serialization

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- MovieLens dataset from GroupLens Research
- Apache Spark for distributed computing
- Apache Kafka for real-time data streaming 