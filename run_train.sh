#!/bin/bash

# Set environment variables
export PYTHONPATH=$PYTHONPATH:$(pwd)
export SPARK_HOME=/Users/cuongdv3/spark
export PYSPARK_PYTHON=python
export PYSPARK_DRIVER_PYTHON=python

# Install required packages in system Python if not already installed
pip3 install numpy pandas pyspark findspark confluent-kafka py4j

# Run spark-submit with required packages
spark-submit \
    --master local \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    --conf "spark.pyspark.python=/usr/bin/python3" \
    --conf "spark.pyspark.driver.python=/usr/bin/python3" \
    --conf "spark.executor.extraClassPath=$(python3 -c 'import site; print(site.getsitepackages()[0])')" \
    --conf "spark.driver.extraClassPath=$(python3 -c 'import site; print(site.getsitepackages()[0])')" \
    train_recommender.py 