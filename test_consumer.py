"""
Throwaway Spark Structured Streaming consumer.
Reads whatever lands in 'test-topic' and prints it to the console continuously.
"""

import os

os.environ["JAVA_HOME"] = r"C:\Users\Muhammad Asim\AppData\Local\Programs\Eclipse Adoptium\jdk-17.0.20.101-hotspot"
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = (
    os.environ["JAVA_HOME"] + r"\bin;"
    + os.environ["HADOOP_HOME"] + r"\bin;"
    + os.environ["PATH"]
)


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = (
    SparkSession.builder
    .appName("KafkaTestConsumer")
    # This tells Spark which extra package to download to understand Kafka.
    # Spark doesn't speak Kafka's protocol natively -- this connector bridges the two.
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0",
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")  # Spark's default logging is very noisy otherwise

# Step 1: read the raw stream from Kafka.
# Note: still port 9092 -- Spark is running as a local Python process on your
# host machine, just like the producer was, so it uses the same host-facing port.
raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "test-topic")
    .option("startingOffsets", "earliest")  # read from the beginning of the topic, not just new messages
    .load()
)

# Step 2: Kafka gives us 'key' and 'value' as raw bytes. Cast value to a string
# so we can parse the JSON that's inside it.
json_strings = raw_stream.selectExpr("CAST(value AS STRING) as json_str")

# Step 3: define the shape of our JSON so Spark can parse it into real columns,
# instead of us handling one big opaque string.
schema = StructType([
    StructField("hello", StringType()),
    StructField("sequence", IntegerType()),
    StructField("timestamp", StringType()),
])

parsed = json_strings.select(
    from_json(col("json_str"), schema).alias("data")
).select("data.*")

# Step 4: write the parsed stream out to the console, continuously, as new
# micro-batches of data arrive.
query = (
    parsed.writeStream
    .format("console")
    .outputMode("append")  # just show new rows as they come, don't try to aggregate anything yet
    .start()
)

print("Listening for messages on 'test-topic'... (Ctrl+C to stop)")
query.awaitTermination()