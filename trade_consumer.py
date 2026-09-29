import os

# Keep these as a safety net so the script works even in a fresh
# terminal where the environment variables weren't set globally.
os.environ["JAVA_HOME"] = r"C:\Users\Muhammad Asim\AppData\Local\Programs\Eclipse Adoptium\jdk-17.0.20.101-hotspot"
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = (
    os.environ["JAVA_HOME"] + r"\bin;"
    + os.environ["HADOOP_HOME"] + r"\bin;"
    + os.environ["PATH"]
)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "crypto-trades"

spark = (
    SparkSession.builder
    .appName("BinanceTradeConsumer")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Matches the clean_trade dict shape sent by binance_producer.py
trade_schema = StructType([
    StructField("symbol", StringType()),
    StructField("price", DoubleType()),
    StructField("quantity", DoubleType()),
    StructField("trade_time", LongType()),  # epoch milliseconds
])

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")   # only new trades from now on
    .load()
)

# Kafka messages arrive as raw bytes in a "value" column; parse the
# JSON string inside it using our schema.
parsed = (
    raw_stream
    .select(from_json(col("value").cast("string"), trade_schema).alias("data"))
    .select("data.*")
    # trade_time is epoch millis -> convert to seconds -> readable timestamp
    .withColumn("event_time", from_unixtime(col("trade_time") / 1000).cast("timestamp"))
)

query = (
    parsed.writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", False)
    .start()
)

print(f"Listening for trades on '{KAFKA_TOPIC}'... (Ctrl+C to stop)")
query.awaitTermination()