import os

os.environ["JAVA_HOME"] = r"C:\Users\Muhammad Asim\AppData\Local\Programs\Eclipse Adoptium\jdk-17.0.20.101-hotspot"
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = (
    os.environ["JAVA_HOME"] + r"\bin;"
    + os.environ["HADOOP_HOME"] + r"\bin;"
    + os.environ["PATH"]
)

import psycopg2

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, from_unixtime, window,
    avg, min as spark_min, max as spark_max, count, sum as spark_sum,
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "crypto-trades"

# Matches the credentials in your docker-compose.yml postgres service
PG_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "crypto_pipeline",
    "user": "streaming_user",
    "password": "streaming_pass",
}

# Width of each aggregation window. Use "30 seconds" while testing
# so you don't have to wait as long to see a window close.
WINDOW_DURATION = "1 minute"

# How much the average price can move between consecutive windows
# before it's flagged as a spike. Simple and explainable, not a
# statistical model -- a fine starting point for a first version.
SPIKE_THRESHOLD_PCT = 0.5  # percent

spark = (
    SparkSession.builder
    .appName("BinanceTradeAnalytics")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0")
    # Pin the driver to loopback so it doesn't get confused between
    # your real network adapter and Docker's virtual ones (this is
    # what causes the "idWithoutTopologyInfo is null" heartbeat error).
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    # Force UTC everywhere so window_start/window_end match Postgres's
    # inserted_at (which defaults to UTC) -- avoids confusing timezone
    # gaps when you later build a dashboard on top of this table.
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

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
    .option("startingOffsets", "latest")
    .load()
)

trades = (
    raw_stream
    .select(from_json(col("value").cast("string"), trade_schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", from_unixtime(col("trade_time") / 1000).cast("timestamp"))
)

# Watermark: how long Spark waits for late-arriving trades before it
# finalizes a window and moves on. 10 seconds is generous for a local
# pipeline with no real network lag between producer and Kafka.
windowed = (
    trades
    .withWatermark("event_time", "10 seconds")
    .groupBy(
        col("symbol"),
        window(col("event_time"), WINDOW_DURATION),
    )
    .agg(
        avg("price").alias("avg_price"),
        spark_min("price").alias("min_price"),
        spark_max("price").alias("max_price"),
        spark_sum("quantity").alias("total_volume"),
        count("*").alias("trade_count"),
    )
    .select(
        col("symbol"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "avg_price", "min_price", "max_price", "total_volume", "trade_count",
    )
)

# Remembers the previous window's average price per symbol, in plain
# Python memory, so each microbatch can compare "now vs last window"
# without a second Spark job or an external database.
previous_avg_price = {}

# One connection, opened once and reused across microbatches, rather
# than reconnecting to Postgres on every batch (which would be slow
# and would quickly exhaust connection limits on a fast stream).
pg_conn = psycopg2.connect(**PG_CONFIG)
pg_conn.autocommit = True

INSERT_SQL = """
    INSERT INTO trade_window_stats
        (symbol, window_start, window_end, avg_price, min_price,
         max_price, total_volume, trade_count, is_anomaly)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""


def process_batch(batch_df, batch_id):
    with pg_conn.cursor() as cur:
        for row in batch_df.collect():
            symbol = row["symbol"]
            avg_price = row["avg_price"]

            print(
                f"[{row['window_start']} - {row['window_end']}] {symbol}: "
                f"avg={avg_price:.2f} min={row['min_price']:.2f} "
                f"max={row['max_price']:.2f} volume={row['total_volume']:.4f} "
                f"trades={row['trade_count']}"
            )

            is_anomaly = False
            prev = previous_avg_price.get(symbol)
            if prev is not None:
                pct_change = abs(avg_price - prev) / prev * 100
                if pct_change >= SPIKE_THRESHOLD_PCT:
                    is_anomaly = True
                    direction = "UP" if avg_price > prev else "DOWN"
                    print(
                        f"  ANOMALY: {symbol} moved {direction} {pct_change:.2f}% "
                        f"between windows (prev avg={prev:.2f}, now={avg_price:.2f})"
                    )

            previous_avg_price[symbol] = avg_price

            try:
                cur.execute(INSERT_SQL, (
                    symbol, row["window_start"], row["window_end"],
                    avg_price, row["min_price"], row["max_price"],
                    row["total_volume"], row["trade_count"], is_anomaly,
                ))
            except Exception as e:
                # Don't let a single bad insert crash the whole stream --
                # print it and keep processing later batches.
                print(f"  Postgres insert failed: {e}")


query = (
    windowed.writeStream
    .outputMode("update")
    .foreachBatch(process_batch)
    .start()
)

print(f"Running windowed aggregation + anomaly detection on '{KAFKA_TOPIC}'... (Ctrl+C to stop)")
query.awaitTermination()