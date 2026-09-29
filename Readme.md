# Real-Time Crypto Trade Pipeline

A real-time data pipeline that ingests live cryptocurrency trades from Binance,
processes them with Spark Structured Streaming, and persists windowed analytics
and anomaly flags to Postgres.

## Architecture

```
Binance WebSocket  --->  Kafka  --->  Spark Structured Streaming  --->  Postgres
 (live BTC/USDT           (topic:       (windowed aggregation +
  trade feed)           crypto-trades)   anomaly detection)
```

**Why this stack:** Kafka decouples ingestion from processing, so the producer
and the analytics job can be restarted, scaled, or replaced independently.
Spark Structured Streaming handles the windowing, watermarking, and
stateful aggregation. Postgres gives the results a durable home that's
queryable with plain SQL, rather than only being visible as console output.

## Components

| File | Role |
|---|---|
| `binance_producer.py` | Connects to Binance's public WebSocket trade stream and forwards each trade into the `crypto-trades` Kafka topic. |
| `trade_analytics.py` | Reads from Kafka, groups trades into 1-minute tumbling windows per symbol, computes avg/min/max price and volume, flags large price swings between windows as anomalies, and writes each result into Postgres. |
| `docker-compose.yml` | Spins up Kafka (KRaft mode, no Zookeeper), Kafka UI, and Postgres locally. |

## How it works

1. **Ingestion** — `binance_producer.py` opens a WebSocket connection to
   `wss://stream.binance.com:9443/ws/btcusdt@trade` and republishes each trade
   event into Kafka as clean JSON (`symbol`, `price`, `quantity`, `trade_time`).

2. **Windowed aggregation** — `trade_analytics.py` reads the stream with Spark
   Structured Streaming, applies a **10-second watermark** (tolerance for
   late-arriving data), and groups trades into **1-minute tumbling windows**
   per symbol, computing:
   - average, minimum, and maximum price
   - total trade volume
   - trade count

3. **Anomaly detection** — each window's average price is compared to the
   previous window's average for the same symbol. A move of **0.5% or more**
   between consecutive windows is flagged as `is_anomaly = true`. This is a
   simple, explainable threshold rule rather than a statistical model —
   a deliberate first version that's easy to extend later (e.g. rolling
   standard deviation, z-scores).

4. **Persistence** — results are written to a `trade_window_stats` table in
   Postgres via Spark's `foreachBatch`, which lets each micro-batch run
   arbitrary Python (including maintaining the "previous window" state
   needed for anomaly detection) rather than being limited to Spark SQL
   sinks.

All timestamps are handled in **UTC** throughout the pipeline (Spark session
timezone and Postgres both), to keep `window_start`/`window_end` consistent
with `inserted_at` and avoid timezone drift in downstream queries or
dashboards.

## Database schema

```sql
CREATE TABLE trade_window_stats (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    avg_price DOUBLE PRECISION,
    min_price DOUBLE PRECISION,
    max_price DOUBLE PRECISION,
    total_volume DOUBLE PRECISION,
    trade_count INTEGER,
    is_anomaly BOOLEAN DEFAULT FALSE,
    inserted_at TIMESTAMP DEFAULT NOW()
);
```

## Running it locally

**Prerequisites:** Docker Desktop, Java 17+, Python 3.11+, `winutils.exe` on
Windows (for local Hadoop filesystem access).

```bash
# 1. Start infrastructure
docker compose up -d

# 2. Create the Postgres table (see schema above)
docker exec -it postgres psql -U streaming_user -d crypto_pipeline

# 3. Install Python dependencies
pip install websocket-client kafka-python psycopg2-binary pyspark

# 4. In one terminal: start the producer
python binance_producer.py

# 5. In another terminal: start the analytics job
python trade_analytics.py
```

Query the results at any time:

```sql
SELECT * FROM trade_window_stats ORDER BY window_start DESC LIMIT 10;
SELECT * FROM trade_window_stats WHERE is_anomaly = true;
```

## Possible next steps

- Dashboard on top of `trade_window_stats` (Grafana, Metabase, or a small
  Streamlit app)
- Swap the fixed anomaly threshold for a rolling standard-deviation model
- Track multiple symbols simultaneously (currently BTCUSDT only)
- Containerize the Python scripts themselves for a fully `docker compose up`
  deployment