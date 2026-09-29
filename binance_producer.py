"""
Binance WebSocket -> Kafka Producer

Connects to Binance's public live trade stream for a symbol and
forwards each trade event into a Kafka topic, so Spark Structured
Streaming can pick it up downstream.
"""

import json
import websocket
from kafka import KafkaProducer

# --- Configuration ---
SYMBOL = "btcusdt"                     # must be lowercase for Binance's URL
KAFKA_TOPIC = "crypto-trades"          # new topic, separate from test-topic
KAFKA_BOOTSTRAP = "localhost:9092"     # matches your docker-compose Kafka
BINANCE_WS_URL = f"wss://stream.binance.com:9443/ws/{SYMBOL}@trade"

# --- Kafka producer setup ---
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


def on_message(ws, message):
    """Called every time Binance pushes a new trade event."""
    trade = json.loads(message)

    # Binance's raw trade event looks like:
    # {
    #   "e": "trade", "E": 123456789, "s": "BTCUSDT",
    #   "p": "0.001", "q": "100", "T": 123456785, ...
    # }
    # Reshape it into clean, simple field names before sending to
    # Kafka, so the Spark schema on the consumer side stays readable.
    clean_trade = {
        "symbol": trade["s"],
        "price": float(trade["p"]),
        "quantity": float(trade["q"]),
        "trade_time": trade["T"],
    }

    print(clean_trade)
    producer.send(KAFKA_TOPIC, clean_trade)


def on_error(ws, error):
    print("WebSocket error:", error)


def on_close(ws, close_status_code, close_msg):
    print("WebSocket closed")


def on_open(ws):
    print(
        f"Connected to Binance stream for {SYMBOL.upper()}. "
        f"Streaming trades into Kafka topic '{KAFKA_TOPIC}'..."
    )


if __name__ == "__main__":
    ws = websocket.WebSocketApp(
        BINANCE_WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )
    ws.run_forever()