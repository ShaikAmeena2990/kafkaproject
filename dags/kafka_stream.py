import uuid
from datetime import datetime
from pathlib import Path
import json
import logging

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer

default_args = {
    "owner": "airscholar",
    "start_date": datetime(2024, 9, 3, 10, 0),
}

# Path to the CSV inside the Airflow container
FILE_PATH = Path("/opt/airflow/data/kafka.csv")
TOPIC      = "cisco_prices"
BROKERS    = ["broker:29092"]


def stream_price_data() -> None:
    if not FILE_PATH.exists():
        raise FileNotFoundError(f"CSV not found: {FILE_PATH}")

    df = pd.read_csv(FILE_PATH, parse_dates=["timestamp"])
    logging.info("Loaded %d rows from %s", len(df), FILE_PATH)

    producer = KafkaProducer(
        bootstrap_servers=BROKERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        max_block_ms=5_000,
    )

    for _, row in df.iterrows():
        payload = {
            "id":        str(uuid.uuid4()),
            "timestamp": row["timestamp"].isoformat(),
            "open":      float(row["open"]),
            "high":      float(row["high"]),
            "low":       float(row["low"]),
            "close":     float(row["close"]),
            "adjclose":  float(row["adjclose"]),
            "volume":    int(row["volume"]),
        }
        try:
            producer.send(TOPIC, payload)
        except Exception as exc:
            logging.error("Kafka send failed: %s", exc)
            continue

    producer.flush()
    producer.close()
    logging.info("Finished streaming to Kafka.")


with DAG(
    dag_id="stream",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["kafka", "cisco"],
) as dag:

    stream_to_kafka = PythonOperator(
        task_id="stream_price_data",
        python_callable=stream_price_data,
    )
