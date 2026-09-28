import os

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
GROUP_ID = os.getenv("GROUP_ID", "demo-group")
PG_CONN = os.getenv(
    "PG_CONN",
    "host=localhost port=5432 dbname=demo user=demo password=demo",
)
TOPIC = "demo-topic"
POLL_TIMEOUT = 1.0