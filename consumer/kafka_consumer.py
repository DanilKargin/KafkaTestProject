from confluent_kafka import Consumer
from config import KAFKA_BOOTSTRAP, GROUP_ID, TOPIC


def create_consumer(group_id: str = GROUP_ID) -> Consumer:
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([TOPIC])
    return consumer