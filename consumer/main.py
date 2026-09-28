import signal
from confluent_kafka import KafkaError
from config import GROUP_ID, TOPIC, POLL_TIMEOUT
from db import connect
from kafka_consumer import create_consumer
from handlers import handle_message

running = True


def stop(*_):
    global running
    running = False


signal.signal(signal.SIGINT, stop)
signal.signal(signal.SIGTERM, stop)


def main() -> None:
    consumer = create_consumer(GROUP_ID)
    print(f"[CONSUMER] Старт. group.id={GROUP_ID}, topic={TOPIC}")

    with connect() as conn:
        while running:
            msg = consumer.poll(timeout=POLL_TIMEOUT)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"[KAFKA ERROR] {msg.error()}")
                continue

            try:
                handle_message(msg, conn)
                # offset коммитим ТОЛЬКО после успешной обработки
                consumer.commit(asynchronous=False)
            except Exception as ex:
                print(f"[ERROR] Не удалось обработать сообщение: {ex}")
                conn.rollback()

    consumer.close()
    print("[CONSUMER] Остановлен.")


if __name__ == "__main__":
    main()