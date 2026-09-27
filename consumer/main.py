import os
import json
import time
from confluent_kafka import Consumer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
GROUP_ID = os.getenv("GROUP_ID", "demo-group")

def create_consumer(group_id: str):
    return Consumer({
        'bootstrap.servers': BOOTSTRAP,
        'group.id': group_id,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
    })

def main():
    # Небольшая пауза, чтобы Kafka успела подняться
    time.sleep(5)

    consumer = create_consumer(GROUP_ID)
    consumer.subscribe(['demo-topic'])

    print(f"[CONSUMER] Старт. group.id={GROUP_ID}, bootstrap={BOOTSTRAP}")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue
            if msg.error():
                print(f"[ERROR] {msg.error()}")
                continue

            data = json.loads(msg.value().decode('utf-8'))

            print(f"[RECV] id={data['id']}, payload={data['payload']}, "
                  f"partition={msg.partition()}, offset={msg.offset()}")

            consumer.commit(asynchronous=False)
            print(f"[COMMIT] offset={msg.offset()}")

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        print("[CONSUMER] Остановлен.")

if __name__ == '__main__':
    main()