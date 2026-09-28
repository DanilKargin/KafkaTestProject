import json
import psycopg
from confluent_kafka import Message
from inbox_repository import try_register_message
from warehouse import reserve_order


def handle_message(msg: Message, conn: psycopg.Connection) -> None:
    """
    Идемпотентная обработка:
    1. Начинаем транзакцию.
    2. Пытаемся вставить message_key в inbox.
    3. Если новое — выполняем бизнес-логику.
    4. Если дубликат — пропускаем.
    5. Коммитим транзакцию (inbox + бизнес-данные).
    """
    data = json.loads(msg.value().decode("utf-8"))
    message_key = str(data["id"])

    with conn.transaction():
        is_new = try_register_message(conn, message_key)

        if not is_new:
            print(
                f"[SKIP] Дубликат message_key={message_key} "
                f"(partition={msg.partition()}, offset={msg.offset()})"
            )
            return

        reserve_order(data.get("payload", {}))
        print(
            f"[PROCESSED] message_key={message_key} "
            f"partition={msg.partition()}, offset={msg.offset()}"
        )