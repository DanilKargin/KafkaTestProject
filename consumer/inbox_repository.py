import psycopg


def try_register_message(conn: psycopg.Connection, message_key: str) -> bool:
    """
    Пытается зарегистрировать сообщение в inbox.
    Возвращает True, если это новое сообщение (rowcount == 1),
    False — если уже было обработано (ON CONFLICT DO NOTHING).
    Должно вызываться ВНУТРИ активной транзакции.
    """
    cur = conn.execute(
        """
        INSERT INTO inbox (message_key)
        VALUES (%s)
        ON CONFLICT (message_key) DO NOTHING
        """,
        (message_key,),
    )
    return cur.rowcount == 1