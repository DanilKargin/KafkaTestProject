import psycopg
from config import PG_CONN


def connect() -> psycopg.Connection:
    """Открывает соединение без autocommit — транзакциями управляем вручную."""
    return psycopg.connect(PG_CONN, autocommit=False)