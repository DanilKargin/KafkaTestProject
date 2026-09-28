def reserve_order(payload: dict) -> None:
    """
    Реальная бизнес-логика склада: зарезервировать товар по заказу.
    Выполняется В ТОЙ ЖЕ транзакции, что и запись в inbox.
    """
    order_id = payload.get("orderId")
    amount = payload.get("amount")
    print(f"[WAREHOUSE] Резервирую заказ {order_id}, сумма {amount}")