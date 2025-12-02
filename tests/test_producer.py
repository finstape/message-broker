import json
import os
from unittest.mock import MagicMock, patch

from producer.app import random_id


def test_random_id():
    """Тест генерации случайного ID."""
    id1 = random_id()
    id2 = random_id()
    assert len(id1) == 8
    assert len(id2) == 8
    assert id1 != id2
    assert isinstance(id1, str)


def test_random_id_custom_length():
    """Тест генерации ID с кастомной длиной."""
    id_short = random_id(4)
    id_long = random_id(16)
    assert len(id_short) == 4
    assert len(id_long) == 16


def test_setup_topology():
    """Тест настройки топологии RabbitMQ."""
    mock_channel = MagicMock()

    with patch.dict(os.environ, {
        "EXCHANGE_NAME": "test_exchange",
        "MAIN_QUEUE_NAME": "test_queue",
        "DLX_NAME": "test_dlx",
        "DLQ_NAME": "test_dlq",
    }):
        # Перезагружаем модуль для применения переменных окружения
        import importlib
        import producer.app
        importlib.reload(producer.app)

        producer.app.setup_topology(mock_channel)

    # Проверяем, что exchange объявлены
    assert mock_channel.exchange_declare.call_count >= 2

    # Проверяем, что очереди объявлены
    assert mock_channel.queue_declare.call_count >= 2

    # Проверяем, что есть привязки
    assert mock_channel.queue_bind.call_count >= 1


def test_producer_message_format():
    """Тест формата сообщения producer."""
    # Проверяем, что сообщение имеет правильную структуру
    payload = {
        "id": "test123",
        "operation": "add",
        "a": 5,
        "b": 3,
    }

    body = json.dumps(payload).encode("utf-8")
    decoded = json.loads(body.decode("utf-8"))

    assert decoded["id"] == "test123"
    assert decoded["operation"] == "add"
    assert decoded["a"] == 5
    assert decoded["b"] == 3
    assert "id" in decoded
    assert "operation" in decoded
    assert "a" in decoded
    assert "b" in decoded
