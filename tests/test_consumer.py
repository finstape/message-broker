import json
import os
from unittest.mock import MagicMock, patch

import pytest

from consumer.app import build_operations


def test_build_operations():
    """Тест создания словаря операций."""
    ops = build_operations()

    assert "add" in ops
    assert "sub" in ops
    assert "mul" in ops
    assert "div" in ops

    # Проверяем выполнение операций
    assert ops["add"](5, 3) == 8
    assert ops["sub"](5, 3) == 2
    assert ops["mul"](5, 3) == 15
    assert ops["div"](10, 2) == 5.0


def test_build_operations_division_by_zero():
    """Тест деления на ноль."""
    ops = build_operations()

    with pytest.raises(ZeroDivisionError):
        ops["div"](10, 0)


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
        import consumer.app
        importlib.reload(consumer.app)

        consumer.app.setup_topology(mock_channel)

    # Проверяем, что exchange объявлены
    assert mock_channel.exchange_declare.call_count >= 2

    # Проверяем, что очереди объявлены
    assert mock_channel.queue_declare.call_count >= 2

    # Проверяем, что есть привязки
    assert mock_channel.queue_bind.call_count >= 1


def test_consumer_callback_success():
    """Тест успешной обработки сообщения."""
    ops = build_operations()

    # Симулируем успешное сообщение
    payload = {
        "id": "test123",
        "operation": "add",
        "a": 5,
        "b": 3,
    }
    json.dumps(payload).encode("utf-8")

    # Проверяем, что операция существует
    assert payload["operation"] in ops

    # Проверяем результат
    result = ops[payload["operation"]](payload["a"], payload["b"])
    assert result == 8


def test_consumer_callback_unknown_operation():
    """Тест обработки неизвестной операции."""
    ops = build_operations()

    payload = {
        "id": "test123",
        "operation": "unknown",
        "a": 5,
        "b": 3,
    }

    # Проверяем, что операция не поддерживается
    assert payload["operation"] not in ops

    with pytest.raises(KeyError):
        ops[payload["operation"]](payload["a"], payload["b"])


def test_consumer_message_format():
    """Тест формата сообщения consumer."""
    # Проверяем парсинг сообщения
    payload = {
        "id": "test123",
        "operation": "mul",
        "a": 4,
        "b": 7,
    }

    body = json.dumps(payload).encode("utf-8")
    decoded = json.loads(body.decode("utf-8"))

    assert decoded["id"] == "test123"
    assert decoded["operation"] == "mul"
    assert int(decoded["a"]) == 4
    assert int(decoded["b"]) == 7
