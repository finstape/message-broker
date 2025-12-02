import json
import logging
import os
from typing import Any, Callable, Dict

import pika


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CONSUMER] %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)


RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "user")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "password")

EXCHANGE_NAME = os.getenv("EXCHANGE_NAME", "calc_exchange")
MAIN_QUEUE_NAME = os.getenv("MAIN_QUEUE_NAME", "calc_queue")
DLX_NAME = os.getenv("DLX_NAME", "calc_dlq_exchange")
DLQ_NAME = os.getenv("DLQ_NAME", "calc_dlq")


def setup_topology(channel: pika.adapters.blocking_connection.BlockingChannel) -> None:
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="direct", durable=True)
    channel.exchange_declare(exchange=DLX_NAME, exchange_type="direct", durable=True)

    channel.queue_declare(queue=DLQ_NAME, durable=True)
    channel.queue_bind(queue=DLQ_NAME, exchange=DLX_NAME, routing_key="dlq")

    args = {
        "x-dead-letter-exchange": DLX_NAME,
        "x-dead-letter-routing-key": "dlq",
    }
    channel.queue_declare(queue=MAIN_QUEUE_NAME, durable=True, arguments=args)


def build_operations() -> Dict[str, Callable[[int, int], Any]]:
    return {
        "add": lambda a, b: a + b,
        "sub": lambda a, b: a - b,
        "mul": lambda a, b: a * b,
        "div": lambda a, b: a / b,
    }


def main() -> None:
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        credentials=credentials,
        heartbeat=30,
    )

    logger.info("Connecting to RabbitMQ at %s:%s", RABBITMQ_HOST, RABBITMQ_PORT)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    setup_topology(channel)
    operations = build_operations()

    def callback(ch, method, properties, body: bytes) -> None:
        try:
            data = json.loads(body.decode("utf-8"))
            msg_id = data.get("id")
            op = data.get("operation")
            a = int(data.get("a"))
            b = int(data.get("b"))

            if op not in operations:
                raise ValueError(f"Unsupported operation: {op}")

            result = operations[op](a, b)
            logger.info(
                "OK id=%s op=%s a=%s b=%s result=%s",
                msg_id,
                op,
                a,
                b,
                result,
            )
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as exc:  # noqa: BLE001
            logger.error("FAIL body=%s error=%s", body, exc)
            # send to DLQ via DLX (no requeue)
            ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=MAIN_QUEUE_NAME, on_message_callback=callback)

    try:
        logger.info("Waiting for messages...")
        channel.start_consuming()
    except KeyboardInterrupt:
        logger.info("Stopping consumer")
    finally:
        if connection and not connection.is_closed:
            connection.close()


if __name__ == "__main__":
    main()
