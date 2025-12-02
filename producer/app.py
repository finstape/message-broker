import json
import logging
import os
import random
import string
import time
from typing import List

import pika


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PRODUCER] %(levelname)s %(message)s",
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

OPERATIONS: List[str] = ["add", "mul", "sub", "div", "unknown"]


def random_id(length: int = 8) -> str:
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=length))


def setup_topology(channel: pika.adapters.blocking_connection.BlockingChannel) -> None:
    # main exchange and queues
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="direct", durable=True)

    # DLQ exchange and queue
    channel.exchange_declare(exchange=DLX_NAME, exchange_type="direct", durable=True)
    channel.queue_declare(queue=DLQ_NAME, durable=True)
    channel.queue_bind(queue=DLQ_NAME, exchange=DLX_NAME, routing_key="dlq")

    args = {
        "x-dead-letter-exchange": DLX_NAME,
        "x-dead-letter-routing-key": "dlq",
    }
    channel.queue_declare(queue=MAIN_QUEUE_NAME, durable=True, arguments=args)

    # bind all operations to same queue
    for op in OPERATIONS:
        channel.queue_bind(queue=MAIN_QUEUE_NAME, exchange=EXCHANGE_NAME, routing_key=op)


def main() -> None:
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        credentials=credentials,
        heartbeat=30,
    )

    logger.info(
        "Connecting to RabbitMQ at %s:%s", RABBITMQ_HOST, RABBITMQ_PORT
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    setup_topology(channel)

    try:
        while True:
            op = random.choice(OPERATIONS)
            a = random.randint(1, 10)
            b = random.randint(0, 10)
            message_id = random_id()

            payload = {
                "id": message_id,
                "operation": op,
                "a": a,
                "b": b,
            }
            body = json.dumps(payload).encode("utf-8")

            channel.basic_publish(
                exchange=EXCHANGE_NAME,
                routing_key=op,
                body=body,
                properties=pika.BasicProperties(
                    delivery_mode=2,  # make message persistent
                    content_type="application/json",
                ),
            )

            logger.info("Sent message: %s", payload)
            time.sleep(2)
    except KeyboardInterrupt:
        logger.info("Stopping producer")
    finally:
        connection.close()


if __name__ == "__main__":
    main()
