import os
from threading import Thread
from time import sleep

import faust
from confluent_kafka import (
    KafkaError, KafkaException, Producer
)
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import (
    JSONDeserializer, JSONSerializer
)
from confluent_kafka.serialization import (
    MessageField, SerializationContext, StringSerializer
)
from dotenv import load_dotenv

load_dotenv()

ACKS_LEVEL = os.getenv('ACKS_LEVEL', 'all')
AUTOOFF_RESET = os.getenv('AUTOCOMMIT_RESET', 'earliest')
ENABLE_AUTOCOMMIT = os.getenv('ENABLE_AUTOCOMMIT', False)
FETCH_MIN_BYTES = os.getenv('FETCH_MIN_BYTES', 1)
FETCH_WAIT_MAX_MS = os.getenv('FETCH_WAIT_MAX_MS', 100)
RETRIES = os.getenv('RETRIES', '3')
SESSION_TIME_MS = os.getenv('SESSION_TIME_MS', 1_000)
TOPIC = os.getenv('TOPIC', 'practice')

schema_registry_config = {
   'url': 'http://localhost:8081'
}

json_schema_str = """
{
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "Product",
    "type": "object",
    "properties": {
        "sender_id": {
            "type": "integer"
        },
        "sender_name": {
            "type": "string"
        },
        "recipient_id": {
            "type": "integer"
        },
        "recipient_name": {
            "type": "string"
        },
        "amount": {
            "type": "number"
        },
        "content": {
            "type": "string"
        }
    },
    "required": ["sender_id", "sender_name",\
        "recipient_id", "recipient_name", "amount", "content"]
}
"""

schema_registry_client = SchemaRegistryClient(schema_registry_config)
json_serializer = JSONSerializer(json_schema_str, schema_registry_client)
key_serializer = StringSerializer('utf_8')
value_serializer = json_serializer

conf = {
    "bootstrap.servers":
    "localhost:9093,localhost:9095,localhost:9097",
}

producer_conf = conf | {
    "acks": ACKS_LEVEL,
    "retries": RETRIES,
}

producer = Producer(producer_conf)

user_ids = {
    "clown": 1,
    "spammer": 2,
    "dodik": 3
}

prohibited_users = {
    "clown": ["spammer"],
    "spammer": ["dodik"],
    "dodik": ["clown"]
}


class BlockedUsers(faust.Record):
    blocker: str
    blocked: list[str]


class Messages(faust.Record):
    sender_id: int
    sender_name: str
    recipient_id: int
    recipient_name: str
    amount: float
    content: str


app = faust.App(
    "pract-task3",
    broker="kafka://localhost:9093,localhost:9095,localhost:9097",
    store="rocksdb://",
)

table = app.Table(
    "blocked_users",
    partitions=3,
    default=str
)

messages_topic = app.topic(
    'messages', key_type=str, value_type=Messages
)
filtered_messages_topic = app.topic(
    'filtered_messages', key_type=str, value_type=Messages
)

blocked_users_topic = app.topic(
    'blocked_users', key_type=str, value_type=BlockedUsers
)


def delivery_report(err, msg) -> None:
    """Отчет о доставке."""
    if err is not None:
        print(f'Сообщение не отправлено: {err}')
    else:
        print(f'Сообщение доставлено в {msg.topic()} [{msg.partition()}]')


def create_message(sender_id: int, sender_name: str,
                   recipient_id: int, recipient_name: str,
                   amount: float, content: str
                   ) -> None:
    """Сериализация сообщения и отправка в брокер."""
    message_value = {
        "sender_id": sender_id,
        "sender_name": sender_name,
        "recipient_id": recipient_id,
        "recipient_name": recipient_name,
        "amount": amount,
        "content": content
        }
    producer.produce(
        topic=TOPIC,
        key=key_serializer(
            "user_key", SerializationContext(TOPIC, MessageField.VALUE)
        ),
        value=value_serializer(
            message_value, SerializationContext(TOPIC, MessageField.VALUE)
        ),
        on_delivery=delivery_report
    )


def producer_infinite_loop():
    """Запуска цикла для генерации сообщения."""
    incr_num: float = 0.0
    recipient_names = [name for name in user_ids]
    try:
        while True:
            for sender_name, id in user_ids.items():
                for recipient_name in recipient_names:
                    if sender_name != recipient_name:
                        create_message(
                            sender_id=id,
                            sender_name=sender_name,
                            recipient_id=user_ids[recipient_name],
                            recipient_name=recipient_name,
                            amount=incr_num,
                            content=f'{incr_num}')
                        incr_num += 1.0
                        if incr_num % 10 == 0:
                            producer.flush()
    except Exception as e:
        raise RuntimeError(e)
    finally:
        producer.flush()


@app.agent(messages_topic)
async def process_messages(stream):
    async for message in stream:
        if message.amount > 10_000:
            await filtered_messages_topic.send(
                value=message
            )


if __name__ == '__main__':
    """Основной код."""
    producer_thread = Thread(
        target=producer_infinite_loop,
        args=(),
        daemon=True
    )
    producer_thread.start()

    while True:
        print('Выполняется программа')
        sleep(10)
