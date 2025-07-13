from threading import Thread

import faust
from confluent_kafka import (
    Consumer, KafkaError, KafkaException, Producer
)


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
    incr_num = 0
    try:
        while True:
            create_message(incr_num)
            incr_num += 1
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
