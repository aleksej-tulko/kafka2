import faust


PROHIBITED_USERS = ['spammer']


class Transaction(faust.Record):
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
    'messages', key_type=str, value_type=Transaction
)
filtered_messages_topic = app.topic(
    'filtered_messages', key_type=str, value_type=Transaction
)

blocked_users_topic = app.topic(
    'blocked_users', key_type=str, value_type=Transaction
)


@app.agent(messages_topic)
async def process_messages(stream):
    async for message in stream:
        if message.amount > 10_000:
            await filtered_messages_topic.send(
                value=message
            )
