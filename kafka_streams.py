import faust


class Transaction(faust.Record):
    sender_id: str
    recipient_id: str
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
    default=lambda x: x == "spammer"
)

transactions_topic = app.topic(
    "transactions", key_type=str, value_type=Transaction
)
fraud_detection_topic = app.topic(
    "fraud-detection", key_type=str, value_type=Transaction
)


@app.agent(transactions_topic)
async def process_transactions(stream):
    async for transaction in stream:
        if transaction.amount > 10_000:
            await fraud_detection_topic.send(
                value=transaction
            )
