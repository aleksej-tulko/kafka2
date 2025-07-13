import faust


class Transaction(faust.Record):
    sender_id: str
    recipient_id: str
    amount: float
    content: str


app = faust.App(
    "pract-task3",
    broker="localhost:9097",
    store="rocksdb://",
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
