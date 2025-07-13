import faust

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
)

messages_topic = app.topic(
    'messages', key_type=str, value_type=Messages
)

@app.agent(messages_topic)
async def filter_blocked_users(stream):
    async for message in stream:
        print(f"Sender: {message.sender_name}, Recipient: {message.recipient_name}")

if __name__ == '__main__':
    app.main()