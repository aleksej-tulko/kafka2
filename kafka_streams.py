import faust


prohibited_users = {
    "clown": ["spammer"],
    "spammer": ["dodik", "payaso"],
    "dodik": ["clown"],
    "payaso": ["clown", "spammer"]
}


class Messages(faust.Record):
    sender_id: int
    sender_name: str
    recipient_id: int
    recipient_name: str
    amount: float
    content: str


class BlockedUsers(faust.Record):
    blocker: str
    blocked: str


app = faust.App(
    "pract-task-3",
    broker="kafka://localhost:9093,localhost:9095,localhost:9097",
    store="rocksdb://",
)


messages_topic = app.topic(
    'messages', key_type=str, value_type=Messages, partitions=1
)

blocked_users_topic = app.topic(
    'blocked_users', key_type=str, value_type=BlockedUsers, partitions=1
)


def output_blocked_users(blocked: BlockedUsers):
    print(f'{blocked.blocker} заблокировал(а) {blocked.blocked}')


@app.agent(messages_topic, sink=[output_blocked_users])
async def filter_blocked_users(stream):
    async for message in stream:
        blocked_list = prohibited_users.get(message.recipient_name, [])
        if message.sender_name in blocked_list:
            await blocked_users_topic.send(
                value=BlockedUsers(
                    blocker=message.recipient_name,
                    blocked=message.sender_name
                )
            )