from threading import Thread
from time import sleep

import faust

prohibited_users = {
    "clown": ["spammer"],
    "spammer": ["dodik", "payaso"],
    "dodik": ["clown"],
    "payaso": ["clown", "spammer"]
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
    "pract-task-3",
    broker="kafka://localhost:9093,localhost:9095,localhost:9097",
    store="rocksdb://",

)

table = app.Table(
    "blocked-users",
    partitions=1,
    default=str
)

messages_topic = app.topic(
    'messages', key_type=str, value_type=Messages, partitions=1
)
filtered_messages_topic = app.topic(
    'filtered_messages', key_type=str, value_type=Messages, partitions=1
)

blocked_users_topic = app.topic(
    'blocked_users', key_type=str, value_type=BlockedUsers, partitions=1
)


@app.agent(messages_topic)
async def filter_blocked_users(stream):
    async for message in stream:
        for senders in prohibited_users.values():
            if message.sender_name in senders:
                table[message.recipient_name] = [sender for sender in senders]
