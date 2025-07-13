from threading import Thread
from time import sleep

import faust

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


@app.agent(messages_topic)
async def filter_blocked_users(stream):
    async for message in stream:
        for blocker in prohibited_users:
            if message.sender_name in prohibited_users[blocker]:
                await blocked_users_topic.send(
                    value={
                        "blocker": message.sender_name,
                        "blocked": prohibited_users[blocker]
                    }
                )
