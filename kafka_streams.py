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
    default=list
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


def capitalize_names(users: Messages) -> Messages:
    sender = users.sender_name
    recipient = users.recipient_name
    if sender and recipient:
        sender = sender.upper()
        recipient = recipient.upper()
    return users


processed_stream = app.stream(messages_topic, processors=[capitalize_names])


def output_blocked_users_from_db(blocked: list) -> None:
    blocker_to_blocked = {}

    for blocker, blocked_list in prohibited_users.items():
        filtered_blocked = [user for user in blocked if user in blocked_list]
        if filtered_blocked:
            blocker_to_blocked[blocker] = filtered_blocked

    if blocker_to_blocked:
        output_lines = []
        for blocker, blocked_users in blocker_to_blocked.items():
            blocked_str = ", ".join(sorted(blocked_users))
            output_lines.append(f"{blocker} заблокировал(а): {blocked_str}")
        print("\n".join(output_lines))


@app.task
async def filter_blocked_users():
    count = 0
    async for message in processed_stream:
        blocked_users = prohibited_users[message.recipient_name]
        if message.sender_name in blocked_users:
            for _ in blocked_users_topic.send(
                value=BlockedUsers(
                    blocker=message.recipient_name,
                    blocked=[message.sender_name]
                )
            ):
                break
            table[message.recipient_name] = blocked_users
        count += 1
        if count % 1000 == 0:
            yield table[message.recipient_name]
