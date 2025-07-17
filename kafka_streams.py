import re
import faust
from collections import defaultdict

prohibited_users = {
    "clown": ["spammer"],
    "spammer": ["dodik", "payaso"],
    "dodik": ["clown"],
    "payaso": ["clown", "spammer"]
}

bad_words_pattern = r"\b(spam\w*|skam\w*|windows\w*)\b"
regex = re.compile(bad_words_pattern, flags=re.IGNORECASE)


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
    "blocked-users-table",
    partitions=2,
    default=list,
    persist=True
)

messages_topic = app.topic(
    'messages',
    key_type=str,
    value_type=Messages
)
filtered_messages_topic = app.topic(
    'filtered_messages',
    key_type=str,
    value_type=Messages
)

blocked_users_topic = app.topic(
    'blocked_users',
    key_type=str,
    value_type=BlockedUsers
)

current_blocked_map = defaultdict(set)


def output_blocked_users_from_db(table_items):
    for blocker, blocked_list in table_items:
        blocked_str = ", ".join(blocked_list)
        print(f"{blocker} заблокировал(а): {blocked_str}")


@app.agent(messages_topic)
async def filter_blocked_users(stream):
    async for message in stream:
        blocked_users = prohibited_users.get(message.recipient_name, [])
        if message.sender_name in blocked_users:
            blocker = message.recipient_name
            sender = message.sender_name
            current_blocked_map[blocker].add(sender)
            await blocked_users_topic.send(
                key=blocker,
                value=BlockedUsers(
                    blocker=blocker,
                    blocked=list(current_blocked_map[blocker])
                )
            )


@app.agent(messages_topic)
async def filter_messages(stream):
    async for message in stream.filter(
        lambda content: not regex.search(content.content)
    ):
        await filtered_messages_topic.send(
            value=message
        )


@app.agent(blocked_users_topic, sink=[output_blocked_users_from_db])
async def save_blocked_to_db(stream):
    async for message in stream:
        current_blocked = table[message.blocker] or []
        new_blocked = list(set(current_blocked + message.blocked))
        table[message.blocker] = new_blocked
        yield table.items()
