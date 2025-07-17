import re

import faust

prohibited_users = {
    "clown": ["spammer"],
    "spammer": ["dodik", "payaso"],
    "dodik": ["clown"],
    "payaso": ["clown", "spammer"]
}

bad_words_pattern =  r"\b(spam\w*|skam\w*|windows\w*)\b"
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
    "blocked-users",
    partitions=2,
    default=list
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


def output_blocked_users_from_db(blocked):
    blocker_to_blocked = {}

    for blocker, blocked_list in prohibited_users.items():
        filtered_blocked = [user for user in blocked if user in blocked_list]
        if filtered_blocked:
            blocker_to_blocked[blocker] = filtered_blocked

    if blocker_to_blocked:
        output_lines = []
        for blocker, blocked_users in blocker_to_blocked.items():
            blocked_str = ", ".join(set(blocked_users))
            output_lines.append(f"{blocker} заблокировал(а): {blocked_str}")
        print("\n".join(output_lines))


@app.agent(messages_topic)
async def filter_blocked_users(stream):
    async for message in stream:
        blocked_users = prohibited_users[message.recipient_name]
        if message.sender_name in blocked_users:
            await blocked_users_topic.send(
                value=BlockedUsers(
                    blocker=message.recipient_name,
                    blocked=[message.sender_name]
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
    count = 0
    async for message in stream:
        if table[message.blocker]:
            current_blocked = table[message.blocker] or []
            new_blocked = current_blocked + message.blocked
            table[message.blocker] = new_blocked
        else:
            table[message.blocker] = message.blocked
        count += 1
        if count % 1000 == 0:
            yield table
