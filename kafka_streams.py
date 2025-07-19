import re
import faust

prohibited_users = {
    "clown": ["spammer"],
    "spammer": ["dodik", "payaso"],
    "dodik": ["clown"],
    "payaso": ["clown", "spammer"]
}

bad_words_regexp = r"\b(spam\w*|skam\w*|windows\w*)\b"
re_pattern = re.compile(bad_words_regexp, re.S)


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
    changelog_topic=app.topic(
        "blocked-users-changelog",
        value_type=list,
        partitions=2
    )
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


def log_blocked(data: tuple) -> None:
    blocker, blocked_users = data
    print(f'{blocker.upper()} заблокировал {", ".join(blocked_users)}')


def lower_str_input(value: Messages) -> Messages:
    value.sender_name = value.sender_name.lower()
    value.recipient_name = value.recipient_name.lower()
    value.content = value.content.lower()
    return value


def mask_bad_words(value: Messages) -> Messages:
    value.content = re_pattern.sub("***", value.content)
    return value


@app.agent(blocked_users_topic, sink=[log_blocked])
async def filter_blocked_users(stream):
    async for user in stream:
        if user.blocker not in table:
            table[user.blocker] = []
        blocked_users = [blocked for blocked in user.blocked
                         if blocked not in table[user.blocker]]
        if blocked_users:
            updated_blocker = table[user.blocker] + blocked_users
            table[user.blocker] = updated_blocker
            yield (user.blocker, updated_blocker)


@app.task
async def filter_messages():
    processed_stream = app.stream(
        messages_topic,
        processors=[lower_str_input, mask_bad_words]
    )
    async for message in processed_stream:
        print(table.keys())
        if message.sender_name not in table:
            await filter_blocked_users.send(value=message)
