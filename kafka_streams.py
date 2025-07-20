import logging
import re
import sys
from datetime import timedelta

import faust
from dotenv import load_dotenv

load_dotenv()

COUNTER_INTERVAL = 30
WINDOW_RANGE = 60
TIMER_INTERVAL = 59

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

bad_words_regexp = r"\b(spam\w*|skam\w*|windows\w*)\b"
re_pattern = re.compile(bad_words_regexp, re.S)


class LoggerMsg:
    """Сообщения для логгирования."""

    BLOCK_RECORD = ('Получатель {blocker} заблокировал '
                    'отправителей {blocked_users}.')


msg = LoggerMsg


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

app.conf.table_cleanup_interval = 1.0

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

messages_frequency_table = app.Table(
    "messages_frequency",
    partitions=2,
    default=int
).hopping(
    WINDOW_RANGE,
    COUNTER_INTERVAL,
    expires=timedelta(hours=1),
    key_index=True,
).relative_to_now()


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
    logger.info(
        msg=msg.BLOCK_RECORD.format(
            blocker=blocker, blocked_users=blocked_users
        )
    )


def lower_str_input(value: Messages) -> Messages:
    value.sender_name = value.sender_name.lower()
    value.recipient_name = value.recipient_name.lower()
    value.content = value.content.lower()
    return value


def mask_bad_words(value: Messages) -> Messages:
    value.content = re_pattern.sub("***", value.content)
    return value


# @app.agent(blocked_users_topic, sink=[log_blocked])
# async def filter_blocked_users(stream):
#     async for user in stream:
#         table[user.blocker] = [blocked for blocked in user.blocked]
#         yield (user.blocker, table[user.blocker])


@app.agent(messages_topic)
async def count_frequency(stream):
    async for message in stream:
        messages_frequency_table[message.sender_name] += 1


@app.task
async def filter_messages():
    processed_stream = app.stream(
        messages_topic,
        processors=[lower_str_input, mask_bad_words]
    )
    async for message in processed_stream:
        if message.sender_name not in table[message.recipient_name]:
            await filtered_messages_topic.send(value=message)


@app.timer(interval=10.0)
async def get_data():
    keys = ["user1", "user2", "user3"]  # или динамически из базы
    for key in keys:
        value = messages_frequency_table[key].now()
        print(f"{key}: {value}")