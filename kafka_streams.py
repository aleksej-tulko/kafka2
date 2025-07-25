import logging
import re
import sys
from datetime import datetime, timedelta

import faust

COUNTER_INTERVAL = 45
WINDOW_RANGE = 60
TIME_INTERVAL = 10
SUBST_STR = '***'


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
    ENOUGH_MSG = ('Отправитель {sender} '
                  'уже отправил {count} сообщений '
                  'в текущем окне.')


msg = LoggerMsg


class CountTimer(faust.Record):
    """Модель таймера с полями."""

    sender_name: str
    count: int
    dt_now: datetime


class BadWords(faust.Record):
    """Модель запрещенных слов с полями."""

    words: list[str]


class BlockedUsers(faust.Record):
    """Модель блокировки с полями."""

    blocker: str
    blocked: list[str]


class Messages(faust.Record):
    """Модель сообщений с полями."""

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

app.conf.consumer_auto_offset_reset = "earliest"

blocked_senders_table = app.Table( # Таблица, где постоянно хранятся списки заблокированных.
    "blocked-users-table",
    partitions=1,
    default=list
)

bad_words_table = app.Table( # Таблица, где постоянно хранится список запретных слов.
    "bad-words-table",
    partitions=1,
    default=list
)

messages_frequency_table = app.Table( # Таблица для отслеживания кол-во сообщений за время жизни окна.
    "messages_frequency",
    partitions=1,
    default=int
).hopping(
    WINDOW_RANGE,
    COUNTER_INTERVAL,
    expires=timedelta(minutes=TIME_INTERVAL),
    key_index=True,
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

bad_words_topic = app.topic(
    'bad_words',
    key_type=str,
    value_type=BadWords
)

timer_topic = app.topic( # Топик, куда дублируется счетчик из БД. Логи идут из него, чтобы не нагружать БД.
    'count_timer',
    key_type=str,
    value_type=CountTimer
)


def log_blocked(data: tuple) -> None: # Логирование блокировок
    blocker, blocked_users = data
    logger.info(
        msg=msg.BLOCK_RECORD.format(
            blocker=blocker, blocked_users=blocked_users
        )
    )


def log_msg_counter(counter: tuple) -> None: # Вывод кол-во сообщение за окно
    sender, count = counter
    if count == 1000: # Когда кол-во сообщений от отправителя достигает 1000, лог сообщает об этом
        logger.info(
            msg=msg.ENOUGH_MSG.format(
                sender=sender, count=count
            )
        )


def lower_str_input(value: Messages) -> Messages: # Перевод строк в нижний регистр
    value.sender_name = value.sender_name.lower()
    value.recipient_name = value.recipient_name.lower()
    value.content = value.content.lower()
    return value


async def mask_bad_words(value: Messages) -> Messages: # Замена запрещеных слов на ***
    if value.content in bad_words_table['words']:
        value.content = SUBST_STR  # Для работы со списком запрещенных слов
    value.content = re_pattern.sub('[CENSORED]', value.content) #Допфильтр: слова skam, spam, windows будут заменены на [CENSORED]
    return value


@app.agent(bad_words_topic) # Сохранение блокировок из топика в БД.
async def add_bad_words(stream):
    async for words in stream:
        for word in words.words:
            if word not in bad_words_table['words']:
                updated_bad_words_list = bad_words_table['words'] + words.words
                bad_words_table['words'] = updated_bad_words_list


@app.agent(blocked_users_topic, sink=[log_blocked]) # Сохранение блокировок из топика в БД.
async def filter_blocked_users(stream):
    async for user in stream:
        blocked_senders_table[user.blocker] = [blocked for blocked in user.blocked]
        yield (user.blocker, blocked_senders_table[user.blocker]) # Вызов логгера


@app.agent(messages_topic, sink=[log_msg_counter]) # Подсчет кол-ва сообщений от отправителей за время жизни окна.
async def count_frequency(stream):
    async for message in stream:
        messages_frequency_table[message.sender_name] += 1 # Запись в БД
        value = messages_frequency_table[message.sender_name] # Создание экземпляра из записи из БД
        now_value = value.now() or 0
        prev_value = value.delta(timedelta(seconds=WINDOW_RANGE)) or 0
        delta_change = now_value - prev_value
        await timer_topic.send( # Дублирование записи из БД в нужном формате в топик
            value=CountTimer(
                sender_name=message.sender_name,
                count=delta_change,
                dt_now=datetime.now()
            )
        )
        yield (message.sender_name, delta_change) # Вызов логгера


@app.agent(messages_topic)
async def filter_messages(stream): # Отправка в отстортированные сообщения
    processed_stream = app.stream(
        stream,
        processors=[lower_str_input, mask_bad_words] # Обработка
    )
    async for message in processed_stream:
        blocked_users = blocked_senders_table.get(message.recipient_name)
        if message.sender_name in blocked_users:
            continue
        await filtered_messages_topic.send(value=message)
