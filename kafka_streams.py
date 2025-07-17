import re
import faust

# --- Константы ---
BAD_WORDS_PATTERN = r"\b(spam\w*|skam\w*|windows\w*)\b"
regex = re.compile(BAD_WORDS_PATTERN, flags=re.IGNORECASE)

# --- Faust модели ---
class BlockedUsers(faust.Record, serializer='json'):
    blocker: str
    blocked: list[str]

class Message(faust.Record, serializer='json'):
    sender_id: int
    sender_name: str
    recipient_id: int
    recipient_name: str
    amount: float
    content: str

# --- Faust приложение ---
app = faust.App(
    "filtering-service",
    broker="kafka://localhost:9093,localhost:9095,localhost:9097",
    store="rocksdb://",
)

# --- Топики ---
messages_topic = app.topic('messages', value_type=Message)
filtered_messages_topic = app.topic('filtered_messages', value_type=Message)
blocked_users_topic = app.topic('blocked_users', value_type=BlockedUsers)

# --- Таблица блокировок ---
blocked_table = app.Table(
    "blocked-users-table",
    default=list,
    partitions=2,
    changelog_topic=app.topic("blocked-users-changelog", value_type=BlockedUsers, partitions=2),
)

# --- Агент для обновления таблицы блокировок ---
@app.agent(blocked_users_topic)
async def update_blocked_table(stream):
    async for record in stream:
        current = set(blocked_table[record.blocker])
        updated = current.union(set(record.blocked))
        blocked_table[record.blocker] = list(updated)
        print(f"[BLOCKED_TABLE_UPDATE] {record.blocker} → {blocked_table[record.blocker]}")

# --- Агент для фильтрации сообщений ---
@app.agent(messages_topic)
async def filter_messages(stream):
    async for message in stream:
        # Фильтрация по запрещённым словам
        if regex.search(message.content):
            print(f"[CENSORED] Сообщение от {message.sender_name} содержит запрещённые слова")
            continue

        # Проверка блокировок
        blocked_list = blocked_table.get(message.recipient_name, [])
        if message.sender_name in blocked_list:
            print(f"[BLOCKED] {message.sender_name} заблокирован {message.recipient_name}, сообщение отклонено")
            continue

        # Отправка в filtered_messages_topic
        print(f"[PASS] {message.sender_name} → {message.recipient_name}: '{message.content}'")
        await filtered_messages_topic.send(value=message)