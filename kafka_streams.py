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
    partitions=2,
    default=list
)

messages_topic = app.topic('messages', key_type=str, value_type=Messages)
blocked_users_topic = app.topic('blocked_users', key_type=str, value_type=BlockedUsers)

# Processor 1: фильтрует только заблокированных отправителей
async def filter_blocked_only(msg: Messages) -> Messages | None:
    blocked = prohibited_users.get(msg.recipient_name, [])
    if msg.sender_name in blocked:
        return msg
    return None

# Processor 2: метит сообщение, просто как пример обработки
async def mark_blocked_flag(msg: Messages) -> Messages:
    msg.content = f"[BLOCKED] {msg.content}"
    return msg

# Основной обработчик — только здесь побочные эффекты
@app.timer(interval=2.0)
async def process_stream():
    async for message in app.stream(
        messages_topic,
        processors=[filter_blocked_only, mark_blocked_flag]
    ):
        await blocked_users_topic.send(
            value=BlockedUsers(
                blocker=message.recipient_name,
                blocked=[message.sender_name]
            )
        )
        table[message.recipient_name] = prohibited_users[message.recipient_name]
        print(f"{message.recipient_name} заблокировал(а) {message.sender_name}")