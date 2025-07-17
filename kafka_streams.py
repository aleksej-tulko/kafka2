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

# Table для хранения блокировок
table = app.Table(
    "blocked-users",
    partitions=1,
    default=list,
)

# Топики
messages_topic = app.topic('messages', key_type=str, value_type=Messages)
filtered_messages_topic = app.topic('filtered_messages', key_type=str, value_type=Messages)
blocked_users_topic = app.topic('blocked_users', key_type=str, value_type=BlockedUsers)

# Функция-процессор
def capitalize_names(msg: Messages) -> Messages | None:
    if msg.sender_name:
        msg.sender_name = msg.sender_name.upper()
    if msg.recipient_name:
        msg.recipient_name = msg.recipient_name.upper()
    return msg

@app.task
async def process_messages():
    stream = app.stream(messages_topic, processors=[capitalize_names])
    async for message in stream:
        sender = message.sender_name.lower()
        recipient = message.recipient_name.lower()

        # Проверка по prohibited_users
        if recipient in prohibited_users and sender in prohibited_users[recipient]:
            # Отправляем в топик
            await blocked_users_topic.send(
                key=recipient,
                value=BlockedUsers(blocker=recipient, blocked=[sender])
            )
            # Обновляем table
            if sender not in table[recipient]:
                table[recipient].append(sender)

        else:
            # Пропускаем дальше, если не заблокирован
            await filtered_messages_topic.send(key=str(message.recipient_id), value=message)


# Отдельный хелпер — просто для принта
def output_blocked_users_from_db(blocked_data: dict[str, list[str]]) -> None:
    for blocker, blocked_list in blocked_data.items():
        if blocked_list:
            blocked_str = ", ".join(sorted(blocked_list))
            print(f"{blocker} заблокировал(а): {blocked_str}")