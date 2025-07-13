import faust

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
)

messages_topic = app.topic(
    'messages', key_type=str, value_type=Messages
)

@app.agent(messages_topic)
async def filter_blocked_users(stream):
    async for message in stream:
        raw_value = message._raw_value  # байты сообщения
        
        # Обрезаем первые 4 байта (примерно, подогнать можно)
        json_bytes = raw_value[4:]  
        json_str = json_bytes.decode('utf-8')
        print("Raw JSON:", json_str)
        
        # Попробуем вручную распарсить
        import json
        try:
            data = json.loads(json_str)
            print("Parsed data:", data)
        except Exception as e:
            print("Ошибка парсинга:", e)