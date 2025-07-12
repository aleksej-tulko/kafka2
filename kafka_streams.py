import faust

app = faust.App(
    "simple-faust-app",
    broker="localhost:9093",
    value_serializer="raw",
)

input_topic = app.topic("pract-task2", key_type=str, value_type=str)

output_topic = app.topic("output-topic", key_type=str, value_type=str)

@app.agent(input_topic)
async def process(stream):
    async for value in stream:
        processed_value = f"Processed: {value}"
        await output_topic.send(value=processed_value)