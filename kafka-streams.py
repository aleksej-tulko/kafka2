import faust

app = faust.App(
    "simple-faust-app",
    broker="localhost:9092",
    value_serializer="raw",
)

input_topic = app.topic("input-topic", key_type=str, value_type=str)

output_topic = app.topic("output-topic", key_type=str, value_type=str)

@app.agent(input_topic)
async def process(stream):
    async for value in stream:
        processed_value = f"Processed: {value}"
        await output_topic.send(value=processed_value)