import json
from kafka import KafkaConsumer

def main():
    #topic = "news.raw"  # change to news.cleaned or news.predictions as needed
    topic = "watchlist.events"


    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=["localhost:9092"],
        group_id="demo-consumer",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        consumer_timeout_ms=15000,  # stop after 15s with no messages
    )

    print(f"Consuming from topic: {topic}")
    for msg in consumer:
        print(f"← offset={msg.offset} key={msg.key} value={msg.value}")

    print("No more messages. Exiting.")

if __name__ == "__main__":
    main()
