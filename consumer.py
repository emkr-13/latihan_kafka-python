from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'online_news',
    bootstrap_servers='10.11.13.81:9092',
    auto_offset_reset='earliest'
)

for msg in consumer:
    print("Message Consumer")
    print(msg.value)

consumer.close()