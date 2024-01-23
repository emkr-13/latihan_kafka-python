from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'coba',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest'
)

for msg in consumer:
    print("Message Consumer")
    print(msg.value)

consumer.close()