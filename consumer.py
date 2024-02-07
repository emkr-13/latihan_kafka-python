from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'twitter',
    bootstrap_servers='10.11.13.81:9092',
    auto_offset_reset='earliest'
)

for msg in consumer:
    print("Message Consumer")
    print(json.loads(msg.value.decode('utf-8')))

consumer.close()