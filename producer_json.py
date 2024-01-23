from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: str(v).encode('utf-8')  # Assuming the message is a string
)

topic_name = 'coba'

try:
    json_data = {
        'title': 'ini berita yang viral', 
        'date': '2024-01-01', 
        'content': 'apa yanng viral semua di bahas '
        }
    producer.send(topic_name, value=json_data)
    print(f"Produced message: {json_data}")

except Exception as e:
    print(f"Error producing message: {e}")

finally:
    producer.flush()
    producer.close()