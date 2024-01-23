from kafka import KafkaProducer
import json

# Tentukan alamat broker Kafka
bootstrap_servers = 'localhost:9092'  # Ganti dengan alamat broker Kafka Anda

# Buat objek KafkaProducer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Tentukan nama topik Kafka
topic_name = 'coba'

# Contoh data JSON
json_data = {'key1': 'value1', 'key2': 'value2', 'key3': 'value3'}

# Kirim pesan JSON ke topik Kafka
producer.send(topic_name, json_data)

# Tunggu agar pesan terkirim
producer.flush()

# Tutup koneksi ke produsen Kafka
producer.close()
