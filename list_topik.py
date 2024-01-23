from kafka import KafkaAdminClient

# Tentukan alamat broker Kafka
bootstrap_servers = 'localhost:9092'  # Ganti dengan alamat broker Kafka Anda

# Buat objek KafkaAdminClient
admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)

# Dapatkan list topik
topics = admin_client.list_topics()

# Cetak list topik
print("List Topik Kafka:")
for topic in topics:
    print(topic)

# Tutup koneksi ke admin client
admin_client.close()
