from kafka.admin import KafkaAdminClient, NewTopic

# Tentukan alamat broker Kafka
bootstrap_servers = 'localhost:9092'  # Ganti dengan alamat broker Kafka Anda

# Buat objek KafkaAdminClient
admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)

# Tentukan nama topik yang akan dihapus dan dibuat
topic_name = 'coba'

# Coba hapus topik (jika sudah ada)
try:
    admin_client.delete_topics(topics=[topic_name])
    print(f"Topik '{topic_name}' berhasil dihapus.")
except Exception as e:
    print(f"Error saat menghapus topik: {e}")

# # Buat objek NewTopic untuk topik baru
# new_topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)

# # Buat topik baru
# admin_client.create_topics(new_topics=[new_topic], validate_only=False)
# print(f"Topik '{topic_name}' berhasil dibuat.")

# Tutup koneksi ke admin client
admin_client.close()
