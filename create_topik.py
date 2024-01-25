from kafka import KafkaAdminClient
from kafka.admin import NewTopic

# Tentukan alamat broker Kafka
bootstrap_servers = '10.11.13.81:9092'  # Ganti dengan alamat broker Kafka Anda

# Buat objek KafkaAdminClient
admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)

# Tentukan nama topik baru dan konfigurasi jika diperlukan
new_topic_name = 'online_news'
new_topic = NewTopic(name=new_topic_name, num_partitions=1, replication_factor=1)

# Buat topik baru menggunakan KafkaAdminClient
admin_client.create_topics(new_topics=[new_topic])

# Cetak pesan bahwa topik telah berhasil dibuat
print(f"Topik '{new_topic_name}' berhasil dibuat.")

# Tutup koneksi ke admin client
admin_client.close()
