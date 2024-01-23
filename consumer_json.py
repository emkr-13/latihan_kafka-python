import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'coba',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=False  # Menonaktifkan commit otomatis
)

try:
    for msg in consumer:
        print("Message Consumer")
        
        # Mengganti tanda kutip satu dengan tanda kutip ganda
        json_data_str = msg.value.decode('utf-8').replace("'", '"')
        
        try:
            # Mengurai nilai JSON dari pesan
            json_data = json.loads(json_data_str)
            
            # Mengambil nilai dari kunci 'title', 'date', dan 'content'
            title = json_data['title']
            date = json_data['date']
            content = json_data['content']
            
            # Menampilkan nilai
            print(f"Title: {title}")
            print(f"Date: {date}")
            print(f"Content: {content}")
        except json.decoder.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")
        
        # Commit offset secara manual
        consumer.commit()

        # Untuk contoh ini, keluar dari loop setelah mendapatkan satu pesan saja
        break

finally:
    # Tutup koneksi ke konsumen setelah selesai
    consumer.close()
