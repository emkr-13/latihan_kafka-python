from kafka import KafkaAdminClient
import time

bootstrap_servers = 'localhost:9092'
max_retries = 3
retry_delay = 5  # seconds

for attempt in range(1, max_retries + 1):
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
        topics = admin_client.list_topics()
        print("List Topik Kafka:")
        for topic in topics:
            print(topic)
        admin_client.close()
        break  # Successful connection, exit loop
    except Exception as e:
        print(f"Attempt {attempt} failed: {e}")
        if attempt < max_retries:
            print(f"Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)
        else:
            print("Max retries reached. Exiting.")
            break
