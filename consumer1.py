from kafka import KafkaConsumer
import json
from datetime import datetime, timezone

bootstrap_servers = ['localhost:9092']
topic_name = 'onlineshop'

consumer = KafkaConsumer(topic_name,
                         bootstrap_servers=bootstrap_servers,
                         auto_offset_reset='earliest',
                         value_deserializer=lambda x: deserialize(x))

def deserialize(message):
    """Safely deserialize Kafka message value to Python object."""
    if message is None or not message.strip():
        return None 
    try:
        return json.loads(message.decode('utf-8'))
    except json.JSONDecodeError:
        print("Error decoding JSON")
        return None

def count_orders():
    order_count = 0
    start_of_day = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

    while True:
        poll_data = consumer.poll(timeout_ms=1000)

        if not poll_data:
            break

        for tp, messages in poll_data.items():
            for message in messages:
                try:
                    order = message.value
                    if order is None:
                        continue

                    order_timestamp = datetime.fromtimestamp(order['timestamp'], tz=timezone.utc)

                    if order_timestamp < start_of_day:
                        continue

                    order_count += 1
                except Exception as e:
                    print(f"Error processing message: {e}")
                    continue

    print(f"Total orders since 00:00: {order_count}")



if __name__ == '__main__':
    count_orders()