from kafka import KafkaConsumer
import json
from datetime import datetime, timezone

bootstrap_servers = ['localhost:9092']
topic_name = 'onlineshop'

consumer = KafkaConsumer(topic_name,
                         bootstrap_servers=bootstrap_servers,
                         auto_offset_reset='earliest',
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                         consumer_timeout_ms=10000)


product_info = {f'P{num}': {'stock': 500, 'sales_count': 0} for num in range(101, 201)}

def daily_report():
    for message in consumer:
        order = message.value
        product_id = order['product_id']
        quantity_sold = order['quantity']

        product_info[product_id]['sales_count'] += quantity_sold
        product_info[product_id]['stock'] = max(product_info[product_id]['stock'] - quantity_sold, 0)

    report_filename = f"Daily_Report_{datetime.now().strftime('%Y-%m-%d')}.txt"
    with open(report_filename, 'w') as report_file:
        report_file.write(f"Daily Sales Report for {datetime.now().strftime('%Y-%m-%d')}\n")
        for product_id, info in product_info.items():
            report_file.write(f"{product_id}: Sold {info['sales_count']} units, Remaining Stock: {info['stock']}\n")

    print(f"Report generated: {report_filename}")

if __name__ == '__main__':
    daily_report()
