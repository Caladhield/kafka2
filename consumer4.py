from kafka import KafkaConsumer
import json

bootstrap_servers = ['localhost:9092']
topic_name = 'onlineshop'

consumer = KafkaConsumer(topic_name,
                         bootstrap_servers=bootstrap_servers,
                         auto_offset_reset='earliest',
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

initial_stock = 500
product_stock = {f'P{num}': initial_stock for num in range(101, 201)}

def update_stock():
    updated_products = set()

    for message in consumer:
        order = message.value
        product_id = order['product_id']
        quantity_sold = order['quantity']

        if product_id not in product_stock:
            print(f"Product {product_id} stock added.")
            product_stock[product_id] = initial_stock

        new_stock_level = max(product_stock[product_id] - quantity_sold, 0)
        product_stock[product_id] = new_stock_level

        if product_id not in updated_products:
            print(f"Updated {product_id} stock: {new_stock_level}")
            updated_products.add(product_id)


if __name__ == '__main__':
    update_stock()
