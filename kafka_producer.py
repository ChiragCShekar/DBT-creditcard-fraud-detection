'''from kafka import KafkaProducer
import json, time, random

producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

cards = ['1111-2222-3333-4444', '5555-6666-7777-8888']
locations = ['NY', 'LA', 'TX', 'SF']

while True:
    transaction = {
        'card_id': random.choice(cards),
        'location': random.choice(locations),
        'amount': round(random.uniform(10.0, 500.0), 2),
        'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
    }
    print("Sending:", transaction)
    producer.send('card-transactions', transaction)
    time.sleep(1)
'''


from kafka import KafkaProducer
import json, time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Simulate duplicate usage of same card in different cities
test_data = [
    {'card_id': '1111-2222-3333-4444', 'location': 'NY', 'amount': 102.50},
    {'card_id': '1111-2222-3333-4444', 'location': 'LA', 'amount': 205.75},
    {'card_id': '5555-6666-7777-8888', 'location': 'TX', 'amount': 150.20},
    {'card_id': '5555-6666-7777-8888', 'location': 'TX', 'amount': 123.00},
    {'card_id': '5555-6666-7777-8888', 'location': 'SF', 'amount': 99.99},
]

print("🚀 Starting Kafka transaction simulation...")

while True:
    for tx in test_data:
        tx['timestamp'] = time.strftime('%Y-%m-%d %H:%M:%S')
        print(f"📤 Sending: {tx}")
        producer.send('card-transactions', tx)
        time.sleep(1)
