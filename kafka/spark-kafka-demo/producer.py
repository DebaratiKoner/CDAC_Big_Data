import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

categories = {
    "Electronics": (500, 5000),
    "Clothing": (200, 2000),
    "Grocery": (50, 800),
    "Books": (100, 1000)
}

cities = [
    "Bengaluru",
    "Mumbai",
    "Delhi",
    "Chennai",
    "Hyderabad"
]

order_id = 1

while True:

    category = random.choice(list(categories.keys()))

    low, high = categories[category]

    event = {
        "order_id": order_id,
        "category": category,
        "city": random.choice(cities),
        "amount": round(random.uniform(low, high), 2),
        "event_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    producer.send("orders", event)

    print("Sent:", event)

    order_id += 1

    time.sleep(0.5)
