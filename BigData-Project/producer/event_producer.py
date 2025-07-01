import json, random, time, uuid, datetime
from kafka import KafkaProducer
from pymongo import MongoClient

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=["kafka:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# MongoDB Client
client = MongoClient("mongodb://mongodb:27017/")
db = client["bigdata"]
collection = db["events"]

EVENT_TYPES = ["page_view", "click", "add_to_cart", "purchase"]

def fake_event():
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": random.randint(1, 5000),
        "event_type": random.choice(EVENT_TYPES),
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        "device": random.choice(["web", "ios", "android"]),
    }

while True:
    event = fake_event()
    producer.send("user_events", event)
    collection.insert_one(event)
    print("sent and saved:", event)
    time.sleep(0.5)
