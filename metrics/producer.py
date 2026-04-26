"""
Producer — reads challenge_set.json and publishes to Kafka topic: playlist-raw
"""
import json
import os
import time
from kafka import KafkaProducer

DATA_FILE = os.getenv("DATA_FILE", "./data/challenge_set.json")
KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "playlist-raw"

print(f"Loading data from {DATA_FILE} ...")
with open(DATA_FILE, "r") as f:
    data = json.load(f)

playlists = data["playlists"]
print(f"Loaded {len(playlists)} playlists")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    linger_ms=10,
    batch_size=32768,
    compression_type="gzip",
)

t0 = time.time()
for i, playlist in enumerate(playlists, 1):
    producer.send(TOPIC, value=playlist)
    if i % 1000 == 0:
        elapsed = time.time() - t0
        print(f"  Sent {i}/{len(playlists)} | {i/elapsed:.0f} msg/s")

producer.flush()
producer.close()

elapsed = time.time() - t0
print(f"Done. Sent {len(playlists)} messages to '{TOPIC}' in {elapsed:.1f}s")
