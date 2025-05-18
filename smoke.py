#!/usr/bin/env python3
from kafka import KafkaConsumer
import json, sys

consumer = KafkaConsumer(
    "pg.public.transactions",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    key_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None,
    consumer_timeout_ms=10000,
)

for msg in consumer:
    print(f"offset={msg.offset} key={msg.key} value={msg.value}")
print("✅ Done – stream exhausted.")
