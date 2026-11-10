"""
LakeForge Solution 10: Kafka Streaming Producer Simulator
Author: Jayaram Pogakula (https://github.com/jayarampogakula)

Simulates continuous event streaming of credit card transactions:
- Regular legitimate purchases
- Sudden bursts of high-velocity card swipes from distinct geographic locations (Fraud Trigger)
"""

import json
import time

SAMPLE_EVENTS = [
    {"txn_id": "TXN-8001", "card_id": "CARD-9912", "amount": 45.0, "merchant": "Coffee Shop", "city": "New York", "event_time": "2026-11-01T10:00:00Z"},
    {"txn_id": "TXN-8002", "card_id": "CARD-4421", "amount": 120.0, "merchant": "Grocery Store", "city": "Dallas", "event_time": "2026-11-01T10:01:00Z"},
    {"txn_id": "TXN-8003", "card_id": "CARD-9912", "amount": 890.0, "merchant": "Electronics Store", "city": "New York", "event_time": "2026-11-01T10:02:30Z"},
    {"txn_id": "TXN-8004", "card_id": "CARD-9912", "amount": 2500.0, "merchant": "Luxury Jewelry", "city": "Paris", "event_time": "2026-11-01T10:04:00Z"},
    {"txn_id": "TXN-8005", "card_id": "CARD-9912", "amount": 1800.0, "merchant": "Electronics Store", "city": "London", "event_time": "2026-11-01T10:05:15Z"},
    {"txn_id": "TXN-8006", "card_id": "CARD-5510", "amount": 35.0, "merchant": "Gas Station", "city": "Seattle", "event_time": "2026-11-01T10:06:00Z"}
]

def simulate_streaming_producer(output_path="dataset/streaming_transactions_stream.json"):
    with open(output_path, "w", encoding="utf-8") as f:
        for event in SAMPLE_EVENTS:
            f.write(json.dumps(event) + "\n")
    print(f"Emitted {len(SAMPLE_EVENTS)} streaming transaction events to {output_path}")

if __name__ == "__main__":
    simulate_streaming_producer()
