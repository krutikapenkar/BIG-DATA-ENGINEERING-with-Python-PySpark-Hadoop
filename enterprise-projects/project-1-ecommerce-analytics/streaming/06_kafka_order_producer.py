"""
Simulates the "live orders" feed a real checkout service would publish to
Kafka. Standalone Python (no Spark needed to produce) - this is meant to
run in one terminal while 07_structured_streaming_to_delta.py runs in
another, so the class can watch rows land in the Delta table in near
real-time as this script fires events.

Prerequisite: `docker compose up -d` from the project root (starts
Zookeeper + a single-broker Kafka on localhost:9092).

Run with: python 06_kafka_order_producer.py [--rate 2] [--count 200]
"""

import sys
import os
import json
import time
import random
import argparse
import uuid

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_ORDERS_TOPIC

PAYMENT_METHODS = ["UPI", "Credit Card", "Debit Card", "Net Banking", "Cash on Delivery"]


def make_order_event():
    payment_success = random.random() > 0.10  # same 10% failure rate as the batch generator
    return {
        "order_id": f"LIVE{uuid.uuid4().hex[:12].upper()}",
        "user_id": f"USR{random.randint(1, 200_000):08d}",
        "product_id": f"PROD{random.randint(1, 5_000):07d}",
        "quantity": random.randint(1, 4),
        "unit_price": round(random.uniform(99, 50000), 2),
        "payment_method": random.choice(PAYMENT_METHODS),
        "payment_status": "success" if payment_success else "failed",
        "order_timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type=float, default=2.0, help="events per second")
    parser.add_argument("--count", type=int, default=0, help="0 = run forever")
    args = parser.parse_args()

    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )

    print(f"Producing to '{KAFKA_ORDERS_TOPIC}' on {KAFKA_BOOTSTRAP_SERVERS} at {args.rate}/sec ...")
    sent = 0
    try:
        while args.count == 0 or sent < args.count:
            event = make_order_event()
            producer.send(KAFKA_ORDERS_TOPIC, key=event["order_id"], value=event)
            sent += 1
            print(f"  sent {sent}: {event['order_id']} {event['payment_status']} Rs.{event['unit_price']}")
            time.sleep(1.0 / args.rate)
    except KeyboardInterrupt:
        print("\nStopped by user.")
    finally:
        producer.flush()
        producer.close()
        print(f"Total events sent: {sent}")


if __name__ == "__main__":
    main()
