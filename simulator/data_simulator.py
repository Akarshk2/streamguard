"""
StreamGuard — Data Simulator
Generates realistic e-commerce event streams across three Kafka topics.
Produces Orders, Clickstream, and Inventory events with configurable fault injection.

Usage:
    python simulator/data_simulator.py
    python simulator/data_simulator.py --fault-rate 0.1 --rate 2.0
"""

import argparse
import json
import logging
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timedelta

from kafka import KafkaProducer
from fault_injector import FaultInjector

# ─── LOGGING ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("streamguard.simulator")

# ─── CONSTANTS ────────────────────────────────────────────────────────────────
PRODUCTS = [f"PROD_{i:03d}" for i in range(1, 201)]
CUSTOMERS = [f"CUST_{i:04d}" for i in range(1000, 5000)]
SKUS = [f"SKU_{i:04d}" for i in range(1000, 2000)]
WAREHOUSES = [f"WH_{i:02d}" for i in range(1, 11)]
REGIONS = ["IN", "US", "EU", "SG", "AU", "AE"]
PAGES = ["/home", "/product", "/cart", "/checkout", "/profile", "/search", "/wishlist"]
ACTIONS = ["view", "click", "scroll", "add_to_cart", "remove_from_cart", "purchase"]
DEVICES = ["mobile", "desktop", "tablet"]
STATUSES = ["placed", "processing", "shipped", "delivered", "cancelled", "returned"]

stats = {"orders": 0, "clicks": 0, "inventory": 0, "errors": 0}
running = True


def signal_handler(sig, frame):
    global running
    logger.info("Shutting down simulator gracefully...")
    running = False


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


# ─── EVENT GENERATORS ─────────────────────────────────────────────────────────

def generate_order() -> dict:
    order_time = datetime.utcnow() - timedelta(seconds=random.randint(0, 300))
    return {
        "order_id": str(uuid.uuid4()),
        "customer_id": random.choice(CUSTOMERS),
        "product_id": random.choice(PRODUCTS),
        "quantity": random.randint(1, 10),
        "amount": round(random.uniform(10.0, 8000.0), 2),
        "currency": "INR" if random.random() < 0.6 else random.choice(["USD", "EUR", "SGD"]),
        "status": random.choice(STATUSES),
        "payment_method": random.choice(["upi", "card", "netbanking", "wallet", "cod"]),
        "region": random.choice(REGIONS),
        "event_time": order_time.isoformat(),
        "created_at": datetime.utcnow().isoformat(),
    }


def generate_click() -> dict:
    return {
        "session_id": str(uuid.uuid4()),
        "user_id": random.choice(CUSTOMERS),
        "page": random.choice(PAGES),
        "action": random.choice(ACTIONS),
        "device": random.choice(DEVICES),
        "browser": random.choice(["chrome", "safari", "firefox", "edge"]),
        "referrer": random.choice(["google", "direct", "email", "social", "paid_ad"]),
        "duration_ms": random.randint(100, 30000),
        "region": random.choice(REGIONS),
        "event_time": datetime.utcnow().isoformat(),
    }


def generate_inventory() -> dict:
    stock = random.randint(0, 10000)
    return {
        "sku": random.choice(SKUS),
        "warehouse_id": random.choice(WAREHOUSES),
        "stock_level": stock,
        "reserved_qty": random.randint(0, min(stock, 200)),
        "restock_flag": stock < 50,
        "restock_qty": random.randint(500, 5000) if stock < 50 else 0,
        "unit_cost": round(random.uniform(5.0, 2000.0), 2),
        "last_movement": random.choice(["sale", "restock", "adjustment", "return"]),
        "event_time": datetime.utcnow().isoformat(),
    }


# ─── PRODUCER ─────────────────────────────────────────────────────────────────

def create_producer(bootstrap_servers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        acks="all",
        retries=3,
        linger_ms=5,
        batch_size=16384,
    )


def send_event(producer: KafkaProducer, topic: str, record: dict):
    try:
        producer.send(topic, value=record)
        return True
    except Exception as e:
        logger.error(f"Failed to send to {topic}: {e}")
        stats["errors"] += 1
        return False


# ─── MAIN LOOP ────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="StreamGuard Data Simulator")
    parser.add_argument("--bootstrap", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--fault-rate", type=float, default=0.08, help="Fraction of faulty records (0.0-1.0)")
    parser.add_argument("--rate", type=float, default=1.0, help="Events per second (orders). Clicks are 4x, inventory 0.4x")
    parser.add_argument("--log-every", type=int, default=50, help="Log stats every N order batches")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("  StreamGuard Data Simulator Starting")
    logger.info(f"  Bootstrap: {args.bootstrap}")
    logger.info(f"  Fault Rate: {args.fault_rate * 100:.1f}%")
    logger.info(f"  Order Rate: {args.rate:.1f}/sec")
    logger.info("=" * 60)

    injector = FaultInjector(fault_rate=args.fault_rate)
    producer = create_producer(args.bootstrap)
    sleep_time = 1.0 / args.rate
    batch_num = 0

    while running:
        batch_num += 1

        # Send 1 order
        order = injector.maybe_inject(generate_order(), "order")
        if send_event(producer, "orders_topic", order):
            stats["orders"] += 1

        # Send 4 click events per order (realistic ratio)
        for _ in range(4):
            click = injector.maybe_inject(generate_click(), "click")
            if send_event(producer, "clickstream_topic", click):
                stats["clicks"] += 1

        # Send inventory update ~40% of the time
        if random.random() < 0.40:
            inv = injector.maybe_inject(generate_inventory(), "inventory")
            if send_event(producer, "inventory_topic", inv):
                stats["inventory"] += 1

        # Periodic stats logging
        if batch_num % args.log_every == 0:
            producer.flush()
            logger.info(
                f"Stats | Orders: {stats['orders']} | "
                f"Clicks: {stats['clicks']} | "
                f"Inventory: {stats['inventory']} | "
                f"Errors: {stats['errors']} | "
                f"Fault distribution: {injector.stats()}"
            )

        time.sleep(sleep_time)

    # Cleanup
    producer.flush()
    producer.close()
    logger.info(f"Simulator stopped. Final stats: {stats}")


if __name__ == "__main__":
    main()
