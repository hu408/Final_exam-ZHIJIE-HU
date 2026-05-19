import argparse
import json
import time
import random
from kafka import KafkaProducer

# 👉 完全匹配你的 3 节点 Kafka 集群
BOOTSTRAP_SERVERS = "localhost:9092,localhost:9094,localhost:9096"
TOPIC = "sensor-events"

RANGES = {
    "temperature": (15.0, 45.0, "C"),
    "humidity": (30.0, 95.0, "%"),
    "pressure": (980.0, 1040.0, "hPa")
}

def make_value(sensor):
    low, high, unit = RANGES[sensor]
    anomaly = random.random() < 0.1
    if anomaly:
        if sensor == "temperature":
            return random.choice([random.uniform(-10, 14), random.uniform(46, 60)]), True
        elif sensor == "humidity":
            return random.choice([random.uniform(0, 29), random.uniform(96, 100)]), True
        else:
            return random.choice([random.uniform(900, 979), random.uniform(1041, 1100)]), True
    else:
        return random.uniform(low, high), False

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--rate", type=int, default=10)
    parser.add_argument("--source", type=str, default="site-B-rack-03")
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS.split(","),
        acks="all",
        retries=5,
        linger_ms=5,
        batch_size=16384,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    sensors = list(RANGES.keys())
    delay = 1.0 / args.rate

    print(f"✅ Sending to Kafka cluster: {BOOTSTRAP_SERVERS}")
    print(f"✅ Topic: {TOPIC}")

    for _ in range(args.count):
        sensor = random.choice(sensors)
        value, anomaly = make_value(sensor)
        msg = {
            "sensor": sensor,
            "value": round(value, 2),
            "unit": RANGES[sensor][2],
            "timestamp": int(time.time() * 1000),
            "source": args.source,
            "anomaly": anomaly
        }
        producer.send(TOPIC, value=msg)
        time.sleep(delay)

    producer.flush()
    producer.close()
    print(f"\n✅ Sent {args.count} events successfully!")

if __name__ == "__main__":
    main()