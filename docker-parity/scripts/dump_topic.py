#!/usr/bin/env python3
"""Dump all messages of the parity Kafka topics to a JSON file.

Usage: dump_topic.py --out dump.json [--bootstrap localhost:9092]
Each entry: {"topic", "partition", "offset", "key": hex, "value": hex}
"""
import argparse
import json
from kafka import KafkaConsumer

TOPICS = ["warp10-data", "warp10-meta"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--bootstrap", default="localhost:9092")
    args = ap.parse_args()

    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=args.bootstrap,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=8000,
        value_deserializer=lambda b: b,
        key_deserializer=lambda b: b,
    )
    out = []
    for tp, msgs in consumer.poll(timeout_ms=8000).items():
        for m in msgs:
            out.append(
                {
                    "topic": tp.topic,
                    "partition": tp.partition,
                    "offset": m.offset,
                    "key": (m.key or b"").hex(),
                    "value": (m.value or b"").hex(),
                }
            )
    out.sort(key=lambda e: (e["topic"], e["partition"], e["offset"]))
    with open(args.out, "w") as f:
        json.dump(out, f, indent=1)
    print(f"dumped {len(out)} messages -> {args.out}")
    for t in TOPICS:
        n = sum(1 for e in out if e["topic"] == t)
        print(f"  {t}: {n}")


if __name__ == "__main__":
    main()
