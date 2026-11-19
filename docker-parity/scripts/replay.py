#!/usr/bin/env python3
"""Replay messages captured from the rust ingress mock producer into the real
Kafka topics (same topic/key/value bytes)."""
import argparse
import json
from kafka import KafkaProducer


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--bootstrap", default="localhost:9092")
    args = ap.parse_args()
    msgs = json.load(open(args.inp))["messages"]
    p = KafkaProducer(bootstrap_servers=args.bootstrap)
    for m in msgs:
        p.send(m["topic"], key=bytes.fromhex(m["key"]), value=bytes.fromhex(m["value"]))
    p.flush()
    print(f"replayed {len(msgs)} messages to Kafka")


if __name__ == "__main__":
    main()
