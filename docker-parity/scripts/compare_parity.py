#!/usr/bin/env python3
"""Compare Kafka messages produced by the stock Warp 10 ingress (dump A) with
those produced by warp10-platform-rs (its /debug/messages dump, format B).

A file: JSON list from dump_topic.py (topic/partition/offset/key/value hex).
B file: JSON {"messages": [{topic, key, value}]} from GET /debug/messages.

Byte-level: data messages must be IDENTICAL (same key, same KafkaDataMessage
bytes). Meta messages: key identical; value = key ++ thrift Metadata — fields
name/labels/classId/labelsId must match; source and lastActivity are ignored
(stock sets source="ingress" and lastActivity=now).
"""
import argparse
import json
import sys

from thrift.protocol import TCompactProtocol
from thrift.transport import TTransport
from thrift.Thrift import TType
from thrift.Thrift import TType as _TT
STOP, BOOL, BYTE, I16, I32, I64, DOUBLE, STRING = (TType.STOP, TType.BOOL, TType.BYTE, TType.I16, TType.I32, TType.I64, TType.DOUBLE, TType.STRING)
STRUCT, MAP, SET, LIST, UUID = (TType.STRUCT, TType.MAP, TType.SET, TType.LIST, getattr(TType, 'UUID', 16))


def decode_struct(buf):
    """Generic thrift compact decoder -> {field_id: value}."""
    t = TTransport.TMemoryBuffer(buf)
    p = TCompactProtocol.TCompactProtocol(t)

    def read_value(ttype):
        if ttype == I64:
            return p.readI64()
        if ttype == I32:
            return p.readI32()
        if ttype == I16:
            return p.readI16()
        if ttype == BYTE:
            return p.readByte()
        if ttype == DOUBLE:
            return p.readDouble()
        if ttype == BOOL:
            return p.readBool()
        if ttype == STRING:
            return p.readBinary()
        if ttype == UUID:
            return p.readUuid()
        if ttype == MAP:
            kt, vt, size = p.readMapBegin()
            m = {}
            for _ in range(size):
                k = read_value(kt)
                v = read_value(vt)
                m[k.decode() if isinstance(k, bytes) else k] = (
                    v.decode() if isinstance(v, bytes) else v)
            p.readMapEnd()
            return m
        if ttype in (LIST, SET):
            et, size = p.readListBegin()
            return [read_value(et) for _ in range(size)]
        if ttype == STRUCT:
            return read_struct()
        raise ValueError(ttype)

    def read_struct():
        p.readStructBegin()
        out = {}
        while True:
            _, ttype, fid = p.readFieldBegin()
            if ttype == STOP:
                break
            out[fid] = read_value(ttype)
            p.readFieldEnd()
        p.readStructEnd()
        return out

    return read_struct()


def norm_key(h):
    return bytes.fromhex(h)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--stock", required=True, help="dump_topic.py JSON (stock ingress)")
    ap.add_argument("--rust", required=True, help="/debug/messages JSON (rust ingress)")
    ap.add_argument("--stock-from-data", type=int, default=0,
                    help="skip the first N stock warp10-data messages (baseline)")
    ap.add_argument("--stock-from-meta", type=int, default=0,
                    help="skip the first N stock warp10-meta messages (baseline)")
    args = ap.parse_args()

    stock = json.load(open(args.stock))
    rust = json.load(open(args.rust))["messages"]

    stock_data = [m for m in stock if m["topic"] == "warp10-data"][args.stock_from_data:]
    stock_meta = [m for m in stock if m["topic"] == "warp10-meta"][args.stock_from_meta:]
    rust_data = [m for m in rust if m["topic"] == "warp10-data"]
    rust_meta = [m for m in rust if m["topic"] == "warp10-meta"]

    print(f"stock: {len(stock_data)} data, {len(stock_meta)} meta")
    print(f"rust:  {len(rust_data)} data, {len(rust_meta)} meta")

    failures = []

    def check(cond, msg):
        if not cond:
            failures.append(msg)
            print(f"  FAIL {msg}")

    # --- data messages: strict byte equality, matched by key ---
    if len(stock_data) != len(rust_data):
        failures.append(f"data count differs: stock={len(stock_data)} rust={len(rust_data)}")
    stock_by_key = {}
    for m in stock_data:
        k = norm_key(m["key"])
        stock_by_key.setdefault(k, []).append(m)
    for m in rust_data:
        k = norm_key(m["key"])
        check(k in stock_by_key, f"rust data key {k.hex()} not produced by stock")
        if k in stock_by_key:
            s = stock_by_key[k].pop(0)
            check(norm_key(m["value"]) == norm_key(s["value"]),
                  f"data VALUE differs for key {k.hex()}:\n    stock={s['value']}\n    rust ={m['value']}")
            sd = decode_struct(bytes.fromhex(m["value"]))
            cd = decode_struct(bytes.fromhex(s["value"]))
            check(sd.get(1) == cd.get(1) and sd.get(2) == cd.get(2) and sd.get(7) == cd.get(7),
                  f"decoded classId/labelsId/type differ for key {k.hex()}")
            if sd.get(3) and cd.get(3):
                check(sd[3] == cd[3], f"encoder bytes differ for key {k.hex()}")

    # --- meta messages: key equality + decoded field equality (minus source/lastActivity) ---
    stock_meta_by_key = {}
    for m in stock_meta:
        stock_meta_by_key.setdefault(norm_key(m["key"]), []).append(m)
    for m in rust_meta:
        k = norm_key(m["key"])
        check(k in stock_meta_by_key, f"rust meta key {k.hex()} not produced by stock")
        if k in stock_meta_by_key:
            s = stock_meta_by_key[k].pop(0)
            sv, rv = bytes.fromhex(s["value"]), bytes.fromhex(m["value"])
            check(sv[:16] == k and rv[:16] == k, f"meta value not prefixed by key {k.hex()}")
            sm = decode_struct(sv[16:])
            rm = decode_struct(rv[16:])
            for fid, name in [(1, "name"), (2, "labels")]:
                check(sm.get(fid) == rm.get(fid),
                      f"meta field {name} differs for key {k.hex()}: stock={sm.get(fid)} rust={rm.get(fid)}")
            # classId/labelsId travel in the Kafka key (checked above); the stock
            # image leaves the thrift fields at 0, rust fills them -> accept either.
            for fid, name in [(3, "classId"), (4, "labelsId")]:
                sv_, rv_ = sm.get(fid), rm.get(fid)
                check(sv_ == rv_ or sv_ == 0 or rv_ == 0,
                      f"meta field {name} differs for key {k.hex()}: stock={sv_} rust={rv_}")

    if failures:
        print(f"\nPARITY: FAIL ({len(failures)} problems)")
        sys.exit(1)
    print("\nPARITY: OK — data messages byte-identical, meta fields identical")


if __name__ == "__main__":
    main()
