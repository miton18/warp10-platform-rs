#!/usr/bin/env bash
# Byte-parity test: stock Warp 10 ingress vs warp10-platform-rs on the real
# distributed docker cluster, plus an end-to-end fetch check.
#
#   ./scripts/setup-cluster.sh   # once (or after `docker compose down -v`)
#   ./scripts/parity.sh
set -euo pipefail
cd "$(dirname "$0")/.."

STOCK=http://localhost:8080
EGRESS=http://localhost:8081
RUST=http://127.0.0.1:18099

echo "== mint fresh tokens (write via ingress, read via egress) =="
write_token=$(docker compose exec -T ingress /opt/warp10/bin/warp10.sh tokengen /work/tokengen-write.mc2 2>/dev/null | python3 -c "import json,sys; print(json.loads(sys.stdin.read().strip().splitlines()[-1])[0]['token'])")
docker compose cp tokengen-read.mc2 egress:/tmp/tokengen-read.mc2 >/dev/null
read_token=$(docker compose exec -T egress /opt/warp10/bin/warp10.sh tokengen /tmp/tokengen-read.mc2 2>/dev/null | python3 -c "import json,sys; print(json.loads(sys.stdin.read().strip().splitlines()[-1])[0]['token'])")
python3 -c "import json; json.dump({'write': '$write_token', 'read': '$read_token'}, open('tokens-out.json','w'))"
echo "tokens minted (write len ${#write_token}, read len ${#read_token})"

start_rust() {
  if ! curl -s -o /dev/null $RUST/healthz; then
    pkill -f 'target/debug/warp10-platform-rs' 2>/dev/null || true
    (cd .. && cargo build -q)
    (cd .. && ./target/debug/warp10-platform-rs --config docker-parity/rust-config.json \
       > rust-ingress.log 2>&1 & echo $! > rust-ingress.pid)
    for i in $(seq 1 30); do curl -s -o /dev/null $RUST/healthz && return; sleep 1; done
    echo "rust ingress did not start" >&2; exit 1
  fi
}
stop_rust() { [ -f rust-ingress.pid ] && kill $(cat rust-ingress.pid) 2>/dev/null || true; rm -f rust-ingress.pid; sleep 1; }

echo "== reset topics, then restart ALL warp10 services (consumers cache topic ids) =="
docker compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic warp10-data 2>/dev/null || true
docker compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic warp10-meta 2>/dev/null || true
sleep 5
docker compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --create --topic warp10-data --partitions 1 --replication-factor 1 2>/dev/null || true
docker compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --create --topic warp10-meta --partitions 1 --replication-factor 1 2>/dev/null || true
sleep 2
docker compose restart directory store ingress egress >/dev/null
for i in $(seq 1 60); do
  curl -s -o /dev/null http://localhost:8080/ && curl -s -o /dev/null http://localhost:8081/ && break
  sleep 2
done
sleep 15

echo "== baseline dump =="
python3 scripts/dump_topic.py --out dump-base.json
base_data=$(python3 -c "import json;print(sum(1 for m in json.load(open('dump-base.json')) if m['topic']=='warp10-data'))")
base_meta=$(python3 -c "import json;print(sum(1 for m in json.load(open('dump-base.json')) if m['topic']=='warp10-meta'))")
echo "baseline messages: data=$base_data meta=$base_meta"

echo "== phase A: stock ingress (:8080) =="
code=$(curl -s -o respA.json -w "%{http_code}" -X POST $STOCK/api/v0/update \
  -H "X-Warp10-Token: $write_token" --data-binary @batch.txt)
echo "stock HTTP $code: $(head -c 200 respA.json)"
sleep 4
python3 scripts/dump_topic.py --out dump-A.json
countA=$(python3 -c "import json;print(len(json.load(open('dump-A.json'))))")
echo "stock produced $((countA - base_data - base_meta)) Kafka messages"

echo "== phase B: rust ingress (:18099, mock producer), same batch =="
start_rust
code=$(curl -s -o respB.json -w "%{http_code}" -X POST $RUST/api/v0/update \
  -H "X-Warp10-Token: rust-dev-token" --data-binary @batch.txt)
echo "rust HTTP $code: $(cat respB.json)"
curl -s $RUST/debug/messages > dump-B.json

echo "== compare A vs B (byte-level) =="
python3 scripts/compare_parity.py --stock dump-A.json --rust dump-B.json --stock-from-data $base_data --stock-from-meta $base_meta

echo "== phase C: end-to-end — rust bytes -> real Kafka -> Store/Directory -> egress fetch =="
stop_rust
start_rust   # fresh mock buffer
code=$(curl -s -o respC.json -w "%{http_code}" -X POST $RUST/api/v0/update \
  -H "X-Warp10-Token: rust-dev-token" --data-binary @batch-rs.txt)
echo "rust (batch-rs) HTTP $code"
curl -s $RUST/debug/messages > dump-C.json
python3 scripts/replay.py --in dump-C.json --bootstrap localhost:9092
sleep 6
python3 scripts/dump_topic.py --out dump-C-kafka.json
python3 - "$base_data" "$base_meta" <<'EOF'
import json, sys
base_data, base_meta = int(sys.argv[1]), int(sys.argv[2])
dumpA = json.load(open('dump-A.json'))
a_data = sum(1 for m in dumpA if m['topic'] == 'warp10-data') - base_data
a_meta = sum(1 for m in dumpA if m['topic'] == 'warp10-meta') - base_meta
msgs = json.load(open('dump-C-kafka.json'))
data_msgs = [m for m in msgs if m['topic'] == 'warp10-data']
meta_msgs = [m for m in msgs if m['topic'] == 'warp10-meta']
new = data_msgs[base_data + a_data:]
meta = meta_msgs[base_meta + a_meta:]
print(f"phase C: {len(new)} data + {len(meta)} meta messages reached real Kafka")
assert len(new) >= 5, "rust -rs data messages not found in Kafka topics"
EOF

echo "== fetch via egress (:8081) with stock READ token (wait for directory/store to consume) =="
for i in $(seq 1 30); do
  curl -s --get "$EGRESS/api/v0/find" \
    --data-urlencode "token=$read_token" \
    --data-urlencode "selector=cpu-rs{}" > find-rs.json
  grep -q cpu-rs find-rs.json && break
  sleep 2
done
echo "find cpu-rs: $(head -c 200 find-rs.json)"
curl -s --get "$EGRESS/api/v0/fetch" \
  --data-urlencode "token=$read_token" \
  --data-urlencode "selector=cpu-rs{host=web1,env=prod}" \
  --data-urlencode "start=1699999999000000" \
  --data-urlencode "stop=1700000001000000" > fetch-rs.json
python3 - <<'EOF'
import json
raw = open('fetch-rs.json').read()
assert 'cpu-rs' in raw, f"rust-written GTS not fetchable: {raw[:300]}"
assert ' 42' in raw and ' 43' in raw, f"unexpected values: {raw[:300]}"
print("fetch OK — rust-written GTS readable through the stock stack:")
print(raw[:500])
EOF

echo "== PARITY SUITE DONE =="
