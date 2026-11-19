#!/usr/bin/env bash
# Bring up the distributed Warp 10 cluster, configure FDB, share fdb.cluster,
# and mint a WRITE + READ token with fixed owner/producer/app.
set -euo pipefail
cd "$(dirname "$0")/.."

echo "== up: zookeeper kafka fdb =="
docker compose up -d zookeeper kafka fdb

echo "== wait for fdb =="
for i in $(seq 1 60); do
  if docker compose exec -T fdb fdbcli --exec "status minimal" >/dev/null 2>&1; then break; fi
  sleep 2
done
docker compose exec -T fdb fdbcli --exec "configure new single memory" || true
docker compose exec -T fdb fdbcli --exec "status minimal"

echo "== write ./fdb.cluster (host networking: 127.0.0.1:4500) =="
cf=$(docker compose exec -T fdb sh -c 'cat /etc/foundationdb/fdb.cluster 2>/dev/null || cat /var/fdb/fdb.cluster')
echo "in-container cluster file: $cf"
id_part=$(echo "$cf" | sed -E 's/@.*//')
echo "${id_part}@127.0.0.1:4500" > fdb.cluster
cat fdb.cluster

echo "== up: directory store ingress egress =="
docker compose up -d directory store ingress egress

echo "== wait for ingress HTTP (expect 403 without token) =="
for i in $(seq 1 90); do
  code=$(curl -s -o /dev/null -w "%{http_code}" -X POST http://localhost:8080/api/v0/update --data-binary '1 x{} 1' || true)
  if [ "$code" = "403" ] || [ "$code" = "400" ]; then echo "ingress up (HTTP $code)"; break; fi
  sleep 2
done

echo "== mint tokens =="
write_token=$(docker compose exec -T ingress /opt/warp10/bin/warp10.sh tokengen /work/tokengen-write.mc2 2>/dev/null | grep '"token"' | head -1 | sed -e 's/.*" : "//' -e 's/".*//')
read_token=$(docker compose exec -T ingress /opt/warp10/bin/warp10.sh tokengen /work/tokengen-read.mc2 2>/dev/null | grep '"token"' | tail -1 | sed -e 's/.*" : "//' -e 's/".*//')
if [ -z "$write_token" ] || [ -z "$read_token" ]; then
  echo "token minting failed; raw output:" >&2
  docker compose exec -T ingress /opt/warp10/bin/warp10.sh tokengen /work/tokengen-write.mc2 >&2 || true
  exit 1
fi
cat > tokens-out.json <<EOF
{"write": "$write_token", "read": "$read_token"}
EOF
echo "tokens written to tokens-out.json (write/read)"

# rust ingress config: same siphash keys as warp10.env, same topics, dev token
# mapped to the SAME owner/producer/app as the minted write token.
cat > rust-config.json <<'EOF'
{
  "listen": "127.0.0.1:18099",
  "tokens_file": "rust-tokens.json",
  "siphash_class_key": "0102030405060708090a0b0c0d0e0f10",
  "siphash_labels_key": "1112131415161718191a1b1c1d1e1f20",
  "data_topic": "warp10-data",
  "meta_topic": "warp10-meta",
  "kafka_brokers": "localhost:9092",
  "max_body_bytes": 33554432
}
EOF
cat > rust-tokens.json <<'EOF'
{
  "tokens": {
    "rust-dev-token": {
      "owner": "11111111-2222-3333-4444-555555555555",
      "producer": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
      "app": "parity-app",
      "labels": {},
      "attributes": [],
      "expiry": null
    }
  }
}
EOF
echo "rust-config.json + rust-tokens.json written"
echo "== setup done =="
