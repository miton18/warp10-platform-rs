# docker-parity — byte-parity tests against a real distributed Warp 10 cluster

Spins up a full distributed Warp 10 3.5.0 cluster in docker (host networking):
zookeeper, kafka, foundationdb, directory, store, ingress (:8080), egress (:8081),
then compares the Kafka messages produced by the **stock ingress** vs the
**rust ingress** (`../src`) byte for byte, and finally replays rust-produced
messages into the real topics and fetches them back through the egress.

## Run

```bash
./scripts/setup-cluster.sh   # once: up + configure FDB + mint tokens
./scripts/parity.sh          # full parity suite (idempotent, resets topics)
```

`parity.sh` phases:
- **A** — POST `batch.txt` to the stock ingress (:8080) with the minted WRITE token;
- **B** — POST the same batch to the rust ingress (:18099, mock producer) with `rust-dev-token`;
- **compare** — `scripts/compare_parity.py`: data messages must be **byte-identical**
  (Kafka key = classId||labelsId BE, value = thrift-compact `KafkaDataMessage`);
  meta messages must match on key + name + labels (+ ids in the key);
- **C** — replay the rust messages (`batch-rs.txt`, distinct class names) into the real
  Kafka topics, wait for Directory/Store to consume, then `GET /api/v0/fetch` on the
  egress (:8081) with the stock READ token and assert the rust-written GTS comes back.

Last run: `PARITY: OK — data messages byte-identical, meta fields identical`, then
`fetch OK — rust-written GTS readable through the stock stack` (values 42/43/43 with
GeoXP location). Log kept in `parity-run.log`.

## Cluster gotchas (all fixed in the checked-in config)

- `network_mode: host` everywhere (bridge/nftables unavailable in some sandboxes);
  WarpStudio gets a unique port per service (18082..18085).
- `99-init.conf` in the warp10 image generates **random per-container** keys
  (`warp.aes.token`, `directory.psk`, `kafka.*.mac`, ...) — `conf.extra/*/zz-keys.conf`
  pins them all to shared dev values (sorts after `99-`).
- FDB must be **7.1.x**: warp10 3.5.0 bundles fdb-java 7.1 (max API 710) and hangs
  against a 7.3 server.
- Tokens must be minted **in the container that validates them** (write via ingress,
  read via egress) once keys are shared.
- `auto.offset.reset=earliest` for the Kafka consumers, and **restart all warp10
  services after deleting/recreating topics** (consumers cache topic ids).
- `conf.extra/*/zz-components.conf` sets `warp.components` per service.

## Files

- `docker-compose.yml`, `warp10.env` (dev keys only), `fdb.cluster`,
  `conf.extra/<svc>/zz-*.conf`
- `tokengen-write.mc2` / `tokengen-read.mc2` (fixed owner/producer/app), `tokens-out.json`
- `batch.txt` (stock) / `batch-rs.txt` (rust, `-rs` classes) — note the stock parser
  requires the `ts//` form (empty location)
- `scripts/`: `setup-cluster.sh`, `parity.sh`, `dump_topic.py`, `replay.py`,
  `compare_parity.py` (thrift-compact decoder + byte compare)
- `rust-config.json` / `rust-tokens.json` — rust ingress pointed at the same
  siphash keys, topics and owner/producer/app as the minted token
