# warp10-platform-rs

Experimental **Rust ingress for Warp 10** with a pluggable `.owner`/`.producer`
policy. The goal: explore an ingress that can write for *several owners* from a
single platform credential, without weakening the tenancy guarantees of the
stock Java ingress.

> ⚠️ **Experimental.** This is a prototype, not a drop-in replacement for
> `io.warp10.standalone.StandaloneIngressHandler`. Known gaps are listed at the
> bottom. The security-sensitive part is `src/policy.rs` — read it before
> running this anywhere real.

## Byte-parity tests against a real cluster (`docker-parity/`)

`docker-parity/` runs a full distributed Warp 10 3.5.0 cluster in docker
(zookeeper / kafka / FDB 7.1 / directory / store / ingress :8080 / egress :8081)
and compares the Kafka messages of the stock ingress vs this crate **byte for
byte** (key = classId||labelsId BE, value = thrift-compact `KafkaDataMessage` /
`Metadata`), then replays rust-produced messages into the real topics and fetches
them back through the egress. See `docker-parity/README.md`; run it with
`docker-parity/scripts/parity.sh`. Latest run: data messages byte-identical, meta
fields identical, and the rust-written GTS is fetchable end-to-end.

Notable ported details verified by the golden tests (`tests/golden/`):
SipHash-2-4 + `hash24_palindromic`, classId/labelsId, the GTSEncoder binary format
(incl. the **BigDecimal** path used by the platform line parser for short decimal
literals), thrift compact serialization, and the Kafka MAC (`hash24_palindromic`,
8 BE bytes appended) when `kafka.*.mac` is configured.

## What it does

```
POST /api/v0/update   (header: X-Warp10-Token)
  body: one datum per line, Warp 10 text format:
        <ts>[/<lat:lon|rawloc>[/<elev>]] <class>{<k>=<v>,...} <value>
GET  /healthz
GET  /debug/messages   (mock mode: dumps the produced Kafka records as hex)
```

Per request: token → claims (403 if unknown/expired/invalid) → parse lines →
apply the owner/producer policy → group by (class, labels) → encode with a
byte-compatible port of `GTSEncoder` → compute `classId`/`labelsId`
(palindromic SipHash-2-4, as `GTSHelper`) → publish on Kafka:

| topic | key | value |
|---|---|---|
| `data_topic` (default `warp10.data`) | 16 bytes: `classId`\|\|`labelsId` (big-endian) | thrift **compact** `KafkaDataMessage` {classId, labelsId, data=encoder bytes, type=STORE(1)} |
| `meta_topic` (default `warp10.meta`) | same 16 bytes | key bytes ++ thrift compact `Metadata` {name, labels (incl. `.owner`/`.producer`/`.app`), classId, labelsId, source} |

Metadata is published once per `gtsId` (dedup, like the stock ingress'
`IngressMetadataConsumer`).

## The owner/producer policy (deny-by-default)

Stock Warp 10 forces `.producer`, `.owner`, `.app` from the write token and
rejects (403) a token without owner/producer — see
`StandaloneIngressHandler.java` (`extraLabels.put(...)` then
`GTSHelper.parse` applies `labels.putAll(extraLabels)`, so the token always
wins over the payload).

This crate keeps that as the **default**. Relaxation is *opt-in per token*, via
attributes in the token directory:

- token without `.allowowner`/`.allowproducer` → stock behavior, token labels
  always win (a client pushing `.owner=<other tenant>` is silently rewritten
  to the token's owner — there is a test for this);
- token **with** `.allowowner` (resp. `.allowproducer`) → a client-pushed label
  is kept **only if it is a valid UUID** (36-char canonical or 32 hex);
  otherwise the token value is used;
- token without valid owner/producer, or expired → `403`.

So a single "platform" token with `.allowowner` can write for many tenants,
while any other token can never impersonate another owner.

## Tokens

Real Warp 10 tokens are AES-encrypted; decrypting them is out of scope. Instead
the ingress loads a JSON directory (`tokens_file`, see `tokens.example.json`)
mapping the presented token string to its claims. In production this lookup is
where an `AuthenticationPlugin` (upstream PR #274: JWT/macaroon →
ReadToken/WriteToken) would plug in.

## Configuration

`--config config.json` (see `config.example.json`), overridable via
`WARP10_RS_*` env vars: `LISTEN`, `TOKENS_FILE`, `SIPHASH_CLASS_KEY`,
`SIPHASH_LABELS_KEY` (both 32 hex chars = 16 bytes, the platform's
`warp.siphash.class` / `warp.siphash.labels`), `DATA_TOPIC`, `META_TOPIC`,
`KAFKA_BROKERS`, `KAFKA=1` (with the `kafka` feature) to use a real producer.

Without the `kafka` cargo feature the binary uses an in-memory mock producer
(perfect for tests and demos; `GET /debug/messages` shows what would be sent).

```sh
cargo run -- --config config.json
curl -X POST localhost:8080/api/v0/update \
  -H 'X-Warp10-Token: dev-token-stock' \
  --data-binary $'1700000000000000 cpu{host=web1} 42'
```

## Byte-compatibility with the platform

Everything on the wire is ported from the upstream sources
(`senx/warp10-platform`, `senx/geoxplib`) and pinned by **golden vectors**
generated from the upstream Java/Python code (`tests/golden/`):

- `siphash.rs` — `SipHashInline.hash24` + `hash24_palindromic` (the "palindromic"
  variant *is* real upstream: it hashes `data ++ reverse(data)`), `getKey`
  (key read big-endian, blocks little-endian);
- `ids.rs` — `GTSHelper.classId` / `labelsId` (per-label palindromic hashes,
  sorted, big-endian concat) / `gtsId` / `fillGTSIds`;
- `encoder.rs` — `GTSEncoder` datum stream (flag byte, optional loc/elev flag,
  zigzag varints, BE raw longs, IEEE754 doubles), base timestamp 0 as the
  ingress/Store use;
- `geoxp.rs` — GeoXPPoint = HHCode at resolution 32 (`HHCodeHelper`);
- `thrift.rs` — thrift **compact** protocol writer for `KafkaDataMessage` and
  `Metadata` (the platform uses `TCompactProtocol`, see `ThriftUtils`).

Run the tests:

```sh
cargo test        # 20 tests: golden vectors + policy + end-to-end HTTP
```

To regenerate `tests/golden/*`, compile the upstream classes
(`crypto/.../SipHashInline.java`, `.../GTSEncoder.java`, …) — the exact
harness used is described in the git history of this repo.

## Known limitations / not done

- No AES wrap / SipHash MAC of Kafka values (`aesDataKey`, `siphashDataKey`,
  `AES_KAFKA_META`, …) — required if the target cluster configures them.
- No `/api/v0/delete`, `/api/v0/meta` — only `update` (STORE).
- No throttling / maxpast / maxfuture / maxsize limits, no Sensision metrics.
- No real token decryption (JSON directory instead of an auth plugin).
- `lat:lon` locations are encoded via HHCode; raw long locations also accepted.
- Doubles are always IEEE754-encoded (no BigDecimal compact path).
- Metadata dedup is in-memory only (resets on restart → possible duplicate
  meta messages after a restart, which the Directory tolerates).

## Layout

```
src/
├── main.rs     binary entrypoint
├── lib.rs      library target (for tests)
├── config.rs   JSON config + env overrides
├── tokens.rs   JSON token directory (403 semantics)
├── policy.rs   .owner/.producer/.app policy (deny-by-default) ← read this
├── gts.rs      /api/v0/update line parser
├── encoder.rs  GTSEncoder binary format port
├── siphash.rs  SipHash-2-4 + palindromic variant port
├── ids.rs      classId/labelsId/gtsId
├── geoxp.rs    GeoXPPoint (HHCode)
├── thrift.rs   thrift compact writer (KafkaDataMessage, Metadata)
├── kafka.rs    Producer trait; MockProducer (+ rdkafka behind "kafka" feature)
└── server.rs   axum app: POST /api/v0/update, /healthz, /debug/messages
tests/
├── vectors.rs      golden vectors (tests/golden/*)
├── policy_gts.rs   parser + policy unit tests
└── e2e.rs          end-to-end HTTP tests with the mock producer
```
