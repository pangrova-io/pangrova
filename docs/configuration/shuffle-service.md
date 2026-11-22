---
title: "External Shuffle Service"
description: "Strict YAML configuration for the durable REMOTE shuffle service"
---

# External Shuffle Service

The durable shuffle service is a separate Java process started by
`bin/pangrova-shuffle.sh`. It stores shuffle partitions as immutable objects in
S3-compatible storage, records publication evidence in PostgreSQL, serves partition reads
over mutual TLS, and advertises half-open partition ranges to Coordinators and Workers.

The service reads one strict YAML file (default `$PANGROVA_CONF_DIR/shuffle.yml`, overridable
with `--config`). Unknown keys anywhere in the document fail startup; there is no implicit
fallback section. Configuration is not merged with the Coordinator or Worker YAML, and the
`PANGROVA_*` dot-path overrides used by those processes do not apply here.

```bash
bin/pangrova-shuffle.sh --config /etc/pangrova/shuffle.yml
bin/pangrova-shuffle.sh --config /etc/pangrova/shuffle.yml --daemon
```

Use REMOTE mode only when every participant agrees:

- `coordinator.shuffle.mode=REMOTE` with `coordinator.shuffle.provider=pangrova`
- `coordinator.shuffle.endpoints` set to one or more advertised `host:port` values, or
  `coordinator.shuffle.provider.pangrova.endpoints`
- `coordinator.transport.tls.*` and `coordinator.shuffle.tls.server-identity` configured
- `worker.shuffle.mode=REMOTE` and matching Worker TLS configuration on every Worker
- A deployed shuffle service with PostgreSQL, an S3-compatible bucket, and the capability
  secret file

`BROADCAST` exchange is not supported in REMOTE mode.

## Minimal example

```yaml
server:
  port: 9090
  advertised-host: "shuffle-0.shuffle.internal"
  tls-identity: "shuffle-0.shuffle.internal"

database:
  jdbc-url: "jdbc:postgresql://postgres:5432/pangrova_meta"
  username: pangrova
  password-file: /etc/pangrova-shuffle/secrets/db/password

object-storage:
  bucket: pangrova-shuffle
  prefix: shuffle
  region: us-east-1

security:
  certificate-chain-path: /etc/pangrova-shuffle/tls/tls.crt
  private-key-path: /etc/pangrova-shuffle/tls/tls.key
  root-ca-path: /etc/pangrova-shuffle/tls/ca.crt
  capability-secret-path: /etc/pangrova-shuffle/secrets/capability/secret
  coordinator-tls-identities:
    - "coordinator.internal"
```

## server

| Key | Type | Default | Description |
|---|---|---|---|
| `server.port` | int | required | gRPC listener port, 1–65535 |
| `server.advertised-host` | string | required | Hostname published to Coordinators and Workers |
| `server.tls-identity` | string | required | Expected certificate DNS identity for this process |
| `server.endpoints` | list | one endpoint derived from the values above | Advertised partition routes; when omitted, the service advertises `advertised-host`, `port`, `tls-identity`, `partition-start=0`, and `partition-end=-1` |

Each endpoint has the following shape:

```yaml
server:
  endpoints:
    - host: "shuffle-0.shuffle.internal"
      port: 9090
      tls-identity: "shuffle-0.shuffle.internal"
      partition-start: 0
      partition-end: 8
    - host: "shuffle-1.shuffle.internal"
      port: 9090
      tls-identity: "shuffle-1.shuffle.internal"
      partition-start: 8
      partition-end: 16
```

| Endpoint key | Rule |
|---|---|
| `host` | Non-blank; reachable by Coordinators and Workers |
| `port` | 1–65535 |
| `tls-identity` | Non-blank; must match the served certificate identity |
| `partition-start` | `>= 0` |
| `partition-end` | `-1` (expand to the allocation size) or strictly greater than `partition-start` |

A non-empty explicit endpoint list must cover the partitions used by the deployment.
Overlapping ranges are a deployment error even though each entry is individually valid.

## database

| Key | Type | Default | Description |
|---|---|---|---|
| `database.jdbc-url` | string | required | Must start with `jdbc:postgresql:` |
| `database.username` | string | required | PostgreSQL user |
| `database.password-file` | path | required | File containing the database password; mount it as a secret |
| `database.maximum-pool-size` | int | `32` | Connection-pool size; must be positive |

The service shares the Coordinator's authority database. Apply the metadata
migrations, including `V120__execution_read_windows.sql`, before starting it.
This release initializes the Shuffle schema through `V10__write_window_reservations.sql`.
Use an empty Shuffle schema and object prefix for the new deployment.
Use the permissions required by the Shuffle schema and grant the service
`SELECT` and the column-level `UPDATE` privilege needed for row locking on
`public.execution_anchor`. Grant `SELECT`, `INSERT`, `UPDATE`, and `DELETE`
on `public.execution_read_window`. [PostgreSQL 16 locking privileges](https://www.postgresql.org/docs/16/sql-select.html)

Each active read obtains up to one second of authority from PostgreSQL. The
read admission transaction loads the requested partition's publication receipt
and installs the object pin. Other published partitions are left out of that
read. The service measures elapsed monotonic and wall-clock time from before
the grant request and stops using it 50 ms early. Over a window the database
clock must advance by less than that margin beyond the reader's elapsed time.
Keep host clocks disciplined with time synchronization that slews adjustments.

A finalized partition with at most 256 batches carries its index in the
publication receipt. For larger partitions the receipt contains a directory of
immutable object pages with up to 4,096 entries each. Reads seek by output
sequence, verify the fetched page and decode at most 256 entries at a time.
PostgreSQL is used for stream admission, timed authority renewal and pin release.

Each producer stream reuses its locally verified task capability until it changes
or expires. One PostgreSQL transaction checks the live execution owner, scope,
allocation and attempt, then reserves the producer's object prefix. A reservation
admits up to 128 distinct batches over at most one second. The writer stops
using the window 50 ms early for clock uncertainty. Object PUTs run outside
transactions. One publication transaction rechecks the live fences, publishes
the ordered inventory as a group and consumes the reservation. The stream
flushes its last group before acknowledging the producer's half-close.

An unconfirmed object PUT surfaces immediately to the write stream when the SDK
reports failure. The SDK performs a single attempt for each PUT. Its reservation
protects the object prefix through the configured call deadline plus 60 seconds
for drain. Cleanup closes new admission, waits for protected writes and records
any expired reservation as a tombstone before deleting objects. Writers for
different stages of the same query share the scope lock. Each allocation
serializes its batch sequence and producer-winner updates.

When terminal cleanup closes the scope, the current execution owner can renew
cleanup access while in-flight I/O drains. A successor can recover cleanup
access after acquiring the durable anchor. The scope and its allocations stay
closed to new data access during recovery.

## object-storage

| Key | Type | Default | Description |
|---|---|---|---|
| `object-storage.bucket` | string | required | Bucket for partition payloads and manifests |
| `object-storage.prefix` | string | `pangrova/shuffle` | Object-key prefix; leading and trailing slashes are removed |
| `object-storage.region` | string | `us-east-1` | S3 region |
| `object-storage.endpoint` | URI | none | Custom S3 endpoint for MinIO or compatible services |
| `object-storage.path-style-access` | bool | `false` | Enable path-style addressing (required by MinIO) |
| `object-storage.replication-enabled` | bool | `false` | Declare that the bucket replicates objects across regions or zones |
| `object-storage.api-call-timeout-seconds` | int | `120` | Total per-call timeout; must be positive |
| `object-storage.api-call-attempt-timeout-seconds` | int | `45` | Per-attempt timeout; must be positive and no greater than the total timeout |

Enabling replication does not by itself establish a recovery guarantee; the deployment must
verify that required objects are readable from the surviving location.

## security

| Key | Type | Default | Description |
|---|---|---|---|
| `security.certificate-chain-path` | path | required | PEM certificate chain served by the process |
| `security.private-key-path` | path | required | PEM private key |
| `security.root-ca-path` | path | required | Root CA used to verify peers |
| `security.capability-secret-path` | path | required | File containing the HMAC capability secret shared with the Coordinator |
| `security.capability-ttl-seconds` | int | `3600` | Access-capability lifetime; must be positive |
| `security.coordinator-tls-identities` | list | required | Non-empty allowlist of Coordinator certificate identities |

All peers use mutual TLS. The advertised host and TLS identity must match the certificate;
an identity mismatch fails the read or registration path rather than falling back to an
unverified connection.

Set the capability lifetime at least as long as the longest task timeout for queries that use remote Shuffle. A Worker verifies that its read capability covers the task deadline before admitting the task. The default task timeout is five minutes.

## limits

| Key | Type | Default | Constraint |
|---|---|---|---|
| `limits.max-chunk-bytes` | int | `1048576` (1 MiB) | Positive |
| `limits.max-batch-bytes` | int | `268435456` (256 MiB) | At least `max-chunk-bytes` |
| `limits.max-inflight-bytes` | int | `805306368` (768 MiB) | At least three times `max-batch-bytes` |
| `limits.max-write-streams` | int | `64` | Positive |
| `limits.io-threads` | int | `32` | Positive |
| `limits.max-queued-operations` | int | `1024` | At least `max-write-streams` |

The `limits` section is optional; defaults apply when it is absent. Values are validated as
exact 32-bit integers.

Open reads share a separate in-memory index allowance of the smaller of 64 MiB
and `limits.max-inflight-bytes`. The allowance covers cached directories and
pages plus decoded entries retained while a client pauses. A rejected read
releases its object pin before responding with `RESOURCE_EXHAUSTED`.

## Validation summary

- The root document accepts only `server`, `database`, `object-storage`, `security`, and
  `limits`.
- `server`, `database`, `object-storage`, and `security` are required.
- Unknown keys, wrong scalar types, empty credential lists, and invalid ranges fail startup
  with the offending key.
- The password is read from the configured file; do not place passwords in YAML.
- Certificates, keys, and the capability secret are mounted files, not configuration values.

**See also:** [Coordinator configuration](coordinator.md#coordinatorshuffle),
[Worker configuration](worker.md#workershuffle),
[Distributed architecture](../architecture/distributed.md),
[Shuffle partition read measurement](../testing/shuffle-read-comparison.md)
