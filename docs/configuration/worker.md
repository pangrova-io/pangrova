---
title: "Worker"
description: "Worker core, memory, shuffle, native cache, and JVM configuration"
---

# Worker

All worker-side configuration keys, as defined by `WorkerOptions` in the
`pangrova-config` module. The bundled defaults ship in
`pangrova-config/src/main/resources/worker.yml`.

> **Protocol mapping.** The Worker receives control commands from the
> Coordinator over HTTP (REST) (control plane) and exchanges shuffle data /
> streams results over Arrow Flight (gRPC) (data plane). For the full protocol
> mapping, see [System Overview §4.6](../architecture/overview.md#46-canonical-protocol-mapping).


## worker (root)

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `worker.host` | string | `"0.0.0.0"` | Bind address for the worker HTTP server |
| `worker.port` | int | `8081` | Listen port for the worker HTTP server |
| `worker.advertised-host` | string | — | Hostname advertised to the coordinator for control and result fetching. Set explicitly when auto-detection fails (e.g. behind NAT) |
| `worker.id` | string | `"worker-1"` | Unique identifier for this worker instance |
| `worker.coordinator-url` | string | — | URL of the coordinator to register with. Required for registration |
| `worker.heartbeat-interval` | duration | `"10s"` | Interval between heartbeat reports to the coordinator. Must be smaller than `coordinator.worker.heartbeat-timeout` |

## worker.transport

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `worker.transport.port` | int | `0` | Preferred Native result transport listen port, in `0..65535` (`0` = actual OS auto-assignment). The Worker advertises the actual bound port; a configured occupied port fails startup instead of switching ports. Set a fixed port in production for firewall rules |
| `worker.transport.tls.enabled` | boolean | `false` | Require mutual TLS for Native shuffle and result transport. Enable for distributed production |
| `worker.transport.tls.certificate-chain-path` | string | `""` | Mounted PEM certificate chain presented by this worker |
| `worker.transport.tls.private-key-path` | string | `""` | Mounted PEM private key for the worker certificate |
| `worker.transport.tls.root-ca-path` | string | `""` | Mounted PEM root CA used to verify both server and client peers |
| `worker.transport.tls.identity` | string | `""` | Certificate DNS identity advertised to peer workers |

When TLS is enabled, all four TLS values are mandatory. Partial configuration,
unreadable PEM files, a TLS endpoint received by a plaintext worker, or a
plaintext endpoint received by a TLS worker fail startup/task admission; there
is no plaintext fallback.

## worker.task

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `worker.task.result-retention-seconds` | int | `300` | Seconds to retain query results before expiration. Clients must fetch results before this window expires |

## worker.shuffle

Native Flight shuffle can use a separate peer-worker route when the control and
result address traverses a proxy or host relay. Set
`worker.shuffle.advertised-host` to a concrete hostname reachable from peer
workers. When omitted, it inherits `worker.advertised-host`; the TLS certificate
identity remains `worker.transport.tls.identity` and is verified independently
of the connection address. Peer workers must be able to reach the Native Flight
server's advertised bound port at this hostname.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `worker.shuffle.advertised-host` | string | `worker.advertised-host` | Hostname advertised to peer workers for Native Flight shuffle; must not be a wildcard bind address |
| `worker.shuffle.buffer-capacity` | int | `100` | Maximum number of batches per shuffle partition buffer |
| `worker.shuffle.max-memory-bytes` | long | `536870912` (512 MB) | Total memory budget in bytes for all shuffle partitions combined |
| `worker.shuffle.spill-threshold` | double | `0.8` | Memory usage ratio (0.0–1.0) that triggers shuffle spill to disk |

## worker.memory

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `worker.memory.budget-bytes` | long | `536870912` (512 MB) | Total memory budget in bytes for the native execution engine |
| `worker.memory.system-reserved-bytes` | long | `67108864` (64 MB) | Memory reserved for system overhead in bytes |
| `worker.memory.node-bytes` | long | `1073741824` (1 GB) | Total node memory pool capacity in bytes |
| `worker.memory.pressure-threshold-low` | double | `50.0` | Memory usage percentage that triggers LOW pressure level |
| `worker.memory.pressure-threshold-medium` | double | `70.0` | Memory usage percentage that triggers MEDIUM pressure level |
| `worker.memory.pressure-threshold-high` | double | `85.0` | Memory usage percentage that triggers HIGH pressure level |
| `worker.memory.pressure-threshold-critical` | double | `95.0` | Memory usage percentage that triggers CRITICAL pressure level |

## worker.threads

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `worker.threads.query` | int | `4` | Number of threads for query execution |
| `worker.threads.io` | int | `2` | Number of threads for I/O operations |
| `worker.threads.scheduler` | int | `1` | Number of threads for the task scheduler |

## worker.spill

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `worker.spill.directory` | string | — | Directory for spilling intermediate data to disk. Leaving it unset disables spill |

Native Flight shuffle has a separate, process-wide service reservation. The Worker refuses to start without an existing, writable, absolute `worker.shuffle.service.spill-directory`; it never uses the system temporary directory implicitly. Ensure the volume has at least the configured service spill quota plus 10% or 64 MiB free headroom, whichever is larger. The service memory quotas are reserved in both the NodePool and shared MemoryArbitrator before task admission.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `worker.shuffle.service.retention-bytes` | long | `16777216` | Native Flight demux retention reservation |
| `worker.shuffle.service.transit-bytes` | long | `16777216` | Separate active Flight replay/IPC/send reservation; must fit three 4 MiB payloads plus 64 KiB metadata |
| `worker.shuffle.service.spill-bytes` | long | `1073741824` | Hard cap for this service's spill bytes |
| `worker.shuffle.service.spill-files` | int | `256` | Hard cap for this service's anonymous spill files |
| `worker.shuffle.service.spill-directory` | string | required | Dedicated absolute spill directory; no fallback |

The service spill cap does not yet share a node-wide disk allocator with task spill. Size their quotas against the same volume explicitly; this remains a distributed resource-governance gap.

## worker.native

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `worker.native.path` | string | — | Explicit path to the native library file. If unset, the library is extracted from the classpath bundle |
| `worker.native.workdir` | string | — | Working directory for native library extraction |
| `worker.native.delete-after-load` | bool | `true` | Delete the extracted native library temp file after loading |

### worker.native.cache

The Velox async data cache absorbs repeated scans of hot data. The RAM cache
is in-process; the SSD cache persists across restarts.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `worker.native.cache.enabled` | bool | `false` | Enable the native async data cache (in-memory + optional SSD) |
| `worker.native.cache.memory-bytes` | long | `8589934592` (8 GB) | RAM budget in bytes for the in-memory data cache |
| `worker.native.cache.ssd-path` | string | `""` | Local filesystem path for SSD-backed cache |
| `worker.native.cache.ssd-bytes` | long | `107374182400` (100 GB) | SSD cache capacity in bytes |
| `worker.native.cache.ssd-shards` | int | `8` | Number of SSD cache shards for parallel I/O |
| `worker.native.cache.checkpoint-interval-bytes` | long | `1073741824` (1 GB) | SSD cache checkpoint interval in bytes. The native engine persists its SSD cache index every N bytes so the cache survives process restart. Set to `0` to disable checkpointing |

## worker.registration

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `worker.registration.max-attempts` | int | `5` | Maximum number of registration attempts to the coordinator |
| `worker.registration.initial-backoff` | duration | `"1s"` | Initial backoff duration for registration retry |
| `worker.registration.max-backoff` | duration | `"30s"` | Maximum backoff duration for registration retry |

## worker.cdc

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `worker.cdc.enabled` | bool | `true` | Enable CDC (Change Data Capture) processing on this worker |

## worker.internal-auth-secret

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `worker.internal-auth-secret` | string | — | Shared secret for the `X-Internal-Auth` header between Coordinator and Worker. Must be identical on all nodes |

---

## worker.ai

AI execution is disabled by default. Enabling it requires both an explicit
authorization policy and at least one exact, versioned model descriptor. There
is no implicit localhost model and no JVM-system-property credential fallback.

Task admission uses the complete canonical task attempt, not only a query/task
name. Cancellation revokes new provider sends and wakes concurrency waiters;
it does not retire physical attempts, erase resource evidence or acknowledge
remote inference termination. Retirement follows actual joined task cleanup.
The response grant bounds retained entity bytes. The canonical report separately
records observed delivered bytes, including a rejected chunk; these can exceed
the retention grant in a failed/cancelled report. Successful completion cannot
hide a rejected response chunk. These are kernel resource counters, not LLM
token, cost or billing statistics.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `worker.ai.authorization.mode` | string | `deny` | `deny` or the built-in `authenticated` admission policy |
| `worker.ai.authorization.allowed-models` | CSV | empty | Exact SQL-visible model names allowed on this worker |
| `worker.ai.authorization.allowed-capabilities` | CSV | empty | Exact capability IDs such as `inference:chat` |
| `worker.ai.authorization.granted-permissions` | CSV | empty | Permissions available to trusted task identities; built-ins require `AI_MODEL_USAGE` |
| `worker.ai.authorization.max-batch-size` | int | `128` | Worker admission ceiling for one model invocation |
| `worker.ai.authorization.max-input-bytes` | long | `8388608` | Aggregate input-byte ceiling for one invocation |
| `worker.ai.models.<id>.name` | string | `<id>` | Exact SQL/provider model name; use this field when the name contains `/`, `:` or `.` |
| `worker.ai.models.<id>.provider` | string | — | Exact wire dialect: `openai-compatible` (chat/embedding/rerank), `openai-responses` (text/image/document), or `openai-transcription` (audio multipart) |
| `worker.ai.models.<id>.provider-model` | string | — | Exact upstream provider model identifier; never inferred from the SQL-visible name |
| `worker.ai.models.<id>.version` | string | — | Immutable, exact model/config version used in dispatch and audit |
| `worker.ai.models.<id>.endpoint` | URI | — | Absolute HTTP(S) endpoint without URI user-info, query, or fragment; Responses and transcription require their exact canonical paths |
| `worker.ai.models.<id>.secret-reference` | string | — | Opaque secret reference. Built-in resolver accepts only `env://VARIABLE`; plugins may add Vault/KMS |
| `worker.ai.models.<id>.authentication` | string | `bearer` | `bearer` requires `secret-reference`; `none` forbids it and is intended only for explicitly trusted endpoints |
| `worker.ai.models.<id>.capabilities` | CSV | — | `inference`, `embedding`, `reranking`, or `multimodal`; binary modalities require the governed FILE loader and their matching explicit provider dialect |
| `worker.ai.models.<id>.modalities` | CSV | `text` | Exact accepted modalities. Chat: `text,image`; Responses: subset of `text,image,document`; transcription: exactly `text,audio` |
| `worker.ai.models.<id>.max-batch-size` | int | `16` | Provider batch limit (`32` for embedding-only models); transcription requires `1` |
| `worker.ai.models.<id>.max-requests-per-second` | double | `10` | Local provider admission rate |
| `worker.ai.models.<id>.timeout-seconds` | int | `30` | Per-request provider timeout |
| `worker.ai.models.<id>.max-input-tokens` | long | — | Required per-request input-token constraint in the signed descriptor; not a usage counter or local tokenizer guarantee |
| `worker.ai.models.<id>.max-output-tokens` | long | — | Required positive limit for inference/multimodal models; optional zero for non-generative models |
| `worker.ai.models.<id>.max-media-bytes` | long | — | Required positive limit for multimodal models; optional zero for text-only models |
| `worker.ai.models.<id>.max-concurrency` | int | — | Required positive per-worker upstream concurrency limit |
| `worker.ai.models.<id>.max-provider-items-per-task` | long | — | Required positive cumulative logical input-item limit per exact task attempt and model |
| `worker.ai.models.<id>.max-provider-attempts-per-task` | long | — | Required positive cumulative application HTTP-send limit, including failed retries |
| `worker.ai.models.<id>.max-request-bytes` | long | — | Required positive limit on one fully encoded request entity |
| `worker.ai.models.<id>.max-response-bytes` | long | — | Required positive limit on one received response entity before accumulation |
| `worker.ai.models.<id>.embedding-dimension` | int | — | Required positive dimension when `embedding` is declared |
| `worker.ai.models.<id>.query-prefix` | string | empty | Prefix only for explicit `QUERY` embedding purpose, regardless of batch size |
| `worker.ai.models.<id>.document-prefix` | string | empty | Prefix only for explicit `DOCUMENT` embedding purpose, regardless of batch size; `RAW` is unchanged |
| `worker.ai.models.<id>.enabled` | bool | `true` | Publishes or suppresses this descriptor at startup |

### worker.ai.file

The governed FILE runtime is disabled when `storage-backend` is absent or blank,
so ordinary and text-only AI Workers do not require object-storage configuration.
Enabling FILE, or configuring a model with a multimodal capability that requires
governed blob loading, requires the complete S3 configuration below and fails
Worker startup if any required value is missing or invalid. The runtime has no
local-filesystem production mode. Every FILE-enabled Worker must use the same
S3-compatible bucket, prefix, region, encryption policy and capability secret
reference. Local staging is per Worker and may be ephemeral; published objects
are durable and content addressed. S3 credentials come only from the AWS SDK
workload-identity/default credential chain. Static access keys are not accepted
as FILE configuration.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `worker.ai.file.storage-backend` | string | — | Explicit FILE enablement switch. When set it must be exactly `s3`; an unsupported value or incomplete FILE configuration fails Worker function registration |
| `worker.ai.file.storage-staging-root` | path | — | Required Worker-local bounded staging directory; incomplete writes are never published and abandoned files are age/count scavenged |
| `worker.ai.file.capability-secret-reference` | string | — | Required external secret reference resolved at startup. The value must be canonical base64 decoding to at least 32 bytes and must be identical across Workers; it is never persisted locally |
| `worker.ai.file.storage.s3.bucket` | string | — | Required durable bucket shared by the Worker fleet |
| `worker.ai.file.storage.s3.region` | string | — | Required signing/service region |
| `worker.ai.file.storage.s3.endpoint` | HTTPS origin | AWS regional endpoint | Optional S3-compatible endpoint. User-info, query, fragment and non-root paths are rejected |
| `worker.ai.file.storage.s3.prefix` | string | `pangrova/ai-files` | Tenant-partitioned object prefix; `..` and absolute prefixes are rejected |
| `worker.ai.file.storage.s3.sse` | string | — | Required `AES256` or `aws:kms`; publication fails when HEAD does not prove the selected encryption |
| `worker.ai.file.storage.s3.kms-key-id` | string | — | Required for `aws:kms`; aliases are rejected because returned object metadata cannot prove alias identity |
| `worker.ai.file.storage.s3.expected-bucket-owner` | string | empty | Optional AWS account owner guard applied to PUT, HEAD and GET |
| `worker.ai.file.storage.s3.path-style-access` | bool | `false` | Enable only for an S3-compatible service that requires path-style addressing |
| `worker.ai.file.max-bytes` | long | `67108864` | Worker hard ceiling for one persisted FILE; task/model media grants may lower it |
| `worker.ai.file.allowed-mime-types` | CSV | images, PDF, text, audio, MP4 | MIME allowlist applied before persistence |
| `worker.ai.file.url.allowed-hosts` | CSV | empty | Exact HTTPS import host allowlist; empty disables URL import |
| `worker.ai.file.url.connect-timeout-ms` | long | `5000` | HTTPS import connect timeout |
| `worker.ai.file.url.read-timeout-ms` | long | `30000` | HTTPS import read timeout |
| `worker.ai.file.url.max-wire-bytes` | long | `67108864` | Maximum compressed/on-wire import bytes |
| `worker.ai.file.url.max-decoded-bytes` | long | `67108864` | Maximum decoded import bytes |
| `worker.ai.file.url.max-connections` | int | `256` | Hard maximum connections in the shared HTTPS import pool |
| `worker.ai.file.url.max-connections-per-route` | int | `32` | Hard maximum concurrent connections to one allowlisted host |
| `worker.ai.file.url.connection-ttl-ms` | long | `60000` | Maximum lifetime of a pooled connection before re-establishment |
| `worker.ai.file.url.idle-eviction-ms` | long | `30000` | Idle time after which a pooled connection is evicted |

The S3 key is derived directly from tenant scope and SHA-256; reads issue an
exact GET and never list a bucket or prefix. PUT uses `If-None-Match: *`, then
HEAD verifies size, SHA-256 metadata, SSE mode and KMS key. Bucket lifecycle
policy owns durable retention; Worker cleanup applies only to unpublished local
staging files.

Example:

```yaml
worker:
  ai:
    authorization:
      mode: authenticated
      allowed-models: "gpt-4o-mini,text-embedding-3-small"
      allowed-capabilities: "inference:gpt-4o-mini,embedding:text-embedding-3-small"
      granted-permissions: "AI_MODEL_USAGE"
    file:
      storage-backend: "s3"
      storage-staging-root: "/var/lib/pangrova/ai-file-staging"
      capability-secret-reference: "env://PANGROVA_FILE_CAPABILITY_KEY_BASE64"
      storage:
        s3:
          bucket: "pangrova-governed-files"
          region: "us-east-1"
          prefix: "production/ai-files"
          sse: "aws:kms"
          kms-key-id: "arn:aws:kms:us-east-1:123456789012:key/uuid"
          expected-bucket-owner: "123456789012"
    models:
      chat:
        name: "gpt-4o-mini"
        provider: "openai-compatible"
        provider-model: "gpt-4o-mini-2026-08-01"
        version: "2026-08-01"
        endpoint: "https://api.example/v1/chat/completions"
        secret-reference: "env://PANGROVA_CHAT_API_KEY"
        capabilities: "inference"
        modalities: "text"
        max-input-tokens: 128000
        max-output-tokens: 16384
        max-media-bytes: 0
        max-concurrency: 8
        max-provider-items-per-task: 10000
        max-provider-attempts-per-task: 40000
        max-request-bytes: 1048576
        max-response-bytes: 1048576
      embedding:
        name: "text-embedding-3-small"
        provider: "openai-compatible"
        provider-model: "text-embedding-3-small"
        version: "3"
        endpoint: "https://api.example/v1/embeddings"
        secret-reference: "env://PANGROVA_EMBEDDING_API_KEY"
        capabilities: "embedding"
        embedding-dimension: 1536
        max-input-tokens: 8191
        max-concurrency: 16
        max-provider-items-per-task: 100000
        max-provider-attempts-per-task: 400000
        max-request-bytes: 1048576
        max-response-bytes: 8388608
      documents:
        provider: "openai-responses"
        provider-model: "gpt-4o-2026-08-01"
        version: "2026-08-01"
        endpoint: "https://api.example/v1/responses"
        secret-reference: "env://PANGROVA_RESPONSES_API_KEY"
        capabilities: "multimodal"
        modalities: "text,image,document"
        max-media-bytes: 16777216
        max-input-tokens: 128000
        max-output-tokens: 16384
        max-concurrency: 8
        max-provider-items-per-task: 10000
        max-provider-attempts-per-task: 40000
        max-request-bytes: 25165824
        max-response-bytes: 1048576
      transcription:
        provider: "openai-transcription"
        provider-model: "gpt-4o-transcribe"
        version: "2026-08-01"
        endpoint: "https://api.example/v1/audio/transcriptions"
        secret-reference: "env://PANGROVA_TRANSCRIPTION_API_KEY"
        capabilities: "multimodal"
        modalities: "text,audio"
        max-batch-size: 1
        max-media-bytes: 26214400
        max-input-tokens: 128000
        max-output-tokens: 16384
        max-concurrency: 4
        max-provider-items-per-task: 10000
        max-provider-attempts-per-task: 40000
        max-request-bytes: 28311552
        max-response-bytes: 1048576
```

See [AI Functions](../sql-reference/functions/ai-functions.md) for details.

---

## JVM Sizing Guide

Workers allocate memory across three pools:

| Component | Setting | Notes |
|-----------|---------|-------|
| JVM heap | `-Xmx` | Task metadata, result buffers, sort tables |
| Arrow off-heap | `-XX:MaxDirectMemorySize` | Arrow columnar buffers |
| Velox RAM cache | `worker.native.cache.memory-bytes` | Velox data cache |

Rule of thumb: `Xmx + MaxDirectMemorySize + native.cache.memory-bytes ≤ 0.85 × total node RAM`

| Node RAM | `-Xmx` | `MaxDirectMemorySize` | Velox RAM cache |
|----------|--------|-----------------------|-----------------|
| 64 GB | `24g` | `8g` | `16g` |
| 128 GB | `32g` | `16g` | `32g` |
| 256 GB | `48g` | `32g` | `128g` |

Recommended JVM flags: `-XX:+UseG1GC`, `-XX:+ExitOnOutOfMemoryError`,
`-XX:+HeapDumpOnOutOfMemoryError`, `-Dfile.encoding=UTF-8`,
`-Duser.timezone=UTC`, `-Djava.net.preferIPv4Stack=true`.

**See Also:** [Configuration model](index.md), [Coordinator](coordinator.md), [Quick Reference](reference.md)
