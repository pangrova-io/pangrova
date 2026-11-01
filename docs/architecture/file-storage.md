# File metadata and object storage ownership

The `pangrova-file` parent aggregates three runtime artifacts. Applications select
the contracts and providers they need; the parent itself supplies no runtime JAR.

| Artifact | Owns | Dependencies and boundary |
|---|---|---|
| `pangrova-file-api` | Volume/Stage definitions, storage profiles, access descriptors, directory index port, COPY claims and object-transfer contracts | Common and JDK only; no Security identity, JDBC connection, Jackson or provider SDK types |
| `pangrova-file-core` | Access and URL services, directory-refresh orchestration, Worker HTTP client and internal current-authority SPI | File API, Security and Jackson; no File JDBC store dependency |
| `pangrova-file-store-jdbc` | PostgreSQL profiles, spaces, current authority, directory snapshots, COPY claims and source-read admission | Implements File contracts; consumes Query API and Security transaction ports; canonical migrations remain in `pangrova-schema-migrations` |

Object transfer types live together in `org.pangrova.file.api.object`, including
locations, multipart identities, part receipts and presigned URLs. Shared storage
errors live in `org.pangrova.file.api.storage`. The authority SPI lives in
`org.pangrova.file.spi.authority` in Core because it consumes Security identities;
it is not part of the provider-free API artifact. Retired package names have no
forwarding classes.

Directory refresh depends on `FileDirectoryIndex`, injected by the application.
The PostgreSQL implementation owns scan attempts, page checkpoints, captured
Stage-definition checks and terminal publication. Core authorizes the scan and requests
Worker listing pages without opening file bytes. A different metadata provider
must preserve the same attempt fencing and publication semantics. Relational
directory scans still use their separate execution adapter.

Worker file endpoints consume File API. Its `ObjectStoreAccessorProvider` contract
lets the host select and own independent backend providers without SDK types in
the factory signature. The filesystem artifact owns confined LOCAL transfer and
multipart staging; the S3 artifact owns S3 transfer, response cancellation, SDK
clients and presigners. Providers have no Worker, File Core or File JDBC dependency.
Worker retains HTTP authorization/deadlines and its directory execution adapter.

Each provider caches by the complete immutable profile, including credentials and
options. Delimiter-containing fields cannot collide, and a rotated secret or token
cannot reuse an earlier client or presigner. Invalid explicit S3 credentials fail
before use. The host stops active reads and closes every owned provider; each
provider closes its resources and preserves cleanup failure on repeated calls.
SDK clients receive a credential view that delegates resolution without exposing
close. The S3 provider closes its owned default chain once; injected credentials
remain caller-owned, including after clients and presigners have been allocated.

S3 deletion pins an immutable version ID where available. For unversioned or
mutable `null` versions, the standard AWS endpoint uses an exact ETag `If-Match`
condition; missing identity fails before DELETE. Custom endpoints are version-only
by default. A host can pass an exact endpoint inventory qualified to enforce
conditional DELETE to the S3 provider. This is local assembly policy, not a field
accepted from an access ticket. The pinned MinIO release ignores this condition,
so mutable-version deletion there fails before DELETE and preserves replacements.
The protocol fixture verifies the qualified path but does not qualify a real AWS
or other cloud deployment. See the official
[conditional-delete semantics](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-deletes.html).

`pangrova-storage-api` has a separate content-publication contract used by governed
FILE values. Its filesystem, S3 and OSS implementations do not grant Volume/Stage
access or provide directory metadata. In particular, OSS governed FILE support
does not implement OSS FileSpace transfer, COPY source admission or managed
storage. Those paths require their own authority, immutable-version and transfer
contracts.

Metadata provider tests live with the JDBC artifact and apply the canonical migrations
through the shared test kit. Object-transfer tests live with their physical provider;
Worker retains the cross-resource multipart renewal and HTTP expiry tests, using
public accessors rather than private SDK-stream classes. Cross-provider authority tests stay with the domain
that supplies the other provider, while Coordinator and integration tests retain
the production assembly and HTTP/SQL paths. File API tests load every API class
using only its own output and Common, without filtering provider URLs out of a
larger runtime classpath.

See [Volume and Stage file access](../getting-started/catalogs/volumes-stages-files.md)
for operations and current qualification limits, and
[Worker configuration](../configuration/worker.md#filespace-storage-credentials)
for credential ownership and cache lifetime.
