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

Worker file endpoints consume File API. Their current LOCAL and S3 accessors and
SDK factory remain in Worker; moving those implementations to independent storage
providers is still outstanding. The factory uses the complete immutable profile
as its cache identity, including credentials and options. Delimiter-containing
profile fields cannot collide, and a rotated secret or session token cannot reuse
an earlier client or presigner. Invalid explicit S3 credentials fail before
provider use. Closing the factory attempts all owned clients, presigners and its
owned default credential chain, preserving cleanup failures.

`pangrova-storage-api` has a separate content-publication contract used by governed
FILE values. Its filesystem, S3 and OSS implementations do not grant Volume/Stage
access or provide directory metadata. In particular, OSS governed FILE support
does not implement OSS FileSpace transfer, COPY source admission or managed
storage. Those paths require their own authority, immutable-version and transfer
contracts.

Provider tests live with the JDBC artifact and apply the canonical migrations
through the shared test kit. Cross-provider authority tests stay with the domain
that supplies the other provider, while Coordinator and integration tests retain
the production assembly and HTTP/SQL paths. File API tests load every API class
using only its own output and Common, without filtering provider URLs out of a
larger runtime classpath.

See [Volume and Stage file access](../getting-started/catalogs/volumes-stages-files.md)
for operations and current qualification limits, and
[Worker configuration](../configuration/worker.md#filespace-storage-credentials)
for credential ownership and cache lifetime.
