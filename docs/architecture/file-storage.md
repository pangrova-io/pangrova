# File metadata and filesystem ownership

Physical I/O belongs to `pangrova-filesystem`; domain control belongs to its caller.
The parent contains api/local/s3/oss artifacts and supplies no runtime JAR.

| Artifact | Owns | Boundary |
|---|---|---|
| `pangrova-filesystem-api` | Blob/object transfer, locations, multipart receipts, presigned URLs and immutable storage profiles | Common/JDK only; no SPI, persistence, runtime or cloud SDK |
| `pangrova-filesystem-local` | Confined LOCAL transfer, byte-derived receipts, durable multipart state and mutation locks | Filesystem API/Common only |
| `pangrova-filesystem-s3` | S3 transfer, publication, cancellation, owned clients and presigners | Filesystem API/Common plus S3 SDK |
| `pangrova-filesystem-oss` | OSS governed FILE publication | Filesystem API/Common plus OSS SDK; object transfer/COPY remains pending |
| `pangrova-spi` | FileSpace definitions, authority and access descriptors, shared Security identities | Shared domain contracts; no Protocol or implementation dependency |
| `pangrova-file` | Access/URL/directory orchestration and Worker HTTP client adapter | SPI, Filesystem API and Persistence API; no Security runtime/JDBC dependency |
| `pangrova-persistence-api` | Registry, directory index, COPY claim and query/authority persistence ports | Store contracts organized by domain/capability |
| `pangrova-persistence-jdbc` | File/Coordination PostgreSQL implementations and Security transaction gate | Implements persistence contracts; migrations remain in `pangrova-schema-migrations` |

`pangrova-file` is a single implementation JAR with sources directly under
`pangrova-file/src/main/java`; the former core child module is retired.
Its domain package names and service behavior are unchanged.

Object transfer types live in `org.pangrova.filesystem.api.object`, profiles in
`org.pangrova.filesystem.api.profile`, domain File control in `org.pangrova.spi.file`,
and stores in `org.pangrova.persistence.{api,jdbc}.file`. Retired package names have
no forwarding classes. Applications and connector plugins must rebuild against
the same SPI/artifact cohort and use its ServiceLoader descriptors. Other domain stores are still being migrated into Persistence.

Directory refresh depends on `FileDirectoryIndex`, injected by the application.
The PostgreSQL implementation owns scan attempts, page checkpoints, captured
Stage-definition checks and terminal publication. Core authorizes the scan and requests
Worker listing pages without opening file bytes. A different metadata provider
must preserve the same attempt fencing and publication semantics. Relational
directory scans still use their separate execution adapter.

Worker file endpoints consume Filesystem API. Its `ObjectStoreAccessorProvider` contract
lets the host select and own independent backend providers without SDK types in
the factory signature. The filesystem artifact owns confined LOCAL transfer and
multipart staging; the S3 artifact owns S3 transfer, response cancellation, SDK
clients and presigners. Providers have no Worker, File Core or Persistence JDBC dependency.
Worker retains HTTP authorization/deadlines and its directory execution adapter.

Each provider caches by the complete immutable profile, including credentials and
options. Delimiter-containing fields cannot collide, and a rotated secret or token
cannot reuse an earlier client or presigner. Invalid explicit S3 credentials fail
before use. The host stops active reads and closes every owned provider; each
provider closes its resources and preserves cleanup failure on repeated calls.
SDK clients receive a credential view that delegates resolution without exposing
close. The S3 provider closes its owned default chain once; injected credentials
remain caller-owned, including after clients and presigners have been allocated.

LOCAL mutation receipts describe the caller's sealed candidate bytes, even if a
concurrent writer replaces the published object before the method returns.
Conditional deletion holds the mutation lease through version comparison and delete.
Providers sharing canonical parent directories cooperate across overlapping roots,
classloaders and JVMs on the same host. JVM admission is acquired before opening the
OS lock channel; closing a waiting channel must not release another holder's lock.
Durability requires working file/directory force, atomic publication and advisory
locks. These tests do not qualify network filesystems or non-cooperating writers.

Multipart staging binds the upload id to the exact object key in a durable private
marker. Completion consumes that marker before publication. If publication or cleanup
has an uncertain outcome, completion cannot be replayed over a later writer. Abort
cannot resurrect parts and concurrent abort is idempotent. Private staging, mutation
lock and draft paths are hidden from object access/listing. Part names are locale
independent so another worker JVM can resume them.

**LOCAL upgrade:** drain or abort existing multipart sessions and deploy a consistent
writer cohort. Legacy unbound sessions fail closed; they cannot be completed by the
new provider. Ordinary object bytes retain their format. Do not mix old writers
that ignore the mutation protocol with the new cohort.

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

`pangrova-filesystem-api` has a separate content-publication contract used by governed
FILE values. Its LOCAL, S3 and OSS implementations do not grant Volume/Stage
access or provide directory metadata. In particular, OSS governed FILE support
does not implement OSS FileSpace transfer, COPY source admission or managed
storage. Those paths require their own authority, immutable-version and transfer
contracts.

Metadata provider tests live with the JDBC artifact and apply the canonical migrations
through the shared test kit. Object-transfer tests live with their physical provider;
Worker retains the cross-resource multipart renewal and HTTP expiry tests, using
public accessors rather than private SDK-stream classes. Cross-provider authority tests stay with the domain
that supplies the other provider, while Coordinator and integration tests retain
the production assembly and HTTP/SQL paths. Filesystem API tests load every API class using only its own output and Common.
SPI and Persistence API have separate exhaustive contract-classpath tests; no provider
URLs are filtered out of a larger runtime classpath.

See [Volume and Stage file access](../getting-started/catalogs/volumes-stages-files.md)
for operations and current qualification limits, and
[Worker configuration](../configuration/worker.md#filespace-storage-credentials)
for credential ownership and cache lifetime.
