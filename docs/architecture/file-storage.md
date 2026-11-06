# File metadata and filesystem ownership

Physical I/O belongs to `pangrova-filesystem`; domain control belongs to its caller.
The parent contains api/local/s3/oss artifacts and supplies no runtime JAR.

| Artifact | Owns | Boundary |
|---|---|---|
| `pangrova-filesystem-api` | Blob/object transfer, locations, multipart receipts, presigned URLs and immutable storage profiles | Common/JDK only; no SPI, persistence, runtime or cloud SDK |
| `pangrova-filesystem-local` | Confined LOCAL transfer, byte-derived receipts, durable multipart state and mutation locks | Filesystem API/Common only |
| `pangrova-filesystem-s3` | S3 transfer, publication, cancellation, owned clients and presigners | Filesystem API/Common plus S3 SDK |
| `pangrova-filesystem-oss` | OSS governed FILE publication | Filesystem API/Common plus OSS SDK; object transfer/COPY remains pending |
| `pangrova-file-api` | FileSpace definitions, access grants, shared validation and typed authority/access/audit ports | Common, Filesystem API and Security API; no Protocol, HTTP/Jackson or implementation dependency |
| `pangrova-file-core` | Authorization, URL issuance and directory refresh | Injected File ports and persistence contracts; no Worker adapter or JDBC implementation dependency |
| `pangrova-file-worker-adapter` | Worker HTTP listing, conditional deletion and signed-URL exchange | File API, Filesystem API, Common and Jackson; no File Core or Worker Core dependency |
| `pangrova-persistence-file-api` | File space/profile revision, directory attempts and COPY claim/admission ports | Control-plane contracts; no JDBC, backend SDK or Worker dependency |
| `pangrova-persistence-file-postgresql` | File PostgreSQL implementations | Implements File persistence contracts; Security authority gates are supplied by `pangrova-persistence-security-postgresql`; initialization remains in `pangrova-schema-migrations` |

`pangrova-file` is an aggregator with API, Core and Worker Adapter child artifacts.
Server composition selects the adapter, and Core consumes its typed ports. The access and
URL services share `core.access` to keep their private authorization receipt together;
directory orchestration lives in `core.directory`. Existing service behavior is preserved.

Object transfer types live in `org.pangrova.filesystem.api.object`, profiles in
`org.pangrova.filesystem.api.profile`, domain File contracts in `org.pangrova.file.api`
and `org.pangrova.file.spi`,
and stores in `org.pangrova.persistence.file.{api,postgresql}`. Retired package names have
no forwarding classes. Applications and connector plugins must rebuild against
the same contract/artifact cohort and use its actual provider descriptors. Other domain stores are still being migrated into Persistence.

FileSpaceType now belongs to File API. File-specific RBAC grant matching belongs to the
built-in Security Policy provider, so Security API does not depend back on File API.
Shared FileSpaceLayout and CopySourceEndpointPolicy validation keeps control-plane admission
and Worker enforcement consistent without introducing a dependency on a storage implementation.

Directory refresh depends on `FileDirectoryIndex`, injected by the application.
The PostgreSQL implementation owns scan attempts, page checkpoints, captured
Stage-definition checks and terminal publication. Core authorizes the scan and requests
Worker listing pages without opening file bytes. A different metadata provider
must preserve the same attempt fencing and publication semantics. Relational
directory scans still use their separate execution adapter.

Worker file endpoints consume Filesystem API and signed File access descriptors, not
File Persistence API or PostgreSQL. The control-plane service resolves the current profile
revision and validates the namespace root before signing; Worker verifies the signature
before selecting the backend. The unused Worker registry resolver has been removed.

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
