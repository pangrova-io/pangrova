# Blob publication and provider ownership

`pangrova-storage` is an aggregator with three independently usable artifacts:

| Artifact | Exported package | Dependencies and responsibility |
|---|---|---|
| `pangrova-storage-api` | `org.pangrova.storage.api.blob` | Common stability annotations and JDK contracts; no provider, JDBC or cloud SDK |
| `pangrova-storage-filesystem` | `org.pangrova.storage.filesystem.blob` | API and JDK filesystem publication |
| `pangrova-storage-s3` | `org.pangrova.storage.s3.blob` | API and AWS SDK publication, encryption, backend identity and bounded S3 transport |

`BlobObjectId` groups the opaque tenant-scope SHA-256 token, content SHA-256 and
exact nonnegative length. It validates both tokens before paths or requests are
constructed. Possessing an object identity does not grant access. Governed FILE
owns current task identity, tenant capabilities, MIME checks and media budgets;
COPY owns admission, source-version proof and retry decisions.

`BlobStorage` publishes immutable content and opens exact objects. Caller staging
remains caller-owned. `ManagedBlobStorage` adds physical backend binding,
pre-request byte admission and an abortable read independent of SDK stream
classes. S3 admitted requests disable hidden payload retries. Aborting a rejected
managed read prevents a later close from draining the rejected body.

The FILE factory explicitly selects `s3` or `local`. A missing secret, missing
backend settings or unsupported backend fails registration. No fallback backend
is selected. The local setting is `worker.ai.file.storage.local.root`; see
[Worker configuration](../configuration/worker.md). Applications needing only the
API or filesystem provider do not acquire the AWS SDK transitively.

The real SQL/JNI acceptance has separate S3 and LOCAL deployments. S3 runs in
the shared E2E process; LOCAL runs `LocalGovernedFileSqlE2ETest` in its own JVM
with `-Dpangrova.e2e.file.backend=local`, because the Native callback catalogue
has one process-wide owner. Both require the temporary FILE capability key and
the prepared model image described in the [build pipeline](../../.pangrova/rules/build-pipeline.md).
LOCAL checks null/conditional behavior, volatile invocations with content
deduplication, independent reads of published bytes, and FILE column writes and
subsequent Iceberg scans. These single-process tests do not qualify a replicated
filesystem deployment or LOCAL COPY sources.

Filesystem publication copies the completed caller draft into a private file in
the destination filesystem. It checks exact length and SHA-256, forces the file,
and atomically creates the public hard link without replacing an existing object.
Directory entries are forced as they are created. Concurrent publishers verify
the winner's bytes. Reads validate the opened file handle and rewind that same
handle before returning it. Modifying or deleting caller staging cannot modify
published content. Failed verification never creates the published object.

Configured roots and their ancestors must be administrator-controlled. Symbolic
links at or below the root and symlink staging files are rejected. Concurrent
external namespace edits by an administrator are outside the isolation boundary.
Workers sharing FILE references must mount the same persistent namespace; local
disk does not provide replication. Private publication drafts are removed on
normal and exceptional completion; abrupt process death can leave unreferenced
drafts, which are never addressable through the object API.

COPY's source adapter still pins exact S3 object versions, rechecks current
admission, and verifies staged bytes before materialization. Its managed backend
is supplied through the API by process assembly. This change does not enable
LOCAL or OSS COPY sources. OSS needs an independent provider with its own client,
version/encryption policy, identity and qualification; an enum value or an
S3-compatible endpoint alone is not evidence that this provider exists.
