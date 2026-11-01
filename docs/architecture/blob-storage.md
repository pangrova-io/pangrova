# Blob publication and provider ownership

`pangrova-storage` is an aggregator with four independently usable artifacts:

| Artifact | Exported package | Dependencies and responsibility |
|---|---|---|
| `pangrova-storage-api` | `org.pangrova.storage.api.blob` | Common stability annotations and JDK contracts; no provider, JDBC or cloud SDK |
| `pangrova-storage-filesystem` | `org.pangrova.storage.filesystem.blob` | API and JDK filesystem publication |
| `pangrova-storage-s3` | `org.pangrova.storage.s3.blob` | API and AWS SDK publication, encryption, backend identity and bounded S3 transport |
| `pangrova-storage-oss` | `org.pangrova.storage.oss.blob` | API and official Alibaba OSS SDK; independent publication, encryption and cancellable HTTPS transport |

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

The FILE factory explicitly selects `s3`, `local` or `oss`. A missing secret, missing
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
LOCAL or OSS COPY sources. The OSS provider implements `BlobStorage`; it does
not implement `ManagedBlobStorage`, source-version resolution or COPY backend
fingerprinting.

OSS uses the official `com.aliyun:alibabacloud-oss-v2:0.6.1` client, rather than
the AWS SDK with another endpoint. Publication requires a dedicated bucket that
has never enabled versioning: OSS ignores `x-oss-forbid-overwrite` when versioning
is enabled or suspended. The provider checks bucket versioning before each PUT,
sends `forbid-overwrite=true`, accepts only `409 FileAlreadyExists` as an existing
winner, and rejects every returned object version id, including the literal
`null`. Administrators must keep bucket versioning and the content namespace
unchanged during use; the preflight cannot atomically fence administrator changes.
See the official [PutObject contract](https://www.alibabacloud.com/help/en/oss/developer-reference/putobject)
and [SDK source](https://github.com/aliyun/alibabacloud-oss-java-sdk-v2).

An OSS publication first creates a private bounded copy of caller staging and
verifies its length/SHA-256. Upload uses that snapshot, a Long body length,
Content-MD5 and the SDK CRC64 check. MD5 is an OSS transport checksum, never the
object identity or an authentication primitive. HEAD must prove the exact
size/hash metadata and requested AES256 or exact KMS key policy; a conflict also
reads and hashes the actual winner. Reads check metadata/encryption before
returning a stream and validate consumed content against SHA-256. A partial close
or integrity rejection cancels the socket before closing the response. The
provider owns its client and connection pool; an embedding constructor can supply
credentials and TLS trust without replacing its transport policy.

OSS transport has verified HTTPS, bounded connect/socket/pool waits, at most
64 connections, a 64 KiB control-response limit, and no redirects or hidden
retries. It streams object responses of every size. This avoids the pinned SDK's
default Apache 5 abort path, whose graceful response close drains unread content.
Private upload drafts are deleted on normal/exceptional completion; abrupt process
death can leave unreferenced system-temporary drafts. No remote object is published
until the entire local snapshot passes validation.

OSS SDK/HTTPS protocol tests and factory registration are local qualification.
The HTTP fixture is not an OSS deployment. Real cloud OSS SQL, RAM/KMS permissions,
and multi-Worker qualification remain pending; passing S3/LOCAL tests does not
close those requirements.
