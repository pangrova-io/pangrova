# Volume and Stage File Access

Volumes and stages name non-table file namespaces. This page documents the file
data plane that sits behind those definitions: how a client obtains access, which
operations exist, and what the coordinator never does.

Implementation ownership and provider boundaries are described in
[File metadata and object storage ownership](../../architecture/file-storage.md).

> Status: the access-ticket flow, single-request transfer, ranged reads, recursive
> listing, version-checked delete, multipart upload, presigned URL issuance, and the
> SQL `LIST @stage`, `REMOVE @stage`, and manual `ALTER STAGE ... REFRESH` are implemented. Local HTTP tests
> cover `Range`, `If-Match`, `If-Range`, and `HEAD`; MinIO contracts cover S3 transfers,
> version conditions, multipart, pagination, and presigning. A real 5 GiB LOCAL
> HTTP transfer passed with full-content checksum and range validation. S3 large-file,
> 100 GiB multipart, and large-directory qualification remain outstanding. SQL-level `DIRECTORY(@stage)`
> and `PUT`/`GET` are not implemented yet; use the HTTP surface below for those operations.

The previous experimental `directory_listing` Java provider has been removed. It
had no SQL execution binding and returned fabricated size and modification-time
values. Its removal does not implement `DIRECTORY(@stage)`; that requires the
authorized Native query path and the six documented Directory Table columns.

`FileSpaceDdlE2ETest` exercises authenticated SQL dispatch, PostgreSQL metadata,
and client result transport for Volume/Stage creation, description, alteration,
renaming, and deletion. It also verifies independent Volume/Stage namespaces,
metadata persistence across dispatcher rebuilds, dedicated external-volume
creation privileges, immediate privilege revocation, and preservation of external
source files on DROP. This uses the in-process Coordinator/Worker harness;
it is not independent-process or large-scale qualification.

Managed volumes and internal stages have a persistent physical root. Renaming a
space retains its files; creating a new space under an old name allocates a new
root and cannot expose the former space's files. Migration V96 preserves the
existing name-based roots in place, while newly created spaces use independent
UUID paths under `managed/volumes/` or `managed/stages/` in their storage profile.
Clients use logical names and must not construct physical paths. DROP currently
removes namespace metadata and access grants, while physical bytes remain;
automatic managed-file retention/deletion is not implemented. Previously issued
access descriptors retain their original physical scope until expiry.

## Why access is split in two steps

The coordinator owns authorization and metadata; the Worker owns storage
credentials. Sending bytes through the coordinator would violate that boundary,
and sending a raw path to a Worker would let any caller read any object the Worker
can reach. Pangrova therefore separates the two:

1. The client asks the coordinator for one operation on one path.
2. The coordinator authorizes the principal against the volume or stage and
   returns a **short-lived, credential-free descriptor** plus a Worker URL.
3. The client sends the descriptor, its HMAC signature, and its bytes to the
   Worker directly.

A descriptor names the space root, the space-relative path, the operation, and an
expiry. It carries no access keys. The Worker rebuilds the storage profile from its
own secret boundary and rejects any descriptor whose operation does not match the
target endpoint.

## Requesting access

```
POST /v1/{volume|stage}/{catalog}/{schema}/{name}/files/access
Content-Type: application/json

{"path": "images/a.jpg", "operation": "read"}
```

Supported operations:

| Operation | Endpoint it unlocks | Required privileges (volume / stage) |
|---|---|---|
| `head` | `POST /v1/file-space/head` | `READ_VOLUME` / `USAGE_STAGE` + `READ_STAGE` |
| `list` | `POST /v1/file-space/list` | `READ_VOLUME` / `USAGE_STAGE` + `READ_STAGE` |
| `read` | `GET /v1/file-space/object` | `READ_VOLUME` / `USAGE_STAGE` + `READ_STAGE` |
| `write` | `PUT /v1/file-space/object`, multipart endpoints | `READ_VOLUME` + `WRITE_VOLUME` / `USAGE_STAGE` + `WRITE_STAGE` |
| `delete` | `DELETE /v1/file-space/object` | `READ_VOLUME` + `WRITE_VOLUME` / `USAGE_STAGE` + `WRITE_STAGE` |

The response contains the Worker base URL, the base64url descriptor, its
signature, the operation, the normalized path, and the expiry in epoch
milliseconds. Paths are normalized before signing; empty or `.`/`..` segments,
backslashes, `%`, `?`, `#`, and prefix escapes are rejected.

## SQL stage listing

`LIST @stage` is the SQL entry point for the `list` operation. The statement
takes one ticket authorized by `USAGE_STAGE` and `READ_STAGE` for the requested prefix, then asks a Worker to
enumerate that prefix recursively:

```sql
LIST @media.raw.stage_landing;
LIST @media.raw.stage_landing/images/;
LIST @media.raw.stage_landing/images/2026/10;
LIST @media.raw.stage_landing/images/ PATTERN = '.*[.]jpg';
```

The result has four columns:

| Column | Type | Meaning |
|---|---|---|
| `name` | `VARCHAR` | `@catalog.schema.stage/<space-relative path>` |
| `size` | `BIGINT` | Object size in bytes |
| `md5` | `VARCHAR` | Content MD5 when the backend reports one, otherwise `NULL` |
| `last_modified` | `VARCHAR` | ISO-8601 instant from provider metadata |

The current generic S3 accessor returns `NULL` for `md5`. A 32-character ETag
does not prove a content MD5, including for encrypted objects. ETags remain opaque
provider identities for conditional access. See the [S3 object contract](https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.html).

`PATTERN` is a Java regular expression matched against the space-relative path
(for example `images/cat.jpg`, not the `@stage/...` display name); a path that
does not match is dropped. An invalid expression fails with
`VALIDATION_INVALID_ARGUMENT` before any listing request is issued.

The Coordinator pages through the Worker listing internally, so one statement can
return more rows than a single page; it gives up with `FILE_STORAGE_UNAVAILABLE`
after 10,000 pages instead of looping forever. The statement emits one
file-operation audit event per `LIST`, and fails closed with
`FILE_STORAGE_NOT_CONFIGURED` when the file plane has no signing secret.

## SQL stage removal

`REMOVE @stage` deletes the objects matched under a prefix. It takes the same
shape as `LIST` plus the `PATTERN` filter, and requires `WRITE_STAGE` on the
stage in addition to `USAGE_STAGE` and `READ_STAGE` (used to enumerate the match set):

```sql
REMOVE @media.raw.stage_landing/images/a.jpg;
REMOVE @media.raw.stage_landing/images/ PATTERN = '.*[.]tmp';
```

The result has two columns:

| Column | Type | Meaning |
|---|---|---|
| `name` | `VARCHAR` | `@catalog.schema.stage/<space-relative path>` |
| `result` | `VARCHAR` | `removed`, or `error: <reason>` when that object could not be deleted |

Behavior:

- The whole match set is listed before the first delete, so a listing failure
  leaves the prefix untouched. A delete invalidates nothing an in-flight list
  depends on.
- Each object is deleted with the provider version or ETag observed in the
  listing. An object that changed after the listing is reported as an error
  instead of being silently overwritten away.
- One audit event is emitted for the listing and one for each deleted path, so a
  bulk removal is attributable per object.
- A single failed delete does not discard the results of the objects that were
  removed; the failure appears in that row.
- The statement refuses to run with `FILE_LIMIT_EXCEEDED` when the match set
  exceeds 100,000 objects; narrow the path or `PATTERN` instead.
- `REMOVE` is the SQL surface for the `delete` operation; deleting an object that
  does not exist is not an error in object storage, and a prefix with no matches
  returns zero rows.

## Directory refresh

```sql
ALTER STAGE media.raw.stage_landing SET DIRECTORY = (ENABLE = TRUE);
ALTER STAGE media.raw.stage_landing REFRESH;
ALTER STAGE media.raw.stage_landing REFRESH SUBPATH = 'images/';
```

Manual refresh requires Stage ownership, `USAGE_STAGE`, and `READ_STAGE`. It
obtains one signed listing descriptor, requests bounded pages from the Worker,
and records actual path, size, last-modified, ETag, content type, provider version,
and optional MD5 in PostgreSQL. No file bodies pass through the coordinator.

Each page and its continuation checkpoint are stored atomically in a private
refresh area. An empty page with a continuation token is not the end of the scan.
Only a terminal page permits publication: the observed directory replaces its
published rows in a single transaction. Failures and privilege revocation leave
the previous published rows intact. Starting another refresh fences the prior
attempt; a stale completion or abort cannot modify the newer attempt.

`SUBPATH` uses directory boundaries, not SQL wildcards. Refreshing `images/`
preserves `images-backup/` and other directories. Dropping a Stage removes its
index; changing its URL/profile or toggling the directory option clears existing
observations and pending scans. Renaming retains metadata under the same registry
identity. A successful refresh emits an ALTER audit event with its subpath and
published object count.

This is a manual metadata refresh, not the `DIRECTORY(@stage)` Native query
implementation. Automatic write-through indexing, event-driven discovery,
restart/resume of an interrupted scan, and large-scale qualification remain
outstanding. A scan uses one immutable storage descriptor; expiration fails
without publication and requires a new refresh. The default descriptor lifetime
is five minutes. Multi-page object-store listings are not point-in-time snapshots.

The Worker index-read foundation has a separate, read-only PostgreSQL snapshot:
bounded keyset pages retain the same published observations across concurrent
refresh, rename or drop. New readers validate the namespace UUID and definition
revision, so an old reference cannot follow a recreated name. Cancellation and
query timeouts fail the read and release its transaction. This internal reader
is not yet connected to the Native Directory operator or a public SQL function;
it does not change the SQL availability stated above.

## Worker endpoints

| Endpoint | Purpose |
|---|---|
| `POST /v1/file-space/head?descriptor=&signature=` | Object metadata: size, last-modified, ETag, version |
| `POST /v1/file-space/list` | One page of a recursive listing under the descriptor path |
| `PUT /v1/file-space/object` | Single-request upload of at most 5 GiB |
| `GET /v1/file-space/object` | Read a byte range, optionally pinned to a version |
| `HEAD /v1/file-space/object` | Read metadata and HTTP validators using a read descriptor, without a body |
| `DELETE /v1/file-space/object` | Delete with a version precondition |
| `POST /v1/file-space/multipart/begin` | Start a multipart upload |
| `PUT /v1/file-space/multipart/part` | Upload one part |
| `POST /v1/file-space/multipart/complete` | Complete from the part ETags |
| `DELETE /v1/file-space/multipart` | Abort and release staged parts |
| `POST /v1/file-space/presign` | Issue a signed provider URL for one object |

The upload endpoints use `X-Pangrova-File-Descriptor` and
`X-Pangrova-File-Signature` headers; the others accept the descriptor and
signature as query parameters or in the JSON body, depending on the endpoint.

## Local storage confinement and listing

The local backend rejects symbolic links in every path component at or below the
configured profile root, including a Volume/Stage root, an intermediate directory,
and multipart staging or part files. Invalid paths return `FILE-0003` (HTTP 400)
before a read, delete, or upload creates directories outside the space. Recursive
listings do not follow symlinks.

The Worker administrator must control the local root and its ancestors. These
path checks do not provide isolation against another process replacing directories
concurrently, or against filesystem hard links. Use a separately owned local
storage root; arbitrary host filesystem mutation is not a supported storage API.

Local listings retain at most the requested page size plus one candidate path and
compute content hashes only for returned objects. The continuation token is a
lexical lower bound, so deleting the previous page's last object does not invalidate
the next page. Listings are not snapshots: concurrent additions before that lower
bound are not returned. Each page still walks the requested tree, so large-directory
qualification and the indexed Directory Table remain outstanding.

## Large objects

Single-request uploads are capped at 5 GiB. Larger objects use multipart:

1. `multipart/begin` returns an upload id and a recommended 64 MiB part size.
2. Upload each part with a 1-based `partNumber`; the response carries the
   provider ETag.
3. `multipart/complete` lists every part with its ETag and returns the final
   object metadata.

LOCAL multipart parts use SHA-256 ETags. Retrying a part replaces it atomically;
a truncated, oversized, or interrupted request preserves the previous accepted
part. Completion checks the supplied part ETags and sizes against the bytes being
assembled. Rejected part validation leaves the published object and staged parts
intact so the client can correct the part list and retry.

The upload id returned by `multipart/begin` is a signed session token bound to the
complete storage profile, its incarnation and revision, stable namespace ID,
authenticated subject and tenant, space root, object path, and an absolute 24-hour expiry.
The response exposes that deadline as `expiresAtEpochMillis`. Any Worker with
access to the same storage can accept the next part, complete, or abort the upload.
The token alone grants no file access: every request also requires a currently
valid signed write descriptor. Before the five-minute descriptor expires, obtain
a fresh `write` grant from the coordinator, which rechecks the current privileges,
and retain the original upload id. Renewal cannot extend the 24-hour session limit
or retarget it to a different subject, tenant, namespace incarnation, profile revision,
storage binding or object. Rename retains the namespace ID and permits continuation;
DROP/re-CREATE on the same external location cannot adopt the old session. This also
applies to raw provider IDs retained by the originating Worker.

Tickets are bearer capabilities until their original bounded deadline. Binding their
subject does not authenticate a different holder of an already issued ticket or
immediately revoke it. Every issuance resolves the authenticated principal's current ACTIVE tenant
membership and its catalog/schema allowlist from PostgreSQL. An authentication
`tenant_id` claim is optional; when present it must match current membership.
The issuer fills the descriptor tenant from that authority, so a claim cannot
invent membership or retain a tenant after removal. The shared authorization
transaction gate covers namespace/profile resolution, current RBAC checks and
signing; queued revocation takes effect before a waiting request can issue a ticket.
The same boundary applies to ordinary file tickets, presigned-URL tickets and
Stage links. A lock/storage failure rejects issuance. Previously issued tickets
retain their bounded bearer lifetime. Descriptors missing the new authority fields and older multipart
signatures are rejected; clients must obtain fresh tickets and begin new sessions
when upgrading across this contract change.

When another upload starts, the originating Worker aborts sessions past their
24-hour deadline. A failed abort retains ownership for a later cleanup attempt;
expiration of an individual request descriptor does not abort an active session.
Configure bucket lifecycle cleanup of incomplete multipart uploads as a backstop
for Worker crashes. Cleanup ownership is currently in memory and cleanup is
triggered by new uploads, so autonomous durable reclamation remains pending.

## Ranged reads and version pinning

`GET /v1/file-space/object` downloads the complete object when no range is supplied.
It accepts standard single-byte `Range` headers: `bytes=2-4`, `bytes=7-`, and
`bytes=-3`. Satisfiable ranges return `206` with the exact `Content-Length` and
`Content-Range`; an end past the object length is clipped. Unsatisfiable ranges
return `416` with `Content-Range: bytes */<size>`. Unknown range units and
multi-range headers are ignored and return the complete representation (`200`).
Malformed single-byte ranges return `400`.

The query parameters `offset` and `length` also select a window. An omitted length
or `length=-1` reads to EOF. Do not combine these parameters with `Range`.
An empty object returns `200` with zero length; a byte range against it returns
`416`.

Responses include a quoted `ETag`. `If-Match` accepts a strong entity tag, a list
of strong tags, or `*`; a nonmatching or weak tag returns `412` (`FILE-0002`).
Conditions are evaluated before range selection. `If-Range` resumes a range only
for the current strong ETag; a stale tag, weak tag, or date causes a complete
`200` response. `HEAD` returns read metadata without opening a body stream.
These behaviors follow [HTTP semantics (RFC 9110)](https://www.rfc-editor.org/rfc/rfc9110.html).

Worker rechecks the original ticket expiry after its metadata lookup, before opening
the provider body and after the open returns. An expired open is closed before
returning a successful HTTP response. Admitted streams check the same deadline
before and after reads. An endpoint-owned timer also closes each opened body at
that original deadline, aborting an unread S3 response instead of draining it.
Closing the stream retires its timer; stopping the endpoint closes active bodies
before provider clients and preserves cleanup failures. This does not impose a
deadline on HEAD/body-open calls, provide an idle-read timeout, or actively revoke
an otherwise unexpired ticket after an RBAC change.

`version` is a provider-version precondition, not an HTTP entity tag or a request
for historical content. A stale version returns `412`. Every download also pins
the opened stream to the version observed during metadata lookup, even when the
client supplies no condition. S3 reads use an exact version ID, or a conditional
GET when the bucket is unversioned. The local backend computes SHA-256 with a
bounded buffer and checks the opened file handle, so same-size replacements
with preserved timestamps invalidate the old validator. This adds content-read
I/O to local metadata/listing operations; large directory performance is not yet
qualified. Local uploads publish by atomic replacement; direct in-place writes
by external filesystem processes during a read are outside that guarantee.

## Signed URLs

A client can exchange one authorized object for a provider-signed URL instead of
streaming bytes through the Worker:

```
POST /v1/{volume|stage}/{catalog}/{schema}/{name}/files/presign
Content-Type: application/json

{"path": "images/a.jpg", "read": true, "ttlSeconds": 600}
```

The response carries the signed URL, its expiry in epoch milliseconds, the
normalized path, and the mode (`presigned-read` or `presigned-write`). The
coordinator authorizes the request exactly like any other transfer, takes the
matching `read` or `write` ticket, and asks the Worker named by that ticket for
the URL:

- `read: true` requires `READ_VOLUME` / `READ_STAGE` plus `USAGE_STAGE`.
- `read: false` requires `READ_VOLUME` + `WRITE_VOLUME` for a volume, or
  `USAGE_STAGE` + `WRITE_STAGE` for a stage.
- A non-positive `ttlSeconds` takes the 3600-second default; values above seven
  days are clamped to seven days.
- Issuance emits a `FILE_URL_ISSUED` audit event containing the mode, lifetime,
  path, and Worker endpoint. The URL itself is a bearer credential and is never
  written to the audit trail or the logs.

The URL is **bearer** access: anyone who holds it can use it until it expires,
regardless of the caller's identity. It is not a scoped URL bound to the
requesting user, and revoking a privilege does not cancel an already-issued URL.

Underneath, `POST /v1/file-space/presign` on the Worker does the signing; S3
compatible backends return a presigned GET or PUT URL, while the local backend
fails closed with `FILE-0006` because it cannot sign URLs.

## Role-authorized Stage URLs

Create a stable, credential-free Stage link through the Coordinator:

```http
POST /v1/stages/{catalog}/{schema}/{name}/files/stage-url
Authorization: Bearer <user-token>
Content-Type: application/json

{"path": "images/a.jpg"}
```

The response is `{"url":"https://<coordinator>/v1/stage-files/<stage-id>?path=images%2Fa.jpg"}`
(the URL encoding of `/` may vary). Each `GET` or `HEAD` of this URL requires user
authentication and checks current durable grants: an internal Stage requires
`READ_STAGE`; an external Stage requires `USAGE_STAGE`. Construction and access
hold the PostgreSQL authority gate while resolving the Stage, grants and storage
profile. A concurrent committed revoke is therefore observed before a queued
access can issue a ticket.

The Stage ID survives rename, while DROP permanently invalidates the link.
Creating another Stage under the old name cannot retarget it. The path refers to
the current object, without pinning a historical provider version. Missing files
are reported by the Worker when accessed; constructing a link does not check
whether the object exists.

An authorized access returns a `307` redirect to a five-minute Worker read ticket
with `Cache-Control: no-store` and `Referrer-Policy: no-referrer`. The Coordinator
never receives file bytes. Follow that redirect with the same `Range` and
`If-Match` headers when needed; `HEAD` remains `HEAD`. User credentials are only
needed at the Coordinator. The stable link itself is role-authorized; the
short-lived redirect target is a bearer ticket and remains usable until its
expiry even if a privilege is subsequently revoked. Do not persist or log the
redirect target. This is not a user-bound scoped URL.

Both LOCAL and S3-compatible storage can serve these links. LOCAL external
Stages require an absolute `file:///...` location inside the configured bucket
root and profile prefix; sibling prefixes and other URI schemes are rejected.
The existing local accessor additionally checks filesystem symlinks.

The REST path is implemented independently of SQL; `BUILD_STAGE_FILE_URL` and
`DIRECTORY(@stage)` still require Native SQL planning/execution wiring.

## FILE column storage modifiers

`FILE` columns keep the same five physical fields in every mode:

```
STRUCT<uri VARCHAR, mime_type VARCHAR, size BIGINT, checksum VARCHAR, created_at TIMESTAMP>
```

| Declaration | Meaning |
|---|---|
| `FILE` or `FILE EXTERNAL` | The row references an existing object; the engine does not own its lifetime |
| `FILE MANAGED` | The declaration records that bytes belong in managed storage |

`FILE MANAGED` is parsed, mapped to the same struct, and recorded in table
properties (`pangrova.file.storage.<column> = MANAGED`). The managed-copy write
path is not implemented yet, so writes fail closed: a non-null value in a
`FILE MANAGED` column is rejected with `FILE-0006` rather than silently storing an
external reference. Upload through the Files API and use `FILE EXTERNAL` until the
copy path lands.

## Not implemented yet

- `DIRECTORY(@stage)`, automatic index updates and refresh scheduling (Volume directories use the Files API)
- `PUT @stage`, `GET @stage`
- SQL URL functions (`GET_PRESIGNED_URL`, `BUILD_SCOPED_FILE_URL`,
  `BUILD_STAGE_FILE_URL`); REST `files/presign` and `files/stage-url` are available
- Scoped (user-bound) URLs and temporary volume credentials
- `COPY INTO` URI sources, `read_files`, and external tables


The Worker owns its file endpoint's object-store clients and closes them when HTTP
service stops or startup fails. Factory closure rejects later client allocation;
repeated closure waits for the same cleanup and preserves its failure. Standalone
embedders must close `FileSpaceFileResource` (or its owning HTTP container). The
factory owns its separate default credential chain; injected credential providers
remain owned by their caller. This lifecycle does not reclaim durable provider
multipart uploads or implement active transfer revocation.


S3 range reads require the provider response to prove the requested start/end, total
object size and body length. Missing or contradictory range evidence fails before
file bytes are delivered. Closing a partial or failed response aborts its connection
without draining the remaining object; completed reads retain normal connection reuse.
This is independent of the remaining idle-deadline and active-revocation work.

S3 PUT and multipart-completion receipts describe the version acknowledged by
that operation. Metadata lookup uses its returned immutable version ID and ETag;
unversioned or mutable `null` versions use an ETag condition. A concurrent writer
cannot turn a newer current object into the earlier operation's receipt. S3
returns version identity with [PUT](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html)
and [multipart completion](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html).
The Worker credentials must permit metadata lookup for that returned version.
If metadata verification fails after acknowledgement, the write has already
committed; the accessor reports that context without replaying it. Durable
operation-outcome reconciliation remains an open transfer-lifecycle gate.
