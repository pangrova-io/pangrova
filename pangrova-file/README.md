# File

`pangrova-file` builds one JAR. Its `org.pangrova.file.api` packages own namespace
values, grants, directory contracts and the authority for current space access.
The built-in Security Policy provider matches file privileges. FileSpaceLayout and
CopySourceEndpointPolicy validate confined roots and admitted COPY origins on both roles.

`file.access` authorizes transfers and verifies signed URL receipts. FileGrantAudit
records signed ticket and URL issuance via the Security AuditEventWriter contract.
`file.directory` manages fenced directory refresh attempts. The PostgreSQL registries
implement the corresponding persistence ports; object bytes are served by the Worker.
`file.worker.WorkerFileClient` performs delegated HTTP operations with the issued ticket.

The directory refresh keeps the immutable grant across pages, rechecks permission, and
atomically publishes or aborts the projection through the persistence port. URL issuance
checks the provider receipt against the authorizing grant. Worker HTTP clients preserve
interruption, timeout and status handling.

The Worker records completed object operations from its data-plane endpoint. Download
completion covers the requested bytes written and flushed on the Worker. A provider
URL can be used independently after issuance. See the [file audit event definitions](../docs/getting-started/catalogs/volumes-stages-files.md#file-audit-events).
