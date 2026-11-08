# File

`pangrova-file` aggregates two artifacts:

| Artifact | Responsibility |
| --- | --- |
| `pangrova-file-api` | Namespace values, grants, directory references, errors, path/origin validation, and typed extension ports |
| `pangrova-file-core` | Current-tenant authorization, signed descriptor issuance, URL receipts, directory refresh, and the Worker HTTP client (`org.pangrova.file.worker`) |

The API artifact owns both `org.pangrova.file.api` and `org.pangrova.file.spi`.
Its SPI packages contain real access, audit and authority ports. The API depends on Common,
Filesystem API and Security API; it does not depend on HTTP/Jackson, persistence, Core, a
storage SDK or Worker implementation. FileSpaceType belongs to this domain rather than Common.
The built-in Security Policy provider owns File-specific privilege matching; the Security
API retains generic privilege values and does not depend back on File API.

FileSpaceLayout and CopySourceEndpointPolicy are shared pure validation contracts. The
control and data planes must agree on confined roots and admitted COPY origins. Their
presence in API does not open a filesystem, instantiate a provider or select a backend.

Core consumes FileListingClient, FileUrlClient and the current authority/audit ports through
constructors. Server composition selects WorkerFileClient. The Worker HTTP client lives in Core and
cannot depend on persistence or Worker Core. Its private wire records
retain the existing Worker request fields, HTTP status handling, timeouts and interruption
behavior. Explicit Jackson time-module dependencies belong to the implementations.

Credentials remain on the Worker. URL issuance validates the provider receipt expiry against
the authorizing grant and omits bearer values from diagnostics. Directory refresh preserves
the immutable grant across pages, rechecks permission, and atomically publishes or aborts the
projection through the existing persistence port. Cleanup exceptions remain suppressed on
the primary failure. This extraction does not alter these guarantees.

The access and URL orchestration classes currently share `core.access`: their private
authorization receipt and audit boundary remain together. Broader protocol/codec separation,
File persistence artifact separation, Worker file-plane capability extraction and process
resource lifecycle review remain part of the approved plan. The Java 17 HTTP client behavior
is unchanged; this batch does not claim a new cancellable asynchronous transport contract.

Existing Core integration tests continue to live with their current Coordinator/SQL fixtures;
the independent HTTP client tests live in Core. File API isolation scans all its
compiled API/SPI types, while the remaining central Indexing contract retains a separate
isolation test. No data migration, compatibility alias or new storage backend is introduced.
