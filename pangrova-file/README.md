# File

`pangrova-file` builds one JAR. Its `.api` packages own namespace values, grants, access and authority ports.
The `.core` packages implement authorization, signed URL issuance and directory refresh, and `.worker` implements the delegated HTTP operations. The File access service only requires space lookup and the current storage profile. The directory refresh service requires a fenced refresh attempt. The persistence registries implement these ports directly.
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
