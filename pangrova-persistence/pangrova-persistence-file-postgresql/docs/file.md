# File persistence

`pangrova-persistence-file-api` owns File space/profile revision, directory refresh
and COPY source claim/admission contracts under `org.pangrova.persistence.file.api`.
File domain values, authorization and access contracts remain in `pangrova-file`;
physical object access belongs to Filesystem. No universal StorageProvider is introduced.

`pangrova-persistence-file-postgresql` owns the existing six PostgreSQL implementations.
Each borrows an externally owned DataSource, acquires and closes operation connections,
and leaves pool and schema lifetime to assembly. Initialization uses only the canonical
`pangrova-schema-migrations` resources. The module extraction preserved SQL, triggers, lock order, commit points and execution
fencing. Subsequent failure-path fixes keep those success semantics intact.

- Storage profile creation preserves immutable incarnation/revision history; historical
  lookup never falls back to current. Durable registration rejects unconditional replacement.
- Volume/Stage mutations preserve immutable identity and managed roots, and invalidate
  affected directory metadata in the same transaction.
- Directory refresh uses captured Stage identity, refresh attempt, expected page token
  and terminal scan publication. Partial scans do not replace the published index.
- File authority resolves current tenant membership under the authorization publication
  gate. Callers supply the same configured tenant authority; no implicit Memory authority exists.
- COPY source admission owns issuance, redemption, immutable staged proof and frozen-input
  derivation. It rechecks current authorization, profile revision and execution ownership.
  `CopySourceReadAdmissions` and its immutable values belong to the persistence API.
  Coordinator resource/binder code uses that port; only Server production assembly chooses
  JdbcCopySourceReadAdmissions. The API has no JDBC/SDK types, configuration loader or
  fallback implementation. Existing canonical S3 value restrictions remain explicit.
- COPY file claims retain their existing explicit incomplete URI-publication wiring status.
  Class presence and backend tests are not evidence of an assembled SQL capability.

The test-only `InMemoryStorageProfileRegistry` belongs to Testing Kit. Runtime modules
consume Testing Kit only in test scope; no production File Memory artifact is created.
The fixture is not advertised as a durable or cross-process implementation.

File Core depends on the narrow persistence API. Server explicitly selects the actual
backend. Worker uses the signed descriptor and Filesystem provider contracts; it does not
carry File Persistence API or PostgreSQL. The unused registry-based Worker resolver has
been removed. Layout, profile selection and backend selection tests remain on the actual
control-plane descriptor and Worker provider paths.

Tests cover contracts with a minimal classpath, the real provider classpath, PostgreSQL
CRUD/identity/concurrency behavior and consuming SQL/HTTP paths. They initialize temporary
schemas; no migration or cleanup of user data is part of this refactor.

The admission API documents exact replay, concurrent callers, current authority checks,
unknown commit outcomes and assembler-owned resources. Local canonical-value validation
is shared within the contract artifact. Existing digest encoding stays unchanged; the
API's value helper delegates to the same encoding implementation. There are no forwarding
classes for the former PostgreSQL nested value FQNs.

| Owner | Depends on |
| --- | --- |
| Server assembly | File PostgreSQL, Coordinator resource/binder |
| Coordinator resource/binder | File persistence API |
| File PostgreSQL | File persistence API, JDBC authority/execution fence |

Coordinator Core carries the File PostgreSQL artifact only in test scope. A production
closure gate enforces that File/Coordinator/Worker Core and the File contract cannot
select this backend transitively. This does not claim other remaining backend families
have already completed their separate extraction.

Each transaction owner explicitly attempts rollback on SQL exceptions, unchecked exceptions
and Errors after transaction initialization. Profile/space authority gate acquisition is
inside this cleanup boundary, before row mutations. Cleanup failures are suppressed on the
original failure (with a self-suppression guard); try-with-resources still closes the owned
connection. The original unchecked/Error instance is rethrown; SQL failures keep the existing
domain exception with the SQL cause. Pool ownership remains with Server.

Real PostgreSQL fault tests inject failures after mutations, directory checkpoint/publication
and authority locks, then compare durable facts and perform a fresh operation. An injected
rollback failure still permits physical connection close; these tests do not prove that a
network failure or lost commit confirmation always rolled back. Unknown commit outcomes
retain the contract's confirmation requirement; no automatic retry or Memory fallback is added.
