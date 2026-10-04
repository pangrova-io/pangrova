# Connector query lifetime

Connector read admission uses the authority issued by the actual Coordinator
query creation owner. The public authority binds the query state machine to the
same `TaskIdentityRegistry.Scope`; session copies preserve that object. A query
identifier or session property cannot create a read lifetime.

The authority is indexed before compilation. A selected catalog lookup retains
an operation permit, retains the original authenticated metadata instance before
calling BEGIN, and keeps a failed BEGIN instance for cleanup. Catalog BEGIN is
single-flight. Schema discovery without an issued query remains metadata-only;
it does not register splits or acquire readers.

Cleanup requires closed admission and the exact Scope's issued tasks and
operations to drain. The drain check claims cleanup and is not a read-only
status query. Metadata cleanup traverses retained instances outside locks;
successful instances are released once and failed instances remain retryable.
Pending terminal transitions reject new operations but are not physical-drain
evidence; a failed terminal recorder can roll back pending admission closure.

Paimon retains selected SDK tables, splits, and readers in the same registry
query Scope. A reader owns its actual operation permit until SDK reader close
succeeds. The first read binding is frozen per table; changing an explicit
snapshot or schema selector fails rather than silently repinning. Reusing the
exact historical bound handle, including its historical schema, remains valid.
Limit-only variation preserves the selected source. This does not implement
multiple historical occurrences of the same table in one query.

Connector shutdown seals admission before checking readers and incomplete SDK
binding resolutions. A rejected close retains ownership and leaves the catalog
open for retry after actual I/O exits. Diagnostics expose only pending split and
active reader counts, not mutable registries or SDK read handles.

The component tests use real file catalogs and SDK scans. A shutdown-race test
uses Paimon's `DelegateCatalog` solely to schedule `getTable`; all catalog
operations and results still delegate to the actual SDK. These tests do not
establish a Coordinator JVM data-reading production route or end-to-end SQL
execution. Compilation, discovery, quality gates, and runtime validation of this
revision remain to be performed by the build owner.
