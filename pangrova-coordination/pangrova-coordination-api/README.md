# Coordination contracts

This artifact owns lease, coordinator/worker registry and immutable topology
contracts. It depends only on Common; applications and ZooKeeper providers depend
on this API without importing each other's runtime implementation.

The Java package is `org.pangrova.coordination.api`, replacing the former
Coordinator-owned package. Consumers must update imports and recompile; no old
package forwarding types remain. Snapshot fields, lease semantics and stored
registry payloads are unchanged. Per-type stability annotations remain explicit.

`CoordinationApiIsolationTest` links every contract using only JDK, Common and this
artifact. The ZooKeeper owner additionally verifies absence of Coordinator runtime
and exercises the real provider against a local ZooKeeper server. The Coordinator
continues to own backend selection and provider shutdown.
