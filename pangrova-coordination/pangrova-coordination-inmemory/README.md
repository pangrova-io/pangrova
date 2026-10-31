# Coordination In-Memory

This provider supplies `InMemoryDistributedLease`, `InMemoryCoordinatorRegistry`
and `InMemoryWorkerRegistry` through `org.pangrova.coordination.inmemory`.
Applications compose these implementations explicitly and consume the contracts
in `pangrova-coordination-api`.

The module depends only on Coordination API and Common. It has no dependency on
the Coordinator process, Metadata, JDBC drivers or quorum SDKs. Its state belongs
to one JVM instance; restarting it discards registry state and lease epochs.
Multi-process deployments select a durable or quorum-backed provider instead.

Lease decisions retain the supplied `Clock` and atomic map operations. Release
and expiry retain the per-key epoch high-water mark during the provider's
lifetime. Registry heartbeats and stale-entry expiry retain their existing wall
clock and atomic update semantics.

Provider unit and property tests live here. HA lifecycle, query ownership and
WorkerManager integration tests remain with their consumers in Coordination
Core. The classpath isolation tests exercise the provider with only its API,
Common and the JDK, and reject Coordinator, database and quorum implementations
on the module's ordinary test classpath.

From the repository root, after installing prerequisites:

```bash
mvn -pl pangrova-coordination/pangrova-coordination-inmemory -Pfull-regression,static-analysis verify
```
