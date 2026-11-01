# Persistence Memory

Process-local backends implement Persistence API and shared SPI contracts. Packages
separate `coordination` leases/registries and Query `coordination`, `state`,
`transaction`, and `audit` storage. Every store belongs to an explicit instance;
there is no mutable global journal or audit cache. None survives process restart
or provides inter-process exclusion. Durable deployments select JDBC or another
explicit durable backend.

The module has no dependency on Query runtime, Coordinator, Security, Metadata,
JDBC drivers, filesystem providers or quorum SDKs. The audit backend uses Caffeine
for bounded cache admission and SLF4J for the existing structured audit log.
Coordination implementations operate without those libraries on their minimal
classpath; isolation tests verify this distinction.

Lease operations sample the authoritative Clock inside the per-key atomic mutation,
including after lock contention. Release and expiry preserve the epoch high-water
mark. Query journals retain their bounded admission and exact event semantics;
write journals admit PENDING intents only and preserve terminal observations on retry.

Application assembly owns sharing: QueryAuditServices connects admission and profile
reads over one store, while separate Coordinator bootstraps get separate state.
Consumer lifecycle and end-to-end tests remain with their application owners.

```bash
mvn -pl pangrova-persistence/pangrova-persistence-memory -Pfull-regression,static-analysis verify
```
