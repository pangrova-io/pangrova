---
title: "Speculative Execution E2E"
description: "Independent-process validation of speculative execution"
---

# Independent speculative execution validation

`IndependentSpeculativeExecutionE2ETest` runs the ten speculative execution cases
through `IndependentLocalProcesses`: one production Coordinator JVM, two production
Worker JVMs with distinct PIDs and endpoints, and separate public CLI JVMs. PostgreSQL
and LDAP use the existing container fixtures. A Pangrova application image is not a
prerequisite for this local validation path.

Provide `pangrova.e2e.local.lib-dir`, `pangrova.e2e.local.jar-manifest` and
`pangrova.e2e.local.native-lib` as for `IndependentLocalProcessesE2ETest`. The runtime
inventory contains `lib/` with ordinary dependencies and the platform Native bundle,
plus `cli/` with exactly one `pangrova-client-cli-*-exec.jar`. `runtime.sha256` is
stored beside those directories and lists their relative paths (`cli/...`, then
`lib/...`, sorted within each directory). Its SHA-256 manifest and
all embedded Native copies must match the qualified Release library. Freeze the
installed production dependencies before running so concurrent builds cannot change
child JVM inputs. This verifies that frozen runtime, not uninstalled working-tree
changes or a Linux release image.

The executable CLI is never a service or test classpath dependency. Its main Maven
artifact remains thin; the attached `exec` artifact contains dependencies for its
separate JVM. This follows Maven Shade's
[attached-artifact contract](https://maven.apache.org/plugins/maven-shade-plugin/examples/attached-artifact.html).
The POM retains normal transitive dependencies for thin-artifact consumers instead
of publishing a dependency-reduced POM.

The report relay retains the original Worker registration and translates only its
HTTP control port to a transparent local forwarding proxy. Capabilities, worker
identity, task reports, ACKs, Native result endpoints and TLS identities remain real
production values. Flight result bytes travel directly between the real processes.
The fault cases delay actual terminal reports, temporarily hide actual source files,
or disconnect a real task submission before its Worker receives it. They still
require canonical execution-drain receipts and durable admission-owner release.

Run one method to qualify startup, then select only the remaining methods for the
incremental pass. The full class remains the final matrix selector:

```bash
mvn -pl pangrova-testing/pangrova-testing-integration-tests test \
  -Dtest=IndependentSpeculativeExecutionE2ETest \
  -Dpangrova.e2e.local.lib-dir="$runtime_dir/lib" \
  -Dpangrova.e2e.local.jar-manifest="$runtime_dir/runtime.sha256" \
  -Dpangrova.e2e.local.native-lib="$release_library"
```

A missing artifact, rejected runtime hash, child startup failure, or failed scenario
is a test failure; the fixture does not skip or replace it with an in-process worker.
