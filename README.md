<p align="center">
  <img src="website/static/img/logo-transparent.png" alt="Pangrova" width="320" />
</p>

<h1 align="center">Pangrova</h1>

<p align="center"><strong>Distributed SQL for open lakehouses.</strong></p>

<p align="center">
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" alt="Apache 2.0" /></a>
  <a href="https://github.com/pangrova-io/pangrova/actions/workflows/ci.yml"><img src="https://img.shields.io/github/actions/workflow/status/pangrova-io/pangrova/ci.yml?branch=main" alt="CI" /></a>
</p>

<p align="center">
  <a href="https://pangrova-io.github.io/pangrova/">Website</a> ·
  <a href="https://pangrova-io.github.io/pangrova/docs/">Documentation</a> ·
  <a href="docs/getting-started/roadmap.md">Roadmap</a>
</p>

Pangrova is an open-source distributed SQL engine for open lakehouses.
It brings relational analytics and AI retrieval to lakehouse data through familiar
SQL clients, for teams building on existing open-format storage.

**Try it:** [Build and start Pangrova](#quick-start), then run the
[one-command Iceberg trial](docs/getting-started/quickstart.md#run-the-iceberg-trial-in-one-command).
The [SQL fixture](examples/iceberg-smoke.sql) creates a local events table and
aggregates it to two clicks and one purchase.

> **Status:** Under active development; production qualification is ongoing.
> Support varies by connector and execution path. Review the
> [current capability status](docs/getting-started/capability-status.md) before
> choosing features or evaluating a workload.

## CLI Demo

https://github.com/user-attachments/assets/ac907ae0-1a61-4912-a43e-805277bd1cc2

The walkthrough covers SQL, multimodal FILE data, scalar/full-text and vector
indexes, relational analytics, and table maintenance in a local Standalone
session. It was recorded from commit `050f8a313` with validated local changes;
it is not a production qualification or a validation of the current release.
See the [demo guide](docs/getting-started/clients/cli-demo.mdx) and
[matching SQL workflow](examples/cli-workflow.sql) for replay instructions.

## Highlights

- **Open lakehouse access** — query Apache Iceberg and Apache Paimon tables, and
  managed Lance datasets, with connector-specific capabilities and limits.
- **Native execution** — distributed, vectorized relational processing over
  lakehouse data, with columnar exchange between Workers.
- **AI through SQL** — vector and full-text retrieval primitives and governed model
  invocation, subject to supported query shapes, index bindings, and configured
  model providers.
- **Familiar clients** — JDBC, REST, and CLI entry points for applications and
  interactive data exploration.

## Architecture

<p align="center">
  <a href="website/static/img/architecture-overview.png">
    <img src="website/static/img/architecture-overview.png" alt="Pangrova architecture: clients, Java Coordinator, native Workers, control state, and lakehouse storage" width="100%" />
  </a>
</p>

The Java Coordinator uses Apache Calcite to plan relational queries and owns
scheduling, authorization, and final catalog commits. Workers execute relational
work through JNI in C++20 / Velox and exchange data and stream results using
Apache Arrow Flight.

See the [architecture overview](docs/architecture/overview.md) for component
boundaries and execution details; the diagram is not a feature-readiness matrix.

## Getting started

### Environment requirements

| Component | Requirement |
|---|---|
| OS | Linux (x86_64/aarch64) or macOS (Apple Silicon) |
| JDK | 17; set `JAVA_HOME` to the JDK installation |
| Maven | 3.9+ |
| Python | 3.10+ for contract generation |
| Gradle | 8.14.4 for the pinned Iceberg SDK |
| Native toolchain | CMake, Ninja, and a C++20 compiler |
| Rust / Cargo | Rust 1.98.1 with Cargo, pinned for the Lance FFI |
| Docker | Required for container-based tests |

Native dependency setup also requires Xcode Command Line Tools and Homebrew on
macOS, or OpenSSL and zlib development headers on Linux. Allow disk space for
native builds and Maven/Cargo caches, and size JVM/native memory for your host.
See the [developer guide](docs/contributing/developer-guide.md) for platform setup.

### First steps

Follow the [Quickstart](docs/getting-started/quickstart.md) to build from source,
configure a standalone instance, and run a first query. The first native build
compiles external dependencies and can take substantial time. Standalone mode
runs a Coordinator and Worker in one JVM for development and evaluation, not as
a production deployment recommendation.

Then run the [three-row Iceberg example](docs/getting-started/quickstart.md#end-to-end-example-query-iceberg-data).
The [SQL fixture](examples/iceberg-smoke.sql) creates a local events table and
aggregates it to two clicks and one purchase. The Quickstart also covers
[Docker setup](docs/getting-started/quickstart.md#docker-quick-start), liveness
versus SQL readiness, and troubleshooting.

Then explore:

- [SQL reference](docs/sql-reference/index.md) — syntax and execution constraints.
- [Configuration](docs/configuration/index.md) — settings and runtime options.
- [Deployment](docs/deployment/planning.md) — deployment choices and qualification.

## Contributing

Contributions, bug reports, and documentation improvements are welcome.
Start with [CONTRIBUTING.md](CONTRIBUTING.md) and the
[developer guide](docs/contributing/developer-guide.md); use the
[repository guide](docs/contributing/repository-guide.md) to find modules and
supported scripts.

Report vulnerabilities through [SECURITY.md](SECURITY.md), not public issues.

## License

[Apache License 2.0](LICENSE). Third-party acknowledgements are in [NOTICE](NOTICE).
