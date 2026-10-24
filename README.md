<p align="center">
  <img src="website/static/img/logo.png" alt="Pangrova" width="360" />
</p>

<h1 align="center">Pangrova</h1>
<p align="center">Distributed SQL for open lakehouses.</p>
<p align="center">
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" alt="Apache 2.0" /></a>
  <a href="https://github.com/pangrova/pangrova/actions/workflows/ci.yml"><img src="https://img.shields.io/github/actions/workflow/status/pangrova/pangrova/ci.yml?branch=main" alt="CI" /></a>
  <img src="https://img.shields.io/badge/Java-17-orange.svg" alt="Java 17" />
  <img src="https://img.shields.io/badge/C++-20-blue.svg" alt="C++20" />
</p>

Pangrova is an open-source distributed SQL engine for lakehouse analytics and AI
retrieval. It combines Apache Calcite planning with C++20 / Velox execution,
Arrow Flight data exchange, and connectors for Apache Iceberg and Apache Paimon.

**Under active development.** Capabilities vary by execution path and connector;
production qualification is ongoing. See [current capability status](docs/getting-started/capability-status.md)
before evaluating a workload.

[Documentation](https://pangrova.github.io/pangrova/) ·
[Quickstart](docs/getting-started/quickstart.md) ·
[Roadmap](docs/getting-started/roadmap.md) ·
[Contributing](CONTRIBUTING.md)

## Architecture

```mermaid
flowchart TB
    clients["Clients: CLI / JDBC / REST"]
    coordinator["Coordinator · Java<br/>Parse · authorize · plan · schedule<br/>Catalog metadata and commit coordination"]
    state[("JDBC control state / catalog metadata")]
    subgraph workers["Worker processes"]
        direction LR
        subgraph wa["Worker A"]
            jvmA["Java task lifecycle / connector callbacks"]
            nativeA["JNI → C++ / Velox<br/>Scan · compute · write"]
            jvmA --> nativeA
        end
        subgraph wb["Worker B"]
            jvmB["Java task lifecycle / connector callbacks"]
            nativeB["JNI → C++ / Velox<br/>Scan · compute · write"]
            jvmB --> nativeB
        end
        nativeA <-->|"Arrow Flight exchange"| nativeB
    end
    lake[("Lakehouse data<br/>Iceberg / Paimon · object storage / files")]
    clients -->|"HTTP: SQL / status / cancel"| coordinator
    coordinator <-->|"Durable control metadata"| state
    coordinator -->|"HTTP: dispatch / control"| jvmA
    coordinator -->|"HTTP: dispatch / control"| jvmB
    jvmA -->|"Status / commit metadata"| coordinator
    jvmB -->|"Status / commit metadata"| coordinator
    nativeA <-->|"Data reads / writes"| lake
    nativeB <-->|"Data reads / writes"| lake
    nativeB -->|"Arrow Flight: results from output Worker"| clients
```

The Coordinator owns planning and metadata publication. Workers execute relational
work through JNI, exchange data with each other, and stream query results directly
to clients. Connector callbacks support Worker writes; the Coordinator publishes
the final catalog commit from commit metadata. See the
[architecture overview](docs/architecture/overview.md) for detailed contracts.

## Quick start

Use JDK 17, Maven 3.9+, Python 3.10+, Gradle 8.14.4, and the native toolchain
listed in the [developer guide](docs/contributing/developer-guide.md).
A first native build compiles external dependencies and can take substantial time.

```bash
git clone https://github.com/pangrova/pangrova.git
cd pangrova
.pangrova/scripts/build-iceberg-sdk-candidate.sh
./pangrova-worker-native/bin/build-native.sh
mvn clean install -DskipTests
./bin/pangrova-standalone.sh
```

In another terminal:

```bash
./bin/pangrova-cli.sh --host localhost --port 8080
```

```sql
SELECT 1 + 1 AS result;
```

The build commands prepare the pinned Iceberg SDK and packaged native library;
they do not run the full test suite. Follow the [quickstart](docs/getting-started/quickstart.md)
for configuration, catalogs, and troubleshooting.

## Community

Use [issues](https://github.com/pangrova/pangrova/issues) for bugs and feature requests.
See [CONTRIBUTING.md](CONTRIBUTING.md), the [Code of Conduct](CODE_OF_CONDUCT.md),
and [security policy](SECURITY.md). Changes are tracked in [CHANGELOG.md](CHANGELOG.md).

## License

[Apache License 2.0](LICENSE). Third-party acknowledgements are in [NOTICE](NOTICE).
