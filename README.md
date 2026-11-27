<p align="center">
  <img src="website/static/img/logo-transparent.png" alt="Pangrova" width="320" />
</p>

<h1 align="center">Pangrova</h1>

<p align="center"><strong>Distributed SQL for open lakehouses.</strong></p>

<p align="center">
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" alt="Apache 2.0" /></a>
  <a href="https://github.com/pangrova-io/pangrova/actions/workflows/ci.yml"><img src="https://github.com/pangrova-io/pangrova/actions/workflows/ci.yml/badge.svg?branch=main" alt="CI" /></a>
</p>

<p align="center">
  <a href="#quick-start">Quick Start</a> ·
  <a href="https://pangrova-io.github.io/pangrova/">Website</a> ·
  <a href="https://pangrova-io.github.io/pangrova/docs/">Documentation</a> ·
  <a href="docs/getting-started/roadmap.md">Roadmap</a>
</p>

Pangrova is an open-source distributed SQL engine for open lakehouses. It brings
relational analytics and AI retrieval to existing lakehouse data through a
shared query and governance framework.

> **Project status:** Under active development. See the
> [current capability status](docs/getting-started/capability-status.md) for
> implementation and production qualification.

## The Name

The name Pangrova evokes two complementary images from nature:

- **Pangolin (Pang):** Resilience, protection, and adaptability reflect our
  commitment to dependable distributed execution and governed data access.
- **Mangrove (grova):** Deep roots and interconnected ecosystems represent an
  open foundation where data formats, storage, and compute can evolve together.

Together, they express Pangrova's vision: a resilient SQL engine that brings
relational analytics and AI retrieval to open lakehouses, grounded in open
standards and interoperability.

## CLI Demo

https://github.com/user-attachments/assets/ac907ae0-1a61-4912-a43e-805277bd1cc2

Explore SQL, multimodal FILE data, scalar, full-text and vector indexes,
relational analytics, and table maintenance in a local Standalone session.
The video illustrates the workflow rather than release validation. See the
[demo guide](docs/getting-started/clients/cli-demo.mdx) for the recording context,
prerequisites, and replay instructions.

## Key Features

- **Open lakehouse access:** Query Apache Iceberg and Apache Paimon tables and
  managed Lance datasets. See [connectors](docs/getting-started/connectors/index.md)
  for supported operations.
- **Distributed Native execution:** Run vectorized relational processing on
  Workers and exchange columnar data through Apache Arrow Flight.
- **AI and retrieval through SQL:** Use vector and full-text retrieval and
  governed model invocation within supported query paths.
- **Familiar SQL clients:** Connect through JDBC, REST, or the CLI for
  applications and interactive data exploration.

## Architecture

<p align="center">
  <a href="website/static/img/architecture-overview.png">
    <img src="website/static/img/architecture-overview.png" alt="Pangrova architecture: clients, Java Coordinator, native Workers, control state, and lakehouse storage" width="100%" />
  </a>
</p>

The Java Coordinator uses Apache Calcite for planning and owns authorization,
admission, scheduling, and final catalog commits. Workers execute relational
queries through C++20 and Velox via JNI, exchanging data and streaming results
through Apache Arrow Flight.

See the [architecture overview](docs/architecture/overview.md) for component
boundaries and execution details.

## Quick Start

1. Follow the [Quickstart](docs/getting-started/quickstart.md) to build from
   source, configure, and start a local Standalone instance.
2. Run the [first query](docs/getting-started/quickstart.md#run-your-first-query).
3. Run the [one-command Iceberg trial](docs/getting-started/quickstart.md#run-the-iceberg-trial-in-one-command)
   against an isolated, configured local instance. The trial verifies
   `click=2, purchase=1` and cleans up its temporary data on success.

The first Native build compiles external dependencies. Standalone mode is for
development and evaluation; use the [deployment guide](docs/deployment/planning.md)
to plan a distributed deployment. For tooling requirements and platform-specific
instructions, see the [Developer Guide](docs/contributing/developer-guide.md).
A [Docker quick start](docs/getting-started/quickstart.md#docker-quick-start)
is also available for isolated development.

## Documentation

- [SQL Reference](docs/sql-reference/index.md)
- [Connectors](docs/getting-started/connectors/index.md)
- [Configuration](docs/configuration/index.md)
- [Deployment](docs/deployment/planning.md)

## Contributing

Contributions, bug reports, and documentation improvements are welcome.
See [CONTRIBUTING.md](CONTRIBUTING.md) and the
[repository guide](docs/contributing/repository-guide.md) to get started.

Report vulnerabilities through [SECURITY.md](SECURITY.md), not public issues.

## License

[Apache License 2.0](LICENSE). Third-party acknowledgements are in [NOTICE](NOTICE).
