---
title: "Operations"
sidebar_position: 1
---

# Pangrova Operations

## Deployment

| Option | Best For | Guide |
|--------|----------|-------|
| Standalone | Local development | [Quickstart](../getting-started/quickstart.md) |
| Docker Compose | Dev/test clusters | [Docker](../deployment/docker.md) |
| Kubernetes (Helm) | Production, HA | [Helm / Kubernetes](../deployment/helm-kubernetes.md) |
| Bare-metal distributed | Production with S3/OSS/MinIO | [Distributed](../deployment/distributed.md) |

## Configuration

| Resource | Description |
|----------|-------------|
| `etc/coordinator.yml` | Coordinator configuration template |
| `etc/worker.yml` | Worker configuration template |
| `etc/standalone.yml` | Standalone mode template |
| `etc/pangrova-env.sh` | Environment variable overrides |
| `etc/logback.xml` | Logging configuration |
| [Configuration Reference](../configuration/reference.md) | Full config key reference |

## Day-2 Operations

| Topic | Reference |
|-------|-----------|
| Rolling Upgrade | [Day-2 Operations](day-2.md) |
| Observability | [Observability](../architecture/observability.md) |
| Resource Management | [Resource Management](../architecture/resource-management.md) |
| Resilience & SOPs | [Resilience](../architecture/resilience.md) |
| Game Day Exercises | [Game Day](game-day/index.md) |

## Release Verification

Run these checks before promoting a build:

```bash
# Clean distribution artifacts and scan for retired classes
mvn clean package -DskipTests
find . -path '*/target/*.jar' -exec unzip -l {} \; | grep -E 'org/pangrova/metadata/(store|checkpoint|generation)/' && exit 1

# Full unit regression (E2E requires a prepared AI image fixture)
mvn -T 1C test -pl '!pangrova/testing/pangrova-testing-integration-tests'

# SBOM
mvn -P sbom -DskipTests verify
```

A two-node smoke is: start PostgreSQL, let a Coordinator run the canonical schema
migrations, register one Catalog, start the Worker, wait for
`GET /v1/health` to return `"ready":true`, then run a query through
`bin/pangrova-cli.sh`. Stop both processes before deleting the database.

For a cross-version rollback rehearsal, build the previous release, stop the new
processes, start the previous Coordinator against the same migrated database,
and verify `/v1/health/live` returns 200 before resuming traffic.

## Software Bill of Materials

Generate an aggregate CycloneDX 1.5 SBOM for all reactor modules and their
runtime dependencies:

```bash
mvn -P sbom -DskipTests verify
```

The result is written to `target/bom.json` at the repository root. The `sbom`
profile is opt-in and does not affect normal development builds; attach the
generated file to release artifacts and archive it with the release evidence.

## Quick Reference

```bash
# Health check
curl -f http://localhost:8080/v1/health

# Start standalone
bin/pangrova-standalone.sh

# Start distributed
bin/pangrova-coordinator.sh --daemon
bin/pangrova-worker.sh --daemon

# Stop all
bin/pangrova-stop.sh --all

# Docker
cd deploy/docker && docker compose up -d

# Kubernetes
helm install pangrova deploy/helm/pangrova \
  --namespace pangrova --create-namespace
```
