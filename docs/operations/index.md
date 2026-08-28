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
