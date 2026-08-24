---
title: Operations
sidebar_position: 1
---

# Pangrova Operations

## Deployment

| Option | Best For | Guide |
|--------|----------|-------|
| Standalone | Local development | [Getting Started](../guide/getting-started.md) |
| Docker Compose | Dev/test clusters | [Deployment Guide](deployment/README.md) |
| Kubernetes (Helm) | Production, HA | [Deployment Guide](deployment/README.md#helm-kubernetes) |

## Configuration

| Resource | Description |
|----------|-------------|
| [coordinator.yml](configuration/coordinator.yml) | Coordinator configuration template |
| [worker.yml](configuration/worker.yml) | Worker configuration template |
| [standalone.yml](configuration/standalone.yml) | Standalone mode template |
| [pangrova-env.sh](configuration/pangrova-env.sh) | Environment variable overrides |
| [logback.xml](configuration/logback.xml) | Logging configuration |
| [Configuration Reference](../architecture/21-configuration-reference.md) | Full config key reference |

## Day-2 Operations

| Topic | Reference |
|-------|-----------|
| Rolling Upgrade | [Day-2 Operations](../architecture/22-operations.md) |
| Observability | [Observability](../architecture/14-observability.md) |
| Resource Management | [Resource Management](../architecture/11-resource-management.md) |
| Resilience & SOPs | [Resilience](../architecture/12-resilience.md) |
| Game Day Exercises | [Game Day](game-day/README.md) |

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
cd docs/develop/deployment/docker && docker compose up -d

# Kubernetes
helm install pangrova docs/develop/deployment/helm/pangrova \
  --namespace pangrova --create-namespace
```
