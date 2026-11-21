---
title: "Deployment"
description: "Choose a deployment model and follow the production acceptance path"
---

# Deployment

Pangrova can run as a single standalone process, as separate Coordinator and Worker
processes, or as containers on Docker, Kubernetes, or bare metal. This page helps select
the path and points to the authoritative configuration and acceptance material.

## Deployment models

| Model | Use for | Entry point | Trade-offs |
|---|---|---|---|
| Standalone | Development, evaluation, single-node tests | [Quickstart](../getting-started/quickstart.md), `bin/pangrova-standalone.sh` | One JVM owns Coordinator and Worker; not an HA topology |
| Distributed processes | Production-style control/execution separation | [Distributed deployment](distributed.md) | Requires external PostgreSQL, TLS, object storage, and Worker lifecycle management |
| Docker | Local clusters and container smoke tests | [Docker](docker.md), `deploy/docker/docker-compose-cluster.yml` | Container images must match the packaged Native library |
| Kubernetes / Helm | Managed clusters | [Helm / Kubernetes](helm-kubernetes.md) | Readiness, TLS, secrets, storage, and upgrade behavior must be configured and tested |

## Planning inputs

- [Planning](planning.md) covers capacity, storage, network, and topology decisions.
- [Production checklist](production-checklist.md) lists the infrastructure, security,
  storage, observability, and acceptance checks required before production.
- [Configuration](../configuration/index.md) defines the typed configuration model and
  process-specific keys.

## Non-negotiable prerequisites

- A compatible JDK and the packaged Native library for every Worker role.
- Durable metadata storage with the migrations required by the deployed release.
- TLS for control-plane and Native transport in distributed deployments.
- Catalog and object-storage credentials scoped to the tenant and deployment.
- A tested recovery procedure for Coordinator loss, Worker loss, and storage interruption.

Deployment readiness is a property of the concrete topology and workload, not of a
successful container start. Record the validation evidence for the release you operate.
