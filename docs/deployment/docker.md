---
title: "Docker"
description: "Docker Compose single-node and cluster deployment, image builds, health checks"
---

# Docker

Run Pangrova with Docker Compose for development and testing, and build container images from source.


## Docker — Single Node (Development)

```bash
cd deploy/docker
cp .env.example .env        # adjust if needed
docker compose up -d
docker compose ps            # verify both services are healthy
```

Coordinator is available at `http://localhost:8080`. Worker registers automatically.

To stop:

```bash
docker compose down
```

## Docker — Distributed Cluster

```bash
cd deploy/docker
docker compose -f docker-compose-cluster.yml up -d
docker compose -f docker-compose-cluster.yml ps
```

This starts 2 Coordinators (ports 8080, 8082) and 3 Workers (ports 8081, 8083, 8084).

## Docker — Build Image from Source

```bash
# From project root
docker build -t pangrova/pangrova:latest -f deploy/docker/Dockerfile .

# Multi-arch build (requires Docker Buildx)
docker buildx build --platform linux/amd64,linux/arm64 \
    -t pangrova/pangrova:latest -f deploy/docker/Dockerfile .
```

---

## Configuration

All services read configuration from `/opt/pangrova/config/` inside the container. Override via:

- Environment variables (`PANGROVA_JAVA_OPTS`, `WORKER_COORDINATOR_URL`, etc.)
- Helm `values.yaml` — `config.coordinator` and `config.worker` embed full YAML content
- Kubernetes ConfigMap — mounted as `coordinator.yml` / `worker.yml`

See the [Configuration Reference](../configuration/reference.md) for the complete configuration reference.

## Health Checks

All services expose `GET /v1/health` for readiness and liveness probes.

| Component   | Default Port | Health Endpoint                    |
|-------------|-------------|------------------------------------|
| Coordinator | 8080        | `http://localhost:8080/v1/health`  |
| Worker      | 8081        | `http://localhost:8081/v1/health`  |

## Image Versioning

Docker Compose files use `${PANGROVA_VERSION:-latest}`. Override via:

```bash
PANGROVA_VERSION=0.1.0 docker compose up -d
```

Helm chart uses `image.tag` in `values.yaml`.
