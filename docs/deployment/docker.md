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
# From the project root; Docker Buildx/Bake is required.
docker buildx bake -f deploy/docker/docker-bake.hcl --print
docker buildx bake -f deploy/docker/docker-bake.hcl --load

# Publish both Linux architectures through the same build graph.
PLATFORMS=linux/amd64,linux/arm64 IMAGE=registry.example.com/pangrova:release \
    docker buildx bake -f deploy/docker/docker-bake.hcl --push
```

Bake builds the Native target and Java runtime image from the same source context
and target platform. The Java image receives Native output through a target
context, not a host library or independently selected image tag. Source equality
is checked before packaging. Host `target/`, Native build trees and development
JNI resources are excluded from the Docker context; new source modules are not.

Native image builds default to one compilation job. The explicit limit reaches
both Pangrova's dependency setup and the upstream dependency installer, as well
as the final CMake build. CPU count alone is not a safe memory budget. On a
builder with measured sufficient memory, override the same limit explicitly:

```bash
docker buildx bake -f deploy/docker/docker-bake.hcl native-build \
    --set native-build.args.NATIVE_BUILD_JOBS=2
```

The setup script rejects non-positive or malformed limits. For a non-container
build, set `CMAKE_BUILD_PARALLEL_LEVEL`; `BUILD_THREADS` is consulted only when
that variable is absent. Increasing parallelism requires measuring peak builder
memory, including resident services; it does not change runtime query admission.

Linux production enables `PANGROVA_PRODUCTION_BUILD=ON`: Arrow, Parquet, Flight
and other third-party dependencies must be static and position-independent. The
packaging check verifies the Lance FFI symbol, ELF architecture, absence of
build-tree RPATH/RUNPATH and a strict system-ABI-only `DT_NEEDED` list. It rejects
missing archives or third-party shared-library dependencies rather than copying
them into the image. The runtime uses the same Ubuntu 22.04 ABI baseline.

The Linux classifier JAR is built and installed explicitly, then Java is built
with `native-runtime`. Runtime dependencies are copied from the actual Maven
dependency graph, including connector providers and JDBC drivers. Native loading
remains classpath resource extraction followed by `System.load()`; no environment
library path or host JNI fallback is used.

`--print` validates only the Bake graph. Release acceptance also requires the
actual Linux image build, Coordinator/Worker startup and SQL-through-JNI container
E2E; a host Maven test is not a substitute. A static or ABI gate failure blocks
the image until the dependency is rebuilt correctly.

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
